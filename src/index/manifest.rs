//! CAS-protected per-shard index manifest.

use crate::{Error, Result};

use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use std::sync::Arc;
use tracing::debug;

/// A single segment entry in the manifest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SegmentEntry {
    /// S3 path to the `.csi` segment file.
    pub path: String,
    /// Minimum timestamp (nanoseconds) of data covered by this segment.
    pub min_time_ns: i64,
    /// Maximum timestamp (nanoseconds) of data covered by this segment.
    pub max_time_ns: i64,
    /// Number of chunks indexed in this segment.
    pub chunk_count: u32,
    /// Compaction level of the indexed chunks.
    pub level: u32,
    /// Size of the `.csi` file in bytes.
    pub size_bytes: u64,
    /// ISO-8601 creation timestamp.
    pub created_at: String,
}

/// Per-shard index manifest stored in S3.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexManifest {
    /// Schema version.
    pub version: u32,
    /// Shard ID this manifest covers.
    pub shard_id: String,
    /// Manifest generation for CAS.
    pub generation: u64,
    /// Watermark: all chunks up to this timestamp are indexed.
    pub indexed_through_ns: i64,
    /// List of segment entries.
    pub segments: Vec<SegmentEntry>,
    /// Whether the manifest is frozen (e.g. during shard splits).
    pub frozen: bool,
}

impl IndexManifest {
    /// Create a new empty manifest for a shard.
    pub fn new(shard_id: &str) -> Self {
        Self {
            version: 1,
            shard_id: shard_id.to_string(),
            generation: 0,
            indexed_through_ns: 0,
            segments: Vec::new(),
            frozen: false,
        }
    }
}

/// Client for loading and saving index manifests with CAS semantics.
pub struct ManifestClient {
    object_store: Arc<dyn ObjectStore>,
    tenant_id: String,
}

impl ManifestClient {
    pub fn new(object_store: Arc<dyn ObjectStore>, tenant_id: &str) -> Self {
        Self {
            object_store,
            tenant_id: tenant_id.to_string(),
        }
    }

    /// S3 path for a shard's manifest.
    fn manifest_path(&self, shard_id: &str) -> Path {
        format!(
            "{}/indexes/shard={}/manifest.json",
            self.tenant_id, shard_id
        )
        .into()
    }

    /// Load manifest for a shard. Returns None if no manifest exists.
    pub async fn load_manifest(
        &self,
        shard_id: &str,
    ) -> Result<Option<(IndexManifest, String)>> {
        let path = self.manifest_path(shard_id);
        match self.object_store.get(&path).await {
            Ok(result) => {
                let etag = result
                    .meta
                    .e_tag
                    .clone()
                    .unwrap_or_default();
                let bytes = result.bytes().await?;
                let manifest: IndexManifest = serde_json::from_slice(&bytes)?;
                Ok(Some((manifest, etag)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Save manifest with CAS via ETag. Returns Err(Conflict) on mismatch.
    pub async fn save_manifest(
        &self,
        manifest: &IndexManifest,
        expected_etag: &str,
    ) -> Result<()> {
        let path = self.manifest_path(&manifest.shard_id);
        let json = serde_json::to_vec_pretty(manifest)?;
        let payload = PutPayload::from(json);

        let opts = if expected_etag.is_empty() {
            // First write: use Create mode (if-none-match: *)
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            }
        } else {
            // Update: use conditional put with ETag
            let update_ver = object_store::UpdateVersion {
                e_tag: Some(expected_etag.to_string()),
                version: None,
            };
            PutOptions {
                mode: PutMode::Update(update_ver),
                ..Default::default()
            }
        };

        match self.object_store.put_opts(&path, payload, opts).await {
            Ok(_) => {
                debug!(shard = %manifest.shard_id, "Manifest saved successfully");
                Ok(())
            }
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                Err(Error::Conflict)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Create a new manifest (unconditional put, for initial creation only).
    pub async fn create_manifest(&self, manifest: &IndexManifest) -> Result<()> {
        let path = self.manifest_path(&manifest.shard_id);
        let json = serde_json::to_vec_pretty(manifest)?;
        self.object_store
            .put(&path, json.into())
            .await?;
        Ok(())
    }

    /// Freeze the manifest (set frozen=true) atomically.
    pub async fn freeze_manifest(&self, shard_id: &str) -> Result<()> {
        let (mut manifest, etag) = self
            .load_manifest(shard_id)
            .await?
            .ok_or_else(|| Error::Index(format!("No manifest for shard '{shard_id}'")))?;

        manifest.frozen = true;
        self.save_manifest(&manifest, &etag).await
    }

    /// Unfreeze the manifest (set frozen=false) atomically.
    pub async fn unfreeze_manifest(&self, shard_id: &str) -> Result<()> {
        let (mut manifest, etag) = self
            .load_manifest(shard_id)
            .await?
            .ok_or_else(|| Error::Index(format!("No manifest for shard '{shard_id}'")))?;

        manifest.frozen = false;
        self.save_manifest(&manifest, &etag).await
    }
}
