//! S3-based metadata client for production deployment
//!
//! This implementation provides durable, persistent metadata storage on S3
//! with atomic operations using compare-and-swap patterns.
//!
//! Benefits vs in-memory LocalMetadataClient:
//! - Persists across process restarts
//! - Supports multiple processes (no single-point-of-failure)
//! - Foundation for multi-tenant isolation
//! - Eventual consistency with atomic writes

use super::{
    CompactionJob, CompactionLease, CompactionLeases, CompactionStatus, LeaseStatus,
    MetadataClient, SplitState, TimeIndexEntry, TimeRange,
};
use crate::ingester::ChunkMetadata;
use crate::sharding::SplitPhase;
use crate::{Error, Result};

use async_trait::async_trait;
use metrics::{counter, gauge, histogram};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Maximum number of CAS retries for atomic operations
const MAX_CAS_RETRIES: u32 = 5;

/// Base backoff duration in milliseconds for exponential backoff
const BASE_BACKOFF_MS: u64 = 100;

/// CAS retry loop with exponential backoff on conflict.
///
/// The body is an async block performing one load-modify-save cycle.
/// Return `Ok(value)` on success, `Err(Error::Conflict)` to trigger retry,
/// or any other `Err` to abort immediately.
macro_rules! cas_retry {
    ($body:block) => {{
        let mut __cas_result = Err(Error::TooManyRetries);
        for __cas_attempt in 0..MAX_CAS_RETRIES {
            match (async $body).await {
                Ok(value) => {
                    __cas_result = Ok(value);
                    break;
                }
                Err(Error::Conflict) => {
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(__cas_attempt);
                    metrics::counter!(
                        "cardinalsin_metadata_cas_retries_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "cas_retry"
                    )
                    .increment(1);
                    metrics::histogram!(
                        "cardinalsin_metadata_cas_backoff_seconds",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "cas_retry"
                    )
                    .record(backoff_ms as f64 / 1000.0);
                    debug!(
                        "CAS conflict on attempt {}, retrying after {}ms",
                        __cas_attempt + 1,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(e) => {
                    __cas_result = Err(e);
                    break;
                }
            }
        }
        __cas_result
    }};
}

/// Chunk metadata with column statistics for predicate pushdown
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkMetadataExtended {
    /// Basic chunk metadata
    #[serde(flatten)]
    pub base: ChunkMetadata,
    /// Column statistics (min/max per column)
    #[serde(default)]
    pub column_stats: HashMap<String, ColumnStats>,
    /// Compaction level (0 = L0/initial, 1+ = compacted)
    #[serde(default)]
    pub level: u32,
    /// Version/ETag for atomic operations
    #[serde(default, skip_serializing)]
    pub version: String,
    /// Shard ID this chunk belongs to (None for legacy data)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<String>,
}

/// Unified metadata catalog -- single S3 object, single ETag
///
/// Merges chunk metadata and time index into a single file to eliminate
/// the non-atomic two-object metadata update problem.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetadataCatalog {
    /// Schema version for forward compatibility (1 = legacy, 2 = unified)
    pub version: u32,
    /// All chunk metadata, keyed by chunk path
    pub chunks: HashMap<String, ChunkMetadataExtended>,
    /// Time index: hour-bucket timestamp -> list of chunk paths
    pub time_index: BTreeMap<i64, Vec<String>>,
}

/// Statistics for a column in a chunk (used for predicate pushdown)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnStats {
    /// Minimum value in this chunk
    pub min: serde_json::Value,
    /// Maximum value in this chunk
    pub max: serde_json::Value,
    /// Null flag - column may have nulls
    pub has_nulls: bool,
}

/// S3 metadata client configuration
#[derive(Debug, Clone)]
pub struct ObjectStoreMetadataConfig {
    /// S3 bucket name
    pub bucket: String,
    /// Prefix for metadata files
    pub metadata_prefix: String,
    /// Enable caching of metadata reads
    pub enable_cache: bool,
    /// Allow explicit fallback to unsafe overwrite when CAS update is not supported
    pub allow_unsafe_overwrite: bool,
}

impl Default for ObjectStoreMetadataConfig {
    fn default() -> Self {
        Self {
            bucket: "cardinalsin-metadata".to_string(),
            metadata_prefix: "metadata/".to_string(),
            enable_cache: true,
            allow_unsafe_overwrite: false,
        }
    }
}

/// S3-based metadata client
///
/// Stores metadata durably on S3 with atomic operations
pub struct ObjectStoreMetadataClient {
    /// Object store for metadata storage
    object_store: Arc<dyn ObjectStore>,
    /// Configuration
    config: ObjectStoreMetadataConfig,
    /// Unified catalog cache with TTL
    catalog_cache: Arc<tokio::sync::RwLock<Option<(MetadataCatalog, Instant)>>>,
    /// Catalog cache TTL duration
    catalog_cache_ttl: Duration,
}

impl ObjectStoreMetadataClient {
    /// Nanoseconds per hour constant
    const NANOS_PER_HOUR: i64 = 3_600_000_000_000;

    /// Create a new S3 metadata client
    pub fn new(object_store: Arc<dyn ObjectStore>, config: ObjectStoreMetadataConfig) -> Self {
        Self {
            object_store,
            config,
            catalog_cache: Arc::new(tokio::sync::RwLock::new(None)),
            catalog_cache_ttl: Duration::from_secs(60),
        }
    }

    async fn put_with_cas(
        &self,
        path: &Path,
        payload: PutPayload,
        expected_etag: &str,
        context: &str,
    ) -> Result<()> {
        let cas_start = Instant::now();
        let operation = context.to_string();
        let opts = if expected_etag == "none" {
            // First write: use Create to prevent racing with another process
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            }
        } else {
            PutOptions {
                mode: PutMode::Update(object_store::UpdateVersion {
                    e_tag: Some(expected_etag.to_string()),
                    version: None,
                }),
                ..Default::default()
            }
        };

        match self
            .object_store
            .put_opts(path, payload.clone(), opts)
            .await
        {
            Ok(_) => {
                counter!(
                    "cardinalsin_metadata_cas_attempts_total",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant(),
                    "operation" => operation.clone(),
                    "result" => "ok"
                )
                .increment(1);
                histogram!(
                    "cardinalsin_metadata_cas_duration_seconds",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant(),
                    "operation" => operation.clone(),
                    "result" => "ok"
                )
                .record(cas_start.elapsed().as_secs_f64());
                Ok(())
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                // Another process created the file first - treat as conflict so caller retries
                counter!(
                    "cardinalsin_metadata_cas_attempts_total",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant(),
                    "operation" => operation.clone(),
                    "result" => "conflict"
                )
                .increment(1);
                counter!(
                    "cardinalsin_metadata_cas_retries_total",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant(),
                    "operation" => operation.clone()
                )
                .increment(1);
                Err(Error::Conflict)
            }
            Err(object_store::Error::Precondition { .. }) => {
                counter!(
                    "cardinalsin_metadata_cas_attempts_total",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant(),
                    "operation" => operation.clone(),
                    "result" => "conflict"
                )
                .increment(1);
                counter!(
                    "cardinalsin_metadata_cas_retries_total",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant(),
                    "operation" => operation.clone()
                )
                .increment(1);
                Err(Error::Conflict)
            }
            Err(object_store::Error::NotImplemented)
            | Err(object_store::Error::NotSupported { .. }) => {
                if !self.config.allow_unsafe_overwrite {
                    counter!(
                        "cardinalsin_metadata_cas_attempts_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => operation.clone(),
                        "result" => "error"
                    )
                    .increment(1);
                    return Err(Error::Metadata(format!(
                        "CAS conditional writes are required for {} at {} but are not supported by the object store. Set allow_unsafe_overwrite=true only for local development with MinIO.",
                        context, path
                    )));
                }

                warn!(
                    "CAS not supported for {} at {} - explicitly opting into unsafe overwrite mode",
                    context, path
                );

                let overwrite = PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                };

                self.object_store
                    .put_opts(path, payload, overwrite)
                    .await
                    .map(|_| {
                        counter!(
                            "cardinalsin_metadata_cas_attempts_total",
                            "service" => crate::telemetry::service(),
                            "run_id" => crate::telemetry::run_id(),
                            "tenant" => crate::telemetry::tenant(),
                            "operation" => operation.clone(),
                            "result" => "unsafe_overwrite"
                        )
                        .increment(1);
                        histogram!(
                            "cardinalsin_metadata_cas_duration_seconds",
                            "service" => crate::telemetry::service(),
                            "run_id" => crate::telemetry::run_id(),
                            "tenant" => crate::telemetry::tenant(),
                            "operation" => operation.clone(),
                            "result" => "unsafe_overwrite"
                        )
                        .record(cas_start.elapsed().as_secs_f64());
                    })
                    .map_err(Error::ObjectStore)
            }
            Err(e) => {
                counter!(
                    "cardinalsin_metadata_cas_attempts_total",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant(),
                    "operation" => operation,
                    "result" => "error"
                )
                .increment(1);
                Err(Error::ObjectStore(e))
            }
        }
    }

    /// Calculate hour bucket for a timestamp
    fn hour_bucket(timestamp: i64) -> i64 {
        (timestamp / Self::NANOS_PER_HOUR) * Self::NANOS_PER_HOUR
    }

    /// Get the S3 path for a chunk's metadata
    #[allow(dead_code)]
    fn chunk_metadata_path(&self, chunk_path: &str) -> Path {
        let filename = format!("{}.json", chunk_path.trim_end_matches(".parquet"));
        Path::from_iter([&self.config.metadata_prefix, "chunks/", &filename])
    }

    /// Get the S3 path for the time index
    fn time_index_path(&self) -> Path {
        Path::from_iter([&self.config.metadata_prefix, "time-index.json"])
    }

    /// Get the S3 path for the unified catalog
    fn catalog_path(&self) -> Path {
        Path::from_iter([&self.config.metadata_prefix, "catalog.json"])
    }

    /// Get the S3 path for compaction jobs
    fn compaction_jobs_path(&self) -> Path {
        Path::from_iter([&self.config.metadata_prefix, "compaction-jobs.json"])
    }

    /// Get the S3 path for compaction leases
    fn compaction_leases_path(&self) -> Path {
        Path::from_iter([&self.config.metadata_prefix, "compaction-leases.json"])
    }

    /// Get the S3 path for shard split states
    fn split_states_path(&self) -> Path {
        Path::from_iter([&self.config.metadata_prefix, "split-states.json"])
    }

    /// Get the S3 path for a specific shard metadata
    fn shard_metadata_path(&self, shard_id: &str) -> Path {
        Path::from_iter([
            &self.config.metadata_prefix,
            "shards/",
            &format!("{}.json", shard_id),
        ])
    }

    /// Load all chunk metadata from S3 (internal method, legacy format)
    #[allow(dead_code)]
    async fn load_chunk_metadata_internal(&self) -> Result<HashMap<String, ChunkMetadataExtended>> {
        let path = Path::from_iter([&self.config.metadata_prefix, "chunks/", "metadata.json"]);

        match self.object_store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);
                if content.is_empty() {
                    debug!("No existing chunk metadata found");
                    return Ok(HashMap::new());
                }

                let metadata: HashMap<String, ChunkMetadataExtended> =
                    serde_json::from_str(&content)
                        .map_err(|e| Error::Metadata(format!("Corrupt chunk metadata: {}", e)))?;

                info!("Loaded {} chunk metadata entries from S3", metadata.len());
                Ok(metadata)
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("No chunk metadata file found, starting fresh");
                Ok(HashMap::new())
            }
            Err(e) => Err(Error::Metadata(format!(
                "Failed to load chunk metadata: {}",
                e
            ))),
        }
    }

    /// Load time index (from unified catalog)
    #[allow(dead_code)]
    async fn load_time_index(&self) -> Result<BTreeMap<i64, Vec<String>>> {
        let catalog = self.load_catalog_cached().await?;
        Ok(catalog.time_index)
    }

    /// Load the catalog with caching. Returns a cached copy if fresh, otherwise loads from S3.
    async fn load_catalog_cached(&self) -> Result<MetadataCatalog> {
        // Check cache first
        {
            let cache = self.catalog_cache.read().await;
            if let Some((catalog, cached_at)) = cache.as_ref() {
                if cached_at.elapsed() < self.catalog_cache_ttl {
                    debug!("Catalog cache hit");
                    return Ok(catalog.clone());
                }
            }
        }

        // Cache miss or expired - load from S3
        debug!("Catalog cache miss, loading from S3");
        let (catalog, _etag) = self.load_catalog_with_etag().await?;

        // Update cache
        {
            let mut cache = self.catalog_cache.write().await;
            *cache = Some((catalog.clone(), Instant::now()));
        }

        Ok(catalog)
    }

    /// Internal save method
    async fn save_chunk_metadata_internal(
        &self,
        metadata: &HashMap<String, ChunkMetadataExtended>,
    ) -> Result<()> {
        let path = Path::from_iter([&self.config.metadata_prefix, "chunks/", "metadata.json"]);

        let content = serde_json::to_string_pretty(metadata)?;
        let bytes = content.into_bytes();

        self.object_store.put(&path, bytes.into()).await?;
        debug!("Saved {} chunk metadata entries to S3", metadata.len());
        Ok(())
    }

    /// Load chunk metadata with ETag for atomic operations
    async fn load_chunk_metadata_with_etag(
        &self,
    ) -> Result<(HashMap<String, ChunkMetadataExtended>, String)> {
        let path = Path::from_iter([&self.config.metadata_prefix, "chunks/", "metadata.json"]);

        match self.object_store.get(&path).await {
            Ok(result) => {
                let e_tag = result
                    .meta
                    .e_tag
                    .clone()
                    .unwrap_or_else(|| "no-etag".to_string());
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);

                if content.is_empty() {
                    debug!("No existing chunk metadata found");
                    return Ok((HashMap::new(), e_tag));
                }

                let metadata: HashMap<String, ChunkMetadataExtended> =
                    serde_json::from_str(&content)
                        .map_err(|e| Error::Metadata(format!("Corrupt chunk metadata: {}", e)))?;

                info!(
                    "Loaded {} chunk metadata entries with ETag: {}",
                    metadata.len(),
                    e_tag
                );
                Ok((metadata, e_tag))
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("No chunk metadata file found, starting fresh");
                Ok((HashMap::new(), "none".to_string()))
            }
            Err(e) => Err(Error::Metadata(format!(
                "Failed to load chunk metadata: {}",
                e
            ))),
        }
    }

    /// Atomically save chunk metadata using ETag-based CAS (legacy, kept for backward compat)
    #[allow(dead_code)]
    async fn atomic_save_chunk_metadata(
        &self,
        metadata: &HashMap<String, ChunkMetadataExtended>,
        expected_etag: String,
    ) -> Result<()> {
        let path = Path::from_iter([&self.config.metadata_prefix, "chunks/", "metadata.json"]);

        let content = serde_json::to_string_pretty(metadata)?;
        let bytes = content.into_bytes();

        self.put_with_cas(
            &path,
            PutPayload::from(bytes),
            &expected_etag,
            "chunk metadata",
        )
        .await
        .map(|_| {
            debug!("Atomically saved {} chunk metadata entries", metadata.len());
        })
    }

    /// Save time index to S3 (non-atomic, for backward compatibility / rebuild_time_index)
    async fn save_time_index(&self, index: &BTreeMap<i64, Vec<String>>) -> Result<()> {
        let path = self.time_index_path();
        let content = serde_json::to_string_pretty(index)?;
        let bytes = content.into_bytes();

        self.object_store.put(&path, bytes.into()).await?;
        debug!("Saved time index with {} hour buckets to S3", index.len());

        // Invalidate catalog cache on write
        {
            let mut cache = self.catalog_cache.write().await;
            *cache = None;
        }

        Ok(())
    }

    /// Load time index with ETag for atomic operations
    async fn load_time_index_with_etag(&self) -> Result<(BTreeMap<i64, Vec<String>>, String)> {
        let path = self.time_index_path();

        match self.object_store.get(&path).await {
            Ok(result) => {
                let e_tag = result
                    .meta
                    .e_tag
                    .clone()
                    .unwrap_or_else(|| "no-etag".to_string());
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);

                if content.is_empty() {
                    debug!("No existing time index found");
                    return Ok((BTreeMap::new(), e_tag));
                }

                let index: BTreeMap<i64, Vec<String>> = serde_json::from_str(&content)
                    .map_err(|e| Error::Metadata(format!("Corrupt time index: {}", e)))?;

                debug!(
                    "Loaded time index with {} hour buckets and ETag: {}",
                    index.len(),
                    e_tag
                );
                Ok((index, e_tag))
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("No time index file found, starting fresh");
                Ok((BTreeMap::new(), "none".to_string()))
            }
            Err(e) => Err(Error::Metadata(format!(
                "Failed to load time index with etag: {}",
                e
            ))),
        }
    }

    /// Atomically save time index using ETag-based CAS (legacy, kept for backward compat)
    #[allow(dead_code)]
    async fn atomic_save_time_index(
        &self,
        index: &BTreeMap<i64, Vec<String>>,
        expected_etag: String,
    ) -> Result<()> {
        let path = self.time_index_path();

        let content = serde_json::to_string_pretty(index)?;
        let bytes = content.into_bytes();

        self.put_with_cas(&path, PutPayload::from(bytes), &expected_etag, "time index")
            .await?;

        debug!(
            "Atomically saved time index with {} hour buckets",
            index.len()
        );

        // Invalidate catalog cache on write
        {
            let mut cache = self.catalog_cache.write().await;
            *cache = None;
        }

        Ok(())
    }

    /// Load the unified catalog with ETag for atomic operations.
    ///
    /// Tries loading `metadata/catalog.json` first. If it does not exist,
    /// falls back to reading legacy `metadata.json` + `time-index.json`
    /// separately and constructing a MetadataCatalog in memory.
    async fn load_catalog_with_etag(&self) -> Result<(MetadataCatalog, String)> {
        let path = self.catalog_path();

        // Try unified catalog first
        match self.object_store.get(&path).await {
            Ok(result) => {
                let e_tag = result
                    .meta
                    .e_tag
                    .clone()
                    .unwrap_or_else(|| "no-etag".to_string());
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);

                if content.is_empty() {
                    debug!("Empty catalog file found");
                    return Ok((
                        MetadataCatalog {
                            version: 2,
                            chunks: HashMap::new(),
                            time_index: BTreeMap::new(),
                        },
                        e_tag,
                    ));
                }

                let catalog: MetadataCatalog = serde_json::from_str(&content)
                    .map_err(|e| Error::Metadata(format!("Corrupt catalog: {}", e)))?;

                info!(
                    "Loaded catalog v{} with {} chunks, {} time buckets (ETag: {})",
                    catalog.version,
                    catalog.chunks.len(),
                    catalog.time_index.len(),
                    e_tag
                );
                Ok((catalog, e_tag))
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("No catalog.json found, falling back to legacy format");
                // Fall back to legacy format
                let (chunks, _) = self.load_chunk_metadata_with_etag().await?;
                let (time_index, _) = self.load_time_index_with_etag().await?;
                let catalog = MetadataCatalog {
                    version: 1, // Legacy
                    chunks,
                    time_index,
                };
                // Return "none" etag since there's no catalog.json yet;
                // the first write will create it.
                Ok((catalog, "none".to_string()))
            }
            Err(e) => Err(Error::Metadata(format!("Failed to load catalog: {}", e))),
        }
    }

    /// Atomically save the unified catalog using ETag-based CAS.
    ///
    /// This performs a single conditional PUT to `metadata/catalog.json`,
    /// replacing the previous two-file (metadata.json + time-index.json) approach.
    async fn atomic_save_catalog(
        &self,
        catalog: &MetadataCatalog,
        expected_etag: String,
    ) -> Result<()> {
        let path = self.catalog_path();

        let content = serde_json::to_string_pretty(catalog)?;
        let bytes = content.into_bytes();

        self.put_with_cas(&path, PutPayload::from(bytes), &expected_etag, "catalog")
            .await
            .map(|_| {
                debug!(
                    "Atomically saved catalog with {} chunks and {} time buckets",
                    catalog.chunks.len(),
                    catalog.time_index.len()
                );
            })
    }

    /// Load chunk metadata (public for testing)
    ///
    /// Returns chunk metadata from the unified catalog (or legacy format as fallback).
    pub async fn load_chunk_metadata(&self) -> Result<HashMap<String, ChunkMetadataExtended>> {
        let catalog = self.load_catalog_cached().await?;
        Ok(catalog.chunks)
    }

    /// Save chunk metadata (public for testing)
    ///
    /// Saves chunk metadata to the legacy format AND updates the catalog.
    /// This method exists for backward compatibility with tests that directly
    /// manipulate chunk metadata (e.g., predicate pushdown tests).
    pub async fn save_chunk_metadata(
        &self,
        metadata: &HashMap<String, ChunkMetadataExtended>,
    ) -> Result<()> {
        // Load current catalog to preserve time_index
        let (mut catalog, _etag) = self.load_catalog_with_etag().await?;
        catalog.chunks = metadata.clone();
        catalog.version = 2;

        // Save as catalog.json (non-atomic, for testing convenience)
        let path = self.catalog_path();
        let content = serde_json::to_string_pretty(&catalog)?;
        let bytes = content.into_bytes();
        self.object_store.put(&path, bytes.into()).await?;

        // Also save to legacy format for backward compat
        self.save_chunk_metadata_internal(metadata).await?;

        // Invalidate cache so next load gets fresh data
        let mut cache = self.catalog_cache.write().await;
        *cache = None;

        Ok(())
    }

    /// Rebuild time index from existing chunk metadata
    /// Use this to fix time index after deploying the bug fix
    pub async fn rebuild_time_index(&self) -> Result<()> {
        info!("Rebuilding time index from chunk metadata...");

        // Load all chunk metadata (from catalog or legacy)
        let (mut catalog, _etag) = self.load_catalog_with_etag().await?;

        // Build new time index from chunk metadata
        let mut time_index = BTreeMap::new();

        for (path, extended) in catalog.chunks.iter() {
            let start_bucket = Self::hour_bucket(extended.base.min_timestamp);
            let end_bucket = Self::hour_bucket(extended.base.max_timestamp);

            let mut bucket = start_bucket;
            while bucket <= end_bucket {
                time_index
                    .entry(bucket)
                    .or_insert_with(Vec::new)
                    .push(path.clone());
                bucket += Self::NANOS_PER_HOUR;
            }
        }

        catalog.time_index = time_index.clone();
        catalog.version = 2;

        // Save the rebuilt catalog
        let path = self.catalog_path();
        let content = serde_json::to_string_pretty(&catalog)?;
        let bytes = content.into_bytes();
        self.object_store.put(&path, bytes.into()).await?;

        // Also save to legacy time index for backward compat
        self.save_time_index(&time_index).await?;

        info!(
            "Rebuilt time index with {} buckets covering {} chunks",
            catalog.time_index.len(),
            catalog.chunks.len()
        );
        Ok(())
    }

    /// Atomically register a chunk with retry logic.
    ///
    /// Uses single-file CAS on catalog.json for atomic metadata + time index updates.
    async fn atomic_register_chunk(&self, path: &str, metadata: &ChunkMetadata) -> Result<()> {
        let extended = ChunkMetadataExtended {
            base: metadata.clone(),
            column_stats: HashMap::new(),
            level: 0, // New chunks start at L0
            version: String::new(),
            shard_id: None,
        };

        let catalog = cas_retry!({
            let (mut catalog, etag) = self.load_catalog_with_etag().await?;
            catalog.chunks.insert(path.to_string(), extended.clone());

            let start_bucket = Self::hour_bucket(metadata.min_timestamp);
            let end_bucket = Self::hour_bucket(metadata.max_timestamp);
            let mut bucket = start_bucket;
            while bucket <= end_bucket {
                catalog
                    .time_index
                    .entry(bucket)
                    .or_default()
                    .push(path.to_string());
                bucket += Self::NANOS_PER_HOUR;
            }
            catalog.version = 2;

            self.atomic_save_catalog(&catalog, etag).await?;
            Ok(catalog)
        })?;

        // Update in-memory cache
        {
            let mut cache = self.catalog_cache.write().await;
            *cache = Some((catalog, Instant::now()));
        }
        info!(
            "Atomically registered chunk: {} (time range: {} to {})",
            path, metadata.min_timestamp, metadata.max_timestamp
        );
        Ok(())
    }

    /// Load compaction jobs with ETag for atomic operations
    async fn load_compaction_jobs_with_etag(&self) -> Result<(Vec<CompactionJob>, String)> {
        let path = self.compaction_jobs_path();

        match self.object_store.get(&path).await {
            Ok(result) => {
                let e_tag = result
                    .meta
                    .e_tag
                    .clone()
                    .unwrap_or_else(|| "no-etag".to_string());
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);

                let jobs: Vec<CompactionJob> = serde_json::from_str(&content)
                    .map_err(|e| Error::Metadata(format!("Corrupt compaction jobs: {}", e)))?;

                debug!("Loaded {} compaction jobs with ETag: {}", jobs.len(), e_tag);
                Ok((jobs, e_tag))
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("No compaction jobs file found, starting fresh");
                Ok((Vec::new(), "none".to_string()))
            }
            Err(e) => Err(Error::Metadata(format!(
                "Failed to load compaction jobs: {}",
                e
            ))),
        }
    }

    /// Atomically save compaction jobs using ETag-based CAS
    async fn atomic_save_compaction_jobs(
        &self,
        jobs: &[CompactionJob],
        expected_etag: String,
    ) -> Result<()> {
        let path = self.compaction_jobs_path();

        let content = serde_json::to_string_pretty(jobs)?;
        let bytes = content.into_bytes();

        self.put_with_cas(
            &path,
            PutPayload::from(bytes),
            &expected_etag,
            "compaction jobs",
        )
        .await
        .map(|_| {
            debug!("Atomically saved {} compaction jobs", jobs.len());
        })
    }

    /// Load split states with ETag for atomic operations
    async fn load_split_states_with_etag(&self) -> Result<(HashMap<String, SplitState>, String)> {
        let path = self.split_states_path();

        match self.object_store.get(&path).await {
            Ok(result) => {
                let e_tag = result
                    .meta
                    .e_tag
                    .clone()
                    .unwrap_or_else(|| "no-etag".to_string());
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);

                let states: HashMap<String, SplitState> = serde_json::from_str(&content)
                    .map_err(|e| Error::Metadata(format!("Corrupt split states: {}", e)))?;

                debug!("Loaded {} split states with ETag: {}", states.len(), e_tag);
                Ok((states, e_tag))
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("No split states file found, starting fresh");
                Ok((HashMap::new(), "none".to_string()))
            }
            Err(e) => Err(Error::Metadata(format!(
                "Failed to load split states: {}",
                e
            ))),
        }
    }

    /// Atomically save split states using ETag-based CAS
    async fn atomic_save_split_states(
        &self,
        states: &HashMap<String, SplitState>,
        expected_etag: String,
    ) -> Result<()> {
        let path = self.split_states_path();

        let content = serde_json::to_string_pretty(states)?;
        let bytes = content.into_bytes();

        self.put_with_cas(
            &path,
            PutPayload::from(bytes),
            &expected_etag,
            "split states",
        )
        .await
        .map(|_| {
            debug!("Atomically saved {} split states", states.len());
        })
    }

    /// Load shard metadata with ETag for atomic operations
    async fn load_shard_with_etag(
        &self,
        shard_id: &str,
    ) -> Result<(crate::sharding::ShardMetadata, String)> {
        let path = self.shard_metadata_path(shard_id);

        match self.object_store.get(&path).await {
            Ok(result) => {
                let e_tag = result
                    .meta
                    .e_tag
                    .clone()
                    .unwrap_or_else(|| "no-etag".to_string());
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);

                let metadata: crate::sharding::ShardMetadata = serde_json::from_str(&content)?;

                debug!(
                    "Loaded shard metadata for {} with ETag: {}",
                    shard_id, e_tag
                );
                Ok((metadata, e_tag))
            }
            Err(object_store::Error::NotFound { .. }) => {
                Err(Error::ShardNotFound(shard_id.to_string()))
            }
            Err(e) => Err(Error::Metadata(format!(
                "Failed to load shard metadata: {}",
                e
            ))),
        }
    }

    /// Atomically save shard metadata using ETag-based CAS
    async fn atomic_save_shard(
        &self,
        shard_id: &str,
        metadata: &crate::sharding::ShardMetadata,
        expected_etag: String,
    ) -> Result<()> {
        let path = self.shard_metadata_path(shard_id);

        let content = serde_json::to_string_pretty(metadata)?;
        let bytes = content.into_bytes();

        self.put_with_cas(
            &path,
            PutPayload::from(bytes),
            &expected_etag,
            "shard metadata",
        )
        .await
        .map(|_| {
            debug!("Atomically saved shard metadata for {}", shard_id);
        })
    }

    /// Load compaction leases with ETag for atomic operations
    async fn load_leases_with_etag(&self) -> Result<(CompactionLeases, String)> {
        let path = self.compaction_leases_path();

        match self.object_store.get(&path).await {
            Ok(result) => {
                let e_tag = result
                    .meta
                    .e_tag
                    .clone()
                    .unwrap_or_else(|| "no-etag".to_string());
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);

                let leases: CompactionLeases = serde_json::from_str(&content).map_err(|e| {
                    warn!("Corrupt compaction leases file, treating as empty: {}", e);
                    Error::Metadata(format!("Corrupt compaction leases: {}", e))
                })?;

                debug!(
                    "Loaded {} compaction leases with ETag: {}",
                    leases.leases.len(),
                    e_tag
                );
                Ok((leases, e_tag))
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("No compaction leases file found, starting fresh");
                Ok((CompactionLeases::default(), "none".to_string()))
            }
            Err(e) => Err(Error::Metadata(format!(
                "Failed to load compaction leases: {}",
                e
            ))),
        }
    }

    /// Atomically save compaction leases using ETag-based CAS
    async fn atomic_save_leases(
        &self,
        leases: &CompactionLeases,
        expected_etag: String,
    ) -> Result<()> {
        let path = self.compaction_leases_path();

        let content = serde_json::to_string_pretty(leases)?;
        let bytes = content.into_bytes();

        self.put_with_cas(
            &path,
            PutPayload::from(bytes),
            &expected_etag,
            "compaction leases",
        )
        .await
        .map(|_| {
            debug!("Atomically saved {} compaction leases", leases.leases.len());
        })
    }
}

#[async_trait]
impl MetadataClient for ObjectStoreMetadataClient {
    async fn register_chunk(&self, path: &str, metadata: &ChunkMetadata) -> Result<()> {
        // Use atomic registration with retry logic
        self.atomic_register_chunk(path, metadata).await
    }

    async fn get_chunks(&self, range: TimeRange) -> Result<Vec<TimeIndexEntry>> {
        // Delegate to the predicate-aware version with no predicates
        self.get_chunks_with_predicates(range, &[]).await
    }

    async fn get_chunks_with_predicates(
        &self,
        range: TimeRange,
        predicates: &[super::predicates::ColumnPredicate],
    ) -> Result<Vec<TimeIndexEntry>> {
        // Load unified catalog (from cache or S3)
        let catalog = self.load_catalog_cached().await?;

        // Find hour buckets that overlap with range
        let nanos_per_hour = 3_600_000_000_000i64;
        let start_bucket = (range.start / nanos_per_hour) * nanos_per_hour;
        let end_bucket = (range.end / nanos_per_hour) * nanos_per_hour;

        let mut results = Vec::new();
        let mut pruned_count = 0;
        let mut seen = std::collections::HashSet::new();

        for (_bucket, paths) in catalog.time_index.range(start_bucket..=end_bucket) {
            for path in paths {
                if seen.contains(path) {
                    continue;
                }
                seen.insert(path.clone());

                if let Some(extended) = catalog.chunks.get(path) {
                    let chunk_range =
                        TimeRange::new(extended.base.min_timestamp, extended.base.max_timestamp);

                    if chunk_range.overlaps(&range) {
                        // Apply column predicate filtering
                        let satisfies_predicates = predicates
                            .iter()
                            .all(|pred| pred.evaluate_against_stats(&extended.column_stats));

                        if satisfies_predicates {
                            results.push(TimeIndexEntry {
                                chunk_path: path.clone(),
                                min_timestamp: extended.base.min_timestamp,
                                max_timestamp: extended.base.max_timestamp,
                                row_count: extended.base.row_count,
                                size_bytes: extended.base.size_bytes,
                            });
                        } else {
                            pruned_count += 1;
                            debug!("Pruned chunk {} based on column predicates", path);
                        }
                    }
                }
            }
        }

        // Sort by min timestamp
        results.sort_by_key(|e| e.min_timestamp);

        info!(
            "get_chunks_with_predicates returned {} chunks for range {:?} ({} pruned by column predicates)",
            results.len(),
            range,
            pruned_count
        );
        Ok(results)
    }

    async fn get_chunk(&self, path: &str) -> Result<Option<ChunkMetadata>> {
        let catalog = self.load_catalog_cached().await?;
        Ok(catalog
            .chunks
            .get(path)
            .map(|extended| extended.base.clone()))
    }

    async fn delete_chunk(&self, path: &str) -> Result<()> {
        let catalog = cas_retry!({
            let (mut catalog, etag) = self.load_catalog_with_etag().await?;
            catalog.chunks.remove(path);
            for chunks in catalog.time_index.values_mut() {
                chunks.retain(|p| p != path);
            }
            catalog.time_index.retain(|_, chunks| !chunks.is_empty());
            catalog.version = 2;

            self.atomic_save_catalog(&catalog, etag).await?;
            Ok(catalog)
        })?;

        // Update in-memory cache
        {
            let mut cache = self.catalog_cache.write().await;
            *cache = Some((catalog, Instant::now()));
        }
        info!("Atomically deleted chunk metadata: {}", path);
        Ok(())
    }

    async fn list_chunks(&self) -> Result<Vec<TimeIndexEntry>> {
        let catalog = self.load_catalog_cached().await?;

        let results: Vec<TimeIndexEntry> = catalog
            .chunks
            .values()
            .map(|extended| TimeIndexEntry {
                chunk_path: extended.base.path.clone(),
                min_timestamp: extended.base.min_timestamp,
                max_timestamp: extended.base.max_timestamp,
                row_count: extended.base.row_count,
                size_bytes: extended.base.size_bytes,
            })
            .collect();

        info!("list_chunks returned {} chunks", results.len());
        Ok(results)
    }

    async fn get_l0_candidates(&self, min_count: usize) -> Result<Vec<Vec<String>>> {
        let catalog = self.load_catalog_cached().await?;

        // Group by hour bucket, but only include L0 chunks (level == 0)
        let mut hour_groups: HashMap<i64, Vec<String>> = HashMap::new();

        for (path, extended) in catalog.chunks.iter() {
            // Only consider L0 chunks for L0 compaction
            if extended.level != 0 {
                continue;
            }

            let nanos_per_hour = 3_600_000_000_000i64;
            let bucket = (extended.base.min_timestamp / nanos_per_hour) * nanos_per_hour;

            hour_groups.entry(bucket).or_default().push(path.clone());
        }

        // Filter groups that meet minimum count
        let candidates: Vec<Vec<String>> = hour_groups
            .into_values()
            .filter(|group| group.len() >= min_count)
            .collect();

        info!(
            "get_l0_candidates returned {} groups (L0 only)",
            candidates.len()
        );
        Ok(candidates)
    }

    async fn get_level_candidates(
        &self,
        level: usize,
        target_size: usize,
    ) -> Result<Vec<Vec<String>>> {
        let catalog = self.load_catalog_cached().await?;

        // Filter chunks at the target level
        let mut chunks: Vec<(String, ChunkMetadataExtended)> = catalog
            .chunks
            .into_iter()
            .filter(|(_, e)| e.level == level as u32)
            .map(|(p, e)| (p.clone(), e.clone()))
            .collect();

        // Sort by min_timestamp
        chunks.sort_by_key(|(_, e)| e.base.min_timestamp);

        // Group chunks until target size is reached
        let mut candidates = Vec::new();
        let mut current_group = Vec::new();
        let mut current_size = 0usize;

        for (path, extended) in chunks {
            current_group.push(path);
            current_size += extended.base.size_bytes as usize;

            if current_size >= target_size {
                candidates.push(std::mem::take(&mut current_group));
                current_size = 0;
            }
        }

        // Don't forget the last group if it has chunks
        if !current_group.is_empty() {
            candidates.push(current_group);
        }

        info!(
            "get_level_candidates(level={}) returned {} groups",
            level,
            candidates.len()
        );
        Ok(candidates)
    }

    async fn create_compaction_job(&self, job: CompactionJob) -> Result<()> {
        let job_id = job.id.clone();
        cas_retry!({
            let (mut jobs, etag) = self.load_compaction_jobs_with_etag().await?;
            jobs.push(job.clone());
            self.atomic_save_compaction_jobs(&jobs, etag).await?;
            Ok(())
        })?;
        info!("Created compaction job: {}", job_id);
        Ok(())
    }

    async fn complete_compaction(
        &self,
        source_chunks: &[String],
        target_chunk: &str,
    ) -> Result<()> {
        let catalog = cas_retry!({
            let (mut catalog, etag) = self.load_catalog_with_etag().await?;

            let new_level = source_chunks
                .iter()
                .filter_map(|p| catalog.chunks.get(p).map(|m| m.level))
                .max()
                .unwrap_or(0)
                + 1;

            for path in source_chunks {
                catalog.chunks.remove(path);
                for chunks in catalog.time_index.values_mut() {
                    chunks.retain(|p| p != path);
                }
            }
            catalog.time_index.retain(|_, chunks| !chunks.is_empty());

            match catalog.chunks.get_mut(target_chunk) {
                Some(meta) => {
                    meta.level = new_level;
                    debug!(
                        "Set compacted chunk {} to level {}",
                        target_chunk, new_level
                    );
                }
                None => {
                    return Err(Error::Metadata(format!(
                        "Compaction target chunk not found in catalog: {}",
                        target_chunk
                    )));
                }
            }
            catalog.version = 2;

            self.atomic_save_catalog(&catalog, etag).await?;
            Ok(catalog)
        })?;

        // Update in-memory cache
        {
            let mut cache = self.catalog_cache.write().await;
            *cache = Some((catalog, Instant::now()));
        }
        info!(
            "Completed compaction: {} chunks -> {}",
            source_chunks.len(),
            target_chunk,
        );
        Ok(())
    }

    async fn update_compaction_status(&self, job_id: &str, status: CompactionStatus) -> Result<()> {
        cas_retry!({
            let (mut jobs, etag) = self.load_compaction_jobs_with_etag().await?;
            if let Some(job) = jobs.iter_mut().find(|j| j.id == job_id) {
                job.status = status;
            } else {
                warn!("Compaction job {} not found for status update", job_id);
                return Ok(());
            }
            self.atomic_save_compaction_jobs(&jobs, etag).await?;
            info!("Updated compaction job {} status to {:?}", job_id, status);
            Ok(())
        })
    }

    async fn cleanup_completed_jobs(&self, max_age_secs: i64) -> Result<usize> {
        let now = chrono::Utc::now().timestamp();
        let cutoff = now - max_age_secs;

        for _attempt in 0..MAX_CAS_RETRIES {
            let (mut jobs, etag) = self.load_compaction_jobs_with_etag().await?;
            let original_count = jobs.len();

            jobs.retain(|j| {
                if j.status == CompactionStatus::Completed || j.status == CompactionStatus::Failed {
                    // Remove if created_at is before cutoff, or if no timestamp (legacy job)
                    match j.created_at {
                        Some(ts) => ts > cutoff,
                        None => false, // remove legacy jobs with no timestamp
                    }
                } else {
                    true // keep Pending/InProgress
                }
            });

            let removed = original_count - jobs.len();
            if removed == 0 {
                return Ok(0);
            }

            match self.atomic_save_compaction_jobs(&jobs, etag).await {
                Ok(()) => {
                    info!(removed, "Cleaned up terminal compaction jobs");
                    return Ok(removed);
                }
                Err(Error::Conflict) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(Error::TooManyRetries)
    }

    async fn get_pending_compaction_jobs(&self) -> Result<Vec<CompactionJob>> {
        let path = self.compaction_jobs_path();

        let jobs: Vec<CompactionJob> = match self.object_store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);
                serde_json::from_str(&content)
                    .map_err(|e| Error::Metadata(format!("Corrupt compaction jobs: {}", e)))?
            }
            Err(object_store::Error::NotFound { .. }) => Vec::new(),
            Err(e) => {
                return Err(Error::Metadata(format!(
                    "Failed to load compaction jobs: {}",
                    e
                )))
            }
        };

        let pending: Vec<CompactionJob> = jobs
            .into_iter()
            .filter(|j| j.status == CompactionStatus::Pending)
            .collect();

        info!(
            "get_pending_compaction_jobs returned {} jobs",
            pending.len()
        );
        Ok(pending)
    }

    // Shard split methods
    async fn start_split(
        &self,
        old_shard: &str,
        new_shards: Vec<String>,
        split_point: Vec<u8>,
    ) -> Result<()> {
        cas_retry!({
            let (mut states, etag) = self.load_split_states_with_etag().await?;
            states.insert(
                old_shard.to_string(),
                SplitState {
                    phase: SplitPhase::Preparation,
                    old_shard: old_shard.to_string(),
                    new_shards: new_shards.clone(),
                    split_point: split_point.clone(),
                    split_timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                    backfill_progress: 0.0,
                },
            );
            self.atomic_save_split_states(&states, etag).await?;
            info!("Started split for shard: {}", old_shard);
            Ok(())
        })
    }

    async fn get_split_state(&self, shard_id: &str) -> Result<Option<SplitState>> {
        let path = self.split_states_path();

        let states: HashMap<String, SplitState> = match self.object_store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);
                serde_json::from_str(&content)
                    .map_err(|e| Error::Metadata(format!("Corrupt split states: {}", e)))?
            }
            Err(object_store::Error::NotFound { .. }) => HashMap::new(),
            Err(e) => {
                return Err(Error::Metadata(format!(
                    "Failed to load split states: {}",
                    e
                )))
            }
        };

        Ok(states.get(shard_id).cloned())
    }

    async fn update_split_progress(
        &self,
        shard_id: &str,
        progress: f64,
        phase: SplitPhase,
    ) -> Result<()> {
        let phase_name = format!("{:?}", phase);
        cas_retry!({
            let (mut states, etag) = self.load_split_states_with_etag().await?;
            if let Some(state) = states.get_mut(shard_id) {
                let previous_phase = format!("{:?}", state.phase);
                state.backfill_progress = progress;
                state.phase = phase;
                if previous_phase != phase_name {
                    counter!(
                        "cardinalsin_split_phase_transitions_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "phase" => phase_name.clone()
                    )
                    .increment(1);
                }
            } else {
                warn!(
                    "No split state found for shard {} during progress update",
                    shard_id
                );
                return Ok(());
            }
            self.atomic_save_split_states(&states, etag).await?;
            info!(
                "Updated split progress for {}: {:.1}%, phase: {:?}",
                shard_id,
                progress * 100.0,
                phase
            );
            Ok(())
        })
    }

    async fn complete_split(&self, old_shard: &str) -> Result<()> {
        cas_retry!({
            let (mut states, etag) = self.load_split_states_with_etag().await?;
            states.remove(old_shard);
            self.atomic_save_split_states(&states, etag).await?;
            info!("Completed split for shard: {}", old_shard);
            Ok(())
        })
    }

    async fn get_chunks_for_shard(&self, shard_id: &str) -> Result<Vec<TimeIndexEntry>> {
        let catalog = self.load_catalog_cached().await?;

        // Filter chunks by shard_id field first, fall back to path.contains() for legacy data
        let results: Vec<TimeIndexEntry> = catalog
            .chunks
            .iter()
            .filter(|(path, extended)| {
                if let Some(ref chunk_shard) = extended.shard_id {
                    chunk_shard == shard_id
                } else {
                    // Legacy fallback: match by path substring
                    path.contains(shard_id)
                }
            })
            .map(|(path, extended)| TimeIndexEntry {
                chunk_path: path.clone(),
                min_timestamp: extended.base.min_timestamp,
                max_timestamp: extended.base.max_timestamp,
                row_count: extended.base.row_count,
                size_bytes: extended.base.size_bytes,
            })
            .collect();

        info!(
            "get_chunks_for_shard({}) returned {} chunks",
            shard_id,
            results.len()
        );
        Ok(results)
    }

    async fn get_shard_metadata(
        &self,
        shard_id: &str,
    ) -> Result<Option<crate::sharding::ShardMetadata>> {
        match self.load_shard_with_etag(shard_id).await {
            Ok((metadata, _)) => Ok(Some(metadata)),
            Err(Error::ShardNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn update_shard_metadata(
        &self,
        shard_id: &str,
        metadata: &crate::sharding::ShardMetadata,
        expected_generation: u64,
    ) -> Result<()> {
        cas_retry!({
            let (current, etag) = match self.load_shard_with_etag(shard_id).await {
                Ok(result) => result,
                Err(Error::ShardNotFound(_)) if expected_generation == 0 => {
                    // New shard - no existing metadata
                    let mut new_metadata = metadata.clone();
                    new_metadata.generation = 1;
                    self.atomic_save_shard(shard_id, &new_metadata, "none".to_string())
                        .await?;
                    info!("Created new shard {} metadata (generation 0 → 1)", shard_id);
                    return Ok(());
                }
                Err(e) => return Err(e),
            };

            if current.generation != expected_generation {
                return Err(Error::StaleGeneration {
                    expected: expected_generation,
                    actual: current.generation,
                });
            }

            let mut new_metadata = metadata.clone();
            new_metadata.generation = expected_generation + 1;
            self.atomic_save_shard(shard_id, &new_metadata, etag)
                .await?;
            info!(
                "Updated shard {} metadata (generation {} → {})",
                shard_id,
                expected_generation,
                expected_generation + 1
            );
            Ok(())
        })
    }

    async fn acquire_lease(
        &self,
        node_id: &str,
        chunks: &[String],
        level: u32,
    ) -> Result<CompactionLease> {
        let lease_ttl = std::time::Duration::from_secs(300); // 5 minutes

        for retry in 0..MAX_CAS_RETRIES {
            let (mut leases, etag) = self.load_leases_with_etag().await?;

            let now = chrono::Utc::now();

            // Scavenge expired active leases
            leases.leases.retain(|_, lease| {
                !(lease.status == LeaseStatus::Active && lease.expires_at <= now)
            });

            // Check if any requested chunks are already leased
            let leased_chunks: std::collections::HashSet<&str> = leases
                .leases
                .values()
                .filter(|l| l.status == LeaseStatus::Active && l.expires_at > now)
                .flat_map(|l| l.chunks.iter().map(|c| c.as_str()))
                .collect();

            let conflicts: Vec<String> = chunks
                .iter()
                .filter(|c| leased_chunks.contains(c.as_str()))
                .cloned()
                .collect();

            if !conflicts.is_empty() {
                debug!("Chunks already leased: {:?}, skipping", conflicts);
                return Err(Error::ChunksAlreadyLeased(conflicts));
            }

            // Create new lease
            let lease = CompactionLease {
                lease_id: uuid::Uuid::new_v4().to_string(),
                holder_id: node_id.to_string(),
                chunks: chunks.to_vec(),
                acquired_at: now,
                expires_at: now + chrono::Duration::from_std(lease_ttl).unwrap(),
                level,
                status: LeaseStatus::Active,
            };

            leases.leases.insert(lease.lease_id.clone(), lease.clone());

            match self.atomic_save_leases(&leases, etag).await {
                Ok(_) => {
                    let active_leases = leases
                        .leases
                        .values()
                        .filter(|l| l.status == LeaseStatus::Active && l.expires_at > now)
                        .count();
                    gauge!(
                        "cardinalsin_metadata_lease_active",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant()
                    )
                    .set(active_leases as f64);
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "acquire",
                        "result" => "ok"
                    )
                    .increment(1);
                    info!(
                        "Acquired lease {} for {} chunks at level {} after {} retries",
                        lease.lease_id,
                        chunks.len(),
                        level,
                        retry
                    );
                    return Ok(lease);
                }
                Err(Error::Conflict) => {
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "acquire",
                        "result" => "conflict"
                    )
                    .increment(1);
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    debug!(
                        "Conflict on acquire_lease attempt {}, retrying after {}ms",
                        retry + 1,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        counter!(
            "cardinalsin_metadata_lease_operations_total",
            "service" => crate::telemetry::service(),
            "run_id" => crate::telemetry::run_id(),
            "tenant" => crate::telemetry::tenant(),
            "operation" => "acquire",
            "result" => "error"
        )
        .increment(1);
        Err(Error::TooManyRetries)
    }

    async fn complete_lease(&self, lease_id: &str) -> Result<()> {
        for retry in 0..MAX_CAS_RETRIES {
            let (mut leases, etag) = self.load_leases_with_etag().await?;

            if let Some(lease) = leases.leases.get_mut(lease_id) {
                lease.status = LeaseStatus::Completed;
            } else {
                warn!(
                    "Lease {} not found during completion -- may have expired",
                    lease_id
                );
                counter!(
                    "cardinalsin_metadata_lease_operations_total",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant(),
                    "operation" => "complete",
                    "result" => "skipped"
                )
                .increment(1);
                return Ok(());
            }

            match self.atomic_save_leases(&leases, etag).await {
                Ok(_) => {
                    let now = chrono::Utc::now();
                    let active_leases = leases
                        .leases
                        .values()
                        .filter(|l| l.status == LeaseStatus::Active && l.expires_at > now)
                        .count();
                    gauge!(
                        "cardinalsin_metadata_lease_active",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant()
                    )
                    .set(active_leases as f64);
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "complete",
                        "result" => "ok"
                    )
                    .increment(1);
                    info!("Completed lease {} after {} retries", lease_id, retry);
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "complete",
                        "result" => "conflict"
                    )
                    .increment(1);
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        counter!(
            "cardinalsin_metadata_lease_operations_total",
            "service" => crate::telemetry::service(),
            "run_id" => crate::telemetry::run_id(),
            "tenant" => crate::telemetry::tenant(),
            "operation" => "complete",
            "result" => "error"
        )
        .increment(1);
        Err(Error::TooManyRetries)
    }

    async fn fail_lease(&self, lease_id: &str) -> Result<()> {
        for retry in 0..MAX_CAS_RETRIES {
            let (mut leases, etag) = self.load_leases_with_etag().await?;

            if let Some(lease) = leases.leases.get_mut(lease_id) {
                lease.status = LeaseStatus::Failed;
            } else {
                warn!(
                    "Lease {} not found during failure marking -- may have expired",
                    lease_id
                );
                counter!(
                    "cardinalsin_metadata_lease_operations_total",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant(),
                    "operation" => "fail",
                    "result" => "skipped"
                )
                .increment(1);
                return Ok(());
            }

            match self.atomic_save_leases(&leases, etag).await {
                Ok(_) => {
                    let now = chrono::Utc::now();
                    let active_leases = leases
                        .leases
                        .values()
                        .filter(|l| l.status == LeaseStatus::Active && l.expires_at > now)
                        .count();
                    gauge!(
                        "cardinalsin_metadata_lease_active",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant()
                    )
                    .set(active_leases as f64);
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "fail",
                        "result" => "ok"
                    )
                    .increment(1);
                    info!(
                        "Marked lease {} as failed after {} retries",
                        lease_id, retry
                    );
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "fail",
                        "result" => "conflict"
                    )
                    .increment(1);
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        counter!(
            "cardinalsin_metadata_lease_operations_total",
            "service" => crate::telemetry::service(),
            "run_id" => crate::telemetry::run_id(),
            "tenant" => crate::telemetry::tenant(),
            "operation" => "fail",
            "result" => "error"
        )
        .increment(1);
        Err(Error::TooManyRetries)
    }

    async fn renew_lease(&self, lease_id: &str) -> Result<()> {
        let extension = std::time::Duration::from_secs(300); // Extend by 5 minutes

        for retry in 0..MAX_CAS_RETRIES {
            let (mut leases, etag) = self.load_leases_with_etag().await?;

            if let Some(lease) = leases.leases.get_mut(lease_id) {
                if lease.status != LeaseStatus::Active {
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "renew",
                        "result" => "error"
                    )
                    .increment(1);
                    return Err(Error::Internal(format!(
                        "Cannot renew non-active lease {}",
                        lease_id
                    )));
                }
                lease.expires_at =
                    chrono::Utc::now() + chrono::Duration::from_std(extension).unwrap();
            } else {
                counter!(
                    "cardinalsin_metadata_lease_operations_total",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant(),
                    "operation" => "renew",
                    "result" => "error"
                )
                .increment(1);
                return Err(Error::Internal(format!("Lease {} not found", lease_id)));
            }

            match self.atomic_save_leases(&leases, etag).await {
                Ok(_) => {
                    let now = chrono::Utc::now();
                    let active_leases = leases
                        .leases
                        .values()
                        .filter(|l| l.status == LeaseStatus::Active && l.expires_at > now)
                        .count();
                    gauge!(
                        "cardinalsin_metadata_lease_active",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant()
                    )
                    .set(active_leases as f64);
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "renew",
                        "result" => "ok"
                    )
                    .increment(1);
                    debug!("Renewed lease {} after {} retries", lease_id, retry);
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "renew",
                        "result" => "conflict"
                    )
                    .increment(1);
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        counter!(
            "cardinalsin_metadata_lease_operations_total",
            "service" => crate::telemetry::service(),
            "run_id" => crate::telemetry::run_id(),
            "tenant" => crate::telemetry::tenant(),
            "operation" => "renew",
            "result" => "error"
        )
        .increment(1);
        Err(Error::TooManyRetries)
    }

    async fn load_leases(&self) -> Result<CompactionLeases> {
        let (leases, _) = self.load_leases_with_etag().await?;
        Ok(leases)
    }

    async fn scavenge_leases(&self) -> Result<usize> {
        for retry in 0..MAX_CAS_RETRIES {
            let (mut leases, etag) = self.load_leases_with_etag().await?;
            let original_count = leases.leases.len();
            let now = chrono::Utc::now();

            // Remove expired active leases and terminal (Completed/Failed) leases
            leases.leases.retain(|_, lease| {
                if lease.status == LeaseStatus::Active {
                    lease.expires_at > now
                } else {
                    // Keep nothing in terminal state -- they've served their purpose
                    false
                }
            });

            let removed = original_count - leases.leases.len();
            if removed == 0 {
                gauge!(
                    "cardinalsin_metadata_lease_active",
                    "service" => crate::telemetry::service(),
                    "run_id" => crate::telemetry::run_id(),
                    "tenant" => crate::telemetry::tenant()
                )
                .set(leases.leases.len() as f64);
                return Ok(0);
            }

            match self.atomic_save_leases(&leases, etag).await {
                Ok(_) => {
                    gauge!(
                        "cardinalsin_metadata_lease_active",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant()
                    )
                    .set(leases.leases.len() as f64);
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "scavenge",
                        "result" => "ok"
                    )
                    .increment(1);
                    info!("Scavenged {} compaction leases", removed);
                    return Ok(removed);
                }
                Err(Error::Conflict) => {
                    counter!(
                        "cardinalsin_metadata_lease_operations_total",
                        "service" => crate::telemetry::service(),
                        "run_id" => crate::telemetry::run_id(),
                        "tenant" => crate::telemetry::tenant(),
                        "operation" => "scavenge",
                        "result" => "conflict"
                    )
                    .increment(1);
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        counter!(
            "cardinalsin_metadata_lease_operations_total",
            "service" => crate::telemetry::service(),
            "run_id" => crate::telemetry::run_id(),
            "tenant" => crate::telemetry::tenant(),
            "operation" => "scavenge",
            "result" => "error"
        )
        .increment(1);
        Err(Error::TooManyRetries)
    }

    async fn has_active_split(&self) -> Result<bool> {
        use crate::sharding::SplitPhase;
        let (states, _) = self.load_split_states_with_etag().await?;
        Ok(states
            .values()
            .any(|s| matches!(s.phase, SplitPhase::DualWrite | SplitPhase::Backfill)))
    }
}

pub type S3MetadataConfig = ObjectStoreMetadataConfig;

pub type S3MetadataClient = ObjectStoreMetadataClient;
