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

use super::{CompactionJob, CompactionStatus, MetadataClient, SplitState, TimeIndexEntry, TimeRange};
use crate::ingester::ChunkMetadata;
use crate::sharding::SplitPhase;
use crate::{Error, Result};

use async_trait::async_trait;
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
pub struct S3MetadataConfig {
    /// S3 bucket name
    pub bucket: String,
    /// Prefix for metadata files
    pub metadata_prefix: String,
    /// Enable caching of metadata reads
    pub enable_cache: bool,
    /// Allow fallback to unsafe overwrite when CAS update is not supported
    pub allow_unsafe_overwrite: bool,
}

impl Default for S3MetadataConfig {
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
pub struct S3MetadataClient {
    /// Object store for metadata storage
    object_store: Arc<dyn ObjectStore>,
    /// Configuration
    config: S3MetadataConfig,
    /// Unified catalog cache with TTL
    catalog_cache: Arc<tokio::sync::RwLock<Option<(MetadataCatalog, Instant)>>>,
    /// Catalog cache TTL duration
    catalog_cache_ttl: Duration,
}

impl S3MetadataClient {
    /// Nanoseconds per hour constant
    const NANOS_PER_HOUR: i64 = 3_600_000_000_000;

    /// Create a new S3 metadata client
    pub fn new(object_store: Arc<dyn ObjectStore>, config: S3MetadataConfig) -> Self {
        Self {
            object_store,
            config,
            catalog_cache: Arc::new(tokio::sync::RwLock::new(None)),
            catalog_cache_ttl: Duration::from_secs(60),
        }
    }

    fn allow_unsafe_overwrite(&self) -> bool {
        if self.config.allow_unsafe_overwrite {
            return true;
        }

        match std::env::var("S3_METADATA_ALLOW_UNSAFE_OVERWRITE") {
            Ok(value) => {
                let value = value.trim();
                value == "1" || value.eq_ignore_ascii_case("true")
            }
            Err(_) => false,
        }
    }

    async fn put_with_fallback(
        &self,
        path: &Path,
        payload: PutPayload,
        expected_etag: &str,
        context: &str,
    ) -> Result<()> {
        let opts = if expected_etag == "none" {
            PutOptions {
                mode: PutMode::Overwrite,
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

        match self.object_store.put_opts(path, payload.clone(), opts).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::Precondition { .. }) => Err(Error::Conflict),
            Err(object_store::Error::NotImplemented) | Err(object_store::Error::NotSupported { .. }) => {
                if !self.allow_unsafe_overwrite() || expected_etag == "none" {
                    return Err(Error::Metadata(format!(
                        "CAS not supported for {} (path: {})",
                        context,
                        path
                    )));
                }

                warn!(
                    "CAS not supported for {} at {} - falling back to unsafe overwrite",
                    context,
                    path
                );

                let overwrite = PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                };

                self.object_store
                    .put_opts(path, payload, overwrite)
                    .await
                    .map(|_| ())
                    .map_err(Error::ObjectStore)
            }
            Err(e) => Err(Error::ObjectStore(e)),
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

                if self.allow_unsafe_overwrite() {
                    Ok(HashMap::new())
                } else {
                    Err(Error::Metadata(
                        "Chunk metadata not initialized (enable S3_METADATA_ALLOW_UNSAFE_OVERWRITE to bootstrap)"
                            .to_string(),
                    ))
                }
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

        self.put_with_fallback(
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

                let index: BTreeMap<i64, Vec<String>> =
                    serde_json::from_str(&content)
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

        self.put_with_fallback(
            &path,
            PutPayload::from(bytes),
            &expected_etag,
            "time index",
        )
        .await?;

        debug!("Atomically saved time index with {} hour buckets", index.len());

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
            Err(e) => Err(Error::Metadata(format!(
                "Failed to load catalog: {}",
                e
            ))),
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

        self.put_with_fallback(
            &path,
            PutPayload::from(bytes),
            &expected_etag,
            "catalog",
        )
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
        for retry in 0..MAX_CAS_RETRIES {
            // Load unified catalog with ETag
            let (mut catalog, catalog_etag) = self.load_catalog_with_etag().await?;

            // Create extended metadata
            let extended = ChunkMetadataExtended {
                base: metadata.clone(),
                column_stats: HashMap::new(),
                level: 0, // New chunks start at L0
                version: String::new(),
                shard_id: None,
            };

            // Update catalog chunks
            catalog.chunks.insert(path.to_string(), extended.clone());

            // Update catalog time index
            let start_bucket = Self::hour_bucket(metadata.min_timestamp);
            let end_bucket = Self::hour_bucket(metadata.max_timestamp);

            let mut bucket = start_bucket;
            while bucket <= end_bucket {
                catalog
                    .time_index
                    .entry(bucket)
                    .or_insert_with(Vec::new)
                    .push(path.to_string());
                bucket += Self::NANOS_PER_HOUR;
            }

            // Ensure version is set to 2 for new writes
            catalog.version = 2;

            // Single atomic save of the entire catalog
            match self.atomic_save_catalog(&catalog, catalog_etag).await {
                Ok(_) => {
                    // Update in-memory cache
                    {
                        let mut cache = self.catalog_cache.write().await;
                        *cache = Some((catalog, Instant::now()));
                    }

                    info!(
                        "Atomically registered chunk: {} (time range: {} to {}) after {} retries",
                        path, metadata.min_timestamp, metadata.max_timestamp, retry
                    );
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    // Conflict detected, retry with exponential backoff
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    debug!(
                        "Conflict detected on attempt {}, retrying after {}ms",
                        retry + 1,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::TooManyRetries)
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

                let jobs: Vec<CompactionJob> =
                    serde_json::from_str(&content)
                        .map_err(|e| Error::Metadata(format!("Corrupt compaction jobs: {}", e)))?;

                debug!(
                    "Loaded {} compaction jobs with ETag: {}",
                    jobs.len(),
                    e_tag
                );
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

        self.put_with_fallback(
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

                let states: HashMap<String, SplitState> =
                    serde_json::from_str(&content)
                        .map_err(|e| Error::Metadata(format!("Corrupt split states: {}", e)))?;

                debug!(
                    "Loaded {} split states with ETag: {}",
                    states.len(),
                    e_tag
                );
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

        self.put_with_fallback(
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

        self.put_with_fallback(
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
}

#[async_trait]
impl MetadataClient for S3MetadataClient {
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
                    let chunk_range = TimeRange::new(
                        extended.base.min_timestamp,
                        extended.base.max_timestamp,
                    );

                    if chunk_range.overlaps(&range) {
                        // Apply column predicate filtering
                        let satisfies_predicates = predicates.iter().all(|pred| {
                            pred.evaluate_against_stats(&extended.column_stats)
                        });

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
        Ok(catalog.chunks.get(path).map(|extended| extended.base.clone()))
    }

    async fn delete_chunk(&self, path: &str) -> Result<()> {
        for retry in 0..MAX_CAS_RETRIES {
            // Load unified catalog with ETag
            let (mut catalog, catalog_etag) = self.load_catalog_with_etag().await?;

            // Remove from catalog chunks
            catalog.chunks.remove(path);

            // Remove from catalog time index (path may appear in multiple buckets)
            for chunks in catalog.time_index.values_mut() {
                chunks.retain(|p| p != path);
            }
            // Clean up empty buckets
            catalog.time_index.retain(|_, chunks| !chunks.is_empty());

            // Ensure version is set to 2 for new writes
            catalog.version = 2;

            // Single atomic save of the entire catalog
            match self.atomic_save_catalog(&catalog, catalog_etag).await {
                Ok(_) => {
                    // Update in-memory cache
                    {
                        let mut cache = self.catalog_cache.write().await;
                        *cache = Some((catalog, Instant::now()));
                    }

                    info!("Atomically deleted chunk metadata: {} after {} retries", path, retry);
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    // Conflict detected, retry with exponential backoff
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    debug!(
                        "Conflict on delete attempt {}, retrying after {}ms",
                        retry + 1,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::TooManyRetries)
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

            hour_groups
                .entry(bucket)
                .or_insert_with(Vec::new)
                .push(path.clone());
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

        for retry in 0..MAX_CAS_RETRIES {
            // Load jobs with ETag for CAS
            let (mut jobs, etag) = self.load_compaction_jobs_with_etag().await?;

            // Add new job
            jobs.push(job.clone());

            // Attempt atomic save
            match self.atomic_save_compaction_jobs(&jobs, etag).await {
                Ok(_) => {
                    info!("Created compaction job: {} after {} retries", job_id, retry);
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    debug!(
                        "Conflict on create_compaction_job attempt {}, retrying after {}ms",
                        retry + 1,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::TooManyRetries)
    }

    async fn complete_compaction(
        &self,
        source_chunks: &[String],
        target_chunk: &str,
    ) -> Result<()> {
        for retry in 0..MAX_CAS_RETRIES {
            // Load unified catalog with ETag
            let (mut catalog, catalog_etag) = self.load_catalog_with_etag().await?;

            // Calculate new level: max(source_levels) + 1
            let new_level = source_chunks
                .iter()
                .filter_map(|p| catalog.chunks.get(p).map(|m| m.level))
                .max()
                .unwrap_or(0)
                + 1;

            // Remove source chunks from catalog
            for path in source_chunks {
                catalog.chunks.remove(path);

                // Remove from time index
                for chunks in catalog.time_index.values_mut() {
                    chunks.retain(|p| p != path);
                }
            }

            // Clean up empty time index buckets
            catalog.time_index.retain(|_, chunks| !chunks.is_empty());

            // Update target chunk level
            if let Some(meta) = catalog.chunks.get_mut(target_chunk) {
                meta.level = new_level;
                debug!(
                    "Set compacted chunk {} to level {}",
                    target_chunk, new_level
                );
            }

            // Ensure version is set to 2 for new writes
            catalog.version = 2;

            // Single atomic save of the entire catalog
            match self.atomic_save_catalog(&catalog, catalog_etag).await {
                Ok(_) => {
                    // Update in-memory cache
                    {
                        let mut cache = self.catalog_cache.write().await;
                        *cache = Some((catalog, Instant::now()));
                    }

                    info!(
                        "Completed compaction: {} chunks -> {} (level {}) after {} retries",
                        source_chunks.len(),
                        target_chunk,
                        new_level,
                        retry
                    );
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    // Conflict detected, retry with exponential backoff
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    debug!(
                        "Conflict detected on compaction attempt {}, retrying after {}ms",
                        retry + 1,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::TooManyRetries)
    }

    async fn update_compaction_status(&self, job_id: &str, status: CompactionStatus) -> Result<()> {
        for retry in 0..MAX_CAS_RETRIES {
            // Load jobs with ETag for CAS
            let (mut jobs, etag) = self.load_compaction_jobs_with_etag().await?;

            // Find and update the job
            let job_found = if let Some(job) = jobs.iter_mut().find(|j| j.id == job_id) {
                job.status = status.clone();
                true
            } else {
                false
            };

            if !job_found {
                // Job not found, nothing to update
                warn!("Compaction job {} not found for status update", job_id);
                return Ok(());
            }

            // Attempt atomic save
            match self.atomic_save_compaction_jobs(&jobs, etag).await {
                Ok(_) => {
                    info!("Updated compaction job {} status to {:?} after {} retries", job_id, status, retry);
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    debug!(
                        "Conflict on update_compaction_status attempt {}, retrying after {}ms",
                        retry + 1,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
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
        for retry in 0..MAX_CAS_RETRIES {
            // Load split states with ETag for CAS
            let (mut states, etag) = self.load_split_states_with_etag().await?;

            // Add new split state
            let split_state = SplitState {
                phase: SplitPhase::Preparation,
                old_shard: old_shard.to_string(),
                new_shards: new_shards.clone(),
                split_point: split_point.clone(),
                split_timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                backfill_progress: 0.0,
            };

            states.insert(old_shard.to_string(), split_state);

            // Attempt atomic save
            match self.atomic_save_split_states(&states, etag).await {
                Ok(_) => {
                    info!("Started split for shard: {} after {} retries", old_shard, retry);
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    debug!(
                        "Conflict on start_split attempt {}, retrying after {}ms",
                        retry + 1,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::TooManyRetries)
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
        for retry in 0..MAX_CAS_RETRIES {
            // Load split states with ETag for CAS
            let (mut states, etag) = self.load_split_states_with_etag().await?;

            // Update state
            let state_found = if let Some(state) = states.get_mut(shard_id) {
                state.backfill_progress = progress;
                state.phase = phase;
                true
            } else {
                false
            };

            if !state_found {
                // No split state found for this shard
                warn!("No split state found for shard {} during progress update", shard_id);
                return Ok(());
            }

            // Attempt atomic save
            match self.atomic_save_split_states(&states, etag).await {
                Ok(_) => {
                    info!(
                        "Updated split progress for {}: {:.1}%, phase: {:?} after {} retries",
                        shard_id,
                        progress * 100.0,
                        phase,
                        retry
                    );
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    debug!(
                        "Conflict on update_split_progress attempt {}, retrying after {}ms",
                        retry + 1,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::TooManyRetries)
    }

    async fn complete_split(&self, old_shard: &str) -> Result<()> {
        for retry in 0..MAX_CAS_RETRIES {
            // Load split states with ETag for CAS
            let (mut states, etag) = self.load_split_states_with_etag().await?;

            // Remove completed split
            states.remove(old_shard);

            // Attempt atomic save
            match self.atomic_save_split_states(&states, etag).await {
                Ok(_) => {
                    info!("Completed split for shard: {} after {} retries", old_shard, retry);
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    debug!(
                        "Conflict on complete_split attempt {}, retrying after {}ms",
                        retry + 1,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::TooManyRetries)
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
        for retry in 0..MAX_CAS_RETRIES {
            // Load current metadata with ETag
            let (current, etag) = match self.load_shard_with_etag(shard_id).await {
                Ok(result) => result,
                Err(Error::ShardNotFound(_)) if expected_generation == 0 => {
                    // New shard - no existing metadata
                    let mut new_metadata = metadata.clone();
                    new_metadata.generation = 1;
                    return self
                        .atomic_save_shard(shard_id, &new_metadata, "none".to_string())
                        .await;
                }
                Err(e) => return Err(e),
            };

            // Verify generation matches
            if current.generation != expected_generation {
                return Err(Error::StaleGeneration {
                    expected: expected_generation,
                    actual: current.generation,
                });
            }

            // Increment generation
            let mut new_metadata = metadata.clone();
            new_metadata.generation = expected_generation + 1;

            // Attempt atomic save with ETag
            match self.atomic_save_shard(shard_id, &new_metadata, etag).await {
                Ok(_) => {
                    info!(
                        "Updated shard {} metadata (generation {} → {}) after {} retries",
                        shard_id,
                        expected_generation,
                        expected_generation + 1,
                        retry
                    );
                    return Ok(());
                }
                Err(Error::Conflict) => {
                    // Another process updated concurrently, retry with standardized backoff
                    let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
                    debug!(
                        "CAS conflict on shard {} update, retrying after {}ms",
                        shard_id, backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::TooManyRetries)
    }
}
