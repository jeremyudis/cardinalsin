//! Query engine for CardinalSin
//!
//! The query node is responsible for:
//! - Executing SQL queries via DataFusion
//! - Managing tiered caching (RAM → NVMe → S3)
//! - Handling streaming queries (historical + live)

mod cache;
mod cached_store;
mod dedup;
mod engine;
mod router;
mod streaming;
mod telemetry;

pub use cache::{CacheConfig, TieredCache};
pub use cached_store::CachedObjectStore;
pub use engine::QueryEngine;
pub use router::QueryRouter;
pub use streaming::{QueryFilter, StreamingQuery, StreamingQueryExecutor};

use crate::compactor::ChunkPinRegistry;
use crate::ingester::FilteredReceiver;
use crate::metadata::MetadataClient;
use crate::{Error, Result, StorageConfig};

use arrow_array::RecordBatch;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tracing::info_span;

/// Configuration for the query node
#[derive(Debug, Clone)]
pub struct QueryConfig {
    /// L1 cache size (RAM) in bytes
    pub l1_cache_size: usize,
    /// L2 cache size (NVMe) in bytes
    pub l2_cache_size: usize,
    /// L2 cache directory
    pub l2_cache_dir: Option<String>,
    /// Maximum concurrent queries
    pub max_concurrent_queries: usize,
    /// Query timeout
    pub query_timeout: std::time::Duration,
    /// Enable streaming queries
    pub streaming_enabled: bool,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            l1_cache_size: 1024 * 1024 * 1024,      // 1GB RAM cache
            l2_cache_size: 10 * 1024 * 1024 * 1024, // 10GB NVMe cache
            l2_cache_dir: None,
            max_concurrent_queries: 100,
            query_timeout: std::time::Duration::from_secs(300),
            streaming_enabled: true,
        }
    }
}

/// Query node for executing queries
pub struct QueryNode {
    /// Configuration
    config: QueryConfig,
    /// DataFusion query engine (public for API access)
    pub engine: QueryEngine,
    /// Tiered cache
    cache: Arc<TieredCache>,
    /// Object storage client
    _object_store: Arc<dyn ObjectStore>,
    /// Metadata client
    metadata: Arc<dyn MetadataClient>,
    /// Storage configuration
    _storage_config: StorageConfig,
    /// Broadcast receiver for streaming queries (legacy)
    broadcast_rx: Option<broadcast::Receiver<RecordBatch>>,
    /// Filtered receiver for topic-based streaming (recommended - 90% less bandwidth)
    filtered_rx: Option<FilteredReceiver>,
    /// Adaptive index controller (optional)
    adaptive_index_controller: Option<Arc<crate::adaptive_index::AdaptiveIndexController>>,
    /// Chunk pin registry shared with compactor to prevent GC during queries
    pin_registry: Option<ChunkPinRegistry>,
}

impl QueryNode {
    /// Create a new query node
    pub async fn new(
        config: QueryConfig,
        object_store: Arc<dyn ObjectStore>,
        metadata: Arc<dyn MetadataClient>,
        storage_config: StorageConfig,
    ) -> Result<Self> {
        let cache: Arc<TieredCache> = Arc::new(
            TieredCache::new(CacheConfig {
                l1_size: config.l1_cache_size,
                l2_size: config.l2_cache_size,
                l2_dir: config.l2_cache_dir.clone(),
            })
            .await?,
        );

        let engine = QueryEngine::new(object_store.clone(), cache.clone()).await?;

        Ok(Self {
            config,
            engine,
            cache,
            _object_store: object_store,
            metadata,
            _storage_config: storage_config,
            broadcast_rx: None,
            filtered_rx: None,
            adaptive_index_controller: None,
            pin_registry: None,
        })
    }

    /// Connect to ingester broadcast for streaming queries (legacy)
    pub fn connect_broadcast(&mut self, rx: broadcast::Receiver<RecordBatch>) {
        self.broadcast_rx = Some(rx);
    }

    /// Connect a filtered receiver for topic-based streaming (recommended)
    ///
    /// This is the preferred approach as it reduces bandwidth waste from 90% to near-zero
    /// by only receiving data matching the subscription filter at the ingester level.
    pub fn with_topic_filter(mut self, filtered_rx: FilteredReceiver) -> Self {
        self.filtered_rx = Some(filtered_rx);
        self
    }

    /// Set a chunk pin registry shared with the compactor.
    ///
    /// When set, chunks are pinned during query execution to prevent
    /// the compactor's GC from deleting them mid-query.
    pub fn with_pin_registry(mut self, registry: ChunkPinRegistry) -> Self {
        self.pin_registry = Some(registry);
        self
    }

    /// Enable adaptive indexing with the provided controller
    pub fn with_adaptive_indexing(
        mut self,
        controller: Arc<crate::adaptive_index::AdaptiveIndexController>,
    ) -> Self {
        self.adaptive_index_controller = Some(controller);
        self
    }

    /// Execute a SQL query
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        self.query_for_tenant(sql, "default").await
    }

    /// Execute a SQL query for a specific tenant with optional adaptive indexing
    pub async fn query_for_tenant(&self, sql: &str, tenant_id: &str) -> Result<Vec<RecordBatch>> {
        let started = Instant::now();
        let run_id = std::env::var("CARDINALSIN_TELEMETRY_RUN_ID")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let span = info_span!(
            "query.execute",
            tenant_id = %tenant_id,
            run_id = run_id.as_deref().unwrap_or("none")
        );
        let _guard = span.enter();
        let cache_before = self.cache.stats();

        let result = async {
            // Get time range from query for chunk pruning
            let time_range = self.engine.extract_time_range(sql).await?;

            // Extract column predicates for metadata-level filtering (CRITICAL: reduces S3 costs)
            let predicates = self.engine.extract_column_predicates(sql).await?;

            // Measure metadata pruning effectiveness
            let candidate_chunks = self.metadata.get_chunks(time_range).await?;
            let chunks = self
                .metadata
                .get_chunks_with_predicates(time_range, &predicates)
                .await?;
            let chunks_candidate_count = candidate_chunks.len() as u64;
            let chunks_selected_count = chunks.len() as u64;
            let bytes_scanned = chunks.iter().map(|chunk| chunk.size_bytes).sum::<u64>();

            // Pin chunks to prevent GC during query execution (RAII guard unpins on drop)
            let chunk_paths: Vec<String> = chunks.iter().map(|c| c.chunk_path.clone()).collect();
            let _pin_guard = self.pin_registry.as_ref().map(|r| r.pin(chunk_paths));

            // Check if any shard is in a dual-write split phase (causes duplicate data)
            let needs_dedup = self.metadata.has_active_split().await.unwrap_or(false);

            // Register chunks with DataFusion
            for chunk in &chunks {
                match self.engine.register_chunk(&chunk.chunk_path).await {
                    Ok(true) => telemetry::record_chunk_registration("new"),
                    Ok(false) => telemetry::record_chunk_registration("already_registered"),
                    Err(e) => {
                        telemetry::record_chunk_registration("error");
                        return Err(e);
                    }
                }
            }

            // Execute query with or without adaptive indexing
            let results = if let Some(ref controller) = self.adaptive_index_controller {
                self.engine
                    .execute_with_indexes(sql, tenant_id, controller.clone())
                    .await?
            } else {
                self.engine.execute(sql).await?
            };

            // Deduplicate if any shard is in dual-write phase
            let final_results = if needs_dedup {
                dedup::dedup_batches(results)?
            } else {
                results
            };

            let rows_returned = final_results
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum();
            let bytes_returned = final_results
                .iter()
                .map(|batch| batch.get_array_memory_size() as u64)
                .sum();

            Ok((
                final_results,
                rows_returned,
                bytes_returned,
                bytes_scanned,
                chunks_selected_count,
                chunks_candidate_count,
            ))
        }
        .await;

        let elapsed = started.elapsed().as_secs_f64();
        let cache_after = self.cache.stats();
        telemetry::record_cache_delta(&cache_before, &cache_after);

        match result {
            Ok((
                final_results,
                rows_returned,
                bytes_returned,
                bytes_scanned,
                chunks_selected_count,
                chunks_candidate_count,
            )) => {
                telemetry::record_query(telemetry::QueryMetrics {
                    outcome: "success",
                    error_class: None,
                    duration_seconds: elapsed,
                    rows_returned,
                    bytes_scanned,
                    bytes_returned,
                    chunks_selected: chunks_selected_count,
                    chunks_candidate: chunks_candidate_count,
                });
                Ok(final_results)
            }
            Err(error) => {
                telemetry::record_query(telemetry::QueryMetrics {
                    outcome: "error",
                    error_class: Some(error_class(&error)),
                    duration_seconds: elapsed,
                    rows_returned: 0,
                    bytes_scanned: 0,
                    bytes_returned: 0,
                    chunks_selected: 0,
                    chunks_candidate: 0,
                });
                Err(error)
            }
        }
    }

    /// Execute a streaming query (historical + live) using legacy broadcast
    pub async fn query_stream(
        &self,
        sql: &str,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<RecordBatch>>> {
        if !self.config.streaming_enabled {
            return Err(Error::Query("Streaming queries are disabled".into()));
        }

        let broadcast_rx = self
            .broadcast_rx
            .as_ref()
            .ok_or_else(|| Error::Query("No broadcast channel connected".into()))?
            .resubscribe();

        let executor =
            StreamingQueryExecutor::new(self.engine.clone(), self.metadata.clone(), broadcast_rx);

        executor.execute(sql).await
    }

    /// Execute a streaming query with topic filtering (recommended)
    ///
    /// Uses FilteredReceiver for efficient topic-based streaming with 90% less
    /// bandwidth than the legacy broadcast approach.
    pub async fn query_stream_filtered(
        &self,
        sql: &str,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<RecordBatch>>> {
        if !self.config.streaming_enabled {
            return Err(Error::Query("Streaming queries are disabled".into()));
        }

        let filtered_rx = self
            .filtered_rx
            .as_ref()
            .ok_or_else(|| {
                Error::Query(
                    "No filtered channel connected. Use with_topic_filter() to connect one.".into(),
                )
            })?
            .resubscribe();

        let executor = StreamingQueryExecutor::new_filtered(
            self.engine.clone(),
            self.metadata.clone(),
            filtered_rx,
        );

        executor.execute(sql).await
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub l1_hits: u64,
    pub l1_misses: u64,
    pub l2_hits: u64,
    pub l2_misses: u64,
    pub l1_evictions: u64,
    pub l1_size_bytes: usize,
    pub l2_size_bytes: usize,
}

fn error_class(error: &Error) -> &'static str {
    match error {
        Error::Arrow(_) => "arrow",
        Error::Parquet(_) => "parquet",
        Error::ObjectStore(_) => "object_store",
        Error::DataFusion(_) => "datafusion",
        Error::Io(_) => "io",
        Error::Serialization(_) => "serialization",
        Error::Config(_) => "config",
        Error::InvalidSchema(_) => "invalid_schema",
        Error::MissingMetricName => "missing_metric_name",
        Error::BufferFull => "buffer_full",
        Error::WalFull => "wal_full",
        Error::Query(_) => "query",
        Error::Metadata(_) => "metadata",
        Error::Shard(_) => "shard",
        Error::Cache(_) => "cache",
        Error::Timeout => "timeout",
        Error::Internal(_) => "internal",
        Error::StaleGeneration { .. } => "stale_generation",
        Error::ShardMoved { .. } => "shard_moved",
        Error::RateLimitExceeded { .. } => "rate_limit",
        Error::TooManySubscriptions => "too_many_subscriptions",
        Error::Conflict => "conflict",
        Error::TooManyRetries => "too_many_retries",
        Error::ShardNotFound(_) => "shard_not_found",
        Error::ChunksAlreadyLeased(_) => "chunks_already_leased",
    }
}
