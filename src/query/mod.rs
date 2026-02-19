//! Query engine for CardinalSin
//!
//! The query node is responsible for:
//! - Executing SQL queries via DataFusion
//! - Managing tiered caching (RAM → NVMe → S3)
//! - Handling streaming queries (historical + live)

mod cache;
mod cached_store;
mod engine;
mod router;
mod streaming;

pub use cache::{CacheConfig, TieredCache};
pub use cached_store::CachedObjectStore;
pub use engine::QueryEngine;
pub use router::QueryRouter;
pub use streaming::{
    Predicate, PredicateOp, PredicateValue, QueryFilter, StreamingQuery, StreamingQueryExecutor,
};

use crate::ingester::FilteredReceiver;
use crate::metadata::MetadataClient;
use crate::{Error, Result, StorageConfig};

use arrow_array::RecordBatch;
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::sync::broadcast;

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
        // Get time range from query for chunk pruning
        let time_range = self.engine.extract_time_range(sql).await?;

        // Extract column predicates for metadata-level filtering (CRITICAL: reduces S3 costs)
        let predicates = self.engine.extract_column_predicates(sql).await?;

        // Get relevant chunks from metadata with predicate pushdown
        let chunks = self
            .metadata
            .get_chunks_with_predicates(time_range, &predicates)
            .await?;

        // Register chunks with DataFusion
        for chunk in &chunks {
            self.engine.register_chunk(&chunk.chunk_path).await?;
        }

        // Execute query with or without adaptive indexing
        let results = if let Some(ref controller) = self.adaptive_index_controller {
            self.engine
                .execute_with_indexes(sql, tenant_id, controller.clone())
                .await?
        } else {
            self.engine.execute(sql).await?
        };

        Ok(results)
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
    pub l1_size_bytes: usize,
    pub l2_size_bytes: usize,
}
