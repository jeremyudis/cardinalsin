//! Ingester module for CardinalSin
//!
//! The ingester is responsible for:
//! - Buffering incoming writes in memory
//! - Batching writes into Parquet files
//! - Flushing to object storage (S3)
//! - Broadcasting new data to streaming query subscribers
//! - Tracking shard metrics for hot shard detection

mod buffer;
mod parquet_writer;
mod broadcast;

pub use buffer::WriteBuffer;
pub use parquet_writer::ParquetWriter;
pub use broadcast::BroadcastChannel;

use crate::metadata::MetadataClient;
use crate::schema::MetricSchema;
use crate::sharding::{ShardMonitor, HotShardConfig, ShardKey};
use crate::{Error, Result, StorageConfig};

use arrow_array::RecordBatch;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{debug, error, info};

/// Configuration for the ingester
#[derive(Debug, Clone)]
pub struct IngesterConfig {
    /// Flush interval (time-based trigger)
    pub flush_interval: Duration,
    /// Maximum rows before flush (row-based trigger)
    pub flush_row_count: usize,
    /// Maximum bytes before flush (size-based trigger)
    pub flush_size_bytes: usize,
    /// Micro-batch collection timeout
    pub batch_timeout: Duration,
    /// Micro-batch size in bytes
    pub batch_size_bytes: usize,
    /// Number of concurrent flushes to S3
    pub flush_parallelism: usize,
    /// Maximum buffer size before rejecting writes
    pub max_buffer_size_bytes: usize,
}

impl Default for IngesterConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(300),        // 5 minutes
            flush_row_count: 1_000_000,                      // 1M rows
            flush_size_bytes: 100 * 1024 * 1024,             // 100MB
            batch_timeout: Duration::from_millis(250),       // 250ms micro-batch
            batch_size_bytes: 8 * 1024 * 1024,               // 8MB
            flush_parallelism: 4,
            max_buffer_size_bytes: 512 * 1024 * 1024,        // 512MB
        }
    }
}

/// Ingester node for receiving and batching metric writes
pub struct Ingester {
    /// Configuration
    config: IngesterConfig,
    /// Write buffer
    buffer: Arc<RwLock<WriteBuffer>>,
    /// Object storage client
    object_store: Arc<dyn ObjectStore>,
    /// Metadata client
    metadata: Arc<dyn MetadataClient>,
    /// Parquet writer
    parquet_writer: ParquetWriter,
    /// Broadcast channel for streaming queries
    broadcast: BroadcastChannel,
    /// Storage configuration
    storage_config: StorageConfig,
    /// Metric schema
    #[allow(dead_code)]
    schema: MetricSchema,
    /// Last flush time
    last_flush: Arc<RwLock<Instant>>,
    /// Shard monitor for hot shard detection
    shard_monitor: Arc<ShardMonitor>,
    /// Default tenant ID (for single-tenant mode)
    default_tenant_id: u32,
}

impl Ingester {
    /// Create a new ingester
    pub fn new(
        config: IngesterConfig,
        object_store: Arc<dyn ObjectStore>,
        metadata: Arc<dyn MetadataClient>,
        storage_config: StorageConfig,
        schema: MetricSchema,
    ) -> Self {
        let broadcast = BroadcastChannel::new(1024);

        // Initialize shard monitor with default config
        let shard_monitor = Arc::new(ShardMonitor::new(HotShardConfig::default()));

        Self {
            config,
            buffer: Arc::new(RwLock::new(WriteBuffer::new())),
            object_store,
            metadata,
            parquet_writer: ParquetWriter::new(),
            broadcast,
            storage_config,
            schema,
            last_flush: Arc::new(RwLock::new(Instant::now())),
            shard_monitor,
            default_tenant_id: 0, // Single-tenant mode by default
        }
    }

    /// Create a new ingester with custom shard config
    pub fn with_shard_config(
        config: IngesterConfig,
        object_store: Arc<dyn ObjectStore>,
        metadata: Arc<dyn MetadataClient>,
        storage_config: StorageConfig,
        schema: MetricSchema,
        shard_config: HotShardConfig,
        tenant_id: u32,
    ) -> Self {
        let broadcast = BroadcastChannel::new(1024);
        let shard_monitor = Arc::new(ShardMonitor::new(shard_config));

        Self {
            config,
            buffer: Arc::new(RwLock::new(WriteBuffer::new())),
            object_store,
            metadata,
            parquet_writer: ParquetWriter::new(),
            broadcast,
            storage_config,
            schema,
            last_flush: Arc::new(RwLock::new(Instant::now())),
            shard_monitor,
            default_tenant_id: tenant_id,
        }
    }

    /// Write metrics to the buffer
    pub async fn write(&self, batch: RecordBatch) -> Result<()> {
        let start_time = std::time::Instant::now();
        let batch_size = batch.get_array_memory_size();

        // Compute shard key for this batch (for monitoring)
        let shard_id = self.compute_shard_id(&batch);

        let mut buffer = self.buffer.write().await;

        // Check if buffer is too full
        if buffer.size_bytes() + batch_size > self.config.max_buffer_size_bytes {
            return Err(Error::BufferFull);
        }

        buffer.append(batch.clone())?;

        // Record write metrics for hot shard detection
        let write_latency = start_time.elapsed();
        self.shard_monitor.record_write(&shard_id, batch_size, write_latency);

        // Check if flush is needed (whichever threshold comes first)
        if self.should_flush(&buffer) {
            let batches = buffer.take();
            drop(buffer); // Release lock before I/O

            self.flush_batches(batches).await?;
        }

        Ok(())
    }

    /// Compute shard ID for a batch based on tenant and metric
    fn compute_shard_id(&self, batch: &RecordBatch) -> String {
        // Extract metric name if available for shard key computation
        let metric_name = if let Some(col) = batch.column_by_name("metric_name") {
            use arrow_array::cast::AsArray;
            if let Some(arr) = col.as_string_opt::<i32>() {
                arr.value(0).to_string()
            } else {
                "unknown".to_string()
            }
        } else {
            "unknown".to_string()
        };

        // Extract timestamp for time-based sharding
        let timestamp = if let Some(col) = batch.column_by_name("timestamp") {
            use arrow_array::cast::AsArray;
            if let Some(arr) = col.as_primitive_opt::<arrow_array::types::TimestampNanosecondType>() {
                arr.value(0)
            } else if let Some(arr) = col.as_primitive_opt::<arrow_array::types::Int64Type>() {
                arr.value(0)
            } else {
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
            }
        } else {
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        };

        // Create shard key and convert to shard ID
        let shard_key = ShardKey::new(self.default_tenant_id, &metric_name, timestamp);
        format!("shard-{:x}", u64::from_be_bytes(shard_key.to_bytes()[0..8].try_into().unwrap_or([0u8; 8])))
    }

    /// Get the shard monitor for external monitoring
    pub fn shard_monitor(&self) -> &Arc<ShardMonitor> {
        &self.shard_monitor
    }

    /// Evaluate shards and get recommended actions (for external orchestration)
    pub fn evaluate_shards(&self) -> Vec<crate::sharding::ShardAction> {
        self.shard_monitor.evaluate_shards()
    }

    /// Check if flush is needed based on thresholds
    fn should_flush(&self, buffer: &WriteBuffer) -> bool {
        buffer.row_count() >= self.config.flush_row_count
            || buffer.size_bytes() >= self.config.flush_size_bytes
    }

    /// Flush batches to object storage
    async fn flush_batches(&self, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        info!(
            batch_count = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Flushing batches to object storage"
        );

        // Concatenate batches
        let combined = arrow::compute::concat_batches(
            &batches[0].schema(),
            batches.iter(),
        )?;

        // Convert to Parquet
        let parquet_bytes = self.parquet_writer.write_batch(&combined)?;
        let parquet_size = parquet_bytes.len() as u64;

        // Generate path
        let path = self.generate_path();
        debug!(path = %path, size_bytes = parquet_size, "Writing Parquet file");

        // Upload to object storage
        self.object_store
            .put(&path.clone().into(), parquet_bytes.into())
            .await?;

        // Register in metadata store
        let chunk_metadata = ChunkMetadata {
            path: path.clone(),
            min_timestamp: self.extract_min_timestamp(&combined)?,
            max_timestamp: self.extract_max_timestamp(&combined)?,
            row_count: combined.num_rows() as u64,
            size_bytes: parquet_size,
        };
        self.metadata.register_chunk(&path, &chunk_metadata).await?;

        // Broadcast to streaming query subscribers
        if let Err(e) = self.broadcast.send(combined) {
            debug!("No streaming subscribers: {}", e);
        }

        // Update last flush time
        *self.last_flush.write().await = Instant::now();

        Ok(())
    }

    /// Background flush timer - ensures data is flushed even during low traffic
    pub async fn run_flush_timer(&self) {
        let mut interval = tokio::time::interval(self.config.flush_interval);

        loop {
            interval.tick().await;

            let should_flush = {
                let buffer = self.buffer.read().await;
                let last_flush = self.last_flush.read().await;

                !buffer.is_empty() && last_flush.elapsed() >= self.config.flush_interval
            };

            if should_flush {
                let batches = {
                    let mut buffer = self.buffer.write().await;
                    buffer.take()
                };

                if let Err(e) = self.flush_batches(batches).await {
                    error!("Flush timer failed: {}", e);
                }
            }
        }
    }

    /// Generate a unique path for the Parquet file
    fn generate_path(&self) -> String {
        let now = chrono::Utc::now();
        let uuid = uuid::Uuid::new_v4();

        format!(
            "{}/data/year={}/month={:02}/day={:02}/hour={:02}/chunk_{}.parquet",
            self.storage_config.tenant_id,
            now.format("%Y"),
            now.format("%m"),
            now.format("%d"),
            now.format("%H"),
            uuid
        )
    }

    /// Extract minimum timestamp from batch
    fn extract_min_timestamp(&self, batch: &RecordBatch) -> Result<i64> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::TimestampNanosecondType;

        let col = batch
            .column_by_name("timestamp")
            .ok_or_else(|| Error::InvalidSchema("Missing timestamp column".into()))?;

        let ts_array = col.as_primitive::<TimestampNanosecondType>();
        let min = arrow::compute::min(ts_array).unwrap_or(0);
        Ok(min)
    }

    /// Extract maximum timestamp from batch
    fn extract_max_timestamp(&self, batch: &RecordBatch) -> Result<i64> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::TimestampNanosecondType;

        let col = batch
            .column_by_name("timestamp")
            .ok_or_else(|| Error::InvalidSchema("Missing timestamp column".into()))?;

        let ts_array = col.as_primitive::<TimestampNanosecondType>();
        let max = arrow::compute::max(ts_array).unwrap_or(0);
        Ok(max)
    }

    /// Subscribe to broadcast channel for streaming queries
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<RecordBatch> {
        self.broadcast.subscribe()
    }

    /// Get current buffer stats
    pub async fn buffer_stats(&self) -> BufferStats {
        let buffer = self.buffer.read().await;
        BufferStats {
            row_count: buffer.row_count(),
            size_bytes: buffer.size_bytes(),
            batch_count: buffer.batch_count(),
        }
    }
}

/// Chunk metadata for registration in metadata store
#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    pub path: String,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub row_count: u64,
    pub size_bytes: u64,
}

/// Buffer statistics
#[derive(Debug, Clone)]
pub struct BufferStats {
    pub row_count: usize,
    pub size_bytes: usize,
    pub batch_count: usize,
}
