//! Ingester module for CardinalSin
//!
//! The ingester is responsible for:
//! - Buffering incoming writes in memory
//! - Batching writes into Parquet files
//! - Flushing to object storage (S3)
//! - Broadcasting new data to streaming query subscribers
//! - Tracking shard metrics for hot shard detection

mod broadcast;
mod buffer;
mod parquet_writer;
mod telemetry;
mod topic_broadcast;
mod wal;

pub use broadcast::BroadcastChannel;
pub use buffer::WriteBuffer;
pub use parquet_writer::ParquetWriter;
pub use topic_broadcast::{
    BatchMetadata, FilteredReceiver, TopicBatch, TopicBroadcastChannel, TopicFilter,
};
pub use wal::{load_flushed_seq, persist_flushed_seq, WalConfig, WalSyncMode, WriteAheadLog};

use crate::clock::BoundedClock;
use crate::metadata::MetadataClient;
use crate::schema::MetricSchema;
use crate::sharding::{
    find_shard_for_hash, hash_metric_name, HotShardConfig, ShardId, ShardMetadata, ShardMonitor,
};
use crate::{Error, Result, StorageConfig};

use arrow_array::RecordBatch;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Initialize WAL from config, returning None if disabled.
/// WAL requires async initialization, so this always returns None.
/// Call `ensure_wal()` after construction to initialize the WAL.
fn init_wal(config: &WalConfig) -> Option<Mutex<WriteAheadLog>> {
    if !config.enabled {
        return None;
    }
    // WAL open is async and cannot be called from a sync context
    // (block_on panics inside a tokio runtime). The write path handles
    // None gracefully. Use ensure_wal() to initialize after construction.
    None
}

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
    /// WAL configuration
    pub wal: WalConfig,
    /// Number of initial shards to provision (default 16)
    pub initial_shard_count: u16,
}

impl Default for IngesterConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(300),  // 5 minutes
            flush_row_count: 1_000_000,                // 1M rows
            flush_size_bytes: 100 * 1024 * 1024,       // 100MB
            batch_timeout: Duration::from_millis(250), // 250ms micro-batch
            batch_size_bytes: 8 * 1024 * 1024,         // 8MB
            flush_parallelism: 4,
            max_buffer_size_bytes: 512 * 1024 * 1024, // 512MB
            wal: WalConfig::default(),
            initial_shard_count: crate::sharding::DEFAULT_INITIAL_SHARD_COUNT,
        }
    }
}

/// Ingester node for receiving and batching metric writes
pub struct Ingester {
    /// Configuration
    config: IngesterConfig,
    /// Per-shard write buffers (shard_id → buffer)
    buffers: Arc<RwLock<HashMap<ShardId, WriteBuffer>>>,
    /// Cached shard ranges for routing (loaded from metadata)
    shard_ranges: Arc<RwLock<Vec<ShardMetadata>>>,
    /// Object storage client
    object_store: Arc<dyn ObjectStore>,
    /// Metadata client
    metadata: Arc<dyn MetadataClient>,
    /// Parquet writer
    parquet_writer: ParquetWriter,
    /// Broadcast channel for streaming queries (deprecated - use topic_broadcast)
    broadcast: BroadcastChannel,
    /// Topic-based broadcast channel for filtered streaming
    topic_broadcast: TopicBroadcastChannel,
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
    /// Write-ahead log for durability
    wal: Option<Mutex<WriteAheadLog>>,
    /// Whether we've already warned about WAL not being initialized
    wal_warned: AtomicBool,
    /// Last WAL sequence number appended (high watermark for current buffer)
    last_wal_seq: AtomicU64,
    /// Last WAL sequence number that was successfully flushed to S3
    last_flushed_seq: AtomicU64,
    /// Cancellation token for graceful shutdown
    shutdown: CancellationToken,
    /// Bounded clock for skew-safe timestamp operations
    clock: Arc<BoundedClock>,
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
        let topic_broadcast = TopicBroadcastChannel::new(1024);

        // Initialize shard monitor with default config
        let shard_monitor = Arc::new(ShardMonitor::new(HotShardConfig::default()));
        let wal = init_wal(&config.wal);

        Self {
            config,
            buffers: Arc::new(RwLock::new(HashMap::new())),
            shard_ranges: Arc::new(RwLock::new(Vec::new())),
            object_store,
            metadata,
            parquet_writer: ParquetWriter::new(),
            broadcast,
            topic_broadcast,
            storage_config,
            schema,
            last_flush: Arc::new(RwLock::new(Instant::now())),
            shard_monitor,
            default_tenant_id: 0, // Single-tenant mode by default
            wal,
            wal_warned: AtomicBool::new(false),
            last_wal_seq: AtomicU64::new(0),
            last_flushed_seq: AtomicU64::new(0),
            shutdown: CancellationToken::new(),
            clock: Arc::new(BoundedClock::default()),
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
        let topic_broadcast = TopicBroadcastChannel::new(1024);
        let shard_monitor = Arc::new(ShardMonitor::new(shard_config));
        let wal = init_wal(&config.wal);

        Self {
            config,
            buffers: Arc::new(RwLock::new(HashMap::new())),
            shard_ranges: Arc::new(RwLock::new(Vec::new())),
            object_store,
            metadata,
            parquet_writer: ParquetWriter::new(),
            broadcast,
            topic_broadcast,
            storage_config,
            schema,
            last_flush: Arc::new(RwLock::new(Instant::now())),
            shard_monitor,
            default_tenant_id: tenant_id,
            wal,
            wal_warned: AtomicBool::new(false),
            last_wal_seq: AtomicU64::new(0),
            last_flushed_seq: AtomicU64::new(0),
            shutdown: CancellationToken::new(),
            clock: Arc::new(BoundedClock::default()),
        }
    }

    /// Get a cancellation token that can be used to trigger graceful shutdown.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Initialize WAL asynchronously and recover any unflushed entries.
    ///
    /// Call after construction in production. This will:
    /// 1. Open the WAL directory
    /// 2. Load the last flushed sequence number
    /// 3. Replay any WAL entries after the last flush into the buffer
    /// 4. Truncate already-flushed WAL segments
    pub async fn ensure_wal(&mut self) -> Result<()> {
        if self.config.wal.enabled && self.wal.is_none() {
            let flushed_seq = load_flushed_seq(&self.config.wal.wal_dir)?;
            self.last_flushed_seq.store(flushed_seq, Ordering::Release);

            let wal = WriteAheadLog::open(self.config.wal.clone()).await?;

            // Recover unflushed entries
            let entries = wal.read_entries_after(flushed_seq)?;
            if !entries.is_empty() {
                let mut replayed = 0u64;
                let mut max_seq = flushed_seq;
                // During WAL recovery, all entries go to shard 0 (default).
                // Shard ranges may not be loaded yet; proper routing happens on new writes.
                let recovery_shard: ShardId = 0;
                for entry in &entries {
                    match entry.batches() {
                        Ok(batches) => {
                            for batch in batches {
                                let mut pending_batch = Some(batch);
                                loop {
                                    let mut buffers = self.buffers.write().await;
                                    let buffer = buffers.entry(recovery_shard).or_default();
                                    let incoming = pending_batch.as_ref().ok_or_else(|| {
                                        Error::Internal("Missing pending batch".to_string())
                                    })?;

                                    // Keep recovery buffer schema-homogeneous so future flushes do not fail.
                                    if !buffer.schema_compatible(incoming) {
                                        let existing = buffer.take();
                                        drop(buffers);
                                        self.last_wal_seq.store(max_seq, Ordering::Release);
                                        self.flush_shard_batches(recovery_shard, existing).await?;
                                        continue;
                                    }

                                    let incoming = pending_batch.take().ok_or_else(|| {
                                        Error::Internal("Missing pending batch".to_string())
                                    })?;
                                    buffer.append(incoming)?;
                                    break;
                                }
                                replayed += 1;
                            }
                            if entry.seq > max_seq {
                                max_seq = entry.seq;
                            }
                        }
                        Err(e) => {
                            warn!(seq = entry.seq, error = %e, "Skipping corrupt WAL entry during recovery");
                        }
                    }
                }
                self.last_wal_seq.store(max_seq, Ordering::Release);
                let buffer_rows = {
                    let buffers = self.buffers.read().await;
                    buffers.values().map(|b| b.row_count()).sum::<usize>()
                };
                info!(
                    replayed_entries = replayed,
                    buffer_rows,
                    wal_seq_range = format!("({}, {}]", flushed_seq, max_seq),
                    "WAL recovery complete"
                );
            }

            // Truncate already-flushed segments
            let mut wal = wal;
            if flushed_seq > 0 {
                wal.truncate_before(flushed_seq + 1).await?;
            }

            self.wal = Some(Mutex::new(wal));
            info!("WAL initialized at {:?}", self.config.wal.wal_dir);
        }
        Ok(())
    }

    /// Write metrics to the buffer.
    ///
    /// Ack semantics: this method returns (acks) after the WAL append completes
    /// (buffered file write), NOT after fsync. The data loss window equals the
    /// WAL sync interval (default 100ms). This matches InfluxDB 3 and Prometheus
    /// behavior — fsync runs asynchronously on the configured interval.
    pub async fn write(&self, batch: RecordBatch) -> Result<()> {
        let start_time = std::time::Instant::now();
        let row_count = batch.num_rows() as u64;

        let result = async {
            // WAL append (whole batch, before partitioning)
            if let Some(wal) = self.wal.as_ref() {
                let seq = match wal.lock().await.append(&batch).await {
                    Ok(seq) => {
                        telemetry::record_wal_operation("append", "ok");
                        seq
                    }
                    Err(e) => {
                        telemetry::record_wal_operation("append", "error");
                        return Err(e);
                    }
                };
                self.last_wal_seq.store(seq, Ordering::Release);
            } else if self.config.wal.enabled {
                telemetry::record_wal_operation("append", "error");
                if !self.wal_warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        "WAL is enabled but not initialized - call ensure_wal() for crash durability"
                    );
                }
            }

            // Partition batch by shard and append to per-shard buffers
            let ranges = self.shard_ranges.read().await;
            let partitioned = self.partition_batch_by_shard(&batch, &ranges)?;
            drop(ranges);

            for (shard_id, shard_batch) in partitioned {
                let batch_size = shard_batch.get_array_memory_size();

                // Check for active split on this shard
                if let Some(split_state) =
                    self.metadata.get_split_state(&shard_id.to_string()).await?
                {
                    use crate::sharding::SplitPhase;
                    if matches!(
                        split_state.phase,
                        SplitPhase::DualWrite | SplitPhase::Backfill
                    ) {
                        info!("Shard {} is splitting, enabling dual-write mode", shard_id);
                        self.write_with_split_awareness(shard_batch, shard_id)
                            .await?;
                        continue;
                    }
                }

                self.append_to_shard_buffer(shard_id, shard_batch, batch_size)
                    .await?;

                // Record write metrics for hot shard detection
                let write_latency = start_time.elapsed();
                self.shard_monitor
                    .record_write(&shard_id, batch_size, write_latency);
            }

            Ok(())
        }
        .await;

        let result_label = if result.is_ok() { "ok" } else { "error" };
        telemetry::record_write(row_count, start_time.elapsed().as_secs_f64(), result_label);
        result
    }

    /// Write with split awareness (dual-write to old and new shards).
    /// WAL has already been appended by the caller.
    async fn write_with_split_awareness(
        &self,
        batch: RecordBatch,
        shard_id: ShardId,
    ) -> Result<()> {
        let split_state = self
            .metadata
            .get_split_state(&shard_id.to_string())
            .await?
            .ok_or_else(|| Error::Internal("Split state disappeared".to_string()))?;

        // Write to old shard first (for consistency during transition)
        let batch_size = batch.get_array_memory_size();
        self.append_to_shard_buffer(shard_id, batch.clone(), batch_size)
            .await?;

        // Split batch by key range and write to new shards
        let (batch_a, batch_b) = self.split_batch_by_key(&batch, &split_state.split_point)?;

        if batch_a.num_rows() > 0 {
            info!(
                "Dual-write: {} rows to shard {}",
                batch_a.num_rows(),
                split_state.new_shards[0]
            );
            self.write_to_shard(&batch_a, &split_state.new_shards[0])
                .await?;
        }

        if batch_b.num_rows() > 0 {
            info!(
                "Dual-write: {} rows to shard {}",
                batch_b.num_rows(),
                split_state.new_shards[1]
            );
            self.write_to_shard(&batch_b, &split_state.new_shards[1])
                .await?;
        }

        Ok(())
    }

    /// Write a batch directly to a specific shard (bypass buffer)
    async fn write_to_shard(&self, batch: &RecordBatch, shard_id: &str) -> Result<()> {
        // Convert batch to Parquet
        let parquet_bytes = self.parquet_writer.write_batch(batch)?;
        let parquet_size = parquet_bytes.len() as u64;

        // Generate path for this shard
        let now = self.clock.now();
        let uuid = uuid::Uuid::new_v4();
        let path = format!(
            "{}/data/shard={}/year={}/month={:02}/day={:02}/hour={:02}/chunk_{}.parquet",
            self.storage_config.tenant_id,
            shard_id,
            now.format("%Y"),
            now.format("%m"),
            now.format("%d"),
            now.format("%H"),
            uuid
        );

        // Upload to object storage
        self.object_store
            .put(&path.clone().into(), parquet_bytes.into())
            .await?;

        // Register in metadata store
        let chunk_metadata = ChunkMetadata {
            path: path.clone(),
            min_timestamp: self.extract_min_timestamp(batch)?,
            max_timestamp: self.extract_max_timestamp(batch)?,
            row_count: batch.num_rows() as u64,
            size_bytes: parquet_size,
            shard_id: 0,
        };
        self.metadata.register_chunk(&path, &chunk_metadata).await?;

        Ok(())
    }

    /// Split a batch by metric hash against the split point (u32 hash midpoint).
    ///
    /// Rows whose `hash_metric_name(metric_name)` falls below the split point
    /// go to batch_a (left child), the rest to batch_b (right child).
    fn split_batch_by_key(
        &self,
        batch: &RecordBatch,
        split_point: &[u8],
    ) -> Result<(RecordBatch, RecordBatch)> {
        use arrow_array::cast::AsArray;

        let split_hash =
            u32::from_be_bytes(split_point.try_into().map_err(|_| {
                Error::Internal("Invalid split point (expected 4 bytes)".to_string())
            })?);

        let metric_col = batch
            .column_by_name("metric_name")
            .and_then(|c| c.as_string_opt::<i32>());

        let mut indices_a = Vec::new();
        let mut indices_b = Vec::new();

        for i in 0..batch.num_rows() {
            let metric_name = metric_col.map(|c| c.value(i)).unwrap_or("unknown");
            let hash = hash_metric_name(metric_name) as u32;
            if hash < split_hash {
                indices_a.push(i as u32);
            } else {
                indices_b.push(i as u32);
            }
        }

        let indices_a_array = arrow::array::UInt32Array::from(indices_a);
        let indices_b_array = arrow::array::UInt32Array::from(indices_b);

        let batch_a = arrow::compute::take_record_batch(batch, &indices_a_array)?;
        let batch_b = arrow::compute::take_record_batch(batch, &indices_b_array)?;

        Ok((batch_a, batch_b))
    }

    /// Partition a batch into per-shard sub-batches using metric hash routing.
    /// Sync, CPU-only — no metadata I/O.
    fn partition_batch_by_shard(
        &self,
        batch: &RecordBatch,
        ranges: &[ShardMetadata],
    ) -> Result<HashMap<ShardId, RecordBatch>> {
        // If no shard ranges loaded, everything goes to shard 0
        if ranges.is_empty() {
            let mut result = HashMap::new();
            result.insert(0, batch.clone());
            return Ok(result);
        }

        use arrow_array::cast::AsArray;
        let metric_col = batch
            .column_by_name("metric_name")
            .and_then(|c| c.as_string_opt::<i32>());

        let mut indices_by_shard: HashMap<ShardId, Vec<u32>> = HashMap::new();
        for i in 0..batch.num_rows() {
            let metric_name = metric_col.map(|c| c.value(i)).unwrap_or("unknown");
            let hash = hash_metric_name(metric_name);
            let shard_id = find_shard_for_hash(hash, ranges).unwrap_or(0);
            indices_by_shard.entry(shard_id).or_default().push(i as u32);
        }

        let mut result = HashMap::with_capacity(indices_by_shard.len());
        for (shard_id, indices) in indices_by_shard {
            let indices_array = arrow::array::UInt32Array::from(indices);
            result.insert(
                shard_id,
                arrow::compute::take_record_batch(batch, &indices_array)?,
            );
        }
        Ok(result)
    }

    /// Refresh shard ranges from metadata. Call after split events.
    pub async fn refresh_shard_ranges(&self) -> Result<()> {
        let shards = self.metadata.list_shards().await?;
        let mut ranges = self.shard_ranges.write().await;
        *ranges = shards;
        info!(shard_count = ranges.len(), "Refreshed shard ranges");
        Ok(())
    }

    /// Write a pre-partitioned batch directly to a specific shard's buffer.
    /// Used by future IngestDispatcher for distributed routing.
    pub async fn write_pre_partitioned(&self, shard_id: ShardId, batch: RecordBatch) -> Result<()> {
        let batch_size = batch.get_array_memory_size();
        self.append_to_shard_buffer(shard_id, batch, batch_size)
            .await
    }

    /// Extract all unique metric names from a batch for topic routing
    fn extract_metrics(&self, batch: &RecordBatch) -> Vec<String> {
        use arrow_array::cast::AsArray;

        if let Some(col) = batch.column_by_name("metric_name") {
            use arrow_array::Array;
            if let Some(arr) = col.as_string_opt::<i32>() {
                let mut metrics = std::collections::HashSet::new();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        metrics.insert(arr.value(i).to_string());
                    }
                }
                return metrics.into_iter().collect();
            }
        }

        vec!["unknown".to_string()]
    }

    /// Get the shard monitor for external monitoring
    pub fn shard_monitor(&self) -> &Arc<ShardMonitor> {
        &self.shard_monitor
    }

    /// Evaluate shards and get recommended actions (for external orchestration)
    pub fn evaluate_shards(&self) -> Vec<crate::sharding::ShardAction> {
        self.shard_monitor.evaluate_shards()
    }

    /// Get a reference to the topic broadcast channel for filtered streaming subscriptions
    pub fn topic_broadcast(&self) -> &TopicBroadcastChannel {
        &self.topic_broadcast
    }

    /// Subscribe to filtered streaming with a topic filter
    pub async fn subscribe_filtered(&self, filter: TopicFilter) -> FilteredReceiver {
        self.topic_broadcast.subscribe(filter).await
    }

    /// Check if flush is needed based on thresholds for a single shard buffer
    fn should_flush(&self, buffer: &WriteBuffer) -> bool {
        buffer.row_count() >= self.config.flush_row_count
            || buffer.size_bytes() >= self.config.flush_size_bytes
    }

    /// Compute aggregate buffer size across all shards
    fn aggregate_buffer_size(buffers: &HashMap<ShardId, WriteBuffer>) -> usize {
        buffers.values().map(|b| b.size_bytes()).sum()
    }

    /// Append a batch to a specific shard's buffer, flushing if needed.
    async fn append_to_shard_buffer(
        &self,
        shard_id: ShardId,
        batch: RecordBatch,
        batch_size: usize,
    ) -> Result<()> {
        let mut pending_batch = Some(batch);

        loop {
            let mut buffers = self.buffers.write().await;

            let incoming = pending_batch
                .as_ref()
                .ok_or_else(|| Error::Internal("Missing pending batch".to_string()))?;

            // Ensure shard buffer exists
            if !buffers.contains_key(&shard_id) {
                buffers.insert(shard_id, WriteBuffer::new());
            }

            // Check schema compatibility
            let schema_ok = buffers.get(&shard_id).unwrap().schema_compatible(incoming);
            if !schema_ok {
                let existing = buffers.get_mut(&shard_id).unwrap().take();
                drop(buffers);
                self.flush_shard_batches(shard_id, existing).await?;
                continue;
            }

            // Aggregate buffer fullness check across all shards
            let total_size = Self::aggregate_buffer_size(&buffers);
            if total_size + batch_size > self.config.max_buffer_size_bytes {
                telemetry::record_buffer_fullness_ratio(1.0);
                return Err(Error::BufferFull);
            }

            let incoming = pending_batch
                .take()
                .ok_or_else(|| Error::Internal("Missing pending batch".to_string()))?;
            {
                let buffer = buffers.get_mut(&shard_id).unwrap();
                buffer.append(incoming)?;
            }

            let max_buffer_size = self.config.max_buffer_size_bytes.max(1) as f64;
            let new_total = Self::aggregate_buffer_size(&buffers);
            telemetry::record_buffer_fullness_ratio(new_total as f64 / max_buffer_size);

            let needs_flush = self.should_flush(buffers.get(&shard_id).unwrap());
            if needs_flush {
                let batches = buffers.get_mut(&shard_id).unwrap().take();
                drop(buffers);
                self.flush_shard_batches(shard_id, batches).await?;
            }

            return Ok(());
        }
    }

    /// Flush batches for a specific shard to object storage
    async fn flush_shard_batches(
        &self,
        shard_id: ShardId,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        info!(
            shard_id,
            batch_count = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Flushing shard batches to object storage"
        );

        // Concatenate batches
        let combined = arrow::compute::concat_batches(&batches[0].schema(), batches.iter())?;

        // Convert to Parquet
        let parquet_bytes = self.parquet_writer.write_batch(&combined)?;
        let parquet_size = parquet_bytes.len() as u64;

        // Generate shard-qualified path
        let path = self.generate_path(shard_id);
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
            shard_id,
        };
        self.metadata.register_chunk(&path, &chunk_metadata).await?;

        // Broadcast to streaming query subscribers (legacy)
        if let Err(e) = self.broadcast.send(combined.clone()) {
            debug!("No streaming subscribers (legacy): {}", e);
        }

        // Broadcast to topic-aware streaming query subscribers
        let metrics = self.extract_metrics(&combined);
        let topic_batch = TopicBatch {
            batch: combined,
            metadata: BatchMetadata {
                shard_id: shard_id.to_string(),
                tenant_id: self.default_tenant_id,
                metrics,
            },
        };
        if let Err(e) = self.topic_broadcast.send(topic_batch) {
            debug!("No topic broadcast subscribers: {}", e);
        }

        // WAL truncation: only truncate when ALL shard buffers are empty
        let all_empty = {
            let buffers = self.buffers.read().await;
            buffers.values().all(|b| b.is_empty())
        };
        let flushed_up_to = self.last_wal_seq.load(Ordering::Acquire);
        if all_empty && flushed_up_to > 0 {
            if let Some(wal) = self.wal.as_ref() {
                if let Err(e) = wal.lock().await.truncate_before(flushed_up_to).await {
                    telemetry::record_wal_operation("truncate", "error");
                    return Err(e);
                }
                telemetry::record_wal_operation("truncate", "ok");
            }
            self.last_flushed_seq
                .store(flushed_up_to, Ordering::Release);
            if let Err(e) = persist_flushed_seq(&self.config.wal.wal_dir, flushed_up_to) {
                telemetry::record_wal_operation("persist_flushed_seq", "error");
                warn!(error = %e, "Failed to persist flushed WAL sequence number");
            } else {
                telemetry::record_wal_operation("persist_flushed_seq", "ok");
            }
        }

        // Update last flush time
        *self.last_flush.write().await = Instant::now();

        Ok(())
    }

    /// Background flush timer - ensures data is flushed even during low traffic.
    /// Returns when the shutdown token is cancelled, after flushing any remaining data.
    pub async fn run_flush_timer(&self) {
        let mut interval = tokio::time::interval(self.config.flush_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let should_flush = {
                        let buffers = self.buffers.read().await;
                        let last_flush = self.last_flush.read().await;
                        let any_non_empty = buffers.values().any(|b| !b.is_empty());
                        any_non_empty && last_flush.elapsed() >= self.config.flush_interval
                    };

                    if should_flush {
                        self.flush_all_shard_buffers().await;
                    }
                }
                _ = self.shutdown.cancelled() => {
                    info!("Flush timer shutting down, flushing remaining data");
                    self.flush_all_shard_buffers().await;
                    break;
                }
            }
        }
    }

    /// Flush all non-empty shard buffers sequentially.
    async fn flush_all_shard_buffers(&self) {
        // Collect shard IDs and their batches under a single lock
        let to_flush: Vec<(ShardId, Vec<RecordBatch>)> = {
            let mut buffers = self.buffers.write().await;
            buffers
                .iter_mut()
                .filter(|(_, buf)| !buf.is_empty())
                .map(|(&shard_id, buf)| (shard_id, buf.take()))
                .collect()
        };

        for (shard_id, batches) in to_flush {
            if let Err(e) = self.flush_shard_batches(shard_id, batches).await {
                error!(shard_id, "Flush timer failed for shard: {}", e);
            }
        }
    }

    /// Generate a unique shard-qualified path for the Parquet file
    fn generate_path(&self, shard_id: ShardId) -> String {
        let now = self.clock.now();
        let uuid = uuid::Uuid::new_v4();

        format!(
            "{}/data/shard={}/year={}/month={:02}/day={:02}/hour={:02}/chunk_{}.parquet",
            self.storage_config.tenant_id,
            shard_id,
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
        use arrow_array::types::{Int64Type, TimestampNanosecondType};

        let col = batch
            .column_by_name("timestamp")
            .ok_or_else(|| Error::InvalidSchema("Missing timestamp column".into()))?;

        if let Some(ts_array) = col.as_primitive_opt::<TimestampNanosecondType>() {
            return Ok(arrow::compute::min(ts_array).unwrap_or(0));
        }
        if let Some(ts_array) = col.as_primitive_opt::<Int64Type>() {
            return Ok(arrow::compute::min(ts_array).unwrap_or(0));
        }

        Err(Error::InvalidSchema(format!(
            "Timestamp column must be Timestamp(Nanosecond) or Int64, got {:?}",
            col.data_type()
        )))
    }

    /// Extract maximum timestamp from batch
    fn extract_max_timestamp(&self, batch: &RecordBatch) -> Result<i64> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Int64Type, TimestampNanosecondType};

        let col = batch
            .column_by_name("timestamp")
            .ok_or_else(|| Error::InvalidSchema("Missing timestamp column".into()))?;

        if let Some(ts_array) = col.as_primitive_opt::<TimestampNanosecondType>() {
            return Ok(arrow::compute::max(ts_array).unwrap_or(0));
        }
        if let Some(ts_array) = col.as_primitive_opt::<Int64Type>() {
            return Ok(arrow::compute::max(ts_array).unwrap_or(0));
        }

        Err(Error::InvalidSchema(format!(
            "Timestamp column must be Timestamp(Nanosecond) or Int64, got {:?}",
            col.data_type()
        )))
    }

    /// Subscribe to broadcast channel for streaming queries
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<RecordBatch> {
        self.broadcast.subscribe()
    }

    /// Get current buffer stats (aggregated across all shard buffers)
    pub async fn buffer_stats(&self) -> BufferStats {
        let buffers = self.buffers.read().await;
        BufferStats {
            row_count: buffers.values().map(|b| b.row_count()).sum(),
            size_bytes: buffers.values().map(|b| b.size_bytes()).sum(),
            batch_count: buffers.values().map(|b| b.batch_count()).sum(),
        }
    }
}

/// Chunk metadata for registration in metadata store
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkMetadata {
    pub path: String,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub row_count: u64,
    pub size_bytes: u64,
    #[serde(default)]
    pub shard_id: crate::sharding::ShardId,
}

/// Buffer statistics
#[derive(Debug, Clone)]
pub struct BufferStats {
    pub row_count: usize,
    pub size_bytes: usize,
    pub batch_count: usize,
}
