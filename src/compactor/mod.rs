//! Compactor for merging and optimizing Parquet files
//!
//! The compactor is responsible for:
//! - Merging small files into larger ones (reduce S3 GET costs)
//! - Leveled compaction (L0 → L1 → L2 → L3)
//! - Applying retention policies
//! - Garbage collection with grace periods
//! - Backpressure signaling to ingesters
//! - **Orchestrating hot shard splits**

mod levels;
mod merge;

pub use levels::Level;
pub use merge::ChunkMerger;

use crate::clock::BoundedClock;
use crate::ingester::ParquetWriter;
use crate::metadata::{CompactionJob, CompactionStatus, MetadataClient, TimeRange};
use crate::sharding::{ShardAction, ShardMonitor, ShardSplitter};
use crate::{Result, StorageConfig};

use object_store::ObjectStore;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// A pending chunk deletion, serializable so it survives compactor restarts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PendingDeletion {
    path: String,
    scheduled_at: chrono::DateTime<chrono::Utc>,
}

/// Compactor configuration
#[derive(Debug, Clone)]
pub struct CompactorConfig {
    /// L0 merge threshold (number of files)
    pub l0_merge_threshold: usize,
    /// L0 target size in bytes
    pub l0_target_size: usize,
    /// L1 target size in bytes
    pub l1_target_size: usize,
    /// L2 target size in bytes
    pub l2_target_size: usize,
    /// Maximum compaction levels
    pub max_levels: usize,
    /// Data retention in days
    pub retention_days: u32,
    /// Downsample after N days
    pub downsample_after_days: u32,
    /// Downsample resolution
    pub downsample_resolution: Duration,
    /// Compaction check interval
    pub check_interval: Duration,
    /// Grace period before deleting old chunks
    pub gc_grace_period: Duration,
    /// Enable automatic sharding
    pub sharding_enabled: bool,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            l0_merge_threshold: 15,                  // 15 files (~75 minutes of data)
            l0_target_size: 250 * 1024 * 1024,       // 250 MB
            l1_target_size: 2 * 1024 * 1024 * 1024,  // 2 GB
            l2_target_size: 10 * 1024 * 1024 * 1024, // 10 GB
            max_levels: 4,
            retention_days: 90,
            downsample_after_days: 7,
            downsample_resolution: Duration::from_secs(60), // 1 minute
            check_interval: Duration::from_secs(60),        // Check every minute
            gc_grace_period: Duration::from_secs(60),       // 60 seconds
            sharding_enabled: true,
        }
    }
}

/// Backpressure state for external monitoring
#[derive(Debug, Clone)]
pub struct CompactionBackpressure {
    /// Number of L0 files pending compaction
    pub l0_pending_files: u64,
    /// Whether compaction is falling behind
    pub is_behind: bool,
    /// Recommended delay for new writes (0 = no delay)
    pub recommended_delay_ms: u64,
}

/// Compactor service
pub struct Compactor {
    config: CompactorConfig,
    object_store: Arc<dyn ObjectStore>,
    metadata: Arc<dyn MetadataClient>,
    merger: ChunkMerger,
    parquet_writer: ParquetWriter,
    storage_config: StorageConfig,
    /// Number of concurrent compactions currently running
    active_compactions: AtomicU64,
    /// Maximum concurrent compactions allowed
    max_concurrent_compactions: u64,
    /// Number of L0 files pending compaction
    l0_pending_count: AtomicU64,
    /// Backpressure threshold (if L0 pending > this, signal backpressure)
    backpressure_threshold: u64,
    /// Flag indicating compaction is falling behind
    is_behind: AtomicBool,
    /// Chunks pending deletion, persisted to S3 to survive restarts
    pending_deletions: std::sync::RwLock<Vec<PendingDeletion>>,
    /// Shard monitor for detecting hot shards
    shard_monitor: Arc<ShardMonitor>,
    /// Shard splitter for executing splits
    shard_splitter: Arc<ShardSplitter>,
    /// Cancellation token for graceful shutdown
    shutdown: CancellationToken,
    /// Bounded clock for skew-safe timestamp operations
    clock: Arc<BoundedClock>,
}

impl Compactor {
    /// Create a new compactor
    pub fn new(
        config: CompactorConfig,
        object_store: Arc<dyn ObjectStore>,
        metadata: Arc<dyn MetadataClient>,
        storage_config: StorageConfig,
        // Pass in the ingester's shard monitor to get write metrics
        shard_monitor: Arc<ShardMonitor>,
    ) -> Self {
        // Backpressure threshold: 3x the merge threshold
        let backpressure_threshold = (config.l0_merge_threshold * 3) as u64;

        let shard_splitter = Arc::new(ShardSplitter::new(
            Arc::clone(&metadata),
            Arc::clone(&object_store),
        ));

        Self {
            config,
            object_store: object_store.clone(),
            metadata,
            merger: ChunkMerger::new(object_store),
            parquet_writer: ParquetWriter::new(),
            storage_config,
            active_compactions: AtomicU64::new(0),
            max_concurrent_compactions: 4, // Allow 4 concurrent compactions
            shard_monitor,
            shard_splitter,
            l0_pending_count: AtomicU64::new(0),
            backpressure_threshold,
            is_behind: AtomicBool::new(false),
            pending_deletions: std::sync::RwLock::new(Vec::new()),
            shutdown: CancellationToken::new(),
            clock: Arc::new(BoundedClock::default()),
        }
    }

    /// Get a cancellation token that can be used to trigger graceful shutdown.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Get current backpressure state (for ingesters to check)
    pub fn backpressure(&self) -> CompactionBackpressure {
        let l0_pending = self.l0_pending_count.load(Ordering::Relaxed);
        let is_behind = self.is_behind.load(Ordering::Relaxed);

        // Calculate recommended delay based on how far behind we are
        let recommended_delay_ms = if l0_pending > self.backpressure_threshold * 2 {
            100 // Significant backpressure: 100ms delay
        } else if l0_pending > self.backpressure_threshold {
            50 // Moderate backpressure: 50ms delay
        } else {
            0 // No backpressure
        };

        CompactionBackpressure {
            l0_pending_files: l0_pending,
            is_behind,
            recommended_delay_ms,
        }
    }

    /// Check if compaction has capacity for more work
    fn has_capacity(&self) -> bool {
        self.active_compactions.load(Ordering::Relaxed) < self.max_concurrent_compactions
    }

    /// Run the main service loop. Returns when the shutdown token is cancelled.
    pub async fn run(&self) {
        // Recover pending deletions from a previous run
        if let Err(e) = self.load_pending_deletions().await {
            warn!("Failed to load persisted pending deletions: {}", e);
        }

        let mut interval = tokio::time::interval(self.config.check_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.run_cycle().await {
                        error!("Main service cycle failed: {}", e);
                    }
                }
                _ = self.shutdown.cancelled() => {
                    info!("Compactor shutting down gracefully");
                    break;
                }
            }
        }
    }

    /// Run a single service cycle (compaction, sharding, etc.)
    pub async fn run_cycle(&self) -> Result<()> {
        self.run_compaction_cycle().await?;

        if self.config.sharding_enabled {
            self.run_sharding_cycle().await?;
        }

        Ok(())
    }

    /// Run a single sharding cycle
    async fn run_sharding_cycle(&self) -> Result<()> {
        info!("Starting sharding cycle: evaluating hot shards");
        let actions = self.shard_monitor.evaluate_shards();
        if actions.is_empty() {
            info!("No hot shards detected");
            return Ok(());
        }

        for action in actions {
            match action {
                ShardAction::Split(shard_id) => {
                    info!(
                        "Hot shard detected: {}. Attempting to initiate split.",
                        shard_id
                    );
                    let metadata_client = Arc::clone(&self.metadata);
                    let splitter = Arc::clone(&self.shard_splitter);

                    // Spawn the split process in the background so it doesn't block the main loop
                    tokio::spawn(async move {
                        let shard_metadata =
                            match metadata_client.get_shard_metadata(&shard_id).await {
                                Ok(Some(meta)) => meta,
                                Ok(None) => {
                                    error!("Cannot split shard {}: metadata not found.", shard_id);
                                    return;
                                }
                                Err(e) => {
                                    error!(
                                        "Cannot split shard {}: failed to get metadata: {}",
                                        shard_id, e
                                    );
                                    return;
                                }
                            };

                        // Ensure we don't try to split a shard that's already splitting
                        if !shard_metadata.is_active() {
                            info!(
                                "Skipping split for shard {}: already in non-active state ({:?}).",
                                shard_id, shard_metadata.state
                            );
                            return;
                        }

                        if let Err(e) = splitter
                            .execute_split_with_monitoring(&shard_metadata)
                            .await
                        {
                            error!("Failed to execute split for shard {}: {}", shard_id, e);
                        }
                    });
                }
                ShardAction::TransferLease(shard_id, target_node) => {
                    info!(
                        "Lease transfer requested for shard {} to node {}",
                        shard_id, target_node
                    );
                    // TODO: Implement lease transfer when cluster mode is enabled
                }
                ShardAction::MoveReplica(shard_id, from_node, to_node) => {
                    info!(
                        "Replica move requested for shard {} from {} to {}",
                        shard_id, from_node, to_node
                    );
                    // TODO: Implement replica movement when cluster mode is enabled
                }
            }
        }
        Ok(())
    }

    /// Run a single compaction cycle
    pub async fn run_compaction_cycle(&self) -> Result<()> {
        // Update L0 pending count for backpressure calculation
        self.update_l0_pending_count().await?;

        // Level 0: Size-tiered compaction for recent data
        self.compact_l0().await?;

        // Level 1+: Leveled compaction for older data
        for level in 1..=self.config.max_levels {
            // Check capacity before starting more work
            if !self.has_capacity() {
                debug!("Compaction at capacity, skipping higher levels");
                break;
            }
            self.compact_level(level).await?;
        }

        // Garbage collection - process pending deletions
        self.garbage_collect().await?;

        // Enforce retention policy
        self.enforce_retention().await?;

        // Clean up completed/failed jobs older than 1 hour
        if let Err(e) = self.metadata.cleanup_completed_jobs(3600).await {
            warn!("Failed to clean up compaction jobs: {}", e);
        }

        // Update backpressure state
        self.update_backpressure_state().await?;

        // Persist pending deletions to survive restarts
        if let Err(e) = self.persist_pending_deletions().await {
            warn!("Failed to persist pending deletions: {}", e);
        }

        Ok(())
    }

    /// Update the count of L0 files pending compaction
    async fn update_l0_pending_count(&self) -> Result<()> {
        let candidates = self.metadata.get_l0_candidates(1).await?;
        let total_files: usize = candidates.iter().map(|g| g.len()).sum();
        self.l0_pending_count
            .store(total_files as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Update backpressure state based on current conditions
    async fn update_backpressure_state(&self) -> Result<()> {
        let l0_pending = self.l0_pending_count.load(Ordering::Relaxed);
        let is_behind = l0_pending > self.backpressure_threshold;
        self.is_behind.store(is_behind, Ordering::Relaxed);

        if is_behind {
            warn!(
                l0_pending = l0_pending,
                threshold = self.backpressure_threshold,
                "Compaction falling behind, signaling backpressure"
            );
        }

        Ok(())
    }

    /// Compact L0 (size-tiered compaction)
    async fn compact_l0(&self) -> Result<()> {
        let candidates = self
            .metadata
            .get_l0_candidates(self.config.l0_merge_threshold)
            .await?;

        for group in candidates {
            // Check capacity before starting compaction
            if !self.has_capacity() {
                debug!("Compaction at capacity, deferring remaining L0 groups");
                break;
            }

            // Track active compaction
            self.active_compactions.fetch_add(1, Ordering::Relaxed);

            info!(file_count = group.len(), "Starting L0 compaction");

            // Create compaction job
            let job = CompactionJob {
                id: uuid::Uuid::new_v4().to_string(),
                source_chunks: group.clone(),
                target_level: 0,
                status: CompactionStatus::InProgress,
                created_at: Some(chrono::Utc::now().timestamp()),
            };
            self.metadata.create_compaction_job(job.clone()).await?;

            // Merge chunks
            match self.merge_chunks(&group, Level::L0).await {
                Ok(target_path) => {
                    self.metadata
                        .complete_compaction(&group, &target_path)
                        .await?;
                    self.metadata
                        .update_compaction_status(&job.id, CompactionStatus::Completed)
                        .await?;

                    // Schedule source chunks for deletion
                    for path in &group {
                        self.schedule_deletion(path);
                    }

                    info!(target = %target_path, "L0 compaction completed");
                }
                Err(e) => {
                    self.metadata
                        .update_compaction_status(&job.id, CompactionStatus::Failed)
                        .await?;
                    error!("L0 compaction failed: {}", e);
                }
            }

            // Track compaction completion
            self.active_compactions.fetch_sub(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Compact a specific level (leveled compaction)
    async fn compact_level(&self, level: usize) -> Result<()> {
        let target_size = self.target_size_for_level(level);
        let candidates = self
            .metadata
            .get_level_candidates(level, target_size)
            .await?;

        for group in candidates {
            if group.len() < 2 {
                continue;
            }

            // Check capacity before starting compaction
            if !self.has_capacity() {
                debug!(
                    level = level,
                    "Compaction at capacity, deferring remaining groups"
                );
                break;
            }

            // Track active compaction
            self.active_compactions.fetch_add(1, Ordering::Relaxed);

            info!(
                level = level,
                file_count = group.len(),
                "Starting level compaction"
            );

            let job = CompactionJob {
                id: uuid::Uuid::new_v4().to_string(),
                source_chunks: group.clone(),
                target_level: level as u32,
                status: CompactionStatus::InProgress,
                created_at: Some(chrono::Utc::now().timestamp()),
            };
            self.metadata.create_compaction_job(job.clone()).await?;

            match self.merge_chunks(&group, Level::L(level)).await {
                Ok(target_path) => {
                    self.metadata
                        .complete_compaction(&group, &target_path)
                        .await?;
                    self.metadata
                        .update_compaction_status(&job.id, CompactionStatus::Completed)
                        .await?;

                    // Schedule source chunks for deletion
                    for path in &group {
                        self.schedule_deletion(path);
                    }

                    info!(level = level, target = %target_path, "Level compaction completed");
                }
                Err(e) => {
                    self.metadata
                        .update_compaction_status(&job.id, CompactionStatus::Failed)
                        .await?;
                    error!(level = level, "Level compaction failed: {}", e);
                }
            }

            // Track compaction completion
            self.active_compactions.fetch_sub(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Merge a group of chunks into one
    async fn merge_chunks(&self, paths: &[String], level: Level) -> Result<String> {
        // Read and merge chunks
        let merged_batch = self.merger.merge(paths).await?;

        // Sort by timestamp and metric name for better compression
        let sorted = self.merger.sort_batch(&merged_batch)?;

        // Write merged Parquet
        let parquet_bytes = self.parquet_writer.write_batch(&sorted)?;

        // Generate target path
        let target_path = self.generate_compacted_path(level);

        // Upload to object storage
        self.object_store
            .put(&target_path.clone().into(), parquet_bytes.into())
            .await?;

        Ok(target_path)
    }

    /// Garbage collect old chunks with grace period
    async fn garbage_collect(&self) -> Result<()> {
        let now = chrono::Utc::now();
        let grace_period = chrono::Duration::from_std(self.config.gc_grace_period)
            .unwrap_or_else(|_| chrono::Duration::seconds(300));
        let cutoff = now - grace_period;

        // Process pending deletions that have passed grace period
        let chunks_to_delete: Vec<String> = {
            let pending = self.pending_deletions.read().unwrap();
            pending
                .iter()
                .filter(|entry| entry.scheduled_at <= cutoff)
                .map(|entry| entry.path.clone())
                .collect()
        };

        if chunks_to_delete.is_empty() {
            debug!("No chunks ready for garbage collection");
            return Ok(());
        }

        info!(count = chunks_to_delete.len(), "Garbage collecting chunks");

        // Delete from object storage
        for path in &chunks_to_delete {
            match self.object_store.delete(&path.clone().into()).await {
                Ok(_) => {
                    debug!(path = %path, "Deleted chunk from object storage");
                }
                Err(e) => {
                    // Log error but continue - chunk may already be deleted
                    warn!(path = %path, error = %e, "Failed to delete chunk");
                }
            }
        }

        // Remove from pending deletions
        {
            let mut pending = self.pending_deletions.write().unwrap();
            pending.retain(|entry| !chunks_to_delete.contains(&entry.path));
        }

        info!(
            deleted = chunks_to_delete.len(),
            "Garbage collection completed"
        );

        Ok(())
    }

    /// Schedule a chunk for deletion (called after compaction)
    pub fn schedule_deletion(&self, path: &str) {
        let mut pending = self.pending_deletions.write().unwrap();
        pending.push(PendingDeletion {
            path: path.to_string(),
            scheduled_at: chrono::Utc::now(),
        });
    }

    /// Enforce data retention policy
    async fn enforce_retention(&self) -> Result<()> {
        let retention_nanos = self.config.retention_days as i64 * 24 * 3600 * 1_000_000_000;
        let cutoff = self.clock.retention_cutoff_nanos(retention_nanos);

        // Find chunks older than retention period
        let old_chunks = self.metadata.get_chunks(TimeRange::new(0, cutoff)).await?;

        if old_chunks.is_empty() {
            return Ok(());
        }

        info!(
            count = old_chunks.len(),
            cutoff_days = self.config.retention_days,
            "Enforcing retention policy"
        );

        for chunk in old_chunks {
            // Delete from metadata
            self.metadata.delete_chunk(&chunk.chunk_path).await?;

            // Schedule for GC (with grace period)
            self.schedule_deletion(&chunk.chunk_path);
        }

        Ok(())
    }

    /// S3 path for persisted pending deletions
    fn pending_deletions_path(&self) -> object_store::path::Path {
        format!(
            "{}/metadata/pending-deletions.json",
            self.storage_config.tenant_id
        )
        .into()
    }

    /// Load pending deletions from S3, merging with any already in memory.
    async fn load_pending_deletions(&self) -> Result<()> {
        let path = self.pending_deletions_path();
        match self.object_store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let loaded: Vec<PendingDeletion> = serde_json::from_slice(&bytes)?;
                let mut pending = self.pending_deletions.write().unwrap();
                for entry in loaded {
                    if !pending.iter().any(|p| p.path == entry.path) {
                        pending.push(entry);
                    }
                }
                info!(count = pending.len(), "Loaded persisted pending deletions");
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist pending deletions to S3 so they survive restarts.
    async fn persist_pending_deletions(&self) -> Result<()> {
        let path = self.pending_deletions_path();
        let bytes = {
            let pending = self.pending_deletions.read().unwrap();
            serde_json::to_vec(&*pending)?
        };
        self.object_store.put(&path, bytes.into()).await?;
        Ok(())
    }

    /// Get target size for a level
    fn target_size_for_level(&self, level: usize) -> usize {
        match level {
            0 => self.config.l0_target_size,
            1 => self.config.l1_target_size,
            2 => self.config.l2_target_size,
            _ => self.config.l2_target_size * 5,
        }
    }

    /// Generate path for compacted file
    fn generate_compacted_path(&self, level: Level) -> String {
        let now = self.clock.now();
        let uuid = uuid::Uuid::new_v4();

        format!(
            "{}/data/compacted/level={}/year={}/month={:02}/chunk_{}.parquet",
            self.storage_config.tenant_id,
            level.as_u32(),
            now.format("%Y"),
            now.format("%m"),
            uuid
        )
    }
}
