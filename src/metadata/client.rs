//! Metadata client trait

use super::{CompactionJob, CompactionStatus, TimeIndexEntry, TimeRange};
use crate::ingester::ChunkMetadata;
use crate::sharding::SplitPhase;
use crate::Result;
use async_trait::async_trait;

/// Shard split state
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SplitState {
    pub phase: SplitPhase,
    pub old_shard: String,
    pub new_shards: Vec<String>,
    pub split_point: Vec<u8>,
    pub split_timestamp: i64,
    pub backfill_progress: f64, // 0.0 to 1.0
}

/// Metadata client interface
///
/// This trait abstracts the metadata storage backend, allowing for
/// different implementations (local SQLite for dev, FoundationDB for prod).
#[async_trait]
pub trait MetadataClient: Send + Sync {
    /// Register a new chunk in the metadata store
    async fn register_chunk(&self, path: &str, metadata: &ChunkMetadata) -> Result<()>;

    /// Get chunks that overlap with the given time range
    async fn get_chunks(&self, range: TimeRange) -> Result<Vec<TimeIndexEntry>>;

    /// Get chunks that overlap with the given time range and satisfy column predicates
    ///
    /// This method enables predicate pushdown to the metadata layer, dramatically
    /// reducing S3 read costs by pruning chunks before DataFusion sees them.
    ///
    /// Example: For query `WHERE metric_name = 'cpu' AND timestamp > ...`,
    /// this will only return chunks where column_stats show metric_name could contain 'cpu'.
    async fn get_chunks_with_predicates(
        &self,
        range: TimeRange,
        _predicates: &[super::predicates::ColumnPredicate],
    ) -> Result<Vec<TimeIndexEntry>> {
        // Default implementation: fallback to time-only filtering
        // (for backwards compatibility with LocalMetadataClient)
        self.get_chunks(range).await
    }

    /// Get chunk metadata by path
    async fn get_chunk(&self, path: &str) -> Result<Option<ChunkMetadata>>;

    /// Delete chunk metadata
    async fn delete_chunk(&self, path: &str) -> Result<()>;

    /// List all chunks (for compaction planning)
    async fn list_chunks(&self) -> Result<Vec<TimeIndexEntry>>;

    /// Get L0 compaction candidates (uncompacted files)
    async fn get_l0_candidates(&self, min_count: usize) -> Result<Vec<Vec<String>>>;

    /// Get level-N compaction candidates
    async fn get_level_candidates(
        &self,
        level: usize,
        target_size: usize,
    ) -> Result<Vec<Vec<String>>>;

    /// Create a compaction job
    async fn create_compaction_job(&self, job: CompactionJob) -> Result<()>;

    /// Complete a compaction job
    async fn complete_compaction(&self, source_chunks: &[String], target_chunk: &str)
        -> Result<()>;

    /// Update compaction job status
    async fn update_compaction_status(&self, job_id: &str, status: CompactionStatus) -> Result<()>;

    /// Get pending compaction jobs
    async fn get_pending_compaction_jobs(&self) -> Result<Vec<CompactionJob>>;

    /// Remove completed/failed compaction jobs older than `max_age_secs`.
    /// Returns the number of jobs removed.
    async fn cleanup_completed_jobs(&self, max_age_secs: i64) -> Result<usize> {
        // Default no-op for backward compatibility
        let _ = max_age_secs;
        Ok(0)
    }

    // Shard split methods
    /// Start a shard split operation
    async fn start_split(
        &self,
        old_shard: &str,
        new_shards: Vec<String>,
        split_point: Vec<u8>,
    ) -> Result<()>;

    /// Get the current split state for a shard
    async fn get_split_state(&self, shard_id: &str) -> Result<Option<SplitState>>;

    /// Update split progress
    async fn update_split_progress(
        &self,
        shard_id: &str,
        progress: f64,
        phase: SplitPhase,
    ) -> Result<()>;

    /// Complete a shard split (atomic cutover)
    async fn complete_split(&self, old_shard: &str) -> Result<()>;

    /// Get all chunks for a specific shard
    async fn get_chunks_for_shard(&self, shard_id: &str) -> Result<Vec<TimeIndexEntry>>;

    /// Get shard metadata by shard ID
    async fn get_shard_metadata(
        &self,
        shard_id: &str,
    ) -> Result<Option<crate::sharding::ShardMetadata>>;

    /// Update shard metadata with generation check (CAS)
    /// Returns an error if the expected generation doesn't match
    async fn update_shard_metadata(
        &self,
        shard_id: &str,
        metadata: &crate::sharding::ShardMetadata,
        expected_generation: u64,
    ) -> Result<()>;

    /// Check if any shard split is currently in dual-write or backfill phase.
    ///
    /// Used by the query engine to enable deduplication when overlapping
    /// data may exist in both old and new shards.
    async fn has_active_split(&self) -> Result<bool> {
        Ok(false) // Default: no splits active
    }
}
