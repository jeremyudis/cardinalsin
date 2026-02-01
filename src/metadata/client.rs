//! Metadata client trait

use super::{TimeRange, TimeIndexEntry, CompactionJob, CompactionStatus};
use crate::ingester::ChunkMetadata;
use crate::Result;
use async_trait::async_trait;

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

    /// Get chunk metadata by path
    async fn get_chunk(&self, path: &str) -> Result<Option<ChunkMetadata>>;

    /// Delete chunk metadata
    async fn delete_chunk(&self, path: &str) -> Result<()>;

    /// List all chunks (for compaction planning)
    async fn list_chunks(&self) -> Result<Vec<TimeIndexEntry>>;

    /// Get L0 compaction candidates (uncompacted files)
    async fn get_l0_candidates(&self, min_count: usize) -> Result<Vec<Vec<String>>>;

    /// Get level-N compaction candidates
    async fn get_level_candidates(&self, level: usize, target_size: usize) -> Result<Vec<Vec<String>>>;

    /// Create a compaction job
    async fn create_compaction_job(&self, job: CompactionJob) -> Result<()>;

    /// Complete a compaction job
    async fn complete_compaction(
        &self,
        source_chunks: &[String],
        target_chunk: &str,
    ) -> Result<()>;

    /// Update compaction job status
    async fn update_compaction_status(&self, job_id: &str, status: CompactionStatus) -> Result<()>;

    /// Get pending compaction jobs
    async fn get_pending_compaction_jobs(&self) -> Result<Vec<CompactionJob>>;
}
