//! Local in-memory metadata client for development and testing

use super::{
    CompactionJob, CompactionLease, CompactionLeases, CompactionStatus, LeaseStatus,
    MetadataClient, SplitState, TimeIndexEntry, TimeRange,
};
use crate::ingester::ChunkMetadata;
use crate::sharding::SplitPhase;
use crate::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};

/// Local in-memory metadata client
///
/// This implementation stores metadata in memory and is suitable for
/// development, testing, and single-node deployments.
///
/// Uses a BTreeMap for time index to enable efficient range queries.
#[derive(Debug)]
pub struct LocalMetadataClient {
    /// Chunk metadata by path
    chunks: DashMap<String, ChunkMetadata>,
    /// Time index using BTreeMap for efficient range queries
    /// Key: hour bucket timestamp, Value: list of chunk paths
    time_index: RwLock<BTreeMap<i64, Vec<String>>>,
    /// Compaction jobs
    compaction_jobs: DashMap<String, CompactionJob>,
    /// Chunk levels (for compaction)
    chunk_levels: DashMap<String, u32>,
    /// Shard split states
    split_states: DashMap<String, SplitState>,
    /// Shard metadata
    shard_metadata: DashMap<String, crate::sharding::ShardMetadata>,
    /// Compaction leases
    compaction_leases: RwLock<CompactionLeases>,
}

impl LocalMetadataClient {
    /// Create a new local metadata client
    pub fn new() -> Self {
        Self {
            chunks: DashMap::new(),
            time_index: RwLock::new(BTreeMap::new()),
            compaction_jobs: DashMap::new(),
            chunk_levels: DashMap::new(),
            split_states: DashMap::new(),
            shard_metadata: DashMap::new(),
            compaction_leases: RwLock::new(CompactionLeases::default()),
        }
    }

    /// Get the hour bucket for a timestamp (nanoseconds)
    fn hour_bucket(timestamp: i64) -> i64 {
        // Convert to hours and back to get bucket start
        let nanos_per_hour = 3_600_000_000_000i64;
        (timestamp / nanos_per_hour) * nanos_per_hour
    }

    /// Nanoseconds per hour constant
    const NANOS_PER_HOUR: i64 = 3_600_000_000_000;
}

impl Default for LocalMetadataClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetadataClient for LocalMetadataClient {
    async fn register_chunk(&self, path: &str, metadata: &ChunkMetadata) -> Result<()> {
        // Store chunk metadata
        self.chunks.insert(path.to_string(), metadata.clone());

        // Index into all hour buckets that this chunk spans
        let start_bucket = Self::hour_bucket(metadata.min_timestamp);
        let end_bucket = Self::hour_bucket(metadata.max_timestamp);

        {
            let mut time_index = self.time_index.write();
            let mut bucket = start_bucket;
            while bucket <= end_bucket {
                time_index.entry(bucket).or_default().push(path.to_string());
                bucket += Self::NANOS_PER_HOUR;
            }
        }

        // Set level to 0 (uncompacted)
        self.chunk_levels.insert(path.to_string(), 0);

        Ok(())
    }

    async fn get_chunks(&self, range: TimeRange) -> Result<Vec<TimeIndexEntry>> {
        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // Calculate hour buckets that overlap with the range
        let start_bucket = Self::hour_bucket(range.start);
        let end_bucket = Self::hour_bucket(range.end);

        // Use BTreeMap range query for efficient bucket lookup
        {
            let time_index = self.time_index.read();

            // BTreeMap.range() provides O(log n + k) access where k is matching entries
            for (_, paths) in time_index.range(start_bucket..=end_bucket) {
                for path in paths.iter() {
                    if seen.contains(path) {
                        continue;
                    }
                    seen.insert(path.clone());

                    if let Some(meta) = self.chunks.get(path) {
                        let chunk_range = TimeRange::new(meta.min_timestamp, meta.max_timestamp);
                        if chunk_range.overlaps(&range) {
                            results.push(TimeIndexEntry::from(meta.value()));
                        }
                    }
                }
            }
        }

        // Sort by min timestamp
        results.sort_by_key(|e| e.min_timestamp);

        Ok(results)
    }

    async fn get_chunk(&self, path: &str) -> Result<Option<ChunkMetadata>> {
        Ok(self.chunks.get(path).map(|e| e.value().clone()))
    }

    async fn delete_chunk(&self, path: &str) -> Result<()> {
        if let Some((_, meta)) = self.chunks.remove(path) {
            // Remove from all hour buckets this chunk was indexed into
            let start_bucket = Self::hour_bucket(meta.min_timestamp);
            let end_bucket = Self::hour_bucket(meta.max_timestamp);

            {
                let mut time_index = self.time_index.write();
                let mut bucket = start_bucket;
                while bucket <= end_bucket {
                    if let Some(paths) = time_index.get_mut(&bucket) {
                        paths.retain(|p| p != path);
                    }
                    bucket += Self::NANOS_PER_HOUR;
                }
            }
        }
        self.chunk_levels.remove(path);
        Ok(())
    }

    async fn list_chunks(&self) -> Result<Vec<TimeIndexEntry>> {
        let results: Vec<TimeIndexEntry> = self
            .chunks
            .iter()
            .map(|entry| TimeIndexEntry::from(entry.value()))
            .collect();

        Ok(results)
    }

    async fn get_l0_candidates(&self, min_count: usize) -> Result<Vec<Vec<String>>> {
        // Get all L0 chunks grouped by hour
        let mut hour_groups: HashMap<i64, Vec<String>> = HashMap::new();

        for entry in self.chunk_levels.iter() {
            if *entry.value() == 0 {
                if let Some(meta) = self.chunks.get(entry.key()) {
                    let bucket = Self::hour_bucket(meta.min_timestamp);
                    hour_groups
                        .entry(bucket)
                        .or_default()
                        .push(entry.key().clone());
                }
            }
        }

        // Filter groups that meet the minimum count
        let candidates: Vec<Vec<String>> = hour_groups
            .into_values()
            .filter(|group| group.len() >= min_count)
            .collect();

        Ok(candidates)
    }

    async fn get_level_candidates(
        &self,
        level: usize,
        target_size: usize,
    ) -> Result<Vec<Vec<String>>> {
        // Get all chunks at the specified level
        let mut level_chunks: Vec<(String, ChunkMetadata)> = Vec::new();

        for entry in self.chunk_levels.iter() {
            if *entry.value() == level as u32 {
                if let Some(meta) = self.chunks.get(entry.key()) {
                    level_chunks.push((entry.key().clone(), meta.clone()));
                }
            }
        }

        // Sort by timestamp
        level_chunks.sort_by_key(|(_, meta)| meta.min_timestamp);

        // Group chunks until target size is reached
        let mut candidates = Vec::new();
        let mut current_group = Vec::new();
        let mut current_size = 0usize;

        for (path, meta) in level_chunks {
            current_group.push(path);
            current_size += meta.size_bytes as usize;

            if current_size >= target_size {
                if current_group.len() >= 2 {
                    candidates.push(std::mem::take(&mut current_group));
                }
                current_size = 0;
            }
        }

        Ok(candidates)
    }

    async fn create_compaction_job(&self, job: CompactionJob) -> Result<()> {
        self.compaction_jobs.insert(job.id.clone(), job);
        Ok(())
    }

    async fn complete_compaction(
        &self,
        source_chunks: &[String],
        target_chunk: &str,
    ) -> Result<()> {
        // Determine the new level (max source level + 1)
        let new_level = source_chunks
            .iter()
            .filter_map(|path| self.chunk_levels.get(path).map(|l| *l))
            .max()
            .unwrap_or(0)
            + 1;

        // Delete source chunks
        for path in source_chunks {
            self.delete_chunk(path).await?;
        }

        // Set level for target chunk
        self.chunk_levels
            .insert(target_chunk.to_string(), new_level);

        Ok(())
    }

    async fn update_compaction_status(&self, job_id: &str, status: CompactionStatus) -> Result<()> {
        if let Some(mut job) = self.compaction_jobs.get_mut(job_id) {
            job.status = status;
        }
        Ok(())
    }

    async fn get_pending_compaction_jobs(&self) -> Result<Vec<CompactionJob>> {
        let pending: Vec<CompactionJob> = self
            .compaction_jobs
            .iter()
            .filter(|entry| entry.value().status == CompactionStatus::Pending)
            .map(|entry| entry.value().clone())
            .collect();

        Ok(pending)
    }

    // Shard split methods
    async fn start_split(
        &self,
        old_shard: &str,
        new_shards: Vec<String>,
        split_point: Vec<u8>,
    ) -> Result<()> {
        let split_state = SplitState {
            phase: SplitPhase::Preparation,
            old_shard: old_shard.to_string(),
            new_shards,
            split_point,
            split_timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            backfill_progress: 0.0,
        };

        self.split_states.insert(old_shard.to_string(), split_state);
        Ok(())
    }

    async fn get_split_state(&self, shard_id: &str) -> Result<Option<SplitState>> {
        Ok(self.split_states.get(shard_id).map(|s| s.value().clone()))
    }

    async fn update_split_progress(
        &self,
        shard_id: &str,
        progress: f64,
        phase: SplitPhase,
    ) -> Result<()> {
        if let Some(mut state) = self.split_states.get_mut(shard_id) {
            state.backfill_progress = progress;
            state.phase = phase;
        }
        Ok(())
    }

    async fn complete_split(&self, old_shard: &str) -> Result<()> {
        // Remove split state - split is complete
        self.split_states.remove(old_shard);
        Ok(())
    }

    async fn get_chunks_for_shard(&self, shard_id: &str) -> Result<Vec<TimeIndexEntry>> {
        // In a real implementation, we'd have shard-to-chunk mappings
        // For now, filter chunks by path pattern
        let results: Vec<TimeIndexEntry> = self
            .chunks
            .iter()
            .filter(|entry| entry.key().contains(shard_id))
            .map(|entry| TimeIndexEntry::from(entry.value()))
            .collect();

        Ok(results)
    }

    async fn get_shard_metadata(
        &self,
        shard_id: &str,
    ) -> Result<Option<crate::sharding::ShardMetadata>> {
        Ok(self.shard_metadata.get(shard_id).map(|entry| entry.clone()))
    }

    async fn update_shard_metadata(
        &self,
        shard_id: &str,
        metadata: &crate::sharding::ShardMetadata,
        expected_generation: u64,
    ) -> Result<()> {
        // Check generation
        if let Some(current) = self.shard_metadata.get(shard_id) {
            if current.generation != expected_generation {
                return Err(crate::Error::StaleGeneration {
                    expected: expected_generation,
                    actual: current.generation,
                });
            }
        } else if expected_generation != 0 {
            return Err(crate::Error::ShardNotFound(shard_id.to_string()));
        }

        // Update with incremented generation
        let mut new_metadata = metadata.clone();
        new_metadata.generation = expected_generation + 1;
        self.shard_metadata
            .insert(shard_id.to_string(), new_metadata);

        Ok(())
    }

    async fn acquire_lease(
        &self,
        node_id: &str,
        chunks: &[String],
        level: u32,
    ) -> Result<CompactionLease> {
        let mut leases = self.compaction_leases.write();
        let now = chrono::Utc::now();

        // Scavenge expired active leases
        leases.leases.retain(|_, lease| {
            !(lease.status == LeaseStatus::Active && lease.expires_at <= now)
        });

        // Check for conflicts
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
            return Err(crate::Error::ChunksAlreadyLeased(conflicts));
        }

        let lease = CompactionLease {
            lease_id: uuid::Uuid::new_v4().to_string(),
            holder_id: node_id.to_string(),
            chunks: chunks.to_vec(),
            acquired_at: now,
            expires_at: now + chrono::Duration::seconds(300),
            level,
            status: LeaseStatus::Active,
        };

        leases.leases.insert(lease.lease_id.clone(), lease.clone());
        Ok(lease)
    }

    async fn complete_lease(&self, lease_id: &str) -> Result<()> {
        let mut leases = self.compaction_leases.write();
        if let Some(lease) = leases.leases.get_mut(lease_id) {
            lease.status = LeaseStatus::Completed;
        }
        Ok(())
    }

    async fn fail_lease(&self, lease_id: &str) -> Result<()> {
        let mut leases = self.compaction_leases.write();
        if let Some(lease) = leases.leases.get_mut(lease_id) {
            lease.status = LeaseStatus::Failed;
        }
        Ok(())
    }

    async fn renew_lease(&self, lease_id: &str) -> Result<()> {
        let mut leases = self.compaction_leases.write();
        if let Some(lease) = leases.leases.get_mut(lease_id) {
            if lease.status != LeaseStatus::Active {
                return Err(crate::Error::Internal(format!(
                    "Cannot renew non-active lease {}",
                    lease_id
                )));
            }
            lease.expires_at = chrono::Utc::now() + chrono::Duration::seconds(300);
        } else {
            return Err(crate::Error::Internal(format!("Lease {} not found", lease_id)));
        }
        Ok(())
    }

    async fn load_leases(&self) -> Result<CompactionLeases> {
        Ok(self.compaction_leases.read().clone())
    }

    async fn scavenge_leases(&self) -> Result<usize> {
        let mut leases = self.compaction_leases.write();
        let original_count = leases.leases.len();
        let now = chrono::Utc::now();

        leases.leases.retain(|_, lease| {
            if lease.status == LeaseStatus::Active {
                lease.expires_at > now
            } else {
                false
            }
        });

        Ok(original_count - leases.leases.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_metadata(path: &str, min_ts: i64, max_ts: i64) -> ChunkMetadata {
        ChunkMetadata {
            path: path.to_string(),
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            row_count: 1000,
            size_bytes: 1024 * 1024,
        }
    }

    #[tokio::test]
    async fn test_register_and_get_chunk() {
        let client = LocalMetadataClient::new();
        let meta = create_test_metadata("chunk1.parquet", 1000, 2000);

        client
            .register_chunk("chunk1.parquet", &meta)
            .await
            .unwrap();

        let result = client.get_chunk("chunk1.parquet").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().path, "chunk1.parquet");
    }

    #[tokio::test]
    async fn test_time_range_query() {
        let client = LocalMetadataClient::new();

        // Register chunks at different times
        let hour = 3_600_000_000_000i64;
        client
            .register_chunk(
                "chunk1.parquet",
                &create_test_metadata("chunk1.parquet", hour, hour * 2),
            )
            .await
            .unwrap();
        client
            .register_chunk(
                "chunk2.parquet",
                &create_test_metadata("chunk2.parquet", hour * 3, hour * 4),
            )
            .await
            .unwrap();
        client
            .register_chunk(
                "chunk3.parquet",
                &create_test_metadata("chunk3.parquet", hour * 5, hour * 6),
            )
            .await
            .unwrap();

        // Query that overlaps first two chunks
        let range = TimeRange::new(hour, hour * 4);
        let results = client.get_chunks(range).await.unwrap();

        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_chunk() {
        let client = LocalMetadataClient::new();
        let meta = create_test_metadata("chunk1.parquet", 1000, 2000);

        client
            .register_chunk("chunk1.parquet", &meta)
            .await
            .unwrap();
        client.delete_chunk("chunk1.parquet").await.unwrap();

        let result = client.get_chunk("chunk1.parquet").await.unwrap();
        assert!(result.is_none());
    }
}
