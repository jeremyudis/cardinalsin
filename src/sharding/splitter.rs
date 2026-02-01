//! Two-phase shard splitting protocol

use super::{ShardId, ShardMetadata, ShardState, TimeBucket};
use crate::Result;
use std::sync::Arc;

/// Split phase
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitPhase {
    /// Preparation phase - creating new shard metadata
    Preparation,
    /// Dual-write phase - writing to both old and new shards
    DualWrite,
    /// Backfill phase - copying historical data
    Backfill,
    /// Cutover phase - atomic switch to new shards
    Cutover,
    /// Cleanup phase - removing old shard
    Cleanup,
}

/// Pending shard (not yet active)
#[derive(Debug, Clone)]
pub struct PendingShard {
    pub id: ShardId,
    pub range: (Vec<u8>, Vec<u8>),
}

/// Shard splitter for zero-downtime splitting
pub struct ShardSplitter {
    // In production, these would be real clients
    // metadata: Arc<MetadataClient>,
    // object_store: Arc<dyn ObjectStore>,
}

impl ShardSplitter {
    /// Create a new shard splitter
    pub fn new() -> Self {
        Self {}
    }

    /// Split a hot shard into two
    pub async fn split_shard(&self, shard: &ShardMetadata) -> Result<(ShardId, ShardId)> {
        // Phase 1: Preparation
        let split_point = self.calculate_split_point(shard);

        let new_shard_a = ShardId::from(uuid::Uuid::new_v4().to_string());
        let new_shard_b = ShardId::from(uuid::Uuid::new_v4().to_string());

        let pending_shards = vec![
            PendingShard {
                id: new_shard_a.clone(),
                range: (shard.key_range.0.clone(), split_point.clone()),
            },
            PendingShard {
                id: new_shard_b.clone(),
                range: (split_point, shard.key_range.1.clone()),
            },
        ];

        // In production:
        // Phase 2: Enable dual-write
        // Phase 3: Backfill historical data
        // Phase 4: Atomic cutover
        // Phase 5: Cleanup

        Ok((new_shard_a, new_shard_b))
    }

    /// Calculate the split point based on time
    fn calculate_split_point(&self, shard: &ShardMetadata) -> Vec<u8> {
        // For time-series: split on time boundary
        let time_range = shard.max_time - shard.min_time;
        let mid_time = shard.min_time + time_range / 2;

        // Round to nearest 5-minute boundary
        let rounded = TimeBucket::round_to_5min(mid_time);

        // Convert to key format
        rounded.to_be_bytes().to_vec()
    }
}

impl Default for ShardSplitter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::ReplicaInfo;

    #[tokio::test]
    async fn test_split_shard() {
        let splitter = ShardSplitter::new();

        let shard = ShardMetadata {
            shard_id: "shard-1".to_string(),
            generation: 1,
            key_range: (vec![0, 0, 0, 0], vec![255, 255, 255, 255]),
            replicas: vec![ReplicaInfo {
                replica_id: "replica-1".to_string(),
                node_id: "node-1".to_string(),
                is_leader: true,
            }],
            state: ShardState::Active,
            min_time: 1000000000,
            max_time: 2000000000,
        };

        let (shard_a, shard_b) = splitter.split_shard(&shard).await.unwrap();

        assert_ne!(shard_a, shard_b);
        assert!(!shard_a.is_empty());
        assert!(!shard_b.is_empty());
    }
}
