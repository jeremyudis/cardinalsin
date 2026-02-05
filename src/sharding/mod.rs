//! Dynamic sharding and hot shard rebalancing
//!
//! Automatically scales ingest/query capacity and rebalances hot shards
//! without downtime.

mod monitor;
mod splitter;
mod rebalancer;
mod router;

pub use monitor::{ShardMonitor, ShardMetrics, HotShardConfig, ShardAction};
pub use splitter::{ShardSplitter, SplitPhase};
pub use rebalancer::RebalanceStrategy;
pub use router::ShardRouter;

use std::time::Duration;

/// Shard identifier
pub type ShardId = String;

/// Time bucket for sharding
#[derive(Debug, Clone, Copy)]
pub struct TimeBucket {
    /// Start of the bucket (nanoseconds)
    pub start: i64,
    /// Duration of the bucket
    pub duration: Duration,
}

impl TimeBucket {
    /// Create a 5-minute bucket for a timestamp
    pub fn five_minute(timestamp: i64) -> Self {
        let nanos_per_5min = 5 * 60 * 1_000_000_000i64;
        let start = (timestamp / nanos_per_5min) * nanos_per_5min;
        Self {
            start,
            duration: Duration::from_secs(300),
        }
    }

    /// Round to nearest 5-minute boundary
    pub fn round_to_5min(timestamp: i64) -> i64 {
        let nanos_per_5min = 5 * 60 * 1_000_000_000i64;
        (timestamp / nanos_per_5min) * nanos_per_5min
    }
}

/// Shard key structure
#[derive(Debug, Clone)]
pub struct ShardKey {
    pub tenant_id: u32,
    pub metric_hash: u16,
    pub time_bucket: TimeBucket,
}

impl ShardKey {
    /// Create a shard key from components
    pub fn new(tenant_id: u32, metric_name: &str, timestamp: i64) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        metric_name.hash(&mut hasher);
        let metric_hash = (hasher.finish() & 0xFFFF) as u16;

        Self {
            tenant_id,
            metric_hash,
            time_bucket: TimeBucket::five_minute(timestamp),
        }
    }

    /// Convert to bytes for routing
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(14);
        bytes.extend_from_slice(&self.tenant_id.to_be_bytes());
        bytes.extend_from_slice(&self.metric_hash.to_be_bytes());
        bytes.extend_from_slice(&self.time_bucket.start.to_be_bytes());
        bytes
    }
}

/// Shard state
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ShardState {
    /// Shard is active and accepting writes/queries
    Active,
    /// Shard is being split
    Splitting { new_shards: Vec<ShardId> },
    /// Shard is pending deletion
    PendingDeletion { delete_after: i64 },
}

/// Replica information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplicaInfo {
    pub replica_id: String,
    pub node_id: String,
    pub is_leader: bool,
}

/// Shard metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShardMetadata {
    pub shard_id: ShardId,
    pub generation: u64,
    pub key_range: (Vec<u8>, Vec<u8>),
    pub replicas: Vec<ReplicaInfo>,
    pub state: ShardState,
    pub min_time: i64,
    pub max_time: i64,
}

impl ShardMetadata {
    /// Get the leader replica
    pub fn leader_replica(&self) -> Option<&ReplicaInfo> {
        self.replicas.iter().find(|r| r.is_leader)
    }

    /// Check if the shard is active
    pub fn is_active(&self) -> bool {
        self.state == ShardState::Active
    }
}
