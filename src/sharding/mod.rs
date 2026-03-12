//! Dynamic sharding and hot shard rebalancing
//!
//! ## Shard Model
//!
//! **Shard key = `metric_hash`** (u16): `xxhash64(metric_name) & 0xFFFF`.
//! Tenant isolation is structural (separate S3 prefix + metadata catalog).
//!
//! **Initial layout**: N fixed shards per tenant (configurable, default 16),
//! each covering `1/N` of the 16-bit metric hash space.
//!
//! **Auto-split**: The existing 5-phase split protocol splits hot shards
//! at the midpoint of their hash range.

mod monitor;
mod rebalancer;
mod router;
mod splitter;

pub use monitor::{HotShardConfig, ShardAction, ShardMetrics, ShardMonitor};
pub use rebalancer::RebalanceStrategy;
pub use router::ShardRouter;
pub use splitter::{ShardSplitter, SplitPhase, SplitProgress};

use std::hash::Hasher;
use std::time::Duration;
use twox_hash::XxHash64;

/// Shard identifier — sequential u32 assigned at shard creation.
///
/// When shard 3 splits, children get the next available IDs (e.g. 16, 17).
pub type ShardId = u32;

/// Default number of initial shards per tenant.
pub const DEFAULT_INITIAL_SHARD_COUNT: u16 = 16;

/// Time bucket for sharding (legacy, kept for backward compat with splitter)
#[derive(Debug, Clone, Copy)]
pub struct TimeBucket {
    pub start: i64,
    pub duration: Duration,
}

impl TimeBucket {
    pub fn five_minute(timestamp: i64) -> Self {
        let nanos_per_5min = 5 * 60 * 1_000_000_000i64;
        let start = (timestamp / nanos_per_5min) * nanos_per_5min;
        Self {
            start,
            duration: Duration::from_secs(300),
        }
    }

    pub fn round_to_5min(timestamp: i64) -> i64 {
        let nanos_per_5min = 5 * 60 * 1_000_000_000i64;
        (timestamp / nanos_per_5min) * nanos_per_5min
    }
}

/// Legacy shard key structure — kept for backward compat with router tests.
/// New code should use `hash_metric_name()` directly.
#[derive(Debug, Clone)]
pub struct ShardKey {
    pub tenant_id: u32,
    pub metric_hash: u16,
    pub time_bucket: TimeBucket,
}

impl ShardKey {
    pub fn new(tenant_id: u32, metric_name: &str, timestamp: i64) -> Self {
        Self {
            tenant_id,
            metric_hash: hash_metric_name(metric_name),
            time_bucket: TimeBucket::five_minute(timestamp),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(14);
        bytes.extend_from_slice(&self.tenant_id.to_be_bytes());
        bytes.extend_from_slice(&self.metric_hash.to_be_bytes());
        bytes.extend_from_slice(&self.time_bucket.start.to_be_bytes());
        bytes
    }
}

// ── Core shard key functions ────────────────────────────────────────────

/// Compute a 16-bit metric hash for shard routing (xxhash64, seed 0).
pub fn hash_metric_name(metric_name: &str) -> u16 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(metric_name.as_bytes());
    (hasher.finish() & 0xFFFF) as u16
}

/// Check if a metric hash falls in a shard's range [start, end).
/// Ranges use u32 to avoid wraparound: the last shard has end = 0x10000.
pub fn hash_in_range(hash: u16, range: (u32, u32)) -> bool {
    let h = hash as u32;
    h >= range.0 && h < range.1
}

/// Find which shard owns a given metric hash.
///
/// Excludes `PendingDeletion` shards: their data has been migrated to child
/// shards, so routing writes there would target a shard scheduled for cleanup.
pub fn find_shard_for_hash(hash: u16, shards: &[ShardMetadata]) -> Option<ShardId> {
    let h = hash as u32;
    shards
        .iter()
        .filter(|s| !matches!(s.state, ShardState::PendingDeletion { .. }))
        .find(|s| h >= s.hash_range.0 && h < s.hash_range.1)
        .map(|s| s.shard_id)
}

/// Create initial shard metadata with `num_shards` partitions covering [0, 0x10000).
pub fn initial_shards(num_shards: u16) -> Vec<ShardMetadata> {
    let step = 0x10000u32 / num_shards as u32;
    (0..num_shards)
        .map(|i| {
            let range_start = i as u32 * step;
            let range_end = if i == num_shards - 1 {
                0x10000u32
            } else {
                (i as u32 + 1) * step
            };
            ShardMetadata {
                shard_id: i as ShardId,
                hash_range: (range_start, range_end),
                generation: 0,
                state: ShardState::Active,
                replicas: vec![],
                key_range: (vec![], vec![]),
                min_time: 0,
                max_time: 0,
            }
        })
        .collect()
}

// ── Data types ──────────────────────────────────────────────────────────

/// Shard state
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ShardState {
    Active,
    Splitting { new_shards: Vec<ShardId> },
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
    /// Metric hash range [start, end) — u32 so last shard can have end = 0x10000
    #[serde(default)]
    pub hash_range: (u32, u32),
    pub generation: u64,
    /// Legacy byte-range key range (kept for backward compat with router/splitter)
    pub key_range: (Vec<u8>, Vec<u8>),
    pub replicas: Vec<ReplicaInfo>,
    pub state: ShardState,
    pub min_time: i64,
    pub max_time: i64,
}

impl ShardMetadata {
    pub fn leader_replica(&self) -> Option<&ReplicaInfo> {
        self.replicas.iter().find(|r| r.is_leader)
    }

    pub fn is_active(&self) -> bool {
        self.state == ShardState::Active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_deterministic() {
        assert_eq!(hash_metric_name("cpu_usage"), hash_metric_name("cpu_usage"));
    }

    #[test]
    fn test_hash_distribution() {
        assert_ne!(
            hash_metric_name("cpu_usage"),
            hash_metric_name("memory_usage")
        );
    }

    #[test]
    fn test_hash_in_range() {
        assert!(hash_in_range(0x0500, (0x0000, 0x1000)));
        assert!(!hash_in_range(0x1500, (0x0000, 0x1000)));
        assert!(!hash_in_range(0x1000, (0x0000, 0x1000))); // half-open
    }

    #[test]
    fn test_initial_shards_16() {
        let shards = initial_shards(16);
        assert_eq!(shards.len(), 16);
        assert_eq!(shards[0].hash_range.0, 0);
        assert_eq!(shards[15].hash_range.1, 0x10000);
        for i in 0..15 {
            assert_eq!(shards[i].hash_range.1, shards[i + 1].hash_range.0);
        }
    }

    #[test]
    fn test_find_shard_for_hash() {
        let shards = initial_shards(4);
        let h = hash_metric_name("cpu_usage");
        let shard_id = find_shard_for_hash(h, &shards);
        assert!(shard_id.is_some());
        let id = shard_id.unwrap();
        let shard = shards.iter().find(|s| s.shard_id == id).unwrap();
        assert!(hash_in_range(h, shard.hash_range));
    }
}
