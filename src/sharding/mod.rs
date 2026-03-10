//! Dynamic sharding and hot shard rebalancing
//!
//! Automatically scales ingest/query capacity and rebalances hot shards
//! without downtime.

mod monitor;
mod rebalancer;
mod router;
mod splitter;

pub use monitor::{HotShardConfig, ShardAction, ShardMetrics, ShardMonitor};
pub use rebalancer::RebalanceStrategy;
pub use router::ShardRouter;
pub use splitter::{ShardSplitter, SplitPhase, SplitProgress};

use std::time::Duration;

/// Shard identifier
pub type ShardId = String;

/// Bootstrap shard window size in nanoseconds.
pub const BOOTSTRAP_SHARD_WINDOW_NANOS: i64 = 60 * 60 * 1_000_000_000;

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

    /// Enumerate all 5-minute bucket starts overlapping a time range.
    pub fn bucket_starts_in_range(start: i64, end: i64) -> Vec<i64> {
        if end < start {
            return Vec::new();
        }

        let mut buckets = Vec::new();
        let mut current = Self::round_to_5min(start);
        let last = Self::round_to_5min(end);
        let step = 5 * 60 * 1_000_000_000i64;

        while current <= last {
            buckets.push(current);
            current += step;
        }

        buckets
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
        Self {
            tenant_id,
            metric_hash: Self::hash_metric_name(metric_name),
            time_bucket: TimeBucket::five_minute(timestamp),
        }
    }

    /// Create a shard key from pre-hashed metric metadata.
    pub fn from_metric_hash(tenant_id: u32, metric_hash: u16, bucket_start: i64) -> Self {
        Self {
            tenant_id,
            metric_hash,
            time_bucket: TimeBucket {
                start: bucket_start,
                duration: Duration::from_secs(300),
            },
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

    /// Convert to a stable shard identifier derived from the full shard key bytes.
    pub fn shard_id(&self) -> ShardId {
        format!("shard-{}", hex_encode(&self.to_bytes()))
    }

    /// Start of the deterministic bootstrap shard window for this key.
    pub fn bootstrap_window_start(&self) -> i64 {
        (self.time_bucket.start / BOOTSTRAP_SHARD_WINDOW_NANOS) * BOOTSTRAP_SHARD_WINDOW_NANOS
    }

    /// End of the deterministic bootstrap shard window for this key.
    pub fn bootstrap_window_end(&self) -> i64 {
        self.bootstrap_window_start() + BOOTSTRAP_SHARD_WINDOW_NANOS
    }

    /// Deterministic shard identifier used before a shard has been split or rebalanced.
    pub fn bootstrap_shard_id(&self) -> ShardId {
        let start = ShardKey::from_metric_hash(
            self.tenant_id,
            self.metric_hash,
            self.bootstrap_window_start(),
        )
        .to_bytes();
        format!("shard-{}", hex_encode(&start))
    }

    /// Key range used when auto-creating shard metadata for a new shard family.
    pub fn bootstrap_key_range(&self) -> (Vec<u8>, Vec<u8>) {
        let start = ShardKey::from_metric_hash(
            self.tenant_id,
            self.metric_hash,
            self.bootstrap_window_start(),
        )
        .to_bytes();
        let end = ShardKey::from_metric_hash(
            self.tenant_id,
            self.metric_hash,
            self.bootstrap_window_end(),
        )
        .to_bytes();
        (start, end)
    }

    /// Compute a stable 16-bit metric hash for shard routing.
    pub fn hash_metric_name(metric_name: &str) -> u16 {
        let mut hash = 0xcbf29ce484222325u64;
        for byte in metric_name.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash & 0xFFFF) as u16
    }
}

/// Check whether a full shard key falls inside a shard range.
pub fn key_in_range(key: &[u8], range: &(Vec<u8>, Vec<u8>)) -> bool {
    key >= range.0.as_slice() && key < range.1.as_slice()
}

/// Return the next lexicographic key for a fixed-width big-endian byte sequence.
pub fn next_key_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut next = bytes.to_vec();
    for idx in (0..next.len()).rev() {
        if next[idx] != u8::MAX {
            next[idx] += 1;
            for trailing in &mut next[idx + 1..] {
                *trailing = 0;
            }
            return Some(next);
        }
    }
    None
}

/// Compute the midpoint between two equally-sized lexicographic byte ranges.
pub fn midpoint_bytes(start: &[u8], end: &[u8]) -> Option<Vec<u8>> {
    if start.len() != end.len() || start >= end {
        return None;
    }

    let mut delta = vec![0u8; start.len()];
    let mut borrow = 0i16;
    for idx in (0..start.len()).rev() {
        let diff = i16::from(end[idx]) - i16::from(start[idx]) - borrow;
        if diff < 0 {
            delta[idx] = (diff + 256) as u8;
            borrow = 1;
        } else {
            delta[idx] = diff as u8;
            borrow = 0;
        }
    }

    if delta.iter().all(|byte| *byte == 0) {
        return None;
    }

    let mut half = delta.clone();
    let mut remainder = 0u16;
    for byte in &mut half {
        let total = (remainder << 8) | u16::from(*byte);
        *byte = (total / 2) as u8;
        remainder = total % 2;
    }

    if half.iter().all(|byte| *byte == 0) {
        return None;
    }

    let mut midpoint = start.to_vec();
    let mut carry = 0u16;
    for idx in (0..midpoint.len()).rev() {
        let total = u16::from(midpoint[idx]) + u16::from(half[idx]) + carry;
        midpoint[idx] = (total & 0xFF) as u8;
        carry = total >> 8;
    }

    if carry > 0 || midpoint.as_slice() <= start || midpoint.as_slice() >= end {
        return None;
    }

    Some(midpoint)
}

/// Encode bytes as lowercase hexadecimal without introducing an extra dependency.
pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_range_groups_adjacent_five_minute_buckets() {
        let ts = 1_700_000_000_000_000_000i64;
        let first = ShardKey::new(7, "cpu_usage", ts);
        let second = ShardKey::new(7, "cpu_usage", ts + 5 * 60 * 1_000_000_000);

        assert_eq!(
            first.bootstrap_window_start(),
            second.bootstrap_window_start()
        );
        assert_eq!(first.bootstrap_shard_id(), second.bootstrap_shard_id());

        let (start, end) = first.bootstrap_key_range();
        assert!(key_in_range(
            &first.to_bytes(),
            &(start.clone(), end.clone())
        ));
        assert!(key_in_range(&second.to_bytes(), &(start, end)));
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
