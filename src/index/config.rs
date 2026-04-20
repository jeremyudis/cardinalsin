//! Configuration for the inverted index subsystem.

/// Configuration for the CSI inverted index.
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Whether the inverted index is enabled.
    pub enabled: bool,
    /// Maximum segments per shard before merge is triggered.
    pub max_segments_per_shard: usize,
    /// Target segment count after a merge.
    pub target_segments_after_merge: usize,
    /// Columns to skip when building the index (e.g. timestamp, value columns).
    pub skip_columns: Vec<String>,
    /// Maximum distinct values for a column to be indexed.
    /// Columns with cardinality above this threshold are skipped.
    pub max_cardinality_for_inverted: usize,
    /// Maximum number of segments held in the in-memory SegmentReader cache.
    pub segment_cache_capacity: u64,
    /// Idle TTL for the segment cache (seconds).
    pub segment_cache_idle_ttl_secs: u64,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_segments_per_shard: 10,
            target_segments_after_merge: 8,
            skip_columns: vec![
                "timestamp".to_string(),
                "value_f64".to_string(),
                "value_i64".to_string(),
                "value_u64".to_string(),
            ],
            max_cardinality_for_inverted: 100_000,
            segment_cache_capacity: 10_000,
            segment_cache_idle_ttl_secs: 600,
        }
    }
}
