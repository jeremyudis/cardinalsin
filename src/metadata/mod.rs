//! Metadata service for CardinalSin
//!
//! Tracks chunks, schemas, shard assignments, and provides coordination
//! between components.

mod client;
mod local;
pub mod predicates;
mod s3;

pub use client::{MetadataClient, SplitState};
pub use local::LocalMetadataClient;
pub use predicates::{ColumnPredicate, PredicateValue};
pub use s3::{
    ChunkMetadataExtended, ColumnStats, MetadataCatalog, ObjectStoreMetadataClient,
    ObjectStoreMetadataConfig, S3MetadataClient, S3MetadataConfig,
};

use crate::ingester::ChunkMetadata;
use std::ops::Range;

/// Time range for queries
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start: i64,
    pub end: i64,
}

impl TimeRange {
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    pub fn contains(&self, timestamp: i64) -> bool {
        timestamp >= self.start && timestamp <= self.end
    }

    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start <= other.end && self.end >= other.start
    }
}

impl From<Range<i64>> for TimeRange {
    fn from(range: Range<i64>) -> Self {
        Self::new(range.start, range.end)
    }
}

/// Time index entry
#[derive(Debug, Clone)]
pub struct TimeIndexEntry {
    pub chunk_path: String,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub row_count: u64,
    pub size_bytes: u64,
}

impl From<&ChunkMetadata> for TimeIndexEntry {
    fn from(meta: &ChunkMetadata) -> Self {
        Self {
            chunk_path: meta.path.clone(),
            min_timestamp: meta.min_timestamp,
            max_timestamp: meta.max_timestamp,
            row_count: meta.row_count,
            size_bytes: meta.size_bytes,
        }
    }
}

/// Compaction job definition
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactionJob {
    pub id: String,
    pub source_chunks: Vec<String>,
    pub target_level: u32,
    pub status: CompactionStatus,
    /// Unix timestamp when this job was created (for TTL-based cleanup)
    #[serde(default)]
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompactionStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// Compaction lease for mutual exclusion between concurrent compactors
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactionLease {
    /// Unique lease ID
    pub lease_id: String,
    /// ID of the compactor holding this lease
    pub holder_id: String,
    /// Chunks claimed by this lease
    pub chunks: Vec<String>,
    /// When the lease was acquired (UTC)
    pub acquired_at: chrono::DateTime<chrono::Utc>,
    /// When the lease expires (must be renewed before this)
    pub expires_at: chrono::DateTime<chrono::Utc>,
    /// Compaction level being targeted
    pub level: u32,
    /// Lease status
    pub status: LeaseStatus,
}

/// Status of a compaction lease
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LeaseStatus {
    /// Lease is active, chunks are being compacted
    Active,
    /// Compaction completed successfully
    Completed,
    /// Compaction failed, chunks should be released
    Failed,
}

/// All compaction leases stored in a single S3 object
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct CompactionLeases {
    pub leases: std::collections::HashMap<String, CompactionLease>,
}
