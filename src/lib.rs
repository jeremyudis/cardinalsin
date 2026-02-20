//! # CardinalSin
//!
//! A high-cardinality time-series database built on object storage.
//!
//! CardinalSin is designed to solve the high-cardinality problem that plagues modern
//! observability systems by using columnar storage (Arrow/Parquet) instead of
//! per-tag-combination indexing.
//!
//! ## Key Features
//!
//! - **Columnar Storage**: Labels are stored as columns, not tag sets, eliminating
//!   the series-set explosion problem
//! - **Zero-Disk Architecture**: Stateless compute nodes with data stored in S3
//! - **3-Tier Caching**: RAM → NVMe → S3 for cost-efficient performance
//! - **Adaptive Indexing**: Automatically promotes hot dimensions based on query patterns
//!
//! ## Architecture
//!
//! - **Ingester**: Buffers writes, batches to Parquet, flushes to S3
//! - **Query Node**: Executes queries via DataFusion with tiered caching
//! - **Compactor**: Merges files, downsamples, enforces retention

pub mod adaptive_index;
pub mod api;
pub mod clock;
pub mod cluster;
pub mod compactor;
pub mod config;
pub mod ingester;
pub mod metadata;
pub mod query;
pub mod schema;
pub mod sharding;

mod error;

pub use error::{Error, Result};

/// Configuration for the CardinalSin system
#[derive(Debug, Clone)]
pub struct Config {
    /// Object storage configuration
    pub storage: StorageConfig,
    /// Ingester configuration
    pub ingester: ingester::IngesterConfig,
    /// Query node configuration
    pub query: query::QueryConfig,
    /// Compactor configuration
    pub compactor: compactor::CompactorConfig,
}

/// Object storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// S3 bucket name
    pub bucket: String,
    /// S3 region
    pub region: String,
    /// S3 endpoint (for MinIO or other S3-compatible storage)
    pub endpoint: Option<String>,
    /// Tenant ID for multi-tenant isolation
    pub tenant_id: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            bucket: "cardinalsin-data".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            tenant_id: "default".to_string(),
        }
    }
}

/// Re-exports for convenience
pub mod prelude {
    pub use crate::compactor::{Compactor, CompactorConfig};
    pub use crate::ingester::{Ingester, IngesterConfig};
    pub use crate::query::{QueryConfig, QueryNode};
    pub use crate::schema::{MetricSchema, MetricType};
    pub use crate::{Config, Error, Result, StorageConfig};
}
