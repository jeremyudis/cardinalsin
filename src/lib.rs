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
//! - **Zero-Disk Architecture**: Stateless compute nodes with data stored in object storage
//! - **3-Tier Caching**: RAM → NVMe → object storage for cost-efficient performance
//! - **Adaptive Indexing**: Automatically promotes hot dimensions based on query patterns
//!
//! ## Architecture
//!
//! - **Ingester**: Buffers writes, batches to Parquet, flushes to object storage
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
pub mod telemetry;

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
    /// Cloud provider for object storage.
    pub provider: CloudProvider,
    /// Provider bucket name.
    pub bucket: String,
    /// Tenant ID for multi-tenant isolation
    pub tenant_id: String,
}

/// Supported object storage cloud providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudProvider {
    Memory,
    Aws,
    Gcp,
    Azure,
}

impl CloudProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Aws => "aws",
            Self::Gcp => "gcp",
            Self::Azure => "azure",
        }
    }

    pub fn object_store_scheme(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Aws => "s3",
            Self::Gcp => "gs",
            Self::Azure => "az",
        }
    }
}

impl std::str::FromStr for CloudProvider {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "memory" => Ok(Self::Memory),
            "aws" | "s3" => Ok(Self::Aws),
            "gcp" | "gcs" => Ok(Self::Gcp),
            "azure" => Ok(Self::Azure),
            other => Err(format!(
                "unknown cloud provider '{}'; expected one of memory, aws, gcp, azure",
                other
            )),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            provider: CloudProvider::Aws,
            bucket: "cardinalsin-data".to_string(),
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
    pub use crate::{CloudProvider, Config, Error, Result, StorageConfig};
}
