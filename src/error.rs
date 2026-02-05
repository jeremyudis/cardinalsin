//! Error types for CardinalSin

use std::fmt;

/// Result type alias for CardinalSin operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for CardinalSin
#[derive(Debug)]
pub enum Error {
    /// Arrow-related errors
    Arrow(arrow::error::ArrowError),
    /// Parquet-related errors
    Parquet(parquet::errors::ParquetError),
    /// Object store errors
    ObjectStore(object_store::Error),
    /// DataFusion errors
    DataFusion(datafusion::error::DataFusionError),
    /// IO errors
    Io(std::io::Error),
    /// Serialization errors
    Serialization(String),
    /// Configuration errors
    Config(String),
    /// Invalid schema
    InvalidSchema(String),
    /// Missing metric name
    MissingMetricName,
    /// Buffer full
    BufferFull,
    /// Query error
    Query(String),
    /// Metadata error
    Metadata(String),
    /// Shard error
    Shard(ShardError),
    /// Cache error
    Cache(String),
    /// Timeout
    Timeout,
    /// Internal error
    Internal(String),
    /// Stale generation (optimistic concurrency)
    StaleGeneration { expected: u64, actual: u64 },
    /// Shard moved during operation
    ShardMoved { new_location: String },
    /// Rate limit exceeded
    RateLimitExceeded { tenant_id: String, limit: u64 },
    /// Too many subscriptions
    TooManySubscriptions,
    /// Metadata conflict (CAS failure)
    Conflict,
    /// Too many retries
    TooManyRetries,
    /// Shard not found
    ShardNotFound(String),
}

/// Shard-specific errors
#[derive(Debug)]
pub enum ShardError {
    /// Shard not found
    NotFound(String),
    /// Shard is splitting
    Splitting(String),
    /// Shard is being deleted
    PendingDeletion(String),
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Arrow(e) => Some(e),
            Error::Parquet(e) => Some(e),
            Error::ObjectStore(e) => Some(e),
            Error::DataFusion(e) => Some(e),
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Arrow(e) => write!(f, "Arrow error: {}", e),
            Error::Parquet(e) => write!(f, "Parquet error: {}", e),
            Error::ObjectStore(e) => write!(f, "Object store error: {}", e),
            Error::DataFusion(e) => write!(f, "DataFusion error: {}", e),
            Error::Io(e) => write!(f, "IO error: {}", e),
            Error::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Error::Config(msg) => write!(f, "Configuration error: {}", msg),
            Error::InvalidSchema(msg) => write!(f, "Invalid schema: {}", msg),
            Error::MissingMetricName => write!(f, "Missing metric name"),
            Error::BufferFull => write!(f, "Write buffer is full"),
            Error::Query(msg) => write!(f, "Query error: {}", msg),
            Error::Metadata(msg) => write!(f, "Metadata error: {}", msg),
            Error::Shard(e) => write!(f, "Shard error: {:?}", e),
            Error::Cache(msg) => write!(f, "Cache error: {}", msg),
            Error::Timeout => write!(f, "Operation timed out"),
            Error::Internal(msg) => write!(f, "Internal error: {}", msg),
            Error::StaleGeneration { expected, actual } => {
                write!(f, "Stale generation: expected {}, got {}", expected, actual)
            }
            Error::ShardMoved { new_location } => {
                write!(f, "Shard moved to: {}", new_location)
            }
            Error::RateLimitExceeded { tenant_id, limit } => {
                write!(f, "Rate limit exceeded for tenant {}: {} req/s", tenant_id, limit)
            }
            Error::TooManySubscriptions => write!(f, "Too many active subscriptions"),
            Error::Conflict => write!(f, "Metadata conflict: concurrent modification detected"),
            Error::TooManyRetries => write!(f, "Too many retries: operation failed after maximum retry attempts"),
            Error::ShardNotFound(shard_id) => write!(f, "Shard not found: {}", shard_id),
        }
    }
}

impl From<arrow::error::ArrowError> for Error {
    fn from(e: arrow::error::ArrowError) -> Self {
        Error::Arrow(e)
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Error::Parquet(e)
    }
}

impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        Error::ObjectStore(e)
    }
}

impl From<datafusion::error::DataFusionError> for Error {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Error::DataFusion(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}
