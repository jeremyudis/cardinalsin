//! Error types for CardinalSin

/// Result type alias for CardinalSin operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for CardinalSin
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Arrow-related errors
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Parquet-related errors
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Object store errors
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// DataFusion errors
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Serialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),
    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),
    /// Invalid schema
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    /// Missing metric name
    #[error("Missing metric name")]
    MissingMetricName,
    /// Buffer full
    #[error("Write buffer is full")]
    BufferFull,
    /// WAL disk is full
    #[error("WAL disk is full")]
    WalFull,
    /// Query error
    #[error("Query error: {0}")]
    Query(String),
    /// Metadata error
    #[error("Metadata error: {0}")]
    Metadata(String),
    /// Shard error
    #[error("Shard error: {0}")]
    Shard(#[from] ShardError),
    /// Cache error
    #[error("Cache error: {0}")]
    Cache(String),
    /// Timeout
    #[error("Operation timed out")]
    Timeout,
    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
    /// Stale generation (optimistic concurrency)
    #[error("Stale generation: expected {expected}, got {actual}")]
    StaleGeneration { expected: u64, actual: u64 },
    /// Shard moved during operation
    #[error("Shard moved to: {new_location}")]
    ShardMoved { new_location: String },
    /// Rate limit exceeded
    #[error("Rate limit exceeded for tenant {tenant_id}: {limit} req/s")]
    RateLimitExceeded { tenant_id: String, limit: u64 },
    /// Too many subscriptions
    #[error("Too many active subscriptions")]
    TooManySubscriptions,
    /// Metadata conflict (CAS failure)
    #[error("Metadata conflict: concurrent modification detected")]
    Conflict,
    /// Too many retries
    #[error("Too many retries: operation failed after maximum retry attempts")]
    TooManyRetries,
    /// Shard not found
    #[error("Shard not found: {0}")]
    ShardNotFound(String),
    /// Chunks are already leased by another compactor
    #[error("Chunks already leased: {0:?}")]
    ChunksAlreadyLeased(Vec<String>),
}

/// Shard-specific errors
#[derive(Debug, thiserror::Error)]
pub enum ShardError {
    /// Shard is splitting
    #[error("Shard is splitting: {0}")]
    Splitting(String),
    /// Shard is being deleted
    #[error("Shard pending deletion: {0}")]
    PendingDeletion(String),
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}
