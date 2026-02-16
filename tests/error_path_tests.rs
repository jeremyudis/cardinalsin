//! Tests for error paths and edge cases
//!
//! These tests verify that the system handles failures gracefully:
//! - Buffer overflow rejection
//! - Empty/boundary inputs
//! - Metadata edge cases
//! - TimeRange boundary conditions
//! - Stale generation exhaustion

use cardinalsin::ingester::{ChunkMetadata, Ingester, IngesterConfig, WriteBuffer};
use cardinalsin::metadata::{
    LocalMetadataClient, MetadataClient, S3MetadataClient, S3MetadataConfig, TimeRange,
};
use cardinalsin::sharding::{ReplicaInfo, ShardMetadata, ShardState};
use cardinalsin::Error;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::Arc;
use tempfile::tempdir;

/// Helper: create a simple RecordBatch with N rows
fn create_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("metric_name", DataType::Utf8, false),
    ]));

    let timestamps: Vec<i64> = (0..n as i64).collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();
    let names: Vec<&str> = (0..n).map(|_| "test_metric").collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

// =========================================================================
// TimeRange boundary condition tests
// =========================================================================

#[test]
fn test_timerange_contains_exact_boundaries() {
    let range = TimeRange::new(100, 200);
    assert!(range.contains(100), "Should contain start boundary");
    assert!(range.contains(200), "Should contain end boundary");
    assert!(range.contains(150), "Should contain middle value");
    assert!(!range.contains(99), "Should not contain value before start");
    assert!(!range.contains(201), "Should not contain value after end");
}

#[test]
fn test_timerange_single_point() {
    let range = TimeRange::new(100, 100);
    assert!(
        range.contains(100),
        "Single-point range should contain its value"
    );
    assert!(
        !range.contains(99),
        "Single-point range should not contain adjacent"
    );
    assert!(
        !range.contains(101),
        "Single-point range should not contain adjacent"
    );
}

#[test]
fn test_timerange_overlaps_adjacent() {
    let r1 = TimeRange::new(0, 100);
    let r2 = TimeRange::new(100, 200);
    assert!(r1.overlaps(&r2), "Touching ranges should overlap");
    assert!(r2.overlaps(&r1), "Overlap should be symmetric");
}

#[test]
fn test_timerange_overlaps_disjoint() {
    let r1 = TimeRange::new(0, 99);
    let r2 = TimeRange::new(100, 200);
    assert!(
        !r1.overlaps(&r2),
        "Non-overlapping ranges should not overlap"
    );
    assert!(!r2.overlaps(&r1), "Non-overlap should be symmetric");
}

#[test]
fn test_timerange_overlaps_subset() {
    let outer = TimeRange::new(0, 1000);
    let inner = TimeRange::new(100, 200);
    assert!(
        outer.overlaps(&inner),
        "Containing range should overlap subset"
    );
    assert!(
        inner.overlaps(&outer),
        "Subset should overlap containing range"
    );
}

#[test]
fn test_timerange_zero_timestamps() {
    let range = TimeRange::new(0, 0);
    assert!(range.contains(0), "Zero-point range should contain 0");
    assert!(!range.contains(1));
    assert!(!range.contains(-1));
}

#[test]
fn test_timerange_negative_timestamps() {
    let range = TimeRange::new(-1000, -100);
    assert!(range.contains(-500), "Should handle negative timestamps");
    assert!(range.contains(-1000), "Should include negative start");
    assert!(range.contains(-100), "Should include negative end");
    assert!(!range.contains(0), "Should not contain positive values");
}

#[test]
fn test_timerange_from_range() {
    let range: TimeRange = (100i64..200i64).into();
    assert_eq!(range.start, 100);
    assert_eq!(range.end, 200);
}

// =========================================================================
// WriteBuffer boundary tests
// =========================================================================

#[test]
fn test_write_buffer_empty_operations() {
    let mut buffer = WriteBuffer::new();
    assert!(buffer.is_empty());
    assert_eq!(buffer.row_count(), 0);
    assert_eq!(buffer.size_bytes(), 0);
    assert_eq!(buffer.batch_count(), 0);

    // Take from empty buffer should return empty vec
    let batches = buffer.take();
    assert!(batches.is_empty());
    assert!(buffer.is_empty());

    // Clear empty buffer should not panic
    buffer.clear();
    assert!(buffer.is_empty());
}

#[test]
fn test_write_buffer_multiple_takes() {
    let mut buffer = WriteBuffer::new();
    buffer.append(create_batch(10)).unwrap();

    let first_take = buffer.take();
    assert_eq!(first_take.len(), 1);
    assert!(buffer.is_empty());

    // Second take should be empty
    let second_take = buffer.take();
    assert!(second_take.is_empty());
    assert!(buffer.is_empty());
}

#[test]
fn test_write_buffer_size_tracking() {
    let mut buffer = WriteBuffer::new();
    let batch = create_batch(100);
    let batch_size = batch.get_array_memory_size();

    buffer.append(batch).unwrap();
    assert_eq!(buffer.size_bytes(), batch_size);
    assert_eq!(buffer.row_count(), 100);

    // Add another batch
    let batch2 = create_batch(50);
    let batch2_size = batch2.get_array_memory_size();
    buffer.append(batch2).unwrap();
    assert_eq!(buffer.size_bytes(), batch_size + batch2_size);
    assert_eq!(buffer.row_count(), 150);

    // After take, should reset
    let _ = buffer.take();
    assert_eq!(buffer.size_bytes(), 0);
    assert_eq!(buffer.row_count(), 0);
}

// =========================================================================
// Metadata edge case tests
// =========================================================================

#[tokio::test]
async fn test_get_nonexistent_chunk() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = S3MetadataClient::new(object_store, config);

    // With the unified catalog approach, an uninitialized metadata store
    // gracefully returns None for nonexistent chunks (the catalog falls back
    // to empty when neither catalog.json nor legacy metadata.json exist).
    let result = client.get_chunk("nonexistent.parquet").await.unwrap();
    assert!(
        result.is_none(),
        "Getting chunk from uninitialized metadata should return None"
    );

    // After registering any chunk, the catalog file is created
    let chunk = ChunkMetadata {
        path: "first.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 10,
        size_bytes: 100,
    };
    client.register_chunk(&chunk.path, &chunk).await.unwrap();

    // Querying for a nonexistent chunk should still return None
    let result = client.get_chunk("nonexistent.parquet").await.unwrap();
    assert!(
        result.is_none(),
        "Getting nonexistent chunk should return None"
    );
}

#[tokio::test]
async fn test_list_chunks_uninitialized_metadata() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = S3MetadataClient::new(object_store, config);

    // With the unified catalog approach, an uninitialized metadata store
    // gracefully returns an empty list (the catalog falls back to empty
    // when neither catalog.json nor legacy metadata.json exist).
    let result = client.list_chunks().await.unwrap();
    assert!(
        result.is_empty(),
        "Listing chunks from uninitialized metadata should return empty"
    );

    // After registering and then deleting a chunk, catalog file exists but has no chunks
    let chunk = ChunkMetadata {
        path: "temp.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 10,
        size_bytes: 100,
    };
    client.register_chunk(&chunk.path, &chunk).await.unwrap();
    client.delete_chunk("temp.parquet").await.unwrap();

    let chunks = client.list_chunks().await.unwrap();
    assert!(
        chunks.is_empty(),
        "Should return empty after all chunks deleted"
    );
}

#[tokio::test]
async fn test_get_chunks_empty_time_range() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = S3MetadataClient::new(object_store, config);

    // Register a chunk
    let chunk = ChunkMetadata {
        path: "chunk.parquet".to_string(),
        min_timestamp: 1000,
        max_timestamp: 2000,
        row_count: 100,
        size_bytes: 1024,
    };
    client.register_chunk(&chunk.path, &chunk).await.unwrap();

    // Query with time range that doesn't overlap
    let chunks = client.get_chunks(TimeRange::new(3000, 4000)).await.unwrap();
    assert!(
        chunks.is_empty(),
        "Non-overlapping time range should return no chunks"
    );
}

// =========================================================================
// MinIO CAS behavior tests
// =========================================================================

#[tokio::test]
async fn test_metadata_cas_fails_loudly_when_unsafe_overwrite_disabled() {
    let temp = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp.path()).unwrap());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "metadata/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let client = S3MetadataClient::new(object_store, config);

    let chunk = ChunkMetadata {
        path: "chunk.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 10,
        size_bytes: 100,
    };

    client
        .register_chunk(&chunk.path, &chunk)
        .await
        .expect("Initial write should succeed without CAS");

    let second = ChunkMetadata {
        path: "chunk-2.parquet".to_string(),
        min_timestamp: 2000,
        max_timestamp: 3000,
        row_count: 20,
        size_bytes: 200,
    };

    let result = client.register_chunk(&second.path, &second).await;
    assert!(
        result.is_err(),
        "CAS should fail without unsafe fallback enabled"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("CAS conditional writes are required"),
        "Error should clearly explain why write failed: {}",
        err
    );

    let chunks = client.list_chunks().await.unwrap();
    assert_eq!(
        chunks.len(),
        1,
        "Failed update must not be silently overwritten"
    );
    assert_eq!(chunks[0].chunk_path, "chunk.parquet");
}

#[tokio::test]
async fn test_metadata_cas_fallback_overwrite_when_enabled() {
    let temp = tempdir().unwrap();
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp.path()).unwrap());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "metadata/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: true,
    };

    let client = S3MetadataClient::new(object_store, config);

    let chunk = ChunkMetadata {
        path: "chunk.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 10,
        size_bytes: 100,
    };

    client
        .register_chunk(&chunk.path, &chunk)
        .await
        .expect("CAS fallback should allow initial write");

    let second = ChunkMetadata {
        path: "chunk-2.parquet".to_string(),
        min_timestamp: 2000,
        max_timestamp: 3000,
        row_count: 20,
        size_bytes: 200,
    };

    client
        .register_chunk(&second.path, &second)
        .await
        .expect("CAS fallback should allow updates");

    let chunks = client.list_chunks().await.unwrap();
    assert_eq!(chunks.len(), 2, "Fallback should persist updates");
}

#[tokio::test]
async fn test_delete_nonexistent_chunk() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = S3MetadataClient::new(object_store, config);

    // Deleting a chunk that doesn't exist should not error
    let result = client.delete_chunk("nonexistent.parquet").await;
    assert!(result.is_ok(), "Deleting nonexistent chunk should not fail");
}

#[tokio::test]
async fn test_register_chunk_with_zero_timestamps() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = S3MetadataClient::new(object_store, config);

    let chunk = ChunkMetadata {
        path: "zero_ts.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 0,
        row_count: 0,
        size_bytes: 0,
    };

    client.register_chunk(&chunk.path, &chunk).await.unwrap();

    let retrieved = client.get_chunk("zero_ts.parquet").await.unwrap();
    assert!(
        retrieved.is_some(),
        "Should retrieve chunk with zero timestamps"
    );
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.min_timestamp, 0);
    assert_eq!(retrieved.max_timestamp, 0);
    assert_eq!(retrieved.row_count, 0);
}

#[tokio::test]
async fn test_register_duplicate_chunk_path() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = S3MetadataClient::new(object_store, config);

    let chunk1 = ChunkMetadata {
        path: "same_path.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 100,
        size_bytes: 1024,
    };

    let chunk2 = ChunkMetadata {
        path: "same_path.parquet".to_string(),
        min_timestamp: 2000,
        max_timestamp: 3000,
        row_count: 200,
        size_bytes: 2048,
    };

    client.register_chunk(&chunk1.path, &chunk1).await.unwrap();
    client.register_chunk(&chunk2.path, &chunk2).await.unwrap();

    // Second registration should overwrite
    let retrieved = client.get_chunk("same_path.parquet").await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(
        retrieved.min_timestamp, 2000,
        "Should be overwritten with new data"
    );
    assert_eq!(retrieved.row_count, 200);
}

// =========================================================================
// Generation CAS edge cases
// =========================================================================

#[tokio::test]
async fn test_cas_stale_generation_error_format() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let shard = ShardMetadata {
        shard_id: "shard-1".to_string(),
        generation: 0,
        key_range: (vec![0u8; 8], vec![255u8; 8]),
        replicas: vec![ReplicaInfo {
            replica_id: "r1".to_string(),
            node_id: "n1".to_string(),
            is_leader: true,
        }],
        state: ShardState::Active,
        min_time: 0,
        max_time: 10000,
    };

    // Create with generation 0
    metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Try with stale generation
    let result = metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();

    // Verify error Display formatting
    let err_string = err.to_string();
    assert!(
        err_string.contains("generation"),
        "Error message should mention generation: {}",
        err_string
    );

    // Verify it's specifically a StaleGeneration variant
    match err {
        Error::StaleGeneration { expected, actual } => {
            assert_eq!(expected, 0, "Expected should be the stale value we passed");
            assert_eq!(actual, 1, "Actual should be the current generation");
        }
        _ => panic!("Expected StaleGeneration error, got: {:?}", err),
    }
}

#[tokio::test]
async fn test_cas_max_generation_values() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let shard = ShardMetadata {
        shard_id: "shard-large-gen".to_string(),
        generation: 0,
        key_range: (vec![0u8; 8], vec![255u8; 8]),
        replicas: vec![],
        state: ShardState::Active,
        min_time: 0,
        max_time: 10000,
    };

    // Initial creation
    metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Perform many updates to get a high generation number
    for gen in 1..=100u64 {
        let current = metadata
            .get_shard_metadata(&shard.shard_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.generation, gen);

        metadata
            .update_shard_metadata(&shard.shard_id, &current, gen)
            .await
            .unwrap();
    }

    let final_shard = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_shard.generation, 101, "Should track 101 generations");
}

// =========================================================================
// Compaction edge cases
// =========================================================================

#[tokio::test]
async fn test_compaction_with_single_source() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = Arc::new(S3MetadataClient::new(object_store, config));

    // Register source and target chunks
    let source = ChunkMetadata {
        path: "single_source.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 100,
        size_bytes: 1024,
    };
    client.register_chunk(&source.path, &source).await.unwrap();

    let target = ChunkMetadata {
        path: "target.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 100,
        size_bytes: 1024,
    };
    client.register_chunk(&target.path, &target).await.unwrap();

    // Compact with single source
    client
        .complete_compaction(&["single_source.parquet".to_string()], "target.parquet")
        .await
        .unwrap();

    // Source should be gone
    let source_check = client.get_chunk("single_source.parquet").await.unwrap();
    assert!(
        source_check.is_none(),
        "Source should be removed after compaction"
    );

    // Target should remain
    let target_check = client.get_chunk("target.parquet").await.unwrap();
    assert!(
        target_check.is_some(),
        "Target should remain after compaction"
    );
}

#[tokio::test]
async fn test_l0_candidates_empty() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = Arc::new(S3MetadataClient::new(object_store, config));

    // Initialize metadata by registering and compacting a chunk away
    let chunk = ChunkMetadata {
        path: "init_src.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 10,
        size_bytes: 100,
    };
    client.register_chunk(&chunk.path, &chunk).await.unwrap();
    let target = ChunkMetadata {
        path: "init_tgt.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 10,
        size_bytes: 100,
    };
    client.register_chunk(&target.path, &target).await.unwrap();
    client
        .complete_compaction(&["init_src.parquet".to_string()], "init_tgt.parquet")
        .await
        .unwrap();

    // Now L0 candidates should be empty (only L1 chunk remains)
    let candidates = client.get_l0_candidates(1).await.unwrap();
    assert!(
        candidates.is_empty(),
        "After compaction, L0 should be empty"
    );
}

#[tokio::test]
async fn test_level_candidates_nonexistent_level() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = S3MetadataClient::new(object_store, config);

    // Initialize metadata
    let chunk = ChunkMetadata {
        path: "l0_chunk.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 10,
        size_bytes: 100,
    };
    client.register_chunk(&chunk.path, &chunk).await.unwrap();

    // Query for high level with no data at that level
    let candidates = client.get_level_candidates(99, 1024).await.unwrap();
    assert!(
        candidates.is_empty(),
        "Non-existent level should have no candidates"
    );
}

// =========================================================================
// Split state edge cases
// =========================================================================

#[tokio::test]
async fn test_split_state_nonexistent_shard() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let split_state = metadata.get_split_state("nonexistent-shard").await.unwrap();
    assert!(
        split_state.is_none(),
        "Non-existent shard should have no split state"
    );
}

#[tokio::test]
async fn test_get_chunks_for_nonexistent_shard() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let chunks = metadata
        .get_chunks_for_shard("nonexistent-shard")
        .await
        .unwrap();
    assert!(
        chunks.is_empty(),
        "Non-existent shard should have no chunks"
    );
}

// =========================================================================
// Error type tests
// =========================================================================

#[test]
fn test_error_display_formats() {
    // Verify all error variants produce meaningful display strings
    let errors: Vec<Error> = vec![
        Error::Serialization("bad json".to_string()),
        Error::Config("bad config".to_string()),
        Error::InvalidSchema("missing column".to_string()),
        Error::MissingMetricName,
        Error::BufferFull,
        Error::Query("invalid sql".to_string()),
        Error::Metadata("corrupt metadata".to_string()),
        Error::Cache("cache miss".to_string()),
        Error::Timeout,
        Error::Internal("unexpected state".to_string()),
        Error::StaleGeneration {
            expected: 1,
            actual: 2,
        },
        Error::ShardMoved {
            new_location: "node-2".to_string(),
        },
        Error::RateLimitExceeded {
            tenant_id: "tenant-1".to_string(),
            limit: 1000,
        },
        Error::TooManySubscriptions,
        Error::Conflict,
        Error::TooManyRetries,
        Error::ShardNotFound("shard-1".to_string()),
    ];

    for err in &errors {
        let display = format!("{}", err);
        assert!(
            !display.is_empty(),
            "Error display should not be empty: {:?}",
            err
        );
    }

    // Check specific formats
    assert!(
        format!("{}", Error::BufferFull).contains("buffer"),
        "BufferFull should mention buffer"
    );
    assert!(
        format!("{}", Error::Conflict).contains("conflict"),
        "Conflict should mention conflict"
    );
    assert!(
        format!("{}", Error::TooManyRetries).contains("retr"),
        "TooManyRetries should mention retries"
    );
    assert!(
        format!("{}", Error::ShardNotFound("x".into())).contains("x"),
        "ShardNotFound should contain shard ID"
    );
}

#[test]
fn test_error_source_chain() {
    use std::error::Error as StdError;

    // Errors wrapping other errors should have a source
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
    let err = Error::Io(io_err);
    assert!(err.source().is_some(), "Io error should have a source");

    // Leaf errors should not have a source
    let err = Error::Timeout;
    assert!(err.source().is_none(), "Timeout should not have a source");

    let err = Error::BufferFull;
    assert!(
        err.source().is_none(),
        "BufferFull should not have a source"
    );
}

// =========================================================================
// Data integrity: write and read back
// =========================================================================

#[tokio::test]
async fn test_ingester_flush_produces_valid_parquet() {
    use arrow_array::TimestampNanosecondArray;
    use arrow_schema::TimeUnit;
    use cardinalsin::schema::MetricSchema;

    let object_store = Arc::new(InMemory::new());
    let metadata: Arc<dyn MetadataClient> = Arc::new(LocalMetadataClient::new());
    let storage_config = cardinalsin::StorageConfig::default();

    let config = IngesterConfig {
        flush_row_count: 50, // Low threshold to trigger flush
        ..Default::default()
    };

    let schema = MetricSchema::default_metrics();
    let ingester = Ingester::new(
        config,
        object_store.clone(),
        metadata.clone(),
        storage_config,
        schema,
    );

    // Create a batch with timestamp in the expected format
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let batch_schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value_f64", DataType::Float64, true),
    ]));

    let timestamps: Vec<i64> = (0..60).map(|i| now + i * 1_000_000).collect();
    let names: Vec<&str> = (0..60).map(|_| "test_metric").collect();
    let values: Vec<f64> = (0..60).map(|i| i as f64 * 0.5).collect();

    let batch = RecordBatch::try_new(
        batch_schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap();

    // Write batch (should trigger flush since 60 > 50 threshold)
    ingester.write(batch).await.unwrap();

    // Verify buffer is empty (flush happened)
    let stats = ingester.buffer_stats().await;
    assert_eq!(stats.row_count, 0, "Buffer should be empty after flush");

    // Verify metadata has the registered chunk
    let chunks = metadata.list_chunks().await.unwrap();
    assert_eq!(
        chunks.len(),
        1,
        "Should have 1 chunk registered in metadata"
    );
    assert_eq!(chunks[0].row_count, 60, "Chunk should have 60 rows");

    // Verify the actual parquet file exists in object store
    let chunk_path = &chunks[0].chunk_path;
    let path = object_store::path::Path::from(chunk_path.as_str());
    let get_result = object_store.get(&path).await.unwrap();
    let data = get_result.bytes().await.unwrap();

    // Read back the parquet file and verify contents
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let read_batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
        .unwrap();
    let total_rows: usize = read_batches
        .iter()
        .map(|b: &RecordBatch| b.num_rows())
        .sum();

    assert_eq!(total_rows, 60, "Parquet file should contain 60 rows");
}

#[tokio::test]
async fn test_ingester_buffer_below_threshold_no_flush() {
    use arrow_array::TimestampNanosecondArray;
    use arrow_schema::TimeUnit;
    use cardinalsin::schema::MetricSchema;

    let object_store = Arc::new(InMemory::new());
    let metadata: Arc<dyn MetadataClient> = Arc::new(LocalMetadataClient::new());
    let storage_config = cardinalsin::StorageConfig::default();

    let config = IngesterConfig {
        flush_row_count: 1000, // High threshold - no flush
        ..Default::default()
    };

    let schema = MetricSchema::default_metrics();
    let ingester = Ingester::new(
        config,
        object_store,
        metadata.clone(),
        storage_config,
        schema,
    );

    // Create a small batch
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let batch_schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value_f64", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        batch_schema,
        vec![
            Arc::new(
                TimestampNanosecondArray::from(vec![now, now + 1_000_000]).with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec!["cpu", "mem"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        ],
    )
    .unwrap();

    ingester.write(batch).await.unwrap();

    // Buffer should still have data (no flush)
    let stats = ingester.buffer_stats().await;
    assert_eq!(
        stats.row_count, 2,
        "Buffer should retain rows below threshold"
    );
    assert_eq!(stats.batch_count, 1);

    // No chunks should be registered
    let chunks = metadata.list_chunks().await.unwrap();
    assert!(chunks.is_empty(), "No flush means no chunks registered");
}

// =========================================================================
// Compaction job lifecycle tests
// =========================================================================

#[tokio::test]
async fn test_compaction_job_lifecycle() {
    use cardinalsin::metadata::{CompactionJob, CompactionStatus};

    let metadata = Arc::new(LocalMetadataClient::new());

    // Create a compaction job
    let job = CompactionJob {
        id: "job-1".to_string(),
        source_chunks: vec!["chunk1.parquet".to_string(), "chunk2.parquet".to_string()],
        target_level: 1,
        status: CompactionStatus::Pending,
    };
    metadata.create_compaction_job(job.clone()).await.unwrap();

    // Verify pending jobs
    let pending = metadata.get_pending_compaction_jobs().await.unwrap();
    assert!(!pending.is_empty(), "Should have pending job");

    // Update to in-progress
    metadata
        .update_compaction_status("job-1", CompactionStatus::InProgress)
        .await
        .unwrap();

    // Update to completed
    metadata
        .update_compaction_status("job-1", CompactionStatus::Completed)
        .await
        .unwrap();

    // Completed jobs should not appear in pending
    let pending = metadata.get_pending_compaction_jobs().await.unwrap();
    let still_pending = pending.iter().any(|j| j.id == "job-1");
    assert!(
        !still_pending,
        "Completed job should not appear in pending list"
    );
}

// =========================================================================
// Concurrent metadata registration with verification
// =========================================================================

#[tokio::test]
async fn test_concurrent_registration_data_integrity() {
    use tokio::task::JoinSet;

    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = Arc::new(S3MetadataClient::new(object_store, config));

    let mut tasks = JoinSet::new();
    let num_tasks = 20;

    for i in 0..num_tasks {
        let client = client.clone();
        tasks.spawn(async move {
            let chunk = ChunkMetadata {
                path: format!("integrity_chunk_{}.parquet", i),
                min_timestamp: i * 1000,
                max_timestamp: (i + 1) * 1000,
                row_count: (i + 1) as u64 * 100, // Unique row count per chunk
                size_bytes: (i + 1) as u64 * 1024,
            };
            client.register_chunk(&chunk.path, &chunk).await.unwrap();
            (i, chunk.row_count)
        });
    }

    let mut expected: std::collections::HashMap<i64, u64> = std::collections::HashMap::new();
    while let Some(result) = tasks.join_next().await {
        let (i, row_count) = result.unwrap();
        expected.insert(i, row_count);
    }

    // Verify all chunks are present and have correct data
    let chunks = client.list_chunks().await.unwrap();
    assert_eq!(
        chunks.len(),
        num_tasks as usize,
        "All chunks should be registered"
    );

    for chunk in &chunks {
        let retrieved = client.get_chunk(&chunk.chunk_path).await.unwrap();
        assert!(
            retrieved.is_some(),
            "Each registered chunk should be retrievable"
        );
        let retrieved = retrieved.unwrap();
        assert!(
            retrieved.row_count > 0,
            "Row count should be preserved: {}",
            chunk.chunk_path
        );
    }
}
