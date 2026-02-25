//! Tests for coverage gaps identified in the test audit
//!
//! These tests cover critical paths not exercised by existing tests:
//! - Buffer overflow rejection (Error::BufferFull)
//! - Ingester broadcast to streaming subscribers
//! - Compactor backpressure signaling
//! - Write -> compact -> verify data integrity roundtrip
//! - ChunkMerger error paths
//! - ParquetWriter edge cases
//! - Split lifecycle (start -> progress -> complete)
//! - ShardMonitor rolling average edge cases
//! - Error type conversion (From implementations)
//! - Time index rebuild
//! - Compaction level tracking through metadata

use cardinalsin::compactor::{CompactorConfig, Level};
use cardinalsin::ingester::{ChunkMetadata, Ingester, IngesterConfig, ParquetWriter, TopicFilter};
use cardinalsin::metadata::{
    CompactionJob, CompactionStatus, LocalMetadataClient, MetadataClient, S3MetadataClient,
    S3MetadataConfig, TimeRange,
};
use cardinalsin::sharding::{HotShardConfig, ShardMonitor};
use cardinalsin::Error;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// Helper: create a timestamped batch with proper nanosecond timestamps
fn create_ts_batch(n: usize, start_ts: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value_f64", DataType::Float64, true),
    ]));

    let timestamps: Vec<i64> = (0..n as i64).map(|i| start_ts + i * 1_000_000).collect();
    let names: Vec<&str> = (0..n).map(|_| "test_metric").collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 0.5).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

/// Helper: create a batch with an intentionally different value column type.
fn create_mixed_type_batch(n: usize, start_ts: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value_f64", DataType::Int64, true),
    ]));

    let timestamps: Vec<i64> = (0..n as i64).map(|i| start_ts + i * 1_000_000).collect();
    let names: Vec<&str> = (0..n).map(|_| "test_metric").collect();
    let values: Vec<i64> = (0..n as i64).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

/// Helper: create an ingester with specified thresholds
fn create_test_ingester(
    flush_row_count: usize,
    max_buffer_size: usize,
) -> (Ingester, Arc<InMemory>, Arc<dyn MetadataClient>) {
    let object_store = Arc::new(InMemory::new());
    let metadata: Arc<dyn MetadataClient> = Arc::new(LocalMetadataClient::new());
    let storage_config = cardinalsin::StorageConfig::default();
    let schema = cardinalsin::schema::MetricSchema::default_metrics();

    let config = IngesterConfig {
        flush_row_count,
        max_buffer_size_bytes: max_buffer_size,
        ..Default::default()
    };

    let ingester = Ingester::new(
        config,
        object_store.clone(),
        metadata.clone(),
        storage_config,
        schema,
    );

    (ingester, object_store, metadata)
}

// =========================================================================
// Buffer overflow (Error::BufferFull) tests
// =========================================================================

#[tokio::test]
async fn test_buffer_full_rejection() {
    // Create a batch and check its actual memory size to set a realistic limit
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let probe_batch = create_ts_batch(10, now);
    let single_batch_size = probe_batch.get_array_memory_size();

    // Create ingester with buffer limit that allows 1 batch but rejects a 2nd
    let (ingester, _, _) = create_test_ingester(
        1_000_000,             // high flush threshold so no auto-flush
        single_batch_size + 1, // just barely allows one batch
    );

    // First write should succeed
    let batch1 = create_ts_batch(10, now);
    let first_result = ingester.write(batch1).await;
    assert!(first_result.is_ok(), "First write should succeed");

    // Second write should be rejected with BufferFull
    let batch2 = create_ts_batch(10, now + 100_000_000);
    let result = ingester.write(batch2).await;
    assert!(result.is_err(), "Second write should be rejected");

    match result.unwrap_err() {
        Error::BufferFull => {} // expected
        other => panic!("Expected BufferFull error, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_buffer_full_after_accumulation() {
    // Create ingester with small buffer that accumulates
    let (ingester, _, _) = create_test_ingester(
        1_000_000, // no auto-flush
        5000,      // small buffer
    );

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    // Write batches until buffer fills up
    let mut writes = 0;
    let mut buffer_full = false;
    for i in 0..100 {
        let batch = create_ts_batch(10, now + i * 10_000_000);
        match ingester.write(batch).await {
            Ok(_) => writes += 1,
            Err(Error::BufferFull) => {
                buffer_full = true;
                break;
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    assert!(buffer_full, "Should eventually get BufferFull");
    assert!(
        writes > 0,
        "Should have accepted at least some writes before full"
    );
}

// =========================================================================
// Ingester broadcast subscriber tests
// =========================================================================

#[tokio::test]
async fn test_ingester_broadcast_on_flush() {
    let (ingester, _, _) = create_test_ingester(
        10, // low threshold to trigger flush
        100_000_000,
    );

    // Subscribe before writing
    let mut rx = ingester.subscribe();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let batch = create_ts_batch(20, now); // 20 > 10 threshold, triggers flush

    ingester.write(batch).await.unwrap();

    // Subscriber should receive the flushed data
    let received = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
    assert!(received.is_ok(), "Should receive broadcast within timeout");
    let batch = received.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 20, "Broadcast should contain all rows");
}

#[tokio::test]
async fn test_ingester_no_broadcast_without_flush() {
    let (ingester, _, _) = create_test_ingester(
        1000, // high threshold, no flush
        100_000_000,
    );

    let mut rx = ingester.subscribe();

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let batch = create_ts_batch(5, now); // 5 < 1000, no flush

    ingester.write(batch).await.unwrap();

    // No broadcast should happen since no flush occurred
    let received = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
    assert!(
        received.is_err(),
        "Should not receive broadcast without flush"
    );
}

#[tokio::test]
async fn test_ingester_topic_broadcast_on_flush() {
    let (ingester, _, _) = create_test_ingester(
        10, // low threshold
        100_000_000,
    );

    // Subscribe with a filter that matches our test metric
    let filter = TopicFilter::Metrics(vec!["test_metric".to_string()]);
    let mut filtered_rx = ingester.subscribe_filtered(filter).await;

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let batch = create_ts_batch(20, now);

    ingester.write(batch).await.unwrap();

    // Filtered subscriber should receive matching data
    let received = tokio::time::timeout(Duration::from_secs(1), filtered_rx.recv()).await;
    assert!(
        received.is_ok(),
        "Filtered subscriber should receive matching broadcast"
    );
}

// =========================================================================
// Write -> flush -> verify data integrity roundtrip
// =========================================================================

#[tokio::test]
async fn test_write_flush_verify_roundtrip() {
    let (ingester, object_store, metadata) = create_test_ingester(
        50, // flush after 50 rows
        100_000_000,
    );

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    // Write enough data to trigger a flush
    let batch = create_ts_batch(60, now);
    ingester.write(batch).await.unwrap();

    // Verify buffer is empty
    let stats = ingester.buffer_stats().await;
    assert_eq!(stats.row_count, 0, "Buffer should be empty after flush");

    // Verify chunk is registered in metadata
    let chunks = metadata.list_chunks().await.unwrap();
    assert_eq!(chunks.len(), 1, "Should have 1 chunk");
    assert_eq!(chunks[0].row_count, 60);

    // Verify the actual parquet file is readable
    let path = object_store::path::Path::from(chunks[0].chunk_path.as_str());
    let data = object_store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

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
    assert_eq!(total_rows, 60, "Round-tripped data should have 60 rows");

    // Verify schema has expected columns
    let schema = read_batches[0].schema();
    assert!(schema.column_with_name("timestamp").is_some());
    assert!(schema.column_with_name("metric_name").is_some());
    assert!(schema.column_with_name("value_f64").is_some());
}

#[tokio::test]
async fn test_multiple_flush_cycles() {
    let (ingester, _, metadata) = create_test_ingester(
        20, // flush every 20 rows
        100_000_000,
    );

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    // Write 3 batches that each trigger a flush
    for i in 0..3 {
        let batch = create_ts_batch(25, now + i * 100_000_000);
        ingester.write(batch).await.unwrap();
    }

    // Should have 3 chunks registered
    let chunks = metadata.list_chunks().await.unwrap();
    assert_eq!(chunks.len(), 3, "Should have 3 chunks from 3 flushes");

    // Each chunk should have the correct row count
    for chunk in &chunks {
        assert_eq!(chunk.row_count, 25);
    }
}

#[tokio::test]
async fn test_schema_mismatch_flush_boundary() {
    let (ingester, _, metadata) = create_test_ingester(
        2, // flush when 2 rows are buffered
        100_000_000,
    );

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    // First batch uses value_f64 as Float64.
    ingester.write(create_ts_batch(1, now)).await.unwrap();

    // Second/third batches use value_f64 as Int64. Previously this caused
    // concat failures when mixed with the first batch in one flush.
    ingester
        .write(create_mixed_type_batch(1, now + 10_000_000))
        .await
        .unwrap();
    ingester
        .write(create_mixed_type_batch(1, now + 20_000_000))
        .await
        .unwrap();

    let stats = ingester.buffer_stats().await;
    assert_eq!(stats.row_count, 0, "all rows should flush successfully");

    let chunks = metadata.list_chunks().await.unwrap();
    assert_eq!(chunks.len(), 2, "batches should flush in schema-homogeneous groups");
}

// =========================================================================
// ParquetWriter edge cases
// =========================================================================

#[test]
fn test_parquet_writer_empty_batches_slice() {
    let writer = ParquetWriter::new();

    let result = writer.write_batches(&[]);
    assert!(result.is_err(), "Writing empty batch slice should error");

    match result.unwrap_err() {
        Error::InvalidSchema(msg) => {
            assert!(
                msg.contains("No batches"),
                "Should mention no batches: {}",
                msg
            );
        }
        other => panic!("Expected InvalidSchema, got: {:?}", other),
    }
}

#[test]
fn test_parquet_writer_multiple_batches_roundtrip() {
    let writer = ParquetWriter::new();

    let batch1 = create_ts_batch(100, 1_000_000_000);
    let batch2 = create_ts_batch(200, 2_000_000_000);

    let bytes = writer.write_batches(&[batch1, batch2]).unwrap();
    assert!(!bytes.is_empty());

    // Read back and verify total rows
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap();

    let total_rows: usize = reader.map(|r| r.unwrap().num_rows()).sum();
    assert_eq!(total_rows, 300, "Should have 300 total rows from 2 batches");
}

// =========================================================================
// ChunkMerger error paths
// =========================================================================

#[tokio::test]
async fn test_chunk_merger_empty_paths() {
    use cardinalsin::compactor::ChunkMerger;

    let store = Arc::new(InMemory::new());
    let merger = ChunkMerger::new(store);

    let result = merger.merge(&[]).await;
    assert!(result.is_err(), "Merging empty paths should error");

    match result.unwrap_err() {
        Error::InvalidSchema(msg) => {
            assert!(msg.contains("No data"), "Should mention no data: {}", msg);
        }
        other => panic!("Expected InvalidSchema, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_chunk_merger_nonexistent_chunk() {
    use cardinalsin::compactor::ChunkMerger;

    let store = Arc::new(InMemory::new());
    let merger = ChunkMerger::new(store);

    let result = merger.merge(&["nonexistent.parquet".to_string()]).await;
    assert!(result.is_err(), "Merging nonexistent chunk should error");
}

// =========================================================================
// Split lifecycle tests (start -> progress -> complete)
// =========================================================================

#[tokio::test]
async fn test_split_full_lifecycle() {
    use cardinalsin::sharding::SplitPhase;

    let metadata = Arc::new(LocalMetadataClient::new());
    let shard_id = "shard-lifecycle";

    // Phase 1: Start split
    metadata
        .start_split(
            shard_id,
            vec!["shard-a".to_string(), "shard-b".to_string()],
            vec![0, 0, 0, 0, 0, 0, 0, 128], // midpoint split
        )
        .await
        .unwrap();

    // Verify split state exists and is in Preparation phase
    let state = metadata.get_split_state(shard_id).await.unwrap();
    assert!(state.is_some(), "Split state should exist");
    let state = state.unwrap();
    assert!(
        matches!(state.phase, SplitPhase::Preparation),
        "Should be in Preparation phase"
    );
    assert_eq!(state.new_shards.len(), 2);

    // Phase 2: Update to DualWrite
    metadata
        .update_split_progress(shard_id, 0.0, SplitPhase::DualWrite)
        .await
        .unwrap();

    let state = metadata.get_split_state(shard_id).await.unwrap().unwrap();
    assert!(matches!(state.phase, SplitPhase::DualWrite));

    // Phase 3: Update to Backfill with progress
    metadata
        .update_split_progress(shard_id, 0.5, SplitPhase::Backfill)
        .await
        .unwrap();

    let state = metadata.get_split_state(shard_id).await.unwrap().unwrap();
    assert!(matches!(state.phase, SplitPhase::Backfill));
    assert!((state.backfill_progress - 0.5).abs() < 0.001);

    // Phase 4: Update to Cutover
    metadata
        .update_split_progress(shard_id, 1.0, SplitPhase::Cutover)
        .await
        .unwrap();

    // Phase 5: Complete split
    metadata.complete_split(shard_id).await.unwrap();

    // Verify split state is gone
    let state = metadata.get_split_state(shard_id).await.unwrap();
    assert!(
        state.is_none(),
        "Split state should be removed after completion"
    );
}

#[tokio::test]
async fn test_split_progress_on_nonexistent_shard() {
    use cardinalsin::sharding::SplitPhase;

    let metadata = Arc::new(LocalMetadataClient::new());

    // Updating progress for non-existent split should not error (it's a no-op)
    let result = metadata
        .update_split_progress("nonexistent", 0.5, SplitPhase::Backfill)
        .await;
    assert!(
        result.is_ok(),
        "Update progress on non-existent should be ok (no-op)"
    );
}

#[tokio::test]
async fn test_complete_split_on_nonexistent_shard() {
    let metadata = Arc::new(LocalMetadataClient::new());

    // Completing a non-existent split should succeed (idempotent)
    let result = metadata.complete_split("nonexistent").await;
    assert!(
        result.is_ok(),
        "Completing non-existent split should succeed"
    );
}

// =========================================================================
// ShardMonitor metrics edge cases (RollingAverage is internal, tested via monitor)
// =========================================================================

#[test]
fn test_shard_monitor_metrics_after_writes() {
    let monitor = ShardMonitor::new(HotShardConfig::default());
    let shard_id = "shard-metrics".to_string();

    // Record several writes
    for _ in 0..10 {
        monitor.record_write(&shard_id, 1000, Duration::from_millis(5));
    }

    let metrics = monitor.get_metrics(&shard_id).unwrap();
    assert!(!metrics.write_qps.is_empty(), "Should have QPS samples");
    assert!(
        !metrics.bytes_per_sec.is_empty(),
        "Should have bytes samples"
    );
    assert!(
        !metrics.p99_latency.is_empty(),
        "Should have latency samples"
    );
    assert!(
        metrics.cpu_utilization.is_empty(),
        "Should have no CPU samples yet"
    );
}

#[test]
fn test_shard_monitor_cpu_recording() {
    let monitor = ShardMonitor::new(HotShardConfig::default());
    let shard_id = "shard-cpu".to_string();

    monitor.record_cpu(&shard_id, 0.65);
    monitor.record_cpu(&shard_id, 0.70);
    monitor.record_cpu(&shard_id, 0.75);

    let metrics = monitor.get_metrics(&shard_id).unwrap();
    assert!(!metrics.cpu_utilization.is_empty());
}

// =========================================================================
// ShardMonitor behavior tests
// =========================================================================

#[test]
fn test_shard_monitor_no_data() {
    let monitor = ShardMonitor::new(HotShardConfig::default());
    let actions = monitor.evaluate_shards();
    assert!(actions.is_empty(), "No shards = no actions");
}

#[test]
fn test_shard_monitor_cool_shard() {
    let monitor = ShardMonitor::new(HotShardConfig::default());

    // Record a single small write - should not be hot
    let shard_id = "shard-1".to_string();
    monitor.record_write(&shard_id, 100, Duration::from_millis(1));

    let actions = monitor.evaluate_shards();
    assert!(actions.is_empty(), "Low-traffic shard should not be hot");
}

#[test]
fn test_shard_monitor_metrics_retrieval() {
    let monitor = ShardMonitor::new(HotShardConfig::default());

    // No metrics for unknown shard
    assert!(monitor.get_metrics(&"unknown".to_string()).is_none());

    // Record a write, then verify metrics exist
    let shard_id = "shard-1".to_string();
    monitor.record_write(&shard_id, 1000, Duration::from_millis(5));
    let metrics = monitor.get_metrics(&"shard-1".to_string());
    assert!(
        metrics.is_some(),
        "Should have metrics after recording write"
    );
}

// =========================================================================
// Compactor backpressure tests
// =========================================================================

#[test]
fn test_compactor_backpressure_initial_state() {
    use cardinalsin::compactor::Compactor;

    let object_store = Arc::new(InMemory::new());
    let metadata: Arc<dyn MetadataClient> = Arc::new(LocalMetadataClient::new());
    let storage_config = cardinalsin::StorageConfig::default();
    let shard_monitor = Arc::new(ShardMonitor::new(HotShardConfig::default()));

    let compactor = Compactor::new(
        CompactorConfig::default(),
        object_store,
        metadata,
        storage_config,
        shard_monitor,
    );

    let bp = compactor.backpressure();
    assert_eq!(bp.l0_pending_files, 0);
    assert!(!bp.is_behind);
    assert_eq!(bp.recommended_delay_ms, 0);
}

// =========================================================================
// Error type conversion tests (From implementations)
// =========================================================================

#[test]
fn test_error_from_arrow() {
    let arrow_err = arrow::error::ArrowError::ComputeError("test".to_string());
    let err: Error = arrow_err.into();
    match err {
        Error::Arrow(_) => {}
        other => panic!("Expected Arrow error, got: {:?}", other),
    }
}

#[test]
fn test_error_from_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let err: Error = io_err.into();
    match err {
        Error::Io(_) => {}
        other => panic!("Expected Io error, got: {:?}", other),
    }
}

#[test]
fn test_error_from_serde_json() {
    let json_err = serde_json::from_str::<String>("not valid json").unwrap_err();
    let err: Error = json_err.into();
    match err {
        Error::Serialization(msg) => {
            assert!(!msg.is_empty(), "Should contain error message");
        }
        other => panic!("Expected Serialization error, got: {:?}", other),
    }
}

// =========================================================================
// Time index rebuild
// =========================================================================

#[tokio::test]
async fn test_time_index_rebuild() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = S3MetadataClient::new(object_store, config);

    // Register chunks spanning different hours
    let nanos_per_hour = 3_600_000_000_000i64;
    for i in 0..3 {
        let chunk = ChunkMetadata {
            path: format!("chunk_{}.parquet", i),
            min_timestamp: i * nanos_per_hour,
            max_timestamp: (i + 1) * nanos_per_hour - 1,
            row_count: 100,
            size_bytes: 1024,
        };
        client.register_chunk(&chunk.path, &chunk).await.unwrap();
    }

    // Rebuild the time index
    client.rebuild_time_index().await.unwrap();

    // Verify chunks are still queryable after rebuild
    let range = TimeRange::new(0, nanos_per_hour * 3);
    let chunks = client.get_chunks(range).await.unwrap();
    assert_eq!(
        chunks.len(),
        3,
        "All 3 chunks should be found after rebuild"
    );
}

// =========================================================================
// Compaction level tracking through S3MetadataClient
// =========================================================================

#[tokio::test]
async fn test_compaction_level_tracking_through_metadata() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = Arc::new(S3MetadataClient::new(object_store, config));

    // Register 3 L0 chunks
    for i in 0..3 {
        let chunk = ChunkMetadata {
            path: format!("l0_{}.parquet", i),
            min_timestamp: i * 1000,
            max_timestamp: (i + 1) * 1000 - 1,
            row_count: 10,
            size_bytes: 100,
        };
        client.register_chunk(&chunk.path, &chunk).await.unwrap();
    }

    // Register the target chunk for compaction
    let target = ChunkMetadata {
        path: "l1_target.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 2999,
        row_count: 30,
        size_bytes: 300,
    };
    client.register_chunk(&target.path, &target).await.unwrap();

    // Verify L0 candidates include our chunks
    let l0_candidates = client.get_l0_candidates(1).await.unwrap();
    let total_l0: usize = l0_candidates.iter().map(|g| g.len()).sum();
    assert!(
        total_l0 >= 3,
        "Should have at least 3 L0 chunks: {}",
        total_l0
    );

    // Complete compaction: L0 sources -> L1 target
    let sources: Vec<String> = (0..3).map(|i| format!("l0_{}.parquet", i)).collect();
    client
        .complete_compaction(&sources, "l1_target.parquet")
        .await
        .unwrap();

    // Verify source chunks are removed
    for i in 0..3 {
        let chunk = client
            .get_chunk(&format!("l0_{}.parquet", i))
            .await
            .unwrap();
        assert!(chunk.is_none(), "Source L0 chunk {} should be removed", i);
    }

    // Verify target is promoted to L1 (level should be max(0,0,0) + 1 = 1)
    let metadata = client.load_chunk_metadata().await.unwrap();
    let target_meta = metadata.get("l1_target.parquet");
    assert!(target_meta.is_some(), "Target chunk should exist");
    assert_eq!(target_meta.unwrap().level, 1, "Target should be L1");

    // L0 candidates should now be empty
    let l0_after = client.get_l0_candidates(1).await.unwrap();
    assert!(l0_after.is_empty(), "No L0 chunks should remain");
}

// =========================================================================
// Compaction job status transitions
// =========================================================================

#[tokio::test]
async fn test_compaction_job_failed_status() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let job = CompactionJob {
        id: "failing-job".to_string(),
        source_chunks: vec!["a.parquet".to_string()],
        target_level: 0,
        status: CompactionStatus::Pending,
        created_at: None,
    };
    metadata.create_compaction_job(job).await.unwrap();

    // Transition to InProgress
    metadata
        .update_compaction_status("failing-job", CompactionStatus::InProgress)
        .await
        .unwrap();

    // Transition to Failed
    metadata
        .update_compaction_status("failing-job", CompactionStatus::Failed)
        .await
        .unwrap();

    // Failed jobs should not appear in pending
    let pending = metadata.get_pending_compaction_jobs().await.unwrap();
    assert!(
        !pending.iter().any(|j| j.id == "failing-job"),
        "Failed job should not be in pending list"
    );
}

#[tokio::test]
async fn test_multiple_compaction_jobs() {
    let metadata = Arc::new(LocalMetadataClient::new());

    // Create 3 jobs
    for i in 0..3 {
        let job = CompactionJob {
            id: format!("job-{}", i),
            source_chunks: vec![format!("chunk_{}.parquet", i)],
            target_level: 0,
            status: CompactionStatus::Pending,
            created_at: None,
        };
        metadata.create_compaction_job(job).await.unwrap();
    }

    // All 3 should be pending
    let pending = metadata.get_pending_compaction_jobs().await.unwrap();
    assert_eq!(pending.len(), 3, "Should have 3 pending jobs");

    // Complete first, fail second, leave third
    metadata
        .update_compaction_status("job-0", CompactionStatus::Completed)
        .await
        .unwrap();
    metadata
        .update_compaction_status("job-1", CompactionStatus::Failed)
        .await
        .unwrap();

    // Only job-2 should be pending
    let pending = metadata.get_pending_compaction_jobs().await.unwrap();
    assert_eq!(pending.len(), 1, "Only 1 job should remain pending");
    assert_eq!(pending[0].id, "job-2");
}

// =========================================================================
// Level enum tests
// =========================================================================

#[test]
fn test_level_enum_values() {
    assert_eq!(Level::L0.as_u32(), 0);
    assert_eq!(Level::L(1).as_u32(), 1);
    assert_eq!(Level::L(3).as_u32(), 3);
    assert_eq!(Level::L(99).as_u32(), 99);
}

// =========================================================================
// S3MetadataClient: shard metadata operations
// =========================================================================

#[tokio::test]
async fn test_s3_shard_metadata_lifecycle() {
    use cardinalsin::sharding::{ReplicaInfo, ShardMetadata, ShardState};

    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = Arc::new(S3MetadataClient::new(object_store, config));

    let shard = ShardMetadata {
        shard_id: "shard-s3-test".to_string(),
        generation: 0,
        key_range: (vec![0u8; 8], vec![255u8; 8]),
        replicas: vec![ReplicaInfo {
            replica_id: "r1".to_string(),
            node_id: "node-1".to_string(),
            is_leader: true,
        }],
        state: ShardState::Active,
        min_time: 0,
        max_time: 100_000,
    };

    // Create shard (generation 0 -> 1)
    client
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Verify it exists
    let retrieved = client.get_shard_metadata("shard-s3-test").await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.generation, 1);
    assert_eq!(retrieved.replicas.len(), 1);

    // Update with correct generation (1 -> 2)
    client
        .update_shard_metadata("shard-s3-test", &retrieved, 1)
        .await
        .unwrap();

    // Verify generation incremented
    let updated = client
        .get_shard_metadata("shard-s3-test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.generation, 2);

    // Stale generation should fail
    let result = client
        .update_shard_metadata("shard-s3-test", &updated, 1) // stale: actual is 2
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::StaleGeneration { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        other => panic!("Expected StaleGeneration, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_s3_shard_not_found() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = S3MetadataClient::new(object_store, config);

    let result = client.get_shard_metadata("nonexistent").await.unwrap();
    assert!(result.is_none(), "Nonexistent shard should return None");
}

// =========================================================================
// S3MetadataClient: split state operations
// =========================================================================

#[tokio::test]
async fn test_s3_split_lifecycle() {
    use cardinalsin::sharding::SplitPhase;

    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };
    let client = Arc::new(S3MetadataClient::new(object_store, config));

    // Start split via S3 metadata client
    client
        .start_split(
            "split-shard",
            vec!["new-a".to_string(), "new-b".to_string()],
            vec![0, 0, 0, 0, 0, 0, 0, 128],
        )
        .await
        .unwrap();

    // Verify state
    let state = client.get_split_state("split-shard").await.unwrap();
    assert!(state.is_some());
    let state = state.unwrap();
    assert!(matches!(state.phase, SplitPhase::Preparation));
    assert_eq!(state.new_shards, vec!["new-a", "new-b"]);

    // Update progress
    client
        .update_split_progress("split-shard", 0.75, SplitPhase::Backfill)
        .await
        .unwrap();

    let state = client
        .get_split_state("split-shard")
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(state.phase, SplitPhase::Backfill));
    assert!((state.backfill_progress - 0.75).abs() < 0.001);

    // Complete
    client.complete_split("split-shard").await.unwrap();

    let state = client.get_split_state("split-shard").await.unwrap();
    assert!(state.is_none(), "Split state should be removed");
}

// =========================================================================
// Ingester buffer stats
// =========================================================================

#[tokio::test]
async fn test_ingester_buffer_stats_accuracy() {
    let (ingester, _, _) = create_test_ingester(
        1_000_000, // no auto-flush
        100_000_000,
    );

    let stats = ingester.buffer_stats().await;
    assert_eq!(stats.row_count, 0);
    assert_eq!(stats.size_bytes, 0);
    assert_eq!(stats.batch_count, 0);

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    // Write first batch
    ingester.write(create_ts_batch(10, now)).await.unwrap();
    let stats = ingester.buffer_stats().await;
    assert_eq!(stats.row_count, 10);
    assert_eq!(stats.batch_count, 1);
    assert!(stats.size_bytes > 0);

    // Write second batch
    ingester
        .write(create_ts_batch(20, now + 100_000_000))
        .await
        .unwrap();
    let stats = ingester.buffer_stats().await;
    assert_eq!(stats.row_count, 30);
    assert_eq!(stats.batch_count, 2);
}

// =========================================================================
// Concurrent operations on S3MetadataClient
// =========================================================================

#[tokio::test]
async fn test_concurrent_split_and_chunk_registration() {
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

    // Register chunks concurrently
    for i in 0..10 {
        let client = client.clone();
        tasks.spawn(async move {
            let chunk = ChunkMetadata {
                path: format!("concurrent_{}.parquet", i),
                min_timestamp: i * 1000,
                max_timestamp: (i + 1) * 1000,
                row_count: 10,
                size_bytes: 100,
            };
            client.register_chunk(&chunk.path, &chunk).await.unwrap();
        });
    }

    // Wait for all registrations
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    // Start a split concurrently with more registrations
    let client2 = client.clone();
    let split_task = tokio::spawn(async move {
        client2
            .start_split(
                "busy-shard",
                vec!["new-1".to_string(), "new-2".to_string()],
                vec![0, 0, 0, 0, 0, 0, 0, 128],
            )
            .await
            .unwrap();
    });

    // More registrations while split is happening
    for i in 10..15 {
        let chunk = ChunkMetadata {
            path: format!("concurrent_{}.parquet", i),
            min_timestamp: i * 1000,
            max_timestamp: (i + 1) * 1000,
            row_count: 10,
            size_bytes: 100,
        };
        client.register_chunk(&chunk.path, &chunk).await.unwrap();
    }

    split_task.await.unwrap();

    // Verify all 15 chunks exist
    let chunks = client.list_chunks().await.unwrap();
    assert_eq!(chunks.len(), 15, "All 15 chunks should be registered");

    // Verify split state exists
    let state = client.get_split_state("busy-shard").await.unwrap();
    assert!(state.is_some(), "Split state should exist");
}
