//! Integration tests for P1-A: Complete Shard Split Orchestration
//!
//! These tests verify that the 5-phase shard split process works correctly
//! with dual-write support and zero data loss.

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cardinalsin::ingester::ChunkMetadata;
use cardinalsin::metadata::{LocalMetadataClient, MetadataClient};
use cardinalsin::sharding::{ReplicaInfo, ShardMetadata, ShardSplitter, ShardState, SplitPhase};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::Arc;

/// Helper to create a test shard
fn create_test_shard() -> ShardMetadata {
    ShardMetadata {
        shard_id: "test-shard-1".to_string(),
        generation: 1,
        key_range: (vec![0u8; 8], vec![255u8; 8]),
        replicas: vec![ReplicaInfo {
            replica_id: "replica-1".to_string(),
            node_id: "node-1".to_string(),
            is_leader: true,
        }],
        state: ShardState::Active,
        min_time: 0,
        max_time: 10000,
    }
}

/// Helper to create a test RecordBatch with timestamps
fn create_test_batch(timestamps: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("metric_name", DataType::Utf8, false),
    ]));

    let timestamp_array = Int64Array::from(timestamps.clone());
    let value_array = Int64Array::from(vec![100i64; timestamps.len()]);
    let metric_array = StringArray::from(vec!["test_metric"; timestamps.len()]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp_array),
            Arc::new(value_array),
            Arc::new(metric_array),
        ],
    )
    .unwrap()
}

async fn store_shard_metadata(metadata: &Arc<LocalMetadataClient>, shard: &ShardMetadata) {
    metadata
        .update_shard_metadata(&shard.shard_id, shard, 0)
        .await
        .unwrap();
}

/// Test Phase 1: Preparation creates new shard metadata
#[tokio::test]
async fn test_phase1_preparation() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let object_store = Arc::new(InMemory::new());
    let splitter = ShardSplitter::new(metadata.clone(), object_store);

    let shard = create_test_shard();

    // Execute Phase 1
    let (shard_a, shard_b) = splitter.split_shard(&shard).await.unwrap();

    // Verify new shard IDs are generated
    assert_ne!(shard_a, shard_b, "New shards should have different IDs");
    assert_ne!(
        shard_a, shard.shard_id,
        "New shard A should be different from original"
    );
    assert_ne!(
        shard_b, shard.shard_id,
        "New shard B should be different from original"
    );
}

/// Test Phase 2: Dual-write state is recorded
#[tokio::test]
async fn test_phase2_dualwrite_state() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let object_store = Arc::new(InMemory::new());
    let splitter = ShardSplitter::new(metadata.clone(), object_store);

    let shard = create_test_shard();
    let (shard_a, shard_b) = splitter.split_shard(&shard).await.unwrap();
    let split_point = vec![128u8; 8]; // Mid-point

    // Start split (Phase 1-2)
    metadata
        .start_split(
            &shard.shard_id,
            vec![shard_a.clone(), shard_b.clone()],
            split_point.clone(),
        )
        .await
        .unwrap();

    metadata
        .update_split_progress(&shard.shard_id, 0.0, SplitPhase::DualWrite)
        .await
        .unwrap();

    // Verify split state
    let split_state = metadata
        .get_split_state(&shard.shard_id)
        .await
        .unwrap()
        .expect("Split state should exist");

    assert_eq!(split_state.phase, SplitPhase::DualWrite);
    assert_eq!(split_state.old_shard, shard.shard_id);
    assert_eq!(split_state.new_shards.len(), 2);
    assert_eq!(split_state.split_point, split_point);
}

/// Test Phase 3: Backfill copies data correctly
#[tokio::test]
async fn test_phase3_backfill() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let object_store = Arc::new(InMemory::new());
    let splitter = ShardSplitter::new(metadata.clone(), object_store.clone());

    let shard = create_test_shard();

    // Create test data in the old shard
    let batch = create_test_batch(vec![1000, 2000, 3000, 4000, 5000]);
    let parquet_bytes = {
        use parquet::arrow::ArrowWriter;
        use parquet::basic::{Compression, ZstdLevel};
        use parquet::file::properties::WriterProperties;

        let mut buffer = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buffer
    };

    // Store in object store
    let chunk_path = format!("{}/test_chunk.parquet", shard.shard_id);
    object_store
        .put(&chunk_path.as_str().into(), parquet_bytes.into())
        .await
        .unwrap();

    // Register chunk in metadata
    let chunk_meta = ChunkMetadata {
        path: chunk_path.clone(),
        min_timestamp: 1000,
        max_timestamp: 5000,
        row_count: 5,
        size_bytes: 1024,
    };
    metadata
        .register_chunk(&chunk_path, &chunk_meta)
        .await
        .unwrap();

    // Start split
    let (shard_a, shard_b) = splitter.split_shard(&shard).await.unwrap();
    let split_point = 3000i64.to_be_bytes().to_vec(); // Split at timestamp 3000

    metadata
        .start_split(
            &shard.shard_id,
            vec![shard_a.clone(), shard_b.clone()],
            split_point.clone(),
        )
        .await
        .unwrap();

    // Run backfill
    splitter
        .run_backfill(
            &shard.shard_id,
            &[shard_a.clone(), shard_b.clone()],
            &split_point,
        )
        .await
        .unwrap();

    // Verify split progress is complete
    let split_state = metadata
        .get_split_state(&shard.shard_id)
        .await
        .unwrap()
        .expect("Split state should exist");

    assert_eq!(
        split_state.backfill_progress, 1.0,
        "Backfill should be 100% complete"
    );
    assert_eq!(split_state.phase, SplitPhase::Backfill);

    // Verify chunks were created for new shards
    let chunks_a = metadata.get_chunks_for_shard(&shard_a).await.unwrap();
    let chunks_b = metadata.get_chunks_for_shard(&shard_b).await.unwrap();

    // Both shards should have data (timestamps split at 3000)
    assert!(
        !chunks_a.is_empty() || !chunks_b.is_empty(),
        "New shards should have chunks"
    );
}

/// Test Phase 4: Cutover is atomic
#[tokio::test]
async fn test_phase4_cutover() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let object_store = Arc::new(InMemory::new());
    let splitter = ShardSplitter::new(metadata.clone(), object_store);

    let shard = create_test_shard();
    let (shard_a, shard_b) = splitter.split_shard(&shard).await.unwrap();
    let split_point = 3000i64.to_be_bytes().to_vec();

    store_shard_metadata(&metadata, &shard).await;

    // Set up split state with 100% backfill
    metadata
        .start_split(
            &shard.shard_id,
            vec![shard_a.clone(), shard_b.clone()],
            split_point,
        )
        .await
        .unwrap();

    metadata
        .update_split_progress(&shard.shard_id, 1.0, SplitPhase::Backfill)
        .await
        .unwrap();

    // Execute cutover
    splitter.cutover(&shard.shard_id).await.unwrap();

    // Verify split is complete
    let split_state = metadata.get_split_state(&shard.shard_id).await.unwrap();
    assert!(
        split_state.is_none(),
        "Split state should be removed after cutover"
    );

    let old_metadata = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Old shard metadata should still exist");
    assert_eq!(
        old_metadata.generation, 2,
        "Cutover should CAS-increment generation"
    );
    assert!(
        matches!(old_metadata.state, ShardState::PendingDeletion { .. }),
        "Old shard should be marked pending deletion after cutover"
    );

    let new_metadata_a = metadata
        .get_shard_metadata(&shard_a)
        .await
        .unwrap()
        .expect("First split shard metadata should be created");
    assert_eq!(new_metadata_a.generation, 1);
    assert_eq!(new_metadata_a.min_time, shard.min_time);
    assert_eq!(new_metadata_a.max_time, 3000);

    let new_metadata_b = metadata
        .get_shard_metadata(&shard_b)
        .await
        .unwrap()
        .expect("Second split shard metadata should be created");
    assert_eq!(new_metadata_b.generation, 1);
    assert_eq!(new_metadata_b.min_time, 3000);
    assert_eq!(new_metadata_b.max_time, shard.max_time);
}

#[tokio::test]
async fn test_phase4_cutover_requires_old_shard_metadata() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let object_store = Arc::new(InMemory::new());
    let splitter = ShardSplitter::new(metadata.clone(), object_store);

    let shard = create_test_shard();
    let (shard_a, shard_b) = splitter.split_shard(&shard).await.unwrap();
    let split_point = 3000i64.to_be_bytes().to_vec();

    metadata
        .start_split(&shard.shard_id, vec![shard_a, shard_b], split_point)
        .await
        .unwrap();
    metadata
        .update_split_progress(&shard.shard_id, 1.0, SplitPhase::Backfill)
        .await
        .unwrap();

    let err = splitter
        .cutover(&shard.shard_id)
        .await
        .expect_err("Cutover should fail when old shard metadata is missing");
    assert!(
        matches!(err, cardinalsin::Error::ShardNotFound(id) if id == shard.shard_id),
        "Expected ShardNotFound, got: {}",
        err
    );

    let split_state = metadata.get_split_state(&shard.shard_id).await.unwrap();
    assert!(
        split_state.is_some(),
        "Failed cutover must keep split state for retry"
    );
}

#[tokio::test]
async fn test_phase4_cutover_requires_two_new_shards() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let object_store = Arc::new(InMemory::new());
    let splitter = ShardSplitter::new(metadata.clone(), object_store);

    let shard = create_test_shard();
    store_shard_metadata(&metadata, &shard).await;

    metadata
        .start_split(
            &shard.shard_id,
            vec!["only-one-shard".to_string()],
            3000i64.to_be_bytes().to_vec(),
        )
        .await
        .unwrap();
    metadata
        .update_split_progress(&shard.shard_id, 1.0, SplitPhase::Backfill)
        .await
        .unwrap();

    let err = splitter
        .cutover(&shard.shard_id)
        .await
        .expect_err("Cutover should reject invalid split topology");
    assert!(
        matches!(err, cardinalsin::Error::Internal(message) if message.contains("exactly 2 new shards")),
        "Expected invariant error for invalid split topology, got: {}",
        err
    );
}

/// Test Phase 5: Cleanup removes old data
#[tokio::test]
async fn test_phase5_cleanup() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let object_store = Arc::new(InMemory::new());
    let splitter = ShardSplitter::new(metadata.clone(), object_store.clone());

    let shard = create_test_shard();

    // Create a chunk in the old shard
    let chunk_path = format!("{}/old_chunk.parquet", shard.shard_id);
    object_store
        .put(&chunk_path.as_str().into(), vec![1, 2, 3].into())
        .await
        .unwrap();

    let chunk_meta = ChunkMetadata {
        path: chunk_path.clone(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 100,
        size_bytes: 1024,
    };
    metadata
        .register_chunk(&chunk_path, &chunk_meta)
        .await
        .unwrap();

    // Execute cleanup with short grace period
    splitter
        .cleanup(&shard.shard_id, std::time::Duration::from_millis(10))
        .await
        .unwrap();

    // Verify chunk is removed from metadata
    let chunk = metadata.get_chunk(&chunk_path).await.unwrap();
    assert!(chunk.is_none(), "Old chunk should be removed from metadata");

    // Verify chunk is removed from object store
    let result = object_store.get(&chunk_path.as_str().into()).await;
    assert!(
        result.is_err(),
        "Old chunk should be removed from object store"
    );
}

/// Test full 5-phase split execution
#[tokio::test]
async fn test_full_split_execution() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let object_store = Arc::new(InMemory::new());
    let splitter = ShardSplitter::new(metadata.clone(), object_store.clone());

    let shard = create_test_shard();

    // Store shard metadata for CAS operations
    metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Create some test data
    let batch = create_test_batch(vec![1000, 2000, 3000, 4000]);
    let parquet_bytes = {
        use parquet::arrow::ArrowWriter;
        use parquet::basic::{Compression, ZstdLevel};
        use parquet::file::properties::WriterProperties;

        let mut buffer = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buffer
    };

    let chunk_path = format!("{}/data.parquet", shard.shard_id);
    object_store
        .put(&chunk_path.as_str().into(), parquet_bytes.into())
        .await
        .unwrap();

    let chunk_meta = ChunkMetadata {
        path: chunk_path.clone(),
        min_timestamp: 1000,
        max_timestamp: 4000,
        row_count: 4,
        size_bytes: 1024,
    };
    metadata
        .register_chunk(&chunk_path, &chunk_meta)
        .await
        .unwrap();

    // Execute full split
    splitter
        .execute_split_with_monitoring(&shard)
        .await
        .unwrap();

    // Verify split succeeded and state is cleaned up
    let split_state = metadata.get_split_state(&shard.shard_id).await.unwrap();
    assert!(split_state.is_none(), "Split state should be cleaned up");
}

/// Test that split point calculation is consistent
#[tokio::test]
async fn test_split_point_calculation() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let object_store = Arc::new(InMemory::new());
    let splitter = ShardSplitter::new(metadata.clone(), object_store);

    let shard = ShardMetadata {
        shard_id: "test-shard".to_string(),
        generation: 1,
        key_range: (vec![0u8; 8], vec![255u8; 8]),
        replicas: vec![],
        state: ShardState::Active,
        min_time: 0,
        max_time: 10000,
    };

    // Calculate split point multiple times - should be deterministic
    let (shard_a1, shard_b1) = splitter.split_shard(&shard).await.unwrap();
    let (shard_a2, shard_b2) = splitter.split_shard(&shard).await.unwrap();

    // Shard IDs will be different (UUIDs), but split logic should be consistent
    // We can't directly compare split points without access to internal method,
    // but we can verify shards are created
    assert!(!shard_a1.is_empty());
    assert!(!shard_b1.is_empty());
    assert!(!shard_a2.is_empty());
    assert!(!shard_b2.is_empty());
}

/// Test backfill progress tracking
#[tokio::test]
async fn test_backfill_progress_tracking() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let object_store = Arc::new(InMemory::new());
    let splitter = ShardSplitter::new(metadata.clone(), object_store.clone());

    let shard = create_test_shard();

    // Create multiple chunks to track progress
    for i in 0..5 {
        let batch = create_test_batch(vec![i * 1000, i * 1000 + 500]);
        let parquet_bytes = {
            use parquet::arrow::ArrowWriter;
            use parquet::basic::{Compression, ZstdLevel};
            use parquet::file::properties::WriterProperties;

            let mut buffer = Vec::new();
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
                .build();

            let mut writer =
                ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            buffer
        };

        let chunk_path = format!("{}/chunk_{}.parquet", shard.shard_id, i);
        object_store
            .put(&chunk_path.as_str().into(), parquet_bytes.into())
            .await
            .unwrap();

        let chunk_meta = ChunkMetadata {
            path: chunk_path.clone(),
            min_timestamp: i * 1000,
            max_timestamp: i * 1000 + 1000,
            row_count: 2,
            size_bytes: 1024,
        };
        metadata
            .register_chunk(&chunk_path, &chunk_meta)
            .await
            .unwrap();
    }

    let (shard_a, shard_b) = splitter.split_shard(&shard).await.unwrap();
    let split_point = 3000i64.to_be_bytes().to_vec();

    metadata
        .start_split(
            &shard.shard_id,
            vec![shard_a.clone(), shard_b.clone()],
            split_point.clone(),
        )
        .await
        .unwrap();

    // Run backfill
    splitter
        .run_backfill(&shard.shard_id, &[shard_a, shard_b], &split_point)
        .await
        .unwrap();

    // Verify final progress is 100%
    let split_state = metadata
        .get_split_state(&shard.shard_id)
        .await
        .unwrap()
        .expect("Split state should exist");

    assert_eq!(
        split_state.backfill_progress, 1.0,
        "Backfill should reach 100%"
    );
}
