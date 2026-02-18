//! Integration tests for P0-A: S3 Metadata Atomicity with ETags
//!
//! These tests verify that atomic metadata operations work correctly under
//! concurrent access patterns.

use cardinalsin::ingester::ChunkMetadata;
use cardinalsin::metadata::{MetadataClient, S3MetadataClient, S3MetadataConfig};
use object_store::memory::InMemory;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Test concurrent chunk registrations to verify atomicity
#[tokio::test]
async fn test_concurrent_chunk_registration() {
    // Use in-memory object store for testing
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let metadata_client = Arc::new(S3MetadataClient::new(object_store, config));

    // Spawn 10 concurrent tasks registering chunks
    let mut tasks = JoinSet::new();

    for i in 0..10 {
        let client = metadata_client.clone();
        tasks.spawn(async move {
            let chunk = ChunkMetadata {
                path: format!("chunk_{}.parquet", i),
                min_timestamp: i * 1000,
                max_timestamp: (i + 1) * 1000,
                row_count: 1000,
                size_bytes: 1024 * 1024,
            };

            client.register_chunk(&chunk.path, &chunk).await
        });
    }

    // Wait for all tasks to complete
    let mut success_count = 0;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(_)) => success_count += 1,
            Ok(Err(e)) => panic!("Chunk registration failed: {}", e),
            Err(e) => panic!("Task panicked: {}", e),
        }
    }

    assert_eq!(success_count, 10, "All 10 chunks should be registered");

    // Verify all chunks were registered
    let chunks = metadata_client.list_chunks().await.unwrap();
    assert_eq!(chunks.len(), 10, "Should have 10 chunks in metadata");
}

/// Test retry behavior on conflicts
#[tokio::test]
async fn test_atomic_retry_on_conflict() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let metadata_client = Arc::new(S3MetadataClient::new(object_store, config));

    // Pre-register a chunk
    let chunk = ChunkMetadata {
        path: "initial.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 100,
        size_bytes: 1024,
    };
    metadata_client
        .register_chunk(&chunk.path, &chunk)
        .await
        .unwrap();

    // Now spawn multiple tasks trying to register different chunks concurrently
    let mut tasks = JoinSet::new();

    for i in 1..=5 {
        let client = metadata_client.clone();
        tasks.spawn(async move {
            let chunk = ChunkMetadata {
                path: format!("concurrent_{}.parquet", i),
                min_timestamp: i * 1000,
                max_timestamp: (i + 1) * 1000,
                row_count: 100,
                size_bytes: 1024,
            };

            client.register_chunk(&chunk.path, &chunk).await
        });
    }

    // All should succeed (with retries if needed)
    let mut results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        results.push(result.unwrap());
    }

    // Verify all succeeded
    for result in results {
        assert!(
            result.is_ok(),
            "All registrations should succeed with retries"
        );
    }

    // Verify final state
    let chunks = metadata_client.list_chunks().await.unwrap();
    assert_eq!(
        chunks.len(),
        6,
        "Should have 6 total chunks (1 initial + 5 concurrent)"
    );
}

/// Test metadata consistency under heavy concurrent load
#[tokio::test]
async fn test_heavy_concurrent_load() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let metadata_client = Arc::new(S3MetadataClient::new(object_store, config));

    const NUM_TASKS: usize = 50;
    const CHUNKS_PER_TASK: usize = 10;

    let mut tasks = JoinSet::new();

    for task_id in 0..NUM_TASKS {
        let client = metadata_client.clone();
        tasks.spawn(async move {
            let mut registered = 0;
            for chunk_id in 0..CHUNKS_PER_TASK {
                let chunk = ChunkMetadata {
                    path: format!("task_{}_chunk_{}.parquet", task_id, chunk_id),
                    min_timestamp: (task_id * CHUNKS_PER_TASK + chunk_id) as i64 * 1000,
                    max_timestamp: ((task_id * CHUNKS_PER_TASK + chunk_id) as i64 + 1) * 1000,
                    row_count: 1000,
                    size_bytes: 1024 * 1024,
                };

                client
                    .register_chunk(&chunk.path, &chunk)
                    .await
                    .map_err(|e| {
                        format!(
                            "register_chunk failed for task {} chunk {}: {}",
                            task_id, chunk_id, e
                        )
                    })?;
                registered += 1;
            }
            Ok::<usize, String>(registered)
        });
    }

    // Collect results
    let mut total_registered = 0;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(count)) => total_registered += count,
            Ok(Err(e)) => panic!("Task registration failed: {}", e),
            Err(e) => panic!("Task failed: {}", e),
        }
    }

    let expected = NUM_TASKS * CHUNKS_PER_TASK;
    assert_eq!(
        total_registered, expected,
        "Should register all {} chunks",
        expected
    );

    // Verify metadata consistency
    let chunks = metadata_client.list_chunks().await.unwrap();
    assert_eq!(
        chunks.len(),
        expected,
        "Metadata should contain all {} chunks",
        expected
    );

    // Verify no duplicate paths
    let mut paths = std::collections::HashSet::new();
    for chunk in chunks {
        assert!(
            paths.insert(chunk.chunk_path.clone()),
            "Duplicate chunk path: {}",
            chunk.chunk_path
        );
    }
}

/// Test atomic compaction completion
#[tokio::test]
async fn test_atomic_compaction_completion() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let metadata_client = Arc::new(S3MetadataClient::new(object_store, config));

    // Register source chunks at level 0
    let source_chunks = vec![
        "source_1.parquet".to_string(),
        "source_2.parquet".to_string(),
        "source_3.parquet".to_string(),
    ];

    for (i, path) in source_chunks.iter().enumerate() {
        let chunk = ChunkMetadata {
            path: path.to_string(),
            min_timestamp: i as i64 * 1000,
            max_timestamp: (i as i64 + 1) * 1000,
            row_count: 1000,
            size_bytes: 1024 * 1024,
        };
        metadata_client.register_chunk(path, &chunk).await.unwrap();
    }

    // Register target chunk
    let target_chunk = ChunkMetadata {
        path: "compacted.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 3000,
        row_count: 3000,
        size_bytes: 3 * 1024 * 1024,
    };
    metadata_client
        .register_chunk("compacted.parquet", &target_chunk)
        .await
        .unwrap();

    // Complete compaction atomically
    metadata_client
        .complete_compaction(&source_chunks, "compacted.parquet")
        .await
        .unwrap();

    // Verify source chunks are removed
    for source in &source_chunks {
        let chunk = metadata_client.get_chunk(source).await.unwrap();
        assert!(chunk.is_none(), "Source chunk {} should be removed", source);
    }

    // Verify target chunk still exists
    let target = metadata_client
        .get_chunk("compacted.parquet")
        .await
        .unwrap();
    assert!(target.is_some(), "Target chunk should exist");

    // Verify total count
    let all_chunks = metadata_client.list_chunks().await.unwrap();
    assert_eq!(
        all_chunks.len(),
        1,
        "Should have only 1 chunk after compaction"
    );
}

/// Test concurrent compactions don't corrupt metadata
#[tokio::test]
async fn test_concurrent_compactions() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let metadata_client = Arc::new(S3MetadataClient::new(object_store, config));

    // Register chunks for multiple independent compaction groups
    for group in 0..5 {
        for chunk in 0..3 {
            let path = format!("group_{}_chunk_{}.parquet", group, chunk);
            let meta = ChunkMetadata {
                path: path.clone(),
                min_timestamp: (group * 3 + chunk) as i64 * 1000,
                max_timestamp: ((group * 3 + chunk) as i64 + 1) * 1000,
                row_count: 1000,
                size_bytes: 1024 * 1024,
            };
            metadata_client.register_chunk(&path, &meta).await.unwrap();
        }

        // Register target for this group
        let target = format!("group_{}_compacted.parquet", group);
        let meta = ChunkMetadata {
            path: target.clone(),
            min_timestamp: (group * 3) as i64 * 1000,
            max_timestamp: ((group * 3) as i64 + 3) * 1000,
            row_count: 3000,
            size_bytes: 3 * 1024 * 1024,
        };
        metadata_client
            .register_chunk(&target, &meta)
            .await
            .unwrap();
    }

    // Run 5 compactions concurrently
    let mut tasks = JoinSet::new();

    for group in 0..5 {
        let client = metadata_client.clone();
        tasks.spawn(async move {
            let sources: Vec<String> = (0..3)
                .map(|chunk| format!("group_{}_chunk_{}.parquet", group, chunk))
                .collect();
            let sources_refs: Vec<String> = sources;
            let target = format!("group_{}_compacted.parquet", group);

            client.complete_compaction(&sources_refs, &target).await
        });
    }

    // All should succeed
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("Compaction failed: {}", e),
            Err(e) => panic!("Task panicked: {}", e),
        }
    }

    // Verify final state: should have 5 compacted chunks only
    let chunks = metadata_client.list_chunks().await.unwrap();
    assert_eq!(chunks.len(), 5, "Should have 5 compacted chunks");

    for chunk in chunks {
        assert!(
            chunk.chunk_path.contains("compacted"),
            "All remaining chunks should be compacted ones"
        );
    }
}
