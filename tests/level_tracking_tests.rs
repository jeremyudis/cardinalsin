//! Integration tests for P0-B: Compaction Level Tracking
//!
//! These tests verify that chunk levels are properly tracked and progressed
//! through the compaction hierarchy (L0 → L1 → L2 → L3).

use cardinalsin::ingester::ChunkMetadata;
use cardinalsin::metadata::{S3MetadataClient, S3MetadataConfig, MetadataClient};
use object_store::memory::InMemory;
use std::sync::Arc;

/// Test that new chunks start at level 0
#[tokio::test]
async fn test_new_chunks_start_at_l0() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let metadata_client = S3MetadataClient::new(object_store, config);

    // Register a new chunk
    let chunk = ChunkMetadata {
        path: "new_chunk.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 1000,
        size_bytes: 1024 * 1024,
    };

    metadata_client.register_chunk(&chunk.path, &chunk).await.unwrap();

    // Verify it appears in L0 candidates
    let l0_candidates = metadata_client.get_l0_candidates(1).await.unwrap();
    assert!(
        !l0_candidates.is_empty(),
        "New chunk should appear in L0 candidates"
    );

    // Verify the chunk is in the candidates
    let found = l0_candidates.iter().any(|group| {
        group.iter().any(|path| path == "new_chunk.parquet")
    });
    assert!(found, "New chunk should be in L0 candidates");
}

/// Test L0 → L1 progression
#[tokio::test]
async fn test_l0_to_l1_progression() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let metadata_client = Arc::new(S3MetadataClient::new(object_store, config));

    // Register 3 L0 chunks
    let l0_chunks = vec!["l0_1.parquet", "l0_2.parquet", "l0_3.parquet"];

    for (i, path) in l0_chunks.iter().enumerate() {
        let chunk = ChunkMetadata {
            path: path.to_string(),
            min_timestamp: i as i64 * 1000,
            max_timestamp: (i as i64 + 1) * 1000,
            row_count: 1000,
            size_bytes: 1024 * 1024,
        };
        metadata_client.register_chunk(path, &chunk).await.unwrap();
    }

    // Register L1 target chunk
    let l1_chunk = ChunkMetadata {
        path: "l1_compacted.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 3000,
        row_count: 3000,
        size_bytes: 3 * 1024 * 1024,
    };
    metadata_client.register_chunk(&l1_chunk.path, &l1_chunk).await.unwrap();

    // Complete compaction (L0 → L1)
    let l0_chunks_owned: Vec<String> = l0_chunks.iter().map(|s| s.to_string()).collect();
    metadata_client
        .complete_compaction(&l0_chunks_owned, "l1_compacted.parquet")
        .await
        .unwrap();

    // Verify L0 candidates are now empty
    let l0_candidates = metadata_client.get_l0_candidates(1).await.unwrap();
    assert!(
        l0_candidates.is_empty(),
        "Should have no L0 candidates after compaction"
    );

    // Verify L1 candidates now contain the compacted chunk
    let l1_candidates = metadata_client.get_level_candidates(1, 1024 * 1024).await.unwrap();
    assert!(
        !l1_candidates.is_empty(),
        "Should have L1 candidates after compaction"
    );

    let found = l1_candidates.iter().any(|group| {
        group.iter().any(|path| path == "l1_compacted.parquet")
    });
    assert!(found, "Compacted chunk should be at L1");
}

/// Test L1 → L2 progression
#[tokio::test]
async fn test_l1_to_l2_progression() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let metadata_client = Arc::new(S3MetadataClient::new(object_store, config));

    // First, create some L0 chunks and compact to L1
    let l0_chunks = vec!["l0_a.parquet", "l0_b.parquet"];
    for (i, path) in l0_chunks.iter().enumerate() {
        let chunk = ChunkMetadata {
            path: path.to_string(),
            min_timestamp: i as i64 * 1000,
            max_timestamp: (i as i64 + 1) * 1000,
            row_count: 1000,
            size_bytes: 1024 * 1024,
        };
        metadata_client.register_chunk(path, &chunk).await.unwrap();
    }

    let l1_chunk_1 = ChunkMetadata {
        path: "l1_first.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 2000,
        row_count: 2000,
        size_bytes: 2 * 1024 * 1024,
    };
    metadata_client.register_chunk(&l1_chunk_1.path, &l1_chunk_1).await.unwrap();
    let l0_chunks_owned: Vec<String> = l0_chunks.iter().map(|s| s.to_string()).collect();
    metadata_client
        .complete_compaction(&l0_chunks_owned, "l1_first.parquet")
        .await
        .unwrap();

    // Create more L0 chunks and compact to another L1
    let l0_chunks_2 = vec!["l0_c.parquet", "l0_d.parquet"];
    for (i, path) in l0_chunks_2.iter().enumerate() {
        let chunk = ChunkMetadata {
            path: path.to_string(),
            min_timestamp: (i + 2) as i64 * 1000,
            max_timestamp: ((i + 2) as i64 + 1) * 1000,
            row_count: 1000,
            size_bytes: 1024 * 1024,
        };
        metadata_client.register_chunk(path, &chunk).await.unwrap();
    }

    let l1_chunk_2 = ChunkMetadata {
        path: "l1_second.parquet".to_string(),
        min_timestamp: 2000,
        max_timestamp: 4000,
        row_count: 2000,
        size_bytes: 2 * 1024 * 1024,
    };
    metadata_client.register_chunk(&l1_chunk_2.path, &l1_chunk_2).await.unwrap();
    let l0_chunks_2_owned: Vec<String> = l0_chunks_2.iter().map(|s| s.to_string()).collect();
    metadata_client
        .complete_compaction(&l0_chunks_2_owned, "l1_second.parquet")
        .await
        .unwrap();

    // Now compact the two L1 chunks to L2
    let l2_chunk = ChunkMetadata {
        path: "l2_compacted.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 4000,
        row_count: 4000,
        size_bytes: 4 * 1024 * 1024,
    };
    metadata_client.register_chunk(&l2_chunk.path, &l2_chunk).await.unwrap();

    metadata_client
        .complete_compaction(
            &["l1_first.parquet".to_string(), "l1_second.parquet".to_string()],
            "l2_compacted.parquet",
        )
        .await
        .unwrap();

    // Verify L1 is now empty
    let l1_candidates = metadata_client.get_level_candidates(1, 1024 * 1024).await.unwrap();
    assert!(
        l1_candidates.is_empty(),
        "L1 should be empty after compaction to L2"
    );

    // Verify L2 contains the compacted chunk
    let l2_candidates = metadata_client.get_level_candidates(2, 1024 * 1024).await.unwrap();
    assert!(
        !l2_candidates.is_empty(),
        "L2 should have candidates after compaction"
    );

    let found = l2_candidates.iter().any(|group| {
        group.iter().any(|path| path == "l2_compacted.parquet")
    });
    assert!(found, "Compacted chunk should be at L2");
}

/// Test full L0 → L1 → L2 → L3 progression
#[tokio::test]
async fn test_full_level_progression() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let metadata_client = Arc::new(S3MetadataClient::new(object_store, config));

    // Helper function to register and compact
    async fn compact_level(
        client: &S3MetadataClient,
        sources: &[&str],
        target: &str,
        min_ts: i64,
        max_ts: i64,
    ) {
        // Register target
        let chunk = ChunkMetadata {
            path: target.to_string(),
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            row_count: 1000,
            size_bytes: 1024 * 1024,
        };
        client.register_chunk(target, &chunk).await.unwrap();

        // Complete compaction
        let sources_owned: Vec<String> = sources.iter().map(|s| s.to_string()).collect();
        client.complete_compaction(&sources_owned, target).await.unwrap();
    }

    // L0: Create 4 chunks
    for i in 0..4 {
        let path = format!("l0_{}.parquet", i);
        let chunk = ChunkMetadata {
            path: path.clone(),
            min_timestamp: i * 1000,
            max_timestamp: (i + 1) * 1000,
            row_count: 1000,
            size_bytes: 1024 * 1024,
        };
        metadata_client.register_chunk(&path, &chunk).await.unwrap();
    }

    // Compact to L1 (2 groups)
    compact_level(
        &metadata_client,
        &["l0_0.parquet", "l0_1.parquet"],
        "l1_a.parquet",
        0,
        2000,
    ).await;

    compact_level(
        &metadata_client,
        &["l0_2.parquet", "l0_3.parquet"],
        "l1_b.parquet",
        2000,
        4000,
    ).await;

    // Verify L0 is empty, L1 has 2 chunks
    let l0 = metadata_client.get_l0_candidates(1).await.unwrap();
    assert!(l0.is_empty(), "L0 should be empty");

    let l1 = metadata_client.get_level_candidates(1, 1).await.unwrap();
    assert!(!l1.is_empty(), "L1 should have chunks");

    // Compact L1 to L2
    compact_level(
        &metadata_client,
        &["l1_a.parquet", "l1_b.parquet"],
        "l2.parquet",
        0,
        4000,
    ).await;

    // Verify L1 is empty, L2 has 1 chunk
    let l1 = metadata_client.get_level_candidates(1, 1).await.unwrap();
    assert!(l1.is_empty(), "L1 should be empty after L2 compaction");

    let l2 = metadata_client.get_level_candidates(2, 1).await.unwrap();
    assert!(!l2.is_empty(), "L2 should have chunks");

    // Create more chunks and compact through levels again
    for i in 4..6 {
        let path = format!("l0_{}.parquet", i);
        let chunk = ChunkMetadata {
            path: path.clone(),
            min_timestamp: i * 1000,
            max_timestamp: (i + 1) * 1000,
            row_count: 1000,
            size_bytes: 1024 * 1024,
        };
        metadata_client.register_chunk(&path, &chunk).await.unwrap();
    }

    compact_level(
        &metadata_client,
        &["l0_4.parquet", "l0_5.parquet"],
        "l1_c.parquet",
        4000,
        6000,
    ).await;

    compact_level(
        &metadata_client,
        &["l1_c.parquet"],
        "l2_b.parquet",
        4000,
        6000,
    ).await;

    // Now compact both L2 chunks to L3
    compact_level(
        &metadata_client,
        &["l2.parquet", "l2_b.parquet"],
        "l3.parquet",
        0,
        6000,
    ).await;

    // Verify final state: L0, L1, L2 empty, L3 has 1 chunk
    let l0 = metadata_client.get_l0_candidates(1).await.unwrap();
    let l1 = metadata_client.get_level_candidates(1, 1).await.unwrap();
    let l2 = metadata_client.get_level_candidates(2, 1).await.unwrap();
    let l3 = metadata_client.get_level_candidates(3, 1).await.unwrap();

    assert!(l0.is_empty(), "L0 should be empty");
    assert!(l1.is_empty(), "L1 should be empty");
    assert!(l2.is_empty(), "L2 should be empty");
    assert!(!l3.is_empty(), "L3 should have the final compacted chunk");

    // Verify only one chunk remains
    let all_chunks = metadata_client.list_chunks().await.unwrap();
    assert_eq!(all_chunks.len(), 1, "Should have only 1 final L3 chunk");
}

/// Test that level filtering works correctly
#[tokio::test]
async fn test_level_filtering() {
    let object_store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: false,
        allow_unsafe_overwrite: false,
    };

    let metadata_client = Arc::new(S3MetadataClient::new(object_store, config));

    // Create chunks at different levels by doing compactions
    // L0 chunk
    let l0_chunk = ChunkMetadata {
        path: "l0_only.parquet".to_string(),
        min_timestamp: 0,
        max_timestamp: 1000,
        row_count: 1000,
        size_bytes: 1024 * 1024,
    };
    metadata_client.register_chunk(&l0_chunk.path, &l0_chunk).await.unwrap();

    // Create and compact to get an L1 chunk
    let sources = vec!["l0_a.parquet", "l0_b.parquet"];
    for (i, path) in sources.iter().enumerate() {
        let chunk = ChunkMetadata {
            path: path.to_string(),
            min_timestamp: (i + 1) as i64 * 1000,
            max_timestamp: ((i + 1) as i64 + 1) * 1000,
            row_count: 1000,
            size_bytes: 1024 * 1024,
        };
        metadata_client.register_chunk(path, &chunk).await.unwrap();
    }

    let l1_target = ChunkMetadata {
        path: "l1_only.parquet".to_string(),
        min_timestamp: 1000,
        max_timestamp: 3000,
        row_count: 2000,
        size_bytes: 2 * 1024 * 1024,
    };
    metadata_client.register_chunk(&l1_target.path, &l1_target).await.unwrap();
    let sources_owned: Vec<String> = sources.iter().map(|s| s.to_string()).collect();
    metadata_client
        .complete_compaction(&sources_owned, "l1_only.parquet")
        .await
        .unwrap();

    // Verify L0 filter returns only L0 chunk
    let l0_candidates = metadata_client.get_l0_candidates(1).await.unwrap();
    assert_eq!(l0_candidates.len(), 1, "Should have 1 L0 group");
    assert!(
        l0_candidates[0].contains(&"l0_only.parquet".to_string()),
        "L0 candidates should contain l0_only.parquet"
    );

    // Verify L1 filter returns only L1 chunk
    let l1_candidates = metadata_client.get_level_candidates(1, 1).await.unwrap();
    assert!(!l1_candidates.is_empty(), "Should have L1 candidates");

    let has_l1 = l1_candidates.iter().any(|group| {
        group.iter().any(|path| path == "l1_only.parquet")
    });
    assert!(has_l1, "L1 candidates should contain l1_only.parquet");

    let has_l0 = l1_candidates.iter().any(|group| {
        group.iter().any(|path| path == "l0_only.parquet")
    });
    assert!(!has_l0, "L1 candidates should NOT contain l0_only.parquet");
}
