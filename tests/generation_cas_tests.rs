//! Integration tests for P1-B: Generation Number CAS Enforcement
//!
//! These tests verify that generation-based compare-and-swap prevents
//! stale updates and routing conflicts during shard operations.

use cardinalsin::metadata::{LocalMetadataClient, MetadataClient};
use cardinalsin::sharding::{ReplicaInfo, ShardMetadata, ShardState};
use cardinalsin::Error;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Helper to create a test shard
fn create_test_shard(id: &str, generation: u64) -> ShardMetadata {
    ShardMetadata {
        shard_id: id.to_string(),
        generation,
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

/// Test that generation increments on each update
#[tokio::test]
async fn test_generation_increments() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let shard = create_test_shard("shard-1", 0);

    // Initial update (generation 0 -> 1)
    metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Verify generation incremented
    let updated = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");

    assert_eq!(updated.generation, 1, "Generation should increment to 1");

    // Update again (generation 1 -> 2)
    metadata
        .update_shard_metadata(&shard.shard_id, &updated, 1)
        .await
        .unwrap();

    let updated2 = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");

    assert_eq!(updated2.generation, 2, "Generation should increment to 2");
}

/// Test that stale generation is rejected
#[tokio::test]
async fn test_stale_generation_rejected() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let shard = create_test_shard("shard-1", 0);

    // Initial update
    metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Try to update with stale generation (0 when current is 1)
    let result = metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await;

    assert!(result.is_err(), "Should reject stale generation");

    match result.unwrap_err() {
        Error::StaleGeneration { expected, actual } => {
            assert_eq!(expected, 0);
            assert_eq!(actual, 1);
        }
        e => panic!("Expected StaleGeneration error, got: {:?}", e),
    }
}

/// Test concurrent updates with CAS
#[tokio::test]
async fn test_concurrent_cas_updates() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let shard = create_test_shard("shard-1", 0);

    // Initial setup
    metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Spawn 10 concurrent updates
    let mut tasks = JoinSet::new();

    for i in 0..10 {
        let client = metadata.clone();
        let shard_id = shard.shard_id.clone();

        tasks.spawn(async move {
            // Each task tries multiple times with the latest generation
            for attempt in 0..5 {
                // Get current metadata
                let current = match client.get_shard_metadata(&shard_id).await.unwrap() {
                    Some(s) => s,
                    None => return Err(format!("Task {} attempt {}: Shard not found", i, attempt)),
                };

                // Try to update with current generation
                let mut updated = current.clone();
                updated.min_time = i * 1000; // Just change something

                match client
                    .update_shard_metadata(&shard_id, &updated, current.generation)
                    .await
                {
                    Ok(_) => return Ok(i),
                    Err(Error::StaleGeneration { .. }) => {
                        // Retry with exponential backoff
                        tokio::time::sleep(std::time::Duration::from_millis(
                            10 * 2u64.pow(attempt),
                        ))
                        .await;
                        continue;
                    }
                    Err(e) => return Err(format!("Task {} failed: {}", i, e)),
                }
            }

            Err(format!("Task {} exceeded max retries", i))
        });
    }

    // Collect results
    let mut successful = 0;
    let mut failed = Vec::new();

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(_)) => successful += 1,
            Ok(Err(e)) => failed.push(e),
            Err(e) => failed.push(format!("Task panicked: {}", e)),
        }
    }

    assert_eq!(
        successful, 10,
        "All 10 tasks should eventually succeed with retries. Failures: {:?}",
        failed
    );

    // Verify final generation
    let final_shard = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");

    assert_eq!(
        final_shard.generation, 11,
        "Generation should be 11 (initial 1 + 10 updates)"
    );
}

/// Test CAS prevents lost updates
#[tokio::test]
async fn test_cas_prevents_lost_updates() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let shard = create_test_shard("shard-1", 0);

    // Initial setup
    metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Simulate two processes with stale reads
    let shard1 = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");

    let shard2 = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");

    // Both have generation 1
    assert_eq!(shard1.generation, 1);
    assert_eq!(shard2.generation, 1);

    // Process 1 updates successfully
    let mut updated1 = shard1.clone();
    updated1.min_time = 1000;

    metadata
        .update_shard_metadata(&shard.shard_id, &updated1, 1)
        .await
        .unwrap();

    // Process 2 tries to update with stale generation - should fail
    let mut updated2 = shard2.clone();
    updated2.min_time = 2000;

    let result = metadata
        .update_shard_metadata(&shard.shard_id, &updated2, 1)
        .await;

    assert!(
        result.is_err(),
        "Second update should fail with stale generation"
    );

    // Verify first update was preserved
    let final_shard = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");

    assert_eq!(
        final_shard.min_time, 1000,
        "Should preserve first update's value"
    );
    assert_eq!(final_shard.generation, 2, "Should have generation 2");
}

/// Test CAS with shard state transitions
#[tokio::test]
async fn test_cas_with_state_transitions() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let mut shard = create_test_shard("shard-1", 0);
    shard.state = ShardState::Active;

    // Initial setup
    metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Transition to Splitting
    let current = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");

    let mut splitting = current.clone();
    splitting.state = ShardState::Splitting {
        new_shards: vec!["shard-2".to_string(), "shard-3".to_string()],
    };

    metadata
        .update_shard_metadata(&shard.shard_id, &splitting, current.generation)
        .await
        .unwrap();

    // Verify state changed
    let updated = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");

    match updated.state {
        ShardState::Splitting { ref new_shards } => {
            assert_eq!(new_shards.len(), 2);
        }
        _ => panic!("Shard should be in Splitting state"),
    }

    // Transition to PendingDeletion
    let mut pending = updated.clone();
    pending.state = ShardState::PendingDeletion {
        delete_after: 12345,
    };

    metadata
        .update_shard_metadata(&shard.shard_id, &pending, updated.generation)
        .await
        .unwrap();

    // Verify final state
    let final_shard = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");

    match final_shard.state {
        ShardState::PendingDeletion { delete_after } => {
            assert_eq!(delete_after, 12345);
        }
        _ => panic!("Shard should be in PendingDeletion state"),
    }

    assert_eq!(final_shard.generation, 3, "Should be at generation 3");
}

/// Test new shard creation with generation 0
#[tokio::test]
async fn test_new_shard_creation() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let shard = create_test_shard("new-shard", 0);

    // Create new shard (expected_generation = 0 for new shards)
    metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Verify it was created with generation 1
    let created = metadata
        .get_shard_metadata(&shard.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");

    assert_eq!(created.generation, 1, "New shard should have generation 1");
}

/// Test multiple shard updates don't interfere
#[tokio::test]
async fn test_independent_shard_updates() {
    let metadata = Arc::new(LocalMetadataClient::new());

    // Create two independent shards
    let shard1 = create_test_shard("shard-1", 0);
    let shard2 = create_test_shard("shard-2", 0);

    metadata
        .update_shard_metadata(&shard1.shard_id, &shard1, 0)
        .await
        .unwrap();

    metadata
        .update_shard_metadata(&shard2.shard_id, &shard2, 0)
        .await
        .unwrap();

    // Update shard1 multiple times
    for i in 1..=3 {
        let current = metadata
            .get_shard_metadata(&shard1.shard_id)
            .await
            .unwrap()
            .expect("Shard should exist");

        let mut updated = current.clone();
        updated.min_time = i * 1000;

        metadata
            .update_shard_metadata(&shard1.shard_id, &updated, current.generation)
            .await
            .unwrap();
    }

    // Verify shard1 has generation 4
    let final1 = metadata
        .get_shard_metadata(&shard1.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");
    assert_eq!(final1.generation, 4);

    // Verify shard2 still has generation 1
    let final2 = metadata
        .get_shard_metadata(&shard2.shard_id)
        .await
        .unwrap()
        .expect("Shard should exist");
    assert_eq!(final2.generation, 1);
}

/// Test retry logic with exponential backoff
#[tokio::test]
async fn test_retry_with_backoff() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let shard = create_test_shard("shard-1", 0);

    metadata
        .update_shard_metadata(&shard.shard_id, &shard, 0)
        .await
        .unwrap();

    // Simulate a process that needs to retry
    let mut attempts = 0;
    let max_attempts = 5;

    loop {
        attempts += 1;

        if attempts > max_attempts {
            panic!("Exceeded maximum retry attempts");
        }

        // Get current generation
        let current = metadata
            .get_shard_metadata(&shard.shard_id)
            .await
            .unwrap()
            .expect("Shard should exist");

        // Try to update
        let mut updated = current.clone();
        updated.min_time = 5000;

        match metadata
            .update_shard_metadata(&shard.shard_id, &updated, current.generation)
            .await
        {
            Ok(_) => break,
            Err(Error::StaleGeneration { .. }) => {
                // Exponential backoff
                let backoff_ms = 10 * 2u64.pow(attempts - 1);
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                continue;
            }
            Err(e) => panic!("Unexpected error: {}", e),
        }
    }

    assert!(
        attempts <= max_attempts,
        "Should succeed within max attempts"
    );
}

/// Test that non-existent shard returns None
#[tokio::test]
async fn test_nonexistent_shard() {
    let metadata = Arc::new(LocalMetadataClient::new());

    let result = metadata.get_shard_metadata("nonexistent").await.unwrap();

    assert!(result.is_none(), "Non-existent shard should return None");

    // Try to update non-existent shard with non-zero generation
    let shard = create_test_shard("nonexistent", 0);
    let result = metadata
        .update_shard_metadata("nonexistent", &shard, 5)
        .await;

    assert!(
        result.is_err(),
        "Updating non-existent shard with generation > 0 should fail"
    );
}
