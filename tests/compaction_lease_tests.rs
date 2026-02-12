//! Integration tests for compaction chunk leasing
//!
//! Tests mutual exclusion, lease lifecycle, renewal, expiry, and scavenging.

use cardinalsin::metadata::{LeaseStatus, LocalMetadataClient, MetadataClient};
use std::sync::Arc;

/// Helper to create a LocalMetadataClient wrapped in Arc
fn new_client() -> Arc<LocalMetadataClient> {
    Arc::new(LocalMetadataClient::new())
}

#[tokio::test]
async fn test_acquire_lease_success() {
    let client = new_client();
    let chunks = vec!["chunk_a.parquet".to_string(), "chunk_b.parquet".to_string()];

    let lease = client.acquire_lease("node-1", &chunks, 0).await.unwrap();

    assert_eq!(lease.holder_id, "node-1");
    assert_eq!(lease.chunks, chunks);
    assert_eq!(lease.level, 0);
    assert_eq!(lease.status, LeaseStatus::Active);
    assert!(!lease.lease_id.is_empty());
    assert!(lease.expires_at > lease.acquired_at);
}

#[tokio::test]
async fn test_acquire_lease_conflict() {
    let client = new_client();
    let chunks = vec!["chunk_a.parquet".to_string(), "chunk_b.parquet".to_string()];

    // First lease succeeds
    let _lease1 = client.acquire_lease("node-1", &chunks, 0).await.unwrap();

    // Second lease on same chunks should fail
    let result = client.acquire_lease("node-2", &chunks, 0).await;
    assert!(result.is_err());

    match result.unwrap_err() {
        cardinalsin::Error::ChunksAlreadyLeased(conflicting) => {
            assert_eq!(conflicting.len(), 2);
            assert!(conflicting.contains(&"chunk_a.parquet".to_string()));
            assert!(conflicting.contains(&"chunk_b.parquet".to_string()));
        }
        other => panic!("Expected ChunksAlreadyLeased, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_acquire_lease_partial_overlap() {
    let client = new_client();

    // Lease chunks A and B
    let _lease1 = client
        .acquire_lease(
            "node-1",
            &["chunk_a.parquet".to_string(), "chunk_b.parquet".to_string()],
            0,
        )
        .await
        .unwrap();

    // Try to lease chunks B and C -- should fail because B is leased
    let result = client
        .acquire_lease(
            "node-2",
            &["chunk_b.parquet".to_string(), "chunk_c.parquet".to_string()],
            0,
        )
        .await;

    match result.unwrap_err() {
        cardinalsin::Error::ChunksAlreadyLeased(conflicting) => {
            assert_eq!(conflicting, vec!["chunk_b.parquet".to_string()]);
        }
        other => panic!("Expected ChunksAlreadyLeased, got: {:?}", other),
    }

    // Non-overlapping chunks should succeed
    let lease2 = client
        .acquire_lease(
            "node-2",
            &["chunk_c.parquet".to_string(), "chunk_d.parquet".to_string()],
            0,
        )
        .await
        .unwrap();
    assert_eq!(lease2.holder_id, "node-2");
}

#[tokio::test]
async fn test_complete_lease() {
    let client = new_client();
    let chunks = vec!["chunk_a.parquet".to_string()];

    let lease = client.acquire_lease("node-1", &chunks, 0).await.unwrap();

    // Complete the lease
    client.complete_lease(&lease.lease_id).await.unwrap();

    // Verify lease is marked completed
    let leases = client.load_leases().await.unwrap();
    let updated = leases.leases.get(&lease.lease_id).unwrap();
    assert_eq!(updated.status, LeaseStatus::Completed);

    // Completed lease should not block new acquisitions on same chunks
    let lease2 = client.acquire_lease("node-2", &chunks, 0).await.unwrap();
    assert_eq!(lease2.holder_id, "node-2");
}

#[tokio::test]
async fn test_fail_lease() {
    let client = new_client();
    let chunks = vec!["chunk_a.parquet".to_string()];

    let lease = client.acquire_lease("node-1", &chunks, 0).await.unwrap();

    // Fail the lease
    client.fail_lease(&lease.lease_id).await.unwrap();

    // Verify lease is marked failed
    let leases = client.load_leases().await.unwrap();
    let updated = leases.leases.get(&lease.lease_id).unwrap();
    assert_eq!(updated.status, LeaseStatus::Failed);

    // Failed lease should not block new acquisitions
    let lease2 = client.acquire_lease("node-2", &chunks, 0).await.unwrap();
    assert_eq!(lease2.holder_id, "node-2");
}

#[tokio::test]
async fn test_renew_lease() {
    let client = new_client();
    let chunks = vec!["chunk_a.parquet".to_string()];

    let lease = client.acquire_lease("node-1", &chunks, 0).await.unwrap();
    let original_expiry = lease.expires_at;

    // Small sleep to ensure time advances
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Renew the lease
    client.renew_lease(&lease.lease_id).await.unwrap();

    // Verify expiry was extended
    let leases = client.load_leases().await.unwrap();
    let renewed = leases.leases.get(&lease.lease_id).unwrap();
    assert!(renewed.expires_at >= original_expiry);
}

#[tokio::test]
async fn test_renew_non_active_lease_fails() {
    let client = new_client();
    let chunks = vec!["chunk_a.parquet".to_string()];

    let lease = client.acquire_lease("node-1", &chunks, 0).await.unwrap();

    // Complete the lease
    client.complete_lease(&lease.lease_id).await.unwrap();

    // Renewing completed lease should fail
    let result = client.renew_lease(&lease.lease_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_renew_nonexistent_lease_fails() {
    let client = new_client();

    let result = client.renew_lease("nonexistent-lease-id").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_scavenge_leases() {
    let client = new_client();
    let chunks1 = vec!["chunk_a.parquet".to_string()];
    let chunks2 = vec!["chunk_b.parquet".to_string()];

    // Create two leases
    let lease1 = client.acquire_lease("node-1", &chunks1, 0).await.unwrap();
    let _lease2 = client.acquire_lease("node-1", &chunks2, 0).await.unwrap();

    // Complete one lease
    client.complete_lease(&lease1.lease_id).await.unwrap();

    // Scavenge should remove the completed lease
    let removed = client.scavenge_leases().await.unwrap();
    assert_eq!(removed, 1);

    // Only the active lease should remain
    let leases = client.load_leases().await.unwrap();
    assert_eq!(leases.leases.len(), 1);
}

#[tokio::test]
async fn test_scavenge_failed_leases() {
    let client = new_client();
    let chunks = vec!["chunk_a.parquet".to_string()];

    let lease = client.acquire_lease("node-1", &chunks, 0).await.unwrap();

    // Fail the lease
    client.fail_lease(&lease.lease_id).await.unwrap();

    // Scavenge should remove the failed lease
    let removed = client.scavenge_leases().await.unwrap();
    assert_eq!(removed, 1);

    let leases = client.load_leases().await.unwrap();
    assert!(leases.leases.is_empty());
}

#[tokio::test]
async fn test_load_leases_empty() {
    let client = new_client();

    let leases = client.load_leases().await.unwrap();
    assert!(leases.leases.is_empty());
}

#[tokio::test]
async fn test_multiple_nodes_non_overlapping() {
    let client = new_client();

    // Different compactor nodes can lease different chunks
    let lease1 = client
        .acquire_lease("node-1", &["chunk_a.parquet".to_string()], 0)
        .await
        .unwrap();
    let lease2 = client
        .acquire_lease("node-2", &["chunk_b.parquet".to_string()], 0)
        .await
        .unwrap();
    let lease3 = client
        .acquire_lease("node-3", &["chunk_c.parquet".to_string()], 1)
        .await
        .unwrap();

    assert_ne!(lease1.lease_id, lease2.lease_id);
    assert_ne!(lease2.lease_id, lease3.lease_id);

    let leases = client.load_leases().await.unwrap();
    assert_eq!(leases.leases.len(), 3);
}

#[tokio::test]
async fn test_complete_nonexistent_lease_is_noop() {
    let client = new_client();

    // Completing a non-existent lease should be a no-op (not an error)
    client.complete_lease("nonexistent-lease").await.unwrap();
}

#[tokio::test]
async fn test_fail_nonexistent_lease_is_noop() {
    let client = new_client();

    // Failing a non-existent lease should be a no-op
    client.fail_lease("nonexistent-lease").await.unwrap();
}

#[tokio::test]
async fn test_lease_lifecycle_full() {
    let client = new_client();
    let chunks = vec!["chunk_a.parquet".to_string(), "chunk_b.parquet".to_string()];

    // 1. Acquire lease
    let lease = client.acquire_lease("node-1", &chunks, 0).await.unwrap();
    assert_eq!(lease.status, LeaseStatus::Active);

    // 2. Verify chunks are locked
    let result = client.acquire_lease("node-2", &chunks, 0).await;
    assert!(result.is_err());

    // 3. Complete lease
    client.complete_lease(&lease.lease_id).await.unwrap();

    // 4. Verify chunks are released
    let lease2 = client.acquire_lease("node-2", &chunks, 0).await.unwrap();
    assert_eq!(lease2.status, LeaseStatus::Active);

    // 5. Fail the second lease
    client.fail_lease(&lease2.lease_id).await.unwrap();

    // 6. Scavenge cleans up terminal leases
    let removed = client.scavenge_leases().await.unwrap();
    assert_eq!(removed, 2); // Both completed and failed leases

    // 7. Everything is clean
    let leases = client.load_leases().await.unwrap();
    assert!(leases.leases.is_empty());
}

#[tokio::test]
async fn test_scavenge_no_leases_returns_zero() {
    let client = new_client();

    let removed = client.scavenge_leases().await.unwrap();
    assert_eq!(removed, 0);
}

#[tokio::test]
async fn test_scavenge_all_active_returns_zero() {
    let client = new_client();

    let _lease = client
        .acquire_lease("node-1", &["chunk_a.parquet".to_string()], 0)
        .await
        .unwrap();

    // Active non-expired leases should not be scavenged
    let removed = client.scavenge_leases().await.unwrap();
    assert_eq!(removed, 0);
}
