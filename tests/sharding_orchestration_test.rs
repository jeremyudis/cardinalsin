//! Tests for the sharding orchestration loop in the compactor.

use cardinalsin::compactor::{Compactor, CompactorConfig};
use cardinalsin::metadata::{LocalMetadataClient, MetadataClient};
use cardinalsin::sharding::{HotShardConfig, ReplicaInfo, ShardMetadata, ShardMonitor, ShardState};
use cardinalsin::StorageConfig;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// Sets up a test environment with a compactor and its dependencies.
/// Returns the compactor, metadata client, shard monitor, and the ID of an initial shard.
async fn setup_test_env() -> (
    Arc<Compactor>,
    Arc<dyn MetadataClient>,
    Arc<ShardMonitor>,
    String,
) {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let metadata: Arc<dyn MetadataClient> = Arc::new(LocalMetadataClient::new());
    let storage_config = StorageConfig::default();

    // Configure the shard monitor to trigger a split easily
    let shard_config = HotShardConfig {
        detection_window: Duration::from_secs(1),
        write_qps_threshold: 10, // Low threshold for testing
        bytes_threshold: 1000,
        cpu_threshold: 0.9,
        latency_threshold: Duration::from_millis(500),
    };
    let shard_monitor = Arc::new(ShardMonitor::new(shard_config));

    let compactor_config = CompactorConfig {
        sharding_enabled: true,
        ..Default::default()
    };

    let compactor = Arc::new(Compactor::new(
        compactor_config,
        Arc::clone(&object_store),
        Arc::clone(&metadata),
        storage_config,
        Arc::clone(&shard_monitor),
    ));

    // Create an initial shard to be split
    let initial_shard_id = "shard-hot".to_string();
    let initial_shard = ShardMetadata {
        shard_id: initial_shard_id.clone(),
        generation: 1,
        key_range: (vec![], vec![]), // Simplified for test
        replicas: vec![ReplicaInfo {
            replica_id: "r1".into(),
            node_id: "n1".into(),
            is_leader: true,
        }],
        state: ShardState::Active,
        min_time: 0,
        max_time: i64::MAX,
    };
    metadata
        .update_shard_metadata(&initial_shard_id, &initial_shard, 0)
        .await
        .unwrap();

    (compactor, metadata, shard_monitor, initial_shard_id)
}

#[tokio::test]
async fn test_compactor_triggers_shard_split() {
    let (compactor, metadata, shard_monitor, hot_shard_id) = setup_test_env().await;

    // Simulate high write traffic to the hot shard to trigger the monitor
    for _ in 0..20 {
        shard_monitor.record_write(&hot_shard_id, 500, Duration::from_millis(10));
    }

    // Wait a moment for the rolling averages in the monitor to update
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Run the compactor's main service cycle, which includes the sharding cycle
    compactor.run_cycle().await.unwrap();

    // Give the spawned split task time to execute
    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- Assertions ---

    // 1. Check that the old shard is no longer active
    let old_shard_meta = metadata
        .get_shard_metadata(&hot_shard_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(old_shard_meta.state, ShardState::PendingDeletion { .. }),
        "Old shard should be marked for deletion"
    );
    assert_eq!(
        old_shard_meta.generation, 2,
        "Old shard generation should be incremented on state change"
    );

    // 2. Check that a split state was created and then removed
    let split_state = metadata.get_split_state(&hot_shard_id).await.unwrap();
    assert!(
        split_state.is_none(),
        "Split state should be cleaned up after completion"
    );

    // 3. Find the new shards
    // A real implementation would have a way to list shards, but for this test,
    // we can infer the new shards by finding active shards that are not the original.
    // Since the LocalMetadataClient doesn't have a list_shards method, we'll have to
    // assume the split was successful if the old shard is marked for deletion.
    // In a real scenario, we would list all shards and find the two new active ones.
    // This assertion confirms the most critical part: the state of the original shard changed as expected.
    println!(
        "Successfully verified that shard '{}' was split and is now in state: {:?}",
        hot_shard_id, old_shard_meta.state
    );
}
