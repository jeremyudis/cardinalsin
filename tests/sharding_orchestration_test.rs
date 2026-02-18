//! Tests for the sharding orchestration loop in the compactor.

use cardinalsin::compactor::{Compactor, CompactorConfig};
use cardinalsin::metadata::{LocalMetadataClient, MetadataClient};
use cardinalsin::sharding::{
    HotShardConfig, ReplicaInfo, ShardMetadata, ShardMonitor, ShardState, SplitPhase,
};
use cardinalsin::StorageConfig;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// Sets up a test environment with a compactor and its dependencies.
/// Returns the compactor, metadata client, shard monitor, and the ID of an initial shard.
async fn setup_test_env() -> (Arc<Compactor>, Arc<dyn MetadataClient>, Arc<ShardMonitor>, String) {
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

    // Simulate high write traffic to the hot shard.
    for _ in 0..20 {
        shard_monitor.record_write(&hot_shard_id, 500, Duration::from_millis(10));
    }

    // First cycle marks the shard as hot.
    compactor.run_cycle().await.unwrap();

    // Wait for the hotness detection window, then provide fresh writes and evaluate again.
    // The second cycle is what actually emits Split(...) for sustained hotness.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    for _ in 0..20 {
        shard_monitor.record_write(&hot_shard_id, 500, Duration::from_millis(10));
    }
    compactor.run_cycle().await.unwrap();

    // Give the spawned split task time to start executing and persist split state.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Assertions ---

    // 1. Check that the old shard still exists and is active while split orchestration runs.
    // The background split flow includes a 10s dual-write wait, so cutover does not complete
    // within this short test window.
    let old_shard_meta = metadata
        .get_shard_metadata(&hot_shard_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(old_shard_meta.state, ShardState::Active),
        "Old shard should remain active while split is in progress"
    );
    assert_eq!(
        old_shard_meta.generation, 1,
        "Old shard generation should remain unchanged before cutover"
    );

    // 2. Check that split orchestration was initiated and persisted.
    let split_state = metadata.get_split_state(&hot_shard_id).await.unwrap();
    assert!(
        split_state.is_some(),
        "Split state should exist once orchestration has been triggered"
    );
    let split_state = split_state.unwrap();
    assert_eq!(
        split_state.phase,
        SplitPhase::DualWrite,
        "Split should be in dual-write phase shortly after orchestration starts"
    );
    assert_eq!(
        split_state.new_shards.len(),
        2,
        "Split orchestration should allocate two new shards"
    );

    // 3. We do not wait for full completion here because the split flow includes deliberate
    // phase delays and cleanup grace periods.
    println!(
        "Successfully verified that shard '{}' split orchestration started (phase: {:?})",
        hot_shard_id, split_state.phase
    );
}
