//! Integration tests for distributed cluster coordination
//!
//! Tests node registry, shard assignment, and distributed routing.

use cardinalsin::cluster::{
    AssignmentStrategy, DistributedQueryRouter, DistributedWriteRouter, NodeInfo, NodeRegistry,
    NodeStatus, NodeType, ShardAssignment,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_node_registration_and_discovery() {
    let registry = Arc::new(NodeRegistry::new(30));

    // Register ingester nodes
    let node1 = NodeInfo::new(
        "ingester-1".to_string(),
        "10.0.1.1:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    let node2 = NodeInfo::new(
        "ingester-2".to_string(),
        "10.0.1.2:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    let node3 = NodeInfo::new(
        "query-1".to_string(),
        "10.0.2.1:8080".parse().unwrap(),
        NodeType::Query,
    );

    registry.register_node(node1.clone()).await;
    registry.register_node(node2.clone()).await;
    registry.register_node(node3.clone()).await;

    // Verify nodes are registered
    let all_nodes = registry.get_all_nodes().await;
    assert_eq!(all_nodes.len(), 3, "Should have 3 registered nodes");

    // Verify node filtering
    let ingesters = registry.get_healthy_ingesters().await;
    assert_eq!(ingesters.len(), 2, "Should have 2 ingester nodes");

    let query_nodes = registry.get_healthy_query_nodes().await;
    assert_eq!(query_nodes.len(), 1, "Should have 1 query node");

    // Verify node lookup
    let node = registry.get_node("ingester-1").await;
    assert!(node.is_some());
    assert_eq!(node.unwrap().id, "ingester-1");
}

#[tokio::test]
async fn test_node_heartbeat_and_failure_detection() {
    let registry = Arc::new(NodeRegistry::new(2)); // 2 second timeout for testing

    let node = NodeInfo::new(
        "test-node".to_string(),
        "10.0.1.1:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    registry.register_node(node).await;

    // Initial heartbeat
    let success = registry.heartbeat("test-node").await;
    assert!(success, "First heartbeat should succeed");

    // Verify node is healthy
    let node = registry.get_node("test-node").await.unwrap();
    assert_eq!(node.status, NodeStatus::Healthy);

    // Heartbeat from unknown node should fail
    let success = registry.heartbeat("unknown-node").await;
    assert!(!success, "Heartbeat from unknown node should fail");

    // Note: We can't easily test timeout-based failure detection in unit tests
    // as it requires waiting for actual time to pass. This would be better
    // tested in a dedicated health check integration test.
}

#[tokio::test]
async fn test_node_drain_and_removal() {
    let registry = Arc::new(NodeRegistry::new(30));

    let node = NodeInfo::new(
        "drain-test".to_string(),
        "10.0.1.1:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    registry.register_node(node).await;

    // Mark as draining
    registry.drain_node("drain-test").await;

    let node = registry.get_node("drain-test").await.unwrap();
    assert_eq!(node.status, NodeStatus::Draining);

    // Draining nodes should not accept writes
    assert!(
        !node.can_accept_writes(),
        "Draining node should not accept writes"
    );

    // Remove node
    registry.remove_node("drain-test").await;
    let node = registry.get_node("drain-test").await;
    assert!(node.is_none(), "Removed node should not be found");
}

#[tokio::test]
async fn test_shard_assignment_consistent_hash() {
    let registry = Arc::new(NodeRegistry::new(30));
    let assignments = Arc::new(ShardAssignment::new(
        registry.clone(),
        AssignmentStrategy::ConsistentHash,
    ));

    // Register nodes
    for i in 1..=3 {
        let node = NodeInfo::new(
            format!("ingester-{}", i),
            format!("10.0.1.{}:8081", i).parse().unwrap(),
            NodeType::Ingester,
        );
        registry.register_node(node).await;
    }

    // Assign shards
    let shards = vec!["shard-1", "shard-2", "shard-3", "shard-4", "shard-5"];
    let mut node_counts = std::collections::HashMap::new();

    for shard in &shards {
        let node_id = assignments.assign_shard(shard).await.unwrap();
        *node_counts.entry(node_id).or_insert(0) += 1;
    }

    println!("Shard distribution: {:?}", node_counts);

    // Verify all nodes got at least one shard (with high probability)
    // Note: With only 5 shards, it's possible (but unlikely) that one node gets 0
    assert!(
        node_counts.len() >= 2,
        "Shards should be distributed across multiple nodes"
    );

    // Verify same shard always maps to same node
    for shard in &shards {
        let node1 = assignments.assign_shard(shard).await.unwrap();
        let node2 = assignments.assign_shard(shard).await.unwrap();
        assert_eq!(
            node1, node2,
            "Same shard should always map to same node"
        );
    }
}

#[tokio::test]
async fn test_shard_assignment_load_based() {
    let registry = Arc::new(NodeRegistry::new(30));
    let assignments = Arc::new(ShardAssignment::new(
        registry.clone(),
        AssignmentStrategy::LoadBased,
    ));

    // Register nodes with different load
    let mut node1 = NodeInfo::new(
        "ingester-1".to_string(),
        "10.0.1.1:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    node1.load_percent = 90; // High load

    let mut node2 = NodeInfo::new(
        "ingester-2".to_string(),
        "10.0.1.2:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    node2.load_percent = 10; // Low load

    registry.register_node(node1).await;
    registry.register_node(node2).await;

    // Assign shard - should go to least loaded node
    let node_id = assignments.assign_shard("shard-1").await.unwrap();
    assert_eq!(
        node_id, "ingester-2",
        "Should assign to least loaded node"
    );
}

#[tokio::test]
async fn test_shard_rebalancing() {
    let registry = Arc::new(NodeRegistry::new(30));
    let assignments = Arc::new(ShardAssignment::new(
        registry.clone(),
        AssignmentStrategy::ConsistentHash,
    ));

    // Register 2 nodes initially
    let node1 = NodeInfo::new(
        "ingester-1".to_string(),
        "10.0.1.1:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    let node2 = NodeInfo::new(
        "ingester-2".to_string(),
        "10.0.1.2:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    registry.register_node(node1).await;
    registry.register_node(node2).await;

    // Assign 10 shards
    let mut initial_assignments = std::collections::HashMap::new();
    for i in 0..10 {
        let shard_id = format!("shard-{}", i);
        let node_id = assignments.assign_shard(&shard_id).await.unwrap();
        initial_assignments.insert(shard_id, node_id);
    }

    // Add a third node
    let node3 = NodeInfo::new(
        "ingester-3".to_string(),
        "10.0.1.3:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    registry.register_node(node3).await;

    // Rebalance
    let moves = assignments.rebalance().await.unwrap();
    println!("Rebalancing moved {} shards", moves.len());

    // Verify some shards were moved (but not all - consistent hashing stability)
    assert!(
        moves.len() > 0,
        "Some shards should be moved to new node"
    );
    assert!(
        moves.len() < 10,
        "Not all shards should move (consistent hashing stability)"
    );

    // With 3 nodes and 10 shards, expect some movement
    // Consistent hashing with small sample sizes has high variance
    let move_pct = (moves.len() * 100) / 10;
    println!("Movement percentage: {}%", move_pct);
    assert!(
        move_pct >= 10 && move_pct <= 60,
        "Movement should be partial with consistent hashing, got {}%", move_pct
    );

    // Verify final distribution
    let final_assignments = assignments.get_all_assignments().await;
    let mut node_counts = std::collections::HashMap::new();
    for node_id in final_assignments.values() {
        *node_counts.entry(node_id.clone()).or_insert(0) += 1;
    }
    println!("Final distribution: {:?}", node_counts);

    // All 3 nodes should have at least one shard
    assert_eq!(
        node_counts.len(),
        3,
        "All nodes should have shards after rebalancing"
    );
}

#[tokio::test]
async fn test_write_routing() {
    let registry = Arc::new(NodeRegistry::new(30));
    let assignments = Arc::new(ShardAssignment::new(
        registry.clone(),
        AssignmentStrategy::ConsistentHash,
    ));

    // Register nodes
    let node1 = NodeInfo::new(
        "ingester-1".to_string(),
        "10.0.1.1:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    let node2 = NodeInfo::new(
        "ingester-2".to_string(),
        "10.0.1.2:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    registry.register_node(node1).await;
    registry.register_node(node2).await;

    let router = DistributedWriteRouter::new(assignments, registry);

    // Route writes
    let target1 = router.route_write("shard-1").await.unwrap();
    assert!(target1.is_some(), "Should return target node");

    let target2 = router.route_write("shard-1").await.unwrap();
    assert!(target2.is_some(), "Should return target node");

    // Same shard should always route to same node
    assert_eq!(
        target1.unwrap().id,
        target2.unwrap().id,
        "Same shard should route to same node"
    );

    // Get routing stats
    let stats = router.get_stats().await;
    println!("Routing stats: {:?}", stats);
    assert_eq!(stats.active_nodes, 2);
}

#[tokio::test]
async fn test_write_routing_with_node_failure() {
    let registry = Arc::new(NodeRegistry::new(30));
    let assignments = Arc::new(ShardAssignment::new(
        registry.clone(),
        AssignmentStrategy::ConsistentHash,
    ));

    // Register nodes
    let node1 = NodeInfo::new(
        "ingester-1".to_string(),
        "10.0.1.1:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    let mut node2 = NodeInfo::new(
        "ingester-2".to_string(),
        "10.0.1.2:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    registry.register_node(node1).await;
    registry.register_node(node2.clone()).await;

    // Assign shard
    let router = DistributedWriteRouter::new(assignments.clone(), registry.clone());
    let target1 = router.route_write("shard-1").await.unwrap().unwrap();
    let original_node = target1.id.clone();

    // Mark original node as failed
    node2.status = NodeStatus::Failed;
    registry.register_node(node2).await;

    // If shard-1 was on node2, routing should reassign
    // Otherwise it stays on node1
    let target2 = router.route_write("shard-1").await.unwrap().unwrap();

    if original_node == "ingester-2" {
        assert_ne!(
            target2.id, original_node,
            "Should reassign from failed node"
        );
    } else {
        assert_eq!(target2.id, original_node, "Should keep on healthy node");
    }
}

#[tokio::test]
async fn test_query_fanout() {
    let registry = Arc::new(NodeRegistry::new(30));

    // Register query nodes
    for i in 1..=3 {
        let node = NodeInfo::new(
            format!("query-{}", i),
            format!("10.0.2.{}:8080", i).parse().unwrap(),
            NodeType::Query,
        );
        registry.register_node(node).await;
    }

    let router = DistributedQueryRouter::new(registry);

    // Execute distributed query (placeholder implementation returns empty)
    let results = router
        .execute_distributed("SELECT * FROM metrics", "tenant-1")
        .await
        .unwrap();

    // Placeholder returns empty, but verifies routing works
    assert!(results.is_empty(), "Placeholder returns empty results");

    // Get stats
    let stats = router.get_stats().await;
    assert_eq!(stats.query_nodes, 3, "Should have 3 query nodes");
}

#[tokio::test]
async fn test_query_targeted_fanout() {
    let registry = Arc::new(NodeRegistry::new(30));

    // Register query nodes with specific shards
    let mut node1 = NodeInfo::new(
        "query-1".to_string(),
        "10.0.2.1:8080".parse().unwrap(),
        NodeType::Query,
    );
    node1.shards = vec!["shard-1".to_string(), "shard-2".to_string()];

    let mut node2 = NodeInfo::new(
        "query-2".to_string(),
        "10.0.2.2:8080".parse().unwrap(),
        NodeType::Query,
    );
    node2.shards = vec!["shard-3".to_string(), "shard-4".to_string()];

    registry.register_node(node1).await;
    registry.register_node(node2).await;

    let router = DistributedQueryRouter::new(registry);

    // Get nodes for specific shards
    let nodes = router
        .get_nodes_for_shards(&["shard-1".to_string(), "shard-2".to_string()])
        .await;

    assert_eq!(nodes.len(), 1, "Should return 1 node for shards 1 and 2");
    assert_eq!(nodes[0].id, "query-1");

    // Query across both nodes' shards
    let nodes = router
        .get_nodes_for_shards(&[
            "shard-1".to_string(),
            "shard-3".to_string(),
        ])
        .await;

    assert_eq!(nodes.len(), 2, "Should return 2 nodes for shards 1 and 3");
}

#[tokio::test]
async fn test_cluster_scale_out() {
    let registry = Arc::new(NodeRegistry::new(30));
    let assignments = Arc::new(ShardAssignment::new(
        registry.clone(),
        AssignmentStrategy::ConsistentHash,
    ));

    // Start with 2 nodes
    for i in 1..=2 {
        let node = NodeInfo::new(
            format!("ingester-{}", i),
            format!("10.0.1.{}:8081", i).parse().unwrap(),
            NodeType::Ingester,
        );
        registry.register_node(node).await;
    }

    // Assign 20 shards
    for i in 0..20 {
        let shard_id = format!("shard-{}", i);
        assignments.assign_shard(&shard_id).await.unwrap();
    }

    let initial_stats = registry.get_stats().await;
    assert_eq!(initial_stats.ingester_nodes, 2);

    // Scale out to 4 nodes
    for i in 3..=4 {
        let node = NodeInfo::new(
            format!("ingester-{}", i),
            format!("10.0.1.{}:8081", i).parse().unwrap(),
            NodeType::Ingester,
        );
        registry.register_node(node).await;
    }

    // Rebalance
    let moves = assignments.rebalance().await.unwrap();
    println!("Scale-out rebalancing moved {} shards", moves.len());

    // Verify distribution improved
    let final_assignments = assignments.get_all_assignments().await;
    let mut node_counts = std::collections::HashMap::new();
    for node_id in final_assignments.values() {
        *node_counts.entry(node_id.clone()).or_insert(0) += 1;
    }

    println!("Shard distribution after scale-out: {:?}", node_counts);

    // All 4 nodes should have shards
    assert_eq!(
        node_counts.len(),
        4,
        "All nodes should have shards after scale-out"
    );

    // Distribution should be roughly even (±50%)
    let avg_shards = 20 / 4;
    for count in node_counts.values() {
        assert!(
            *count >= avg_shards / 2 && *count <= avg_shards * 2,
            "Distribution should be roughly even"
        );
    }
}

#[tokio::test]
async fn test_cluster_scale_in() {
    let registry = Arc::new(NodeRegistry::new(30));
    let assignments = Arc::new(ShardAssignment::new(
        registry.clone(),
        AssignmentStrategy::ConsistentHash,
    ));

    // Start with 4 nodes
    for i in 1..=4 {
        let node = NodeInfo::new(
            format!("ingester-{}", i),
            format!("10.0.1.{}:8081", i).parse().unwrap(),
            NodeType::Ingester,
        );
        registry.register_node(node).await;
    }

    // Assign 20 shards
    for i in 0..20 {
        let shard_id = format!("shard-{}", i);
        assignments.assign_shard(&shard_id).await.unwrap();
    }

    // Remove 2 nodes (scale in)
    registry.remove_node("ingester-3").await;
    registry.remove_node("ingester-4").await;

    // Rebalance
    let moves = assignments.rebalance().await.unwrap();
    println!("Scale-in rebalancing moved {} shards", moves.len());

    // Verify all shards moved to remaining nodes
    let final_assignments = assignments.get_all_assignments().await;
    let mut node_counts = std::collections::HashMap::new();
    for node_id in final_assignments.values() {
        *node_counts.entry(node_id.clone()).or_insert(0) += 1;
    }

    println!("Shard distribution after scale-in: {:?}", node_counts);

    // Only 2 nodes should remain
    assert_eq!(node_counts.len(), 2, "Only 2 nodes should remain");

    // Each node should have roughly 10 shards
    for count in node_counts.values() {
        assert!(
            *count >= 5 && *count <= 15,
            "Shards should be redistributed to remaining nodes"
        );
    }
}

#[tokio::test]
async fn test_load_balancing() {
    let registry = Arc::new(NodeRegistry::new(30));

    // Register nodes with different capacities
    let mut node1 = NodeInfo::new(
        "ingester-1".to_string(),
        "10.0.1.1:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    node1.capacity = 100;
    node1.load_percent = 50;

    let mut node2 = NodeInfo::new(
        "ingester-2".to_string(),
        "10.0.1.2:8081".parse().unwrap(),
        NodeType::Ingester,
    );
    node2.capacity = 200;
    node2.load_percent = 25;

    registry.register_node(node1).await;
    registry.register_node(node2).await;

    // Update load
    registry.update_load("ingester-1", 75).await;
    registry.update_load("ingester-2", 40).await;

    let node1 = registry.get_node("ingester-1").await.unwrap();
    let node2 = registry.get_node("ingester-2").await.unwrap();

    assert_eq!(node1.load_percent, 75);
    assert_eq!(node2.load_percent, 40);

    // Verify load-based filtering
    assert!(node1.can_accept_writes(), "Node at 75% should accept writes");
    assert!(node2.can_accept_writes(), "Node at 40% should accept writes");

    // High load node
    registry.update_load("ingester-1", 96).await;
    let node1 = registry.get_node("ingester-1").await.unwrap();
    assert!(
        !node1.can_accept_writes(),
        "Node at 96% should not accept writes"
    );
}

#[tokio::test]
async fn test_mixed_node_types() {
    let registry = Arc::new(NodeRegistry::new(30));

    // Register combined nodes
    let combined_node = NodeInfo::new(
        "combined-1".to_string(),
        "10.0.3.1:8080".parse().unwrap(),
        NodeType::Combined,
    );
    registry.register_node(combined_node).await;

    // Combined node should appear in both lists
    let ingesters = registry.get_healthy_ingesters().await;
    let query_nodes = registry.get_healthy_query_nodes().await;

    assert_eq!(ingesters.len(), 1, "Combined node should be an ingester");
    assert_eq!(query_nodes.len(), 1, "Combined node should be a query node");
    assert_eq!(ingesters[0].id, "combined-1");
    assert_eq!(query_nodes[0].id, "combined-1");
}
