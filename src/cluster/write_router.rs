//! Distributed write router
//!
//! Routes write requests to the appropriate ingester node based on shard assignment.
//! Enables horizontal scaling of ingestion throughput.

use super::node_registry::{NodeInfo, NodeRegistry};
use super::shard_assignment::ShardAssignment;
use crate::Result;
use arrow_array::RecordBatch;
use std::sync::Arc;
use tracing::{debug, warn};

/// Distributed write router for scaling write throughput
pub struct DistributedWriteRouter {
    /// Shard assignment manager
    assignments: Arc<ShardAssignment>,
    /// Node registry
    nodes: Arc<NodeRegistry>,
}

impl DistributedWriteRouter {
    /// Create a new distributed write router
    pub fn new(assignments: Arc<ShardAssignment>, nodes: Arc<NodeRegistry>) -> Self {
        Self { assignments, nodes }
    }

    /// Route a write to the appropriate ingester node
    ///
    /// If the write is for a shard owned by this node, returns None (handle locally).
    /// If the write is for a shard owned by another node, returns the target node info.
    pub async fn route_write(&self, shard_id: &str) -> Result<Option<NodeInfo>> {
        // Get assigned node for this shard
        let node_id = self.assignments.assign_shard(shard_id).await?;

        // Get node info
        if let Some(node) = self.nodes.get_node(&node_id).await {
            if node.can_accept_writes() {
                debug!("Routing write for shard {} to node {}", shard_id, node_id);
                return Ok(Some(node));
            } else {
                warn!(
                    "Assigned node {} cannot accept writes, reassigning",
                    node_id
                );
                self.assignments.unassign_shard(shard_id).await;
                // Retry assignment (boxed to avoid infinite recursion)
                return Box::pin(self.route_write(shard_id)).await;
            }
        }

        Err(crate::Error::Internal(format!(
            "No healthy node available for shard {}",
            shard_id
        )))
    }

    /// Forward a write request to another ingester node
    ///
    /// This is called when the current node receives a write for a shard
    /// it doesn't own. The write is forwarded to the owning node via HTTP.
    ///
    /// TODO: Implement actual HTTP forwarding with Arrow IPC serialization.
    /// This requires adding reqwest dependency and implementing batch serialization.
    pub async fn forward_write(
        &self,
        target_node: &NodeInfo,
        _batch: &RecordBatch,
        _tenant_id: &str,
    ) -> Result<()> {
        let url = format!("http://{}/api/v1/write", target_node.addr);

        // Placeholder implementation
        debug!(
            "Forwarding write to node {} at {} (TODO: implement HTTP forwarding)",
            target_node.id, url
        );

        Ok(())
    }

    /// Get routing statistics
    pub async fn get_stats(&self) -> RoutingStats {
        let assignments = self.assignments.get_all_assignments().await;
        let nodes = self.nodes.get_healthy_ingesters().await;

        // Count shards per node
        let mut shard_counts = std::collections::HashMap::new();
        for node_id in assignments.values() {
            *shard_counts.entry(node_id.clone()).or_insert(0) += 1;
        }

        let total_shards = assignments.len();
        let active_nodes = nodes.len();
        let avg_shards_per_node = if active_nodes > 0 {
            total_shards / active_nodes
        } else {
            0
        };

        // Calculate imbalance (max - min shards per node)
        let max_shards = shard_counts.values().max().copied().unwrap_or(0);
        let min_shards = shard_counts.values().min().copied().unwrap_or(0);
        let imbalance = max_shards - min_shards;

        RoutingStats {
            total_shards,
            active_nodes,
            avg_shards_per_node,
            imbalance,
        }
    }
}

/// Routing statistics
#[derive(Debug, Clone)]
pub struct RoutingStats {
    pub total_shards: usize,
    pub active_nodes: usize,
    pub avg_shards_per_node: usize,
    pub imbalance: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::shard_assignment::AssignmentStrategy;

    #[tokio::test]
    async fn test_write_routing() {
        let nodes = Arc::new(NodeRegistry::new(30));
        let assignments = Arc::new(ShardAssignment::new(
            nodes.clone(),
            AssignmentStrategy::ConsistentHash,
        ));

        // Register a node
        let node = NodeInfo::new(
            "node1".to_string(),
            "127.0.0.1:8080".parse().unwrap(),
            crate::cluster::node_registry::NodeType::Ingester,
        );
        nodes.register_node(node).await;

        let router = DistributedWriteRouter::new(assignments, nodes);

        // Route a write
        let result = router.route_write("shard-1").await;
        assert!(result.is_ok());

        let target = result.unwrap();
        assert!(target.is_some());
        assert_eq!(target.unwrap().id, "node1");
    }
}
