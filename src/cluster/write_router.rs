//! Distributed write router
//!
//! Routes write requests to the appropriate ingester node based on shard assignment.
//! Enables horizontal scaling of ingestion throughput.

use super::node_registry::{NodeInfo, NodeRegistry};
use super::shard_assignment::ShardAssignment;
use crate::api::ingest::ShardWriteRouter;
use crate::ingester::encode_record_batch_ipc;
use crate::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, warn};

/// Distributed write router for scaling write throughput
pub struct DistributedWriteRouter {
    /// Shard assignment manager
    assignments: Arc<ShardAssignment>,
    /// Node registry
    nodes: Arc<NodeRegistry>,
    /// Shared HTTP client for remote forwarding
    http_client: reqwest::Client,
}

impl DistributedWriteRouter {
    /// Create a new distributed write router
    pub fn new(assignments: Arc<ShardAssignment>, nodes: Arc<NodeRegistry>) -> Self {
        Self {
            assignments,
            nodes,
            http_client: reqwest::Client::new(),
        }
    }

    /// Route a write to the appropriate ingester node.
    pub async fn route_write(&self, shard_id: &str) -> Result<Option<NodeInfo>> {
        <Self as ShardWriteRouter>::route_write(self, shard_id).await
    }

    /// Forward a shard-local Arrow batch to another ingester node.
    pub async fn forward_write(
        &self,
        target_node: &NodeInfo,
        batch: &RecordBatch,
        tenant_id: &str,
    ) -> Result<()> {
        <Self as ShardWriteRouter>::forward_write(self, target_node, batch, tenant_id).await
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

#[async_trait]
impl ShardWriteRouter for DistributedWriteRouter {
    /// Route a write to the appropriate ingester node.
    async fn route_write(&self, shard_id: &str) -> Result<Option<NodeInfo>> {
        let node_id = self.assignments.assign_shard(shard_id).await?;

        if let Some(node) = self.nodes.get_node(&node_id).await {
            if node.can_accept_writes() {
                debug!("Routing write for shard {} to node {}", shard_id, node_id);
                return Ok(Some(node));
            }

            warn!(
                "Assigned node {} cannot accept writes, reassigning",
                node_id
            );
            self.assignments.unassign_shard(shard_id).await;
            return Box::pin(self.route_write(shard_id)).await;
        }

        Err(crate::Error::Internal(format!(
            "No healthy node available for shard {}",
            shard_id
        )))
    }

    /// Forward a shard-local Arrow batch to another ingester node.
    async fn forward_write(
        &self,
        target_node: &NodeInfo,
        batch: &RecordBatch,
        tenant_id: &str,
    ) -> Result<()> {
        let url = format!("http://{}/internal/v1/ingest/arrow", target_node.addr);
        let payload = encode_record_batch_ipc(batch)?;
        let response = self
            .http_client
            .post(&url)
            .header("content-type", "application/vnd.apache.arrow.stream")
            .header("x-cardinalsin-tenant-id", tenant_id)
            .body(payload)
            .send()
            .await
            .map_err(|e| {
                crate::Error::Internal(format!(
                    "Failed to forward shard write to {}: {}",
                    target_node.id, e
                ))
            })?;

        if !response.status().is_success() {
            return Err(crate::Error::Internal(format!(
                "Remote ingester {} rejected forwarded shard write with status {}",
                target_node.id,
                response.status()
            )));
        }

        Ok(())
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
