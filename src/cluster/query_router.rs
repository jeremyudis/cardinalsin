//! Distributed query router
//!
//! Fans out queries to all relevant query nodes and merges results.
//! Enables horizontal scaling of query throughput.

use super::node_registry::{NodeInfo, NodeRegistry};
use crate::Result;
use arrow_array::RecordBatch;
use std::sync::Arc;
use tracing::{debug, warn};

/// Distributed query router for scaling query throughput
pub struct DistributedQueryRouter {
    /// Node registry
    nodes: Arc<NodeRegistry>,
    /// Maximum fanout parallelism
    max_fanout: usize,
}

impl DistributedQueryRouter {
    /// Create a new distributed query router
    pub fn new(nodes: Arc<NodeRegistry>) -> Self {
        Self {
            nodes,
            max_fanout: 10,
        }
    }

    /// Create with custom fanout limit
    pub fn with_max_fanout(mut self, max_fanout: usize) -> Self {
        self.max_fanout = max_fanout;
        self
    }

    /// Execute a query across all query nodes and merge results
    ///
    /// This is the main entry point for distributed query execution.
    /// The query is sent to all healthy query nodes in parallel, and
    /// results are merged.
    pub async fn execute_distributed(
        &self,
        sql: &str,
        tenant_id: &str,
    ) -> Result<Vec<RecordBatch>> {
        let nodes = self.nodes.get_healthy_query_nodes().await;

        if nodes.is_empty() {
            return Err(crate::Error::Internal("No healthy query nodes available".to_string()));
        }

        debug!("Executing query across {} nodes", nodes.len());

        // Limit fanout to avoid overwhelming the system
        let nodes_to_query = if nodes.len() > self.max_fanout {
            warn!(
                "Limiting query fanout from {} to {} nodes",
                nodes.len(),
                self.max_fanout
            );
            &nodes[..self.max_fanout]
        } else {
            &nodes[..]
        };

        // Fan out queries in parallel
        let mut handles = Vec::new();
        for node in nodes_to_query {
            let sql = sql.to_string();
            let tenant_id = tenant_id.to_string();
            let node = node.clone();

            let handle = tokio::spawn(async move {
                Self::execute_on_node(&node, &sql, &tenant_id).await
            });

            handles.push(handle);
        }

        // Collect results
        let mut all_batches = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(batches)) => {
                    all_batches.extend(batches);
                }
                Ok(Err(e)) => {
                    warn!("Query failed on node: {}", e);
                    // Continue with other nodes
                }
                Err(e) => {
                    warn!("Query task panicked: {}", e);
                }
            }
        }

        // Merge results
        // In a real implementation, this would need to handle:
        // - Deduplication (if shards are replicated)
        // - Sorting (if ORDER BY is present)
        // - Aggregation (if GROUP BY is present)
        // For now, just concatenate
        Ok(all_batches)
    }

    /// Execute a query on a single node
    ///
    /// TODO: Implement actual HTTP query execution with Arrow IPC.
    /// This requires adding reqwest dependency and implementing result deserialization.
    async fn execute_on_node(
        node: &NodeInfo,
        _sql: &str,
        _tenant_id: &str,
    ) -> Result<Vec<RecordBatch>> {
        let url = format!("http://{}/api/v1/query", node.addr);

        debug!("Executing query on node {} at {} (TODO: implement HTTP query)", node.id, url);

        // Placeholder - in production, this would:
        // 1. Send query via HTTP POST
        // 2. Receive Arrow IPC stream
        // 3. Deserialize to RecordBatches
        Ok(Vec::new())
    }

    /// Get shard coverage for a query
    ///
    /// Returns the list of nodes that need to be queried based on shard assignments.
    /// This enables targeted fanout instead of querying all nodes.
    pub async fn get_nodes_for_shards(&self, shard_ids: &[String]) -> Vec<NodeInfo> {
        let mut nodes = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for shard_id in shard_ids {
            for node in self.nodes.get_nodes_for_shard(shard_id).await {
                if seen.insert(node.id.clone()) {
                    nodes.push(node);
                }
            }
        }

        nodes
    }

    /// Execute a query with targeted fanout
    ///
    /// Only queries nodes that own the relevant shards, reducing unnecessary work.
    pub async fn execute_targeted(
        &self,
        sql: &str,
        tenant_id: &str,
        shard_ids: &[String],
    ) -> Result<Vec<RecordBatch>> {
        let nodes = self.get_nodes_for_shards(shard_ids).await;

        if nodes.is_empty() {
            return Err(crate::Error::Internal(
                "No nodes available for requested shards".to_string(),
            ));
        }

        debug!(
            "Executing targeted query across {} nodes for {} shards",
            nodes.len(),
            shard_ids.len()
        );

        // Fan out queries in parallel (same as execute_distributed)
        let mut handles = Vec::new();
        for node in &nodes {
            let sql = sql.to_string();
            let tenant_id = tenant_id.to_string();
            let node = node.clone();

            let handle = tokio::spawn(async move {
                Self::execute_on_node(&node, &sql, &tenant_id).await
            });

            handles.push(handle);
        }

        // Collect and merge results
        let mut all_batches = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(batches)) => {
                    all_batches.extend(batches);
                }
                Ok(Err(e)) => {
                    warn!("Query failed on node: {}", e);
                }
                Err(e) => {
                    warn!("Query task panicked: {}", e);
                }
            }
        }

        Ok(all_batches)
    }

    /// Get query routing statistics
    pub async fn get_stats(&self) -> QueryRoutingStats {
        let cluster_stats = self.nodes.get_stats().await;

        QueryRoutingStats {
            query_nodes: cluster_stats.query_nodes,
            max_fanout: self.max_fanout,
        }
    }
}

/// Query routing statistics
#[derive(Debug, Clone)]
pub struct QueryRoutingStats {
    pub query_nodes: usize,
    pub max_fanout: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node_registry::NodeType;

    #[tokio::test]
    async fn test_query_routing() {
        let nodes = Arc::new(NodeRegistry::new(30));

        // Register query nodes
        let node1 = NodeInfo::new(
            "query1".to_string(),
            "127.0.0.1:8080".parse().unwrap(),
            NodeType::Query,
        );
        let node2 = NodeInfo::new(
            "query2".to_string(),
            "127.0.0.1:8081".parse().unwrap(),
            NodeType::Query,
        );

        nodes.register_node(node1).await;
        nodes.register_node(node2).await;

        let router = DistributedQueryRouter::new(nodes);

        let stats = router.get_stats().await;
        assert_eq!(stats.query_nodes, 2);
    }

    #[tokio::test]
    async fn test_targeted_fanout() {
        let nodes = Arc::new(NodeRegistry::new(30));

        let mut node1 = NodeInfo::new(
            "query1".to_string(),
            "127.0.0.1:8080".parse().unwrap(),
            NodeType::Query,
        );
        node1.shards = vec!["shard-1".to_string(), "shard-2".to_string()];

        let mut node2 = NodeInfo::new(
            "query2".to_string(),
            "127.0.0.1:8081".parse().unwrap(),
            NodeType::Query,
        );
        node2.shards = vec!["shard-3".to_string()];

        nodes.register_node(node1).await;
        nodes.register_node(node2).await;

        let router = DistributedQueryRouter::new(nodes);

        // Query only shard-1
        let target_nodes = router
            .get_nodes_for_shards(&["shard-1".to_string()])
            .await;

        assert_eq!(target_nodes.len(), 1);
        assert_eq!(target_nodes[0].id, "query1");
    }
}
