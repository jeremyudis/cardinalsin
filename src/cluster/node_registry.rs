//! Node registry for tracking active cluster members
//!
//! Maintains a registry of all ingester and query nodes in the cluster,
//! with health checking and automatic failure detection.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Type of node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeType {
    /// Ingester node (receives writes)
    Ingester,
    /// Query node (executes queries)
    Query,
    /// Combined node (both ingester and query)
    Combined,
}

/// Status of a node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is healthy and accepting traffic
    Healthy,
    /// Node is suspected failed (missed heartbeats)
    Suspected,
    /// Node is confirmed failed
    Failed,
    /// Node is draining (preparing to shut down)
    Draining,
}

/// Information about a cluster node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID (unique identifier)
    pub id: String,
    /// Node address
    pub addr: SocketAddr,
    /// Node type
    pub node_type: NodeType,
    /// Node status
    pub status: NodeStatus,
    /// Last heartbeat time
    #[serde(skip, default = "Instant::now")]
    pub last_heartbeat: Instant,
    /// Shard IDs owned by this node
    pub shards: Vec<String>,
    /// Node capacity (arbitrary units, for load balancing)
    pub capacity: u32,
    /// Current load (0-100%)
    pub load_percent: u8,
}

impl NodeInfo {
    /// Create a new node info
    pub fn new(id: String, addr: SocketAddr, node_type: NodeType) -> Self {
        Self {
            id,
            addr,
            node_type,
            status: NodeStatus::Healthy,
            last_heartbeat: Instant::now(),
            shards: Vec::new(),
            capacity: 100,
            load_percent: 0,
        }
    }

    /// Check if this node can accept new writes
    pub fn can_accept_writes(&self) -> bool {
        matches!(self.status, NodeStatus::Healthy)
            && matches!(self.node_type, NodeType::Ingester | NodeType::Combined)
            && self.load_percent < 95
    }

    /// Check if this node can accept queries
    pub fn can_accept_queries(&self) -> bool {
        matches!(self.status, NodeStatus::Healthy | NodeStatus::Draining)
            && matches!(self.node_type, NodeType::Query | NodeType::Combined)
    }
}

/// Node registry maintains the list of active nodes
pub struct NodeRegistry {
    /// Map of node ID -> node info
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
    /// Node timeout duration
    timeout: Duration,
}

impl NodeRegistry {
    /// Create a new node registry
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Register a new node or update existing one
    pub async fn register_node(&self, node: NodeInfo) {
        let mut nodes = self.nodes.write().await;
        info!(
            "Registering node {} ({:?}) at {}",
            node.id, node.node_type, node.addr
        );
        nodes.insert(node.id.clone(), node);
    }

    /// Update node heartbeat
    pub async fn heartbeat(&self, node_id: &str) -> bool {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.last_heartbeat = Instant::now();
            if matches!(node.status, NodeStatus::Suspected) {
                node.status = NodeStatus::Healthy;
                info!("Node {} recovered", node_id);
            }
            true
        } else {
            warn!("Heartbeat from unknown node: {}", node_id);
            false
        }
    }

    /// Update node's shard assignments
    pub async fn update_shards(&self, node_id: &str, shards: Vec<String>) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            debug!("Updating shards for node {}: {:?}", node_id, shards);
            node.shards = shards;
        }
    }

    /// Update node load
    pub async fn update_load(&self, node_id: &str, load_percent: u8) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.load_percent = load_percent;
        }
    }

    /// Mark a node as draining (preparing for shutdown)
    pub async fn drain_node(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            info!("Marking node {} as draining", node_id);
            node.status = NodeStatus::Draining;
        }
    }

    /// Remove a node from the registry
    pub async fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.remove(node_id) {
            info!("Removed node {} from registry", node.id);
        }
    }

    /// Get all nodes
    pub async fn get_all_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.read().await.values().cloned().collect()
    }

    /// Get healthy ingester nodes
    pub async fn get_healthy_ingesters(&self) -> Vec<NodeInfo> {
        self.nodes
            .read()
            .await
            .values()
            .filter(|n| n.can_accept_writes())
            .cloned()
            .collect()
    }

    /// Get healthy query nodes
    pub async fn get_healthy_query_nodes(&self) -> Vec<NodeInfo> {
        self.nodes
            .read()
            .await
            .values()
            .filter(|n| n.can_accept_queries())
            .cloned()
            .collect()
    }

    /// Get node by ID
    pub async fn get_node(&self, node_id: &str) -> Option<NodeInfo> {
        self.nodes.read().await.get(node_id).cloned()
    }

    /// Get nodes owning a specific shard
    pub async fn get_nodes_for_shard(&self, shard_id: &str) -> Vec<NodeInfo> {
        self.nodes
            .read()
            .await
            .values()
            .filter(|n| n.shards.contains(&shard_id.to_string()))
            .cloned()
            .collect()
    }

    /// Run periodic health checks
    pub async fn run_health_checks(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            interval.tick().await;

            let mut nodes = self.nodes.write().await;
            let now = Instant::now();

            for node in nodes.values_mut() {
                let elapsed = now.duration_since(node.last_heartbeat);

                if elapsed > self.timeout {
                    if matches!(node.status, NodeStatus::Healthy | NodeStatus::Suspected) {
                        warn!(
                            "Node {} missed heartbeat ({}s), marking as failed",
                            node.id,
                            elapsed.as_secs()
                        );
                        node.status = NodeStatus::Failed;
                    }
                } else if elapsed > self.timeout / 2 {
                    if matches!(node.status, NodeStatus::Healthy) {
                        warn!(
                            "Node {} missed heartbeat ({}s), marking as suspected",
                            node.id,
                            elapsed.as_secs()
                        );
                        node.status = NodeStatus::Suspected;
                    }
                }
            }
        }
    }

    /// Get cluster statistics
    pub async fn get_stats(&self) -> ClusterStats {
        let nodes = self.nodes.read().await;

        let total_nodes = nodes.len();
        let healthy_nodes = nodes.values().filter(|n| matches!(n.status, NodeStatus::Healthy)).count();
        let ingester_nodes = nodes
            .values()
            .filter(|n| matches!(n.node_type, NodeType::Ingester | NodeType::Combined))
            .count();
        let query_nodes = nodes
            .values()
            .filter(|n| matches!(n.node_type, NodeType::Query | NodeType::Combined))
            .count();

        ClusterStats {
            total_nodes,
            healthy_nodes,
            ingester_nodes,
            query_nodes,
        }
    }
}

/// Cluster statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStats {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub ingester_nodes: usize,
    pub query_nodes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_registration() {
        let registry = NodeRegistry::new(30);

        let node = NodeInfo::new(
            "node1".to_string(),
            "127.0.0.1:8080".parse().unwrap(),
            NodeType::Ingester,
        );

        registry.register_node(node.clone()).await;

        let retrieved = registry.get_node("node1").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().addr, node.addr);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let registry = NodeRegistry::new(30);

        let node = NodeInfo::new(
            "node1".to_string(),
            "127.0.0.1:8080".parse().unwrap(),
            NodeType::Ingester,
        );

        registry.register_node(node).await;

        // Simulate heartbeat
        let success = registry.heartbeat("node1").await;
        assert!(success);

        // Unknown node
        let success = registry.heartbeat("node2").await;
        assert!(!success);
    }

    #[tokio::test]
    async fn test_shard_filtering() {
        let registry = NodeRegistry::new(30);

        let mut node1 = NodeInfo::new(
            "node1".to_string(),
            "127.0.0.1:8081".parse().unwrap(),
            NodeType::Ingester,
        );
        node1.shards = vec!["shard-1".to_string(), "shard-2".to_string()];

        let mut node2 = NodeInfo::new(
            "node2".to_string(),
            "127.0.0.1:8082".parse().unwrap(),
            NodeType::Ingester,
        );
        node2.shards = vec!["shard-3".to_string()];

        registry.register_node(node1).await;
        registry.register_node(node2).await;

        let nodes = registry.get_nodes_for_shard("shard-1").await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "node1");
    }
}
