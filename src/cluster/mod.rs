//! Cluster coordination for distributed CardinalSin
//!
//! This module provides distributed routing to enable horizontal scaling across
//! multiple ingester and query nodes. Without this, sharding is local-only and
//! the system cannot scale beyond a single node.

pub mod node_registry;
pub mod query_router;
pub mod shard_assignment;
pub mod write_router;

pub use node_registry::{NodeInfo, NodeRegistry, NodeStatus, NodeType};
pub use query_router::DistributedQueryRouter;
pub use shard_assignment::{AssignmentStrategy, ShardAssignment};
pub use write_router::DistributedWriteRouter;

use std::net::SocketAddr;

/// Cluster configuration
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's address
    pub node_addr: SocketAddr,
    /// This node's type (ingester or query)
    pub node_type: NodeType,
    /// Cluster coordinator address (for centralized mode)
    pub coordinator_addr: Option<SocketAddr>,
    /// Heartbeat interval for node health checks
    pub heartbeat_interval_secs: u64,
    /// Node timeout (mark as failed if no heartbeat)
    pub node_timeout_secs: u64,
    /// Rebalancing strategy
    pub rebalance_strategy: AssignmentStrategy,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_addr: "127.0.0.1:8080".parse().unwrap(),
            node_type: NodeType::Ingester,
            coordinator_addr: None,
            heartbeat_interval_secs: 10,
            node_timeout_secs: 30,
            rebalance_strategy: AssignmentStrategy::ConsistentHash,
        }
    }
}
