//! Shard-to-node assignment strategies
//!
//! Maps shards to ingester nodes for distributed write routing.
//! Uses consistent hashing to minimize reassignments during scale-up/down.

use super::node_registry::NodeRegistry;
use crate::Result;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Strategy for assigning shards to nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentStrategy {
    /// Consistent hashing (minimizes reassignments)
    ConsistentHash,
    /// Round-robin (simple but causes reassignments)
    RoundRobin,
    /// Load-based (assigns to least-loaded node)
    LoadBased,
}

/// Manages shard assignments across the cluster
pub struct ShardAssignment {
    /// Current shard -> node assignments
    assignments: Arc<RwLock<HashMap<String, String>>>,
    /// Consistent hash ring (shard_id -> node_id)
    hash_ring: Arc<RwLock<ConsistentHashRing>>,
    /// Node registry reference
    node_registry: Arc<NodeRegistry>,
    /// Assignment strategy
    strategy: AssignmentStrategy,
}

impl ShardAssignment {
    /// Create a new shard assignment manager
    pub fn new(node_registry: Arc<NodeRegistry>, strategy: AssignmentStrategy) -> Self {
        Self {
            assignments: Arc::new(RwLock::new(HashMap::new())),
            hash_ring: Arc::new(RwLock::new(ConsistentHashRing::new())),
            node_registry,
            strategy,
        }
    }

    /// Assign a shard to a node
    pub async fn assign_shard(&self, shard_id: &str) -> Result<String> {
        // Check if already assigned
        {
            let assignments = self.assignments.read().await;
            if let Some(node_id) = assignments.get(shard_id) {
                // Verify node is still healthy
                if let Some(node) = self.node_registry.get_node(node_id).await {
                    if node.can_accept_writes() {
                        debug!("Shard {} already assigned to node {}", shard_id, node_id);
                        return Ok(node_id.clone());
                    }
                }
            }
        }

        // Assign using configured strategy
        let node_id = match self.strategy {
            AssignmentStrategy::ConsistentHash => self.assign_consistent_hash(shard_id).await?,
            AssignmentStrategy::RoundRobin => self.assign_round_robin(shard_id).await?,
            AssignmentStrategy::LoadBased => self.assign_load_based(shard_id).await?,
        };

        // Update assignment
        {
            let mut assignments = self.assignments.write().await;
            assignments.insert(shard_id.to_string(), node_id.clone());
        }

        // Update node's shard list
        self.update_node_shards(&node_id).await;

        info!("Assigned shard {} to node {}", shard_id, node_id);
        Ok(node_id)
    }

    /// Get the node assigned to a shard
    pub async fn get_node_for_shard(&self, shard_id: &str) -> Option<String> {
        self.assignments.read().await.get(shard_id).cloned()
    }

    /// Get all assignments
    pub async fn get_all_assignments(&self) -> HashMap<String, String> {
        self.assignments.read().await.clone()
    }

    /// Remove shard assignment
    pub async fn unassign_shard(&self, shard_id: &str) {
        let mut assignments = self.assignments.write().await;
        if let Some(node_id) = assignments.remove(shard_id) {
            info!("Unassigned shard {} from node {}", shard_id, node_id);
        }
    }

    /// Rebalance shards across nodes
    ///
    /// Called when nodes are added/removed to redistribute load
    pub async fn rebalance(&self) -> Result<Vec<(String, String, String)>> {
        info!("Rebalancing shard assignments");

        let nodes = self.node_registry.get_healthy_ingesters().await;
        if nodes.is_empty() {
            return Ok(Vec::new());
        }

        // Rebuild hash ring with current nodes
        {
            let mut ring = self.hash_ring.write().await;
            ring.clear();
            for node in &nodes {
                ring.add_node(&node.id);
            }
        }

        let moves;

        // Scope the assignments write lock so it's dropped before update_node_shards
        {
            let ring = self.hash_ring.read().await;
            let mut assignments = self.assignments.write().await;

            // Re-assign all shards using new ring
            moves = assignments
                .iter()
                .filter_map(|(shard_id, old_node_id)| {
                    ring.get_node(shard_id).and_then(|new_node_id| {
                        if new_node_id != *old_node_id {
                            Some((shard_id.clone(), old_node_id.clone(), new_node_id))
                        } else {
                            None
                        }
                    })
                })
                .collect::<Vec<_>>();

            // Apply moves
            for (shard_id, _old_node, new_node) in &moves {
                assignments.insert(shard_id.clone(), new_node.clone());
            }
        }
        // Write lock on assignments is now dropped

        // Update all node shard lists (needs read lock on assignments)
        for node in nodes {
            self.update_node_shards(&node.id).await;
        }

        info!("Rebalancing complete: {} shards moved", moves.len());
        Ok(moves)
    }

    /// Assign using consistent hashing
    async fn assign_consistent_hash(&self, shard_id: &str) -> Result<String> {
        let ring = self.hash_ring.read().await;

        if let Some(node_id) = ring.get_node(shard_id) {
            Ok(node_id)
        } else {
            // Ring is empty, populate it
            drop(ring);
            let mut ring = self.hash_ring.write().await;
            let nodes = self.node_registry.get_healthy_ingesters().await;

            if nodes.is_empty() {
                return Err(crate::Error::Internal(
                    "No healthy ingester nodes".to_string(),
                ));
            }

            for node in &nodes {
                ring.add_node(&node.id);
            }

            ring.get_node(shard_id)
                .ok_or_else(|| crate::Error::Internal("Failed to assign shard".to_string()))
        }
    }

    /// Assign using round-robin
    async fn assign_round_robin(&self, _shard_id: &str) -> Result<String> {
        let nodes = self.node_registry.get_healthy_ingesters().await;

        if nodes.is_empty() {
            return Err(crate::Error::Internal(
                "No healthy ingester nodes".to_string(),
            ));
        }

        // Simple: pick node with fewest shards
        let node = nodes
            .iter()
            .min_by_key(|n| n.shards.len())
            .ok_or_else(|| crate::Error::Internal("No nodes available".to_string()))?;

        Ok(node.id.clone())
    }

    /// Assign based on node load
    async fn assign_load_based(&self, _shard_id: &str) -> Result<String> {
        let nodes = self.node_registry.get_healthy_ingesters().await;

        if nodes.is_empty() {
            return Err(crate::Error::Internal(
                "No healthy ingester nodes".to_string(),
            ));
        }

        // Pick least-loaded node
        let node = nodes
            .iter()
            .min_by_key(|n| n.load_percent)
            .ok_or_else(|| crate::Error::Internal("No nodes available".to_string()))?;

        Ok(node.id.clone())
    }

    /// Update a node's shard list in the registry
    async fn update_node_shards(&self, node_id: &str) {
        let assignments = self.assignments.read().await;

        let shards: Vec<String> = assignments
            .iter()
            .filter(|(_, nid)| *nid == node_id)
            .map(|(shard_id, _)| shard_id.clone())
            .collect();

        self.node_registry.update_shards(node_id, shards).await;
    }
}

/// Consistent hash ring implementation
struct ConsistentHashRing {
    /// Virtual nodes (hash -> node_id)
    ring: std::collections::BTreeMap<u64, String>,
    /// Number of virtual nodes per physical node
    virtual_nodes: usize,
}

impl ConsistentHashRing {
    fn new() -> Self {
        Self {
            ring: std::collections::BTreeMap::new(),
            virtual_nodes: 100, // 100 virtual nodes per physical node
        }
    }

    fn add_node(&mut self, node_id: &str) {
        for i in 0..self.virtual_nodes {
            let key = format!("{}:{}", node_id, i);
            let hash = Self::hash_key(&key);
            self.ring.insert(hash, node_id.to_string());
        }
    }

    #[allow(dead_code)]
    fn remove_node(&mut self, node_id: &str) {
        let hashes_to_remove: Vec<u64> = self
            .ring
            .iter()
            .filter(|(_, nid)| *nid == node_id)
            .map(|(h, _)| *h)
            .collect();

        for hash in hashes_to_remove {
            self.ring.remove(&hash);
        }
    }

    fn get_node(&self, key: &str) -> Option<String> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = Self::hash_key(key);

        // Find first node with hash >= key_hash
        if let Some((_, node_id)) = self.ring.range(hash..).next() {
            Some(node_id.clone())
        } else {
            // Wrap around to first node
            self.ring.values().next().cloned()
        }
    }

    fn clear(&mut self) {
        self.ring.clear();
    }

    fn hash_key(key: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_consistent_hash_ring() {
        let mut ring = ConsistentHashRing::new();

        ring.add_node("node1");
        ring.add_node("node2");
        ring.add_node("node3");

        // Same key should always map to same node
        let node1 = ring.get_node("shard-1").unwrap();
        let node2 = ring.get_node("shard-1").unwrap();
        assert_eq!(node1, node2);

        // Different keys should distribute
        let assignments: HashSet<String> = (0..100)
            .map(|i| ring.get_node(&format!("shard-{}", i)).unwrap())
            .collect();

        // Should use multiple nodes (with high probability)
        assert!(assignments.len() > 1);
    }

    #[test]
    fn test_consistent_hash_stability() {
        let mut ring1 = ConsistentHashRing::new();
        ring1.add_node("node1");
        ring1.add_node("node2");
        ring1.add_node("node3");

        let mut ring2 = ConsistentHashRing::new();
        ring2.add_node("node1");
        ring2.add_node("node2");
        ring2.add_node("node3");
        ring2.add_node("node4");

        // Most shards should remain on same nodes
        let mut same_count = 0;
        for i in 0..100 {
            let shard_id = format!("shard-{}", i);
            let node1 = ring1.get_node(&shard_id).unwrap();
            let node2 = ring2.get_node(&shard_id).unwrap();
            if node1 == node2 {
                same_count += 1;
            }
        }

        // At least 70% should remain stable
        assert!(same_count >= 70);
    }
}
