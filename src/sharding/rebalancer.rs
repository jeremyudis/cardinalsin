//! Lease transfer and replica rebalancing

use super::{ReplicaInfo, ShardId, ShardMetadata};
use crate::Result;

/// Rebalancing strategy
pub struct RebalanceStrategy {
    /// Minimum load difference to trigger rebalance
    pub min_load_diff: f64,
}

impl RebalanceStrategy {
    /// Create a new rebalance strategy
    pub fn new(min_load_diff: f64) -> Self {
        Self { min_load_diff }
    }

    /// Rebalance a hot shard
    ///
    /// Prefers lease transfers (cheap) before replica movement (expensive)
    pub async fn rebalance_hot_shard(
        &self,
        shard: &ShardMetadata,
        node_loads: &[(String, f64)],
    ) -> Result<RebalanceAction> {
        // Find least loaded replica
        let least_loaded = self.find_least_loaded_replica(shard, node_loads);

        if let Some(target_replica) = least_loaded {
            if let Some(current_leader) = shard.leader_replica() {
                if target_replica.replica_id != current_leader.replica_id {
                    // Try lease transfer first (cheap, ~100ms, no data copy)
                    return Ok(RebalanceAction::TransferLease {
                        shard_id: shard.shard_id.clone(),
                        from_replica: current_leader.replica_id.clone(),
                        to_replica: target_replica.replica_id.clone(),
                    });
                }
            }
        }

        // If lease transfer not possible, consider replica movement
        if let Some((target_node, _)) = self.find_underloaded_node(node_loads) {
            if let Some(leader) = shard.leader_replica() {
                return Ok(RebalanceAction::MoveReplica {
                    shard_id: shard.shard_id.clone(),
                    from_node: leader.node_id.clone(),
                    to_node: target_node,
                });
            }
        }

        Ok(RebalanceAction::None)
    }

    /// Find the least loaded replica for a shard
    fn find_least_loaded_replica<'a>(
        &self,
        shard: &'a ShardMetadata,
        node_loads: &[(String, f64)],
    ) -> Option<&'a ReplicaInfo> {
        let load_map: std::collections::HashMap<_, _> = node_loads.iter().cloned().collect();

        shard.replicas.iter().min_by(|a, b| {
            let load_a = load_map.get(&a.node_id).unwrap_or(&1.0);
            let load_b = load_map.get(&b.node_id).unwrap_or(&1.0);
            load_a.partial_cmp(load_b).unwrap()
        })
    }

    /// Find an underloaded node for replica placement
    fn find_underloaded_node(&self, node_loads: &[(String, f64)]) -> Option<(String, f64)> {
        let avg_load: f64 =
            node_loads.iter().map(|(_, l)| l).sum::<f64>() / node_loads.len() as f64;

        node_loads
            .iter()
            .filter(|(_, load)| *load < avg_load - self.min_load_diff)
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .cloned()
    }
}

/// Rebalancing action to take
#[derive(Debug, Clone)]
pub enum RebalanceAction {
    /// No action needed
    None,
    /// Transfer lease to another replica (fast, no data movement)
    TransferLease {
        shard_id: ShardId,
        from_replica: String,
        to_replica: String,
    },
    /// Move replica to another node (slow, requires data copy)
    MoveReplica {
        shard_id: ShardId,
        from_node: String,
        to_node: String,
    },
}

impl Default for RebalanceStrategy {
    fn default() -> Self {
        Self::new(0.2) // 20% load difference
    }
}

#[cfg(test)]
mod tests {
    use super::super::ShardState;
    use super::*;

    #[tokio::test]
    async fn test_lease_transfer() {
        let strategy = RebalanceStrategy::default();

        let shard = ShardMetadata {
            shard_id: "shard-1".to_string(),
            generation: 1,
            key_range: (vec![], vec![]),
            replicas: vec![
                ReplicaInfo {
                    replica_id: "replica-1".to_string(),
                    node_id: "node-1".to_string(),
                    is_leader: true,
                },
                ReplicaInfo {
                    replica_id: "replica-2".to_string(),
                    node_id: "node-2".to_string(),
                    is_leader: false,
                },
            ],
            state: ShardState::Active,
            min_time: 0,
            max_time: 0,
        };

        let node_loads = vec![
            ("node-1".to_string(), 0.9), // High load
            ("node-2".to_string(), 0.2), // Low load
        ];

        let action = strategy
            .rebalance_hot_shard(&shard, &node_loads)
            .await
            .unwrap();

        match action {
            RebalanceAction::TransferLease {
                from_replica,
                to_replica,
                ..
            } => {
                assert_eq!(from_replica, "replica-1");
                assert_eq!(to_replica, "replica-2");
            }
            _ => panic!("Expected lease transfer"),
        }
    }
}
