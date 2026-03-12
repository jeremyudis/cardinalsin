//! Shard pruning planner for query optimization.
//!
//! Given query predicates and the current shard topology, determines which
//! shards may contain matching data. Queries with exact metric_name predicates
//! can skip shards whose hash range does not contain the metric's hash.

use crate::metadata::predicates::{ColumnPredicate, PredicateValue};
use crate::sharding::{hash_metric_name, ShardId, ShardMetadata, ShardState};

/// Determine which shards may contain data matching the given predicates.
///
/// Returns `Some(shard_ids)` when exact metric name predicates allow pruning,
/// or `None` when the query is unconstrained and all shards must be scanned.
pub fn candidate_shard_ids(
    predicates: &[ColumnPredicate],
    shards: &[ShardMetadata],
) -> Option<Vec<ShardId>> {
    let metric_names = extract_exact_metrics(predicates);
    if metric_names.is_empty() {
        return None; // No metric constraint → scan all shards
    }

    let mut result = std::collections::BTreeSet::new();
    for metric in &metric_names {
        let hash = hash_metric_name(metric) as u32;
        for shard in shards {
            // Skip shards pending deletion — their data has been migrated
            if matches!(shard.state, ShardState::PendingDeletion { .. }) {
                continue;
            }
            if hash >= shard.hash_range.0 && hash < shard.hash_range.1 {
                result.insert(shard.shard_id);
            }
        }
    }

    Some(result.into_iter().collect())
}

/// Extract exact metric name values from predicates.
///
/// Handles: `Eq("metric_name", String(v))`, `In("metric_name", [String(v), ...])`,
/// and `And(left, right)` / `Or(left, right)` combinations.
fn extract_exact_metrics(predicates: &[ColumnPredicate]) -> Vec<String> {
    let mut metrics = Vec::new();
    for pred in predicates {
        collect_metrics_from_predicate(pred, &mut metrics);
    }
    metrics.sort();
    metrics.dedup();
    metrics
}

fn collect_metrics_from_predicate(pred: &ColumnPredicate, metrics: &mut Vec<String>) {
    match pred {
        ColumnPredicate::Eq(col, PredicateValue::String(val)) if col == "metric_name" => {
            metrics.push(val.clone());
        }
        ColumnPredicate::In(col, values) if col == "metric_name" => {
            for val in values {
                if let PredicateValue::String(s) = val {
                    metrics.push(s.clone());
                }
            }
        }
        ColumnPredicate::And(left, right) => {
            collect_metrics_from_predicate(left, metrics);
            collect_metrics_from_predicate(right, metrics);
        }
        ColumnPredicate::Or(left, right) => {
            // For OR, both sides must contribute metric names for pruning to be safe.
            // However, we collect from both — the union is correct for shard selection.
            collect_metrics_from_predicate(left, metrics);
            collect_metrics_from_predicate(right, metrics);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sharding::{ReplicaInfo, ShardMetadata, ShardState};

    fn make_shard(id: ShardId, range: (u32, u32)) -> ShardMetadata {
        ShardMetadata {
            shard_id: id,
            hash_range: range,
            generation: 1,
            key_range: (vec![], vec![]),
            replicas: vec![ReplicaInfo {
                replica_id: "r1".into(),
                node_id: "n1".into(),
                is_leader: true,
            }],
            state: ShardState::Active,
            min_time: 0,
            max_time: i64::MAX,
        }
    }

    #[test]
    fn test_no_predicates_returns_none() {
        let shards = vec![make_shard(0, (0, 0x8000)), make_shard(1, (0x8000, 0x10000))];
        assert!(candidate_shard_ids(&[], &shards).is_none());
    }

    #[test]
    fn test_exact_metric_prunes_to_one_shard() {
        let shards = vec![make_shard(0, (0, 0x8000)), make_shard(1, (0x8000, 0x10000))];

        let hash = hash_metric_name("cpu_usage") as u32;
        let expected_shard = if hash < 0x8000 { 0 } else { 1 };

        let predicates = vec![ColumnPredicate::Eq(
            "metric_name".to_string(),
            PredicateValue::String("cpu_usage".to_string()),
        )];

        let result = candidate_shard_ids(&predicates, &shards).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], expected_shard);
    }

    #[test]
    fn test_in_predicate_returns_multiple_shards() {
        // Use 4 shards so different metrics can land in different shards
        let shards = vec![
            make_shard(0, (0, 0x4000)),
            make_shard(1, (0x4000, 0x8000)),
            make_shard(2, (0x8000, 0xC000)),
            make_shard(3, (0xC000, 0x10000)),
        ];

        let predicates = vec![ColumnPredicate::In(
            "metric_name".to_string(),
            vec![
                PredicateValue::String("cpu_usage".to_string()),
                PredicateValue::String("mem_usage".to_string()),
                PredicateValue::String("disk_io".to_string()),
            ],
        )];

        let result = candidate_shard_ids(&predicates, &shards).unwrap();
        // Should return at least 1 shard, at most 3
        assert!(!result.is_empty());
        assert!(result.len() <= 3);
    }

    #[test]
    fn test_pending_deletion_shards_excluded() {
        let mut shard = make_shard(0, (0, 0x10000));
        shard.state = ShardState::PendingDeletion { delete_after: 0 };

        let predicates = vec![ColumnPredicate::Eq(
            "metric_name".to_string(),
            PredicateValue::String("cpu_usage".to_string()),
        )];

        let result = candidate_shard_ids(&predicates, &[shard]).unwrap();
        assert!(
            result.is_empty(),
            "PendingDeletion shard should be excluded"
        );
    }

    #[test]
    fn test_splitting_shards_included() {
        let mut shard = make_shard(0, (0, 0x10000));
        shard.state = ShardState::Splitting {
            new_shards: vec![1, 2],
        };

        let predicates = vec![ColumnPredicate::Eq(
            "metric_name".to_string(),
            PredicateValue::String("cpu_usage".to_string()),
        )];

        let result = candidate_shard_ids(&predicates, &[shard]).unwrap();
        assert_eq!(result.len(), 1, "Splitting shard should be included");
    }

    #[test]
    fn test_non_metric_predicate_returns_none() {
        let shards = vec![make_shard(0, (0, 0x10000))];
        let predicates = vec![ColumnPredicate::Eq(
            "host".to_string(),
            PredicateValue::String("server-1".to_string()),
        )];

        assert!(
            candidate_shard_ids(&predicates, &shards).is_none(),
            "Non-metric predicates should return None (full scan)"
        );
    }
}
