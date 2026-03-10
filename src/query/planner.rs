use std::collections::BTreeSet;

use crate::metadata::predicates::{ColumnPredicate, PredicateValue};
use crate::metadata::TimeRange;
use crate::sharding::{key_in_range, ShardKey, ShardMetadata, ShardState, TimeBucket};

#[derive(Debug, Clone, PartialEq, Eq)]
enum MetricConstraint {
    Unconstrained,
    Exact(BTreeSet<String>),
    Unknown,
}

pub(crate) fn tenant_id_for_sharding(tenant_id: &str) -> u32 {
    tenant_id.parse().unwrap_or(0)
}

pub(crate) fn exact_metric_names(predicates: &[ColumnPredicate]) -> Option<BTreeSet<String>> {
    let constraint = predicates
        .iter()
        .fold(MetricConstraint::Unconstrained, |acc, predicate| {
            merge_and(acc, metric_constraint(predicate))
        });

    match constraint {
        MetricConstraint::Exact(metrics) => Some(metrics),
        MetricConstraint::Unconstrained | MetricConstraint::Unknown => None,
    }
}

pub(crate) fn shard_ids_for_metrics(
    tenant_id: u32,
    time_range: TimeRange,
    metric_names: &BTreeSet<String>,
    shards: &[ShardMetadata],
) -> Vec<String> {
    if metric_names.is_empty() {
        return Vec::new();
    }

    let bucket_starts = TimeBucket::bucket_starts_in_range(time_range.start, time_range.end);
    if bucket_starts.is_empty() {
        return Vec::new();
    }

    let mut shard_ids = BTreeSet::new();
    for metric_name in metric_names {
        let metric_hash = ShardKey::hash_metric_name(metric_name);
        for bucket_start in &bucket_starts {
            let key = ShardKey::from_metric_hash(tenant_id, metric_hash, *bucket_start);
            let key_bytes = key.to_bytes();
            for shard in shards {
                if matches!(shard.state, ShardState::PendingDeletion { .. }) {
                    continue;
                }
                if key_in_range(&key_bytes, &shard.key_range) {
                    shard_ids.insert(shard.shard_id.clone());
                }
            }
        }
    }

    shard_ids.into_iter().collect()
}

fn metric_constraint(predicate: &ColumnPredicate) -> MetricConstraint {
    match predicate {
        ColumnPredicate::Eq(column, PredicateValue::String(value)) if column == "metric_name" => {
            MetricConstraint::Exact(BTreeSet::from([value.clone()]))
        }
        ColumnPredicate::In(column, values) if column == "metric_name" => values
            .iter()
            .map(|value| match value {
                PredicateValue::String(value) => Some(value.clone()),
                _ => None,
            })
            .collect::<Option<BTreeSet<_>>>()
            .map(MetricConstraint::Exact)
            .unwrap_or(MetricConstraint::Unknown),
        ColumnPredicate::Eq(column, _)
        | ColumnPredicate::NotEq(column, _)
        | ColumnPredicate::Lt(column, _)
        | ColumnPredicate::LtEq(column, _)
        | ColumnPredicate::Gt(column, _)
        | ColumnPredicate::GtEq(column, _)
        | ColumnPredicate::In(column, _)
        | ColumnPredicate::NotIn(column, _)
        | ColumnPredicate::Between(column, _, _)
            if column == "metric_name" =>
        {
            MetricConstraint::Unknown
        }
        ColumnPredicate::And(left, right) => {
            merge_and(metric_constraint(left), metric_constraint(right))
        }
        ColumnPredicate::Or(left, right) => {
            merge_or(metric_constraint(left), metric_constraint(right))
        }
        ColumnPredicate::Not(inner) => {
            if mentions_metric_name(inner) {
                MetricConstraint::Unknown
            } else {
                MetricConstraint::Unconstrained
            }
        }
        _ => MetricConstraint::Unconstrained,
    }
}

fn merge_and(left: MetricConstraint, right: MetricConstraint) -> MetricConstraint {
    match (left, right) {
        (MetricConstraint::Unknown, _) | (_, MetricConstraint::Unknown) => {
            MetricConstraint::Unknown
        }
        (MetricConstraint::Unconstrained, other) | (other, MetricConstraint::Unconstrained) => {
            other
        }
        (MetricConstraint::Exact(left), MetricConstraint::Exact(right)) => {
            MetricConstraint::Exact(left.intersection(&right).cloned().collect())
        }
    }
}

fn merge_or(left: MetricConstraint, right: MetricConstraint) -> MetricConstraint {
    match (left, right) {
        (MetricConstraint::Exact(mut left), MetricConstraint::Exact(right)) => {
            left.extend(right);
            MetricConstraint::Exact(left)
        }
        (MetricConstraint::Unconstrained, MetricConstraint::Unconstrained) => {
            MetricConstraint::Unconstrained
        }
        _ => MetricConstraint::Unknown,
    }
}

fn mentions_metric_name(predicate: &ColumnPredicate) -> bool {
    match predicate {
        ColumnPredicate::Eq(column, _)
        | ColumnPredicate::NotEq(column, _)
        | ColumnPredicate::Lt(column, _)
        | ColumnPredicate::LtEq(column, _)
        | ColumnPredicate::Gt(column, _)
        | ColumnPredicate::GtEq(column, _)
        | ColumnPredicate::In(column, _)
        | ColumnPredicate::NotIn(column, _)
        | ColumnPredicate::Between(column, _, _) => column == "metric_name",
        ColumnPredicate::And(left, right) | ColumnPredicate::Or(left, right) => {
            mentions_metric_name(left) || mentions_metric_name(right)
        }
        ColumnPredicate::Not(inner) => mentions_metric_name(inner),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric_eq(value: &str) -> ColumnPredicate {
        ColumnPredicate::Eq(
            "metric_name".to_string(),
            PredicateValue::String(value.to_string()),
        )
    }

    fn service_eq(value: &str) -> ColumnPredicate {
        ColumnPredicate::Eq(
            "service".to_string(),
            PredicateValue::String(value.to_string()),
        )
    }

    fn shard_for(metric_name: &str, ts: i64) -> ShardMetadata {
        let key = ShardKey::new(0, metric_name, ts);
        let start = key.to_bytes();
        let mut end = start.clone();
        *end.last_mut().unwrap() += 1;
        ShardMetadata {
            shard_id: key.shard_id(),
            generation: 1,
            key_range: (start, end),
            replicas: Vec::new(),
            state: ShardState::Active,
            min_time: ts,
            max_time: ts + 300_000_000_000,
        }
    }

    #[test]
    fn exact_metric_names_intersects_and_constraints() {
        let metrics = exact_metric_names(&[
            ColumnPredicate::Or(Box::new(metric_eq("cpu")), Box::new(metric_eq("mem"))),
            ColumnPredicate::Or(Box::new(metric_eq("cpu")), Box::new(metric_eq("disk"))),
            service_eq("api"),
        ])
        .unwrap();

        assert_eq!(metrics, BTreeSet::from(["cpu".to_string()]));
    }

    #[test]
    fn exact_metric_names_rejects_mixed_metric_or_non_metric_or() {
        let metrics = exact_metric_names(&[ColumnPredicate::Or(
            Box::new(metric_eq("cpu")),
            Box::new(service_eq("api")),
        )]);

        assert!(metrics.is_none());
    }

    #[test]
    fn shard_ids_for_metrics_matches_full_shard_keys() {
        let ts = 1_700_000_000_000_000_000i64;
        let shards = vec![shard_for("cpu", ts), shard_for("mem", ts)];
        let metrics = BTreeSet::from(["cpu".to_string()]);

        let shard_ids = shard_ids_for_metrics(
            0,
            TimeRange::new(ts, ts + 60_000_000_000),
            &metrics,
            &shards,
        );

        assert_eq!(shard_ids, vec![shards[0].shard_id.clone()]);
    }
}
