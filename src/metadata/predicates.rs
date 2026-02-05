//! Column predicates for metadata-level filtering
//!
//! This module defines predicates that can be pushed down to the metadata layer
//! to prune chunks before they're registered with DataFusion. This dramatically
//! reduces S3 read costs by avoiding fetching chunks that don't match query predicates.

use serde::{Deserialize, Serialize};

/// A predicate on a column value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnPredicate {
    /// Equality: column = value
    Eq(String, PredicateValue),
    /// Not equal: column != value
    NotEq(String, PredicateValue),
    /// Less than: column < value
    Lt(String, PredicateValue),
    /// Less than or equal: column <= value
    LtEq(String, PredicateValue),
    /// Greater than: column > value
    Gt(String, PredicateValue),
    /// Greater than or equal: column >= value
    GtEq(String, PredicateValue),
    /// In list: column IN (value1, value2, ...)
    In(String, Vec<PredicateValue>),
    /// Not in list: column NOT IN (value1, value2, ...)
    NotIn(String, Vec<PredicateValue>),
    /// Between: column BETWEEN low AND high
    Between(String, PredicateValue, PredicateValue),
    /// Logical AND
    And(Box<ColumnPredicate>, Box<ColumnPredicate>),
    /// Logical OR
    Or(Box<ColumnPredicate>, Box<ColumnPredicate>),
    /// Logical NOT
    Not(Box<ColumnPredicate>),
}

/// Value type for predicates (subset of ScalarValue)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PredicateValue {
    /// String value
    String(String),
    /// Integer value
    Int64(i64),
    /// Floating point value
    Float64(f64),
    /// Boolean value
    Boolean(bool),
    /// Null value
    Null,
}

impl ColumnPredicate {
    /// Check if a chunk's column statistics satisfy this predicate
    ///
    /// Returns:
    /// - `true` if the chunk might contain matching rows (include it)
    /// - `false` if the chunk definitely doesn't contain matching rows (prune it)
    pub fn evaluate_against_stats(
        &self,
        column_stats: &std::collections::HashMap<String, super::s3::ColumnStats>,
    ) -> bool {
        match self {
            ColumnPredicate::Eq(col, val) => {
                if let Some(stats) = column_stats.get(col) {
                    // If value is outside [min, max], definitely no match
                    Self::value_in_range(val, &stats.min, &stats.max)
                } else {
                    // No stats for this column - conservatively include
                    true
                }
            }
            ColumnPredicate::NotEq(col, _val) => {
                // Can't rule out chunks for != predicate
                // (chunk might have both matching and non-matching values)
                if column_stats.contains_key(col) {
                    true
                } else {
                    true
                }
            }
            ColumnPredicate::Lt(col, val) | ColumnPredicate::LtEq(col, val) => {
                if let Some(stats) = column_stats.get(col) {
                    // If min >= val, no rows satisfy col < val
                    // Use conservative check: include if min might be < val
                    !Self::value_gte(&stats.min, val)
                } else {
                    true
                }
            }
            ColumnPredicate::Gt(col, val) | ColumnPredicate::GtEq(col, val) => {
                if let Some(stats) = column_stats.get(col) {
                    // If max <= val, no rows satisfy col > val
                    // Use conservative check: include if max might be > val
                    !Self::value_lte(&stats.max, val)
                } else {
                    true
                }
            }
            ColumnPredicate::In(col, values) => {
                if let Some(stats) = column_stats.get(col) {
                    // Include chunk if any value in the list overlaps [min, max]
                    values.iter().any(|v| Self::value_in_range(v, &stats.min, &stats.max))
                } else {
                    true
                }
            }
            ColumnPredicate::NotIn(col, _values) => {
                // Can't rule out chunks for NOT IN predicate
                if column_stats.contains_key(col) {
                    true
                } else {
                    true
                }
            }
            ColumnPredicate::Between(col, low, high) => {
                if let Some(stats) = column_stats.get(col) {
                    // Include if [min, max] overlaps [low, high]
                    // No overlap if: max < low OR min > high
                    // Overlap if: !(max < low OR min > high)
                    !(Self::value_lt(&stats.max, low) || Self::value_gt(&stats.min, high))
                } else {
                    true
                }
            }
            ColumnPredicate::And(left, right) => {
                // Both must be satisfied
                left.evaluate_against_stats(column_stats)
                    && right.evaluate_against_stats(column_stats)
            }
            ColumnPredicate::Or(left, right) => {
                // At least one must be satisfied
                left.evaluate_against_stats(column_stats)
                    || right.evaluate_against_stats(column_stats)
            }
            ColumnPredicate::Not(inner) => {
                // Negation: if inner is definitely false, return true
                // But we can't safely prune on NOT predicates, so be conservative
                let inner_result = inner.evaluate_against_stats(column_stats);
                // If inner is definitely false, chunk doesn't match
                // But if inner is maybe true, we can't rule it out
                // Be conservative: only prune if we're certain
                inner_result
            }
        }
    }

    /// Check if a value is within [min, max] range (used for stats)
    fn value_in_range(
        val: &PredicateValue,
        min: &serde_json::Value,
        max: &serde_json::Value,
    ) -> bool {
        // Convert to comparable form and check if val is in [min, max]
        match val {
            PredicateValue::String(s) => {
                if let (Some(min_str), Some(max_str)) = (min.as_str(), max.as_str()) {
                    s.as_str() >= min_str && s.as_str() <= max_str
                } else {
                    true
                }
            }
            PredicateValue::Int64(i) => {
                if let (Some(min_i), Some(max_i)) = (min.as_i64(), max.as_i64()) {
                    *i >= min_i && *i <= max_i
                } else {
                    true
                }
            }
            PredicateValue::Float64(f) => {
                if let (Some(min_f), Some(max_f)) = (min.as_f64(), max.as_f64()) {
                    *f >= min_f && *f <= max_f
                } else {
                    true
                }
            }
            PredicateValue::Boolean(_) => true, // Can't filter booleans effectively
            PredicateValue::Null => true,        // Conservative: include nulls
        }
    }

    /// Check if a value is >= another
    fn value_gte(val: &serde_json::Value, other: &PredicateValue) -> bool {
        match other {
            PredicateValue::Int64(i) => val.as_i64().map(|v| v >= *i).unwrap_or(false),
            PredicateValue::Float64(f) => val.as_f64().map(|v| v >= *f).unwrap_or(false),
            PredicateValue::String(s) => val.as_str().map(|v| v >= s.as_str()).unwrap_or(false),
            _ => false,
        }
    }

    /// Check if a value is <= another
    fn value_lte(val: &serde_json::Value, other: &PredicateValue) -> bool {
        match other {
            PredicateValue::Int64(i) => val.as_i64().map(|v| v <= *i).unwrap_or(false),
            PredicateValue::Float64(f) => val.as_f64().map(|v| v <= *f).unwrap_or(false),
            PredicateValue::String(s) => val.as_str().map(|v| v <= s.as_str()).unwrap_or(false),
            _ => false,
        }
    }

    /// Check if a value is < another
    fn value_lt(val: &serde_json::Value, other: &PredicateValue) -> bool {
        match other {
            PredicateValue::Int64(i) => val.as_i64().map(|v| v < *i).unwrap_or(false),
            PredicateValue::Float64(f) => val.as_f64().map(|v| v < *f).unwrap_or(false),
            PredicateValue::String(s) => val.as_str().map(|v| v < s.as_str()).unwrap_or(false),
            _ => false,
        }
    }

    /// Check if a value is > another
    fn value_gt(val: &serde_json::Value, other: &PredicateValue) -> bool {
        match other {
            PredicateValue::Int64(i) => val.as_i64().map(|v| v > *i).unwrap_or(false),
            PredicateValue::Float64(f) => val.as_f64().map(|v| v > *f).unwrap_or(false),
            PredicateValue::String(s) => val.as_str().map(|v| v > s.as_str()).unwrap_or(false),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn make_stats(min: i64, max: i64) -> HashMap<String, super::super::s3::ColumnStats> {
        let mut stats = HashMap::new();
        stats.insert(
            "metric_name".to_string(),
            super::super::s3::ColumnStats {
                min: json!(min),
                max: json!(max),
                has_nulls: false,
            },
        );
        stats
    }

    #[test]
    fn test_eq_predicate_pruning() {
        let stats = make_stats(100, 200);

        // Value in range - include
        let pred = ColumnPredicate::Eq("metric_name".to_string(), PredicateValue::Int64(150));
        assert!(pred.evaluate_against_stats(&stats));

        // Value below range - prune
        let pred = ColumnPredicate::Eq("metric_name".to_string(), PredicateValue::Int64(50));
        assert!(!pred.evaluate_against_stats(&stats));

        // Value above range - prune
        let pred = ColumnPredicate::Eq("metric_name".to_string(), PredicateValue::Int64(300));
        assert!(!pred.evaluate_against_stats(&stats));
    }

    #[test]
    fn test_lt_predicate_pruning() {
        let stats = make_stats(100, 200);

        // All values < 50 (entire chunk above threshold) - prune
        let pred = ColumnPredicate::Lt("metric_name".to_string(), PredicateValue::Int64(50));
        assert!(!pred.evaluate_against_stats(&stats));

        // Some values < 150 (chunk spans threshold) - include
        let pred = ColumnPredicate::Lt("metric_name".to_string(), PredicateValue::Int64(150));
        assert!(pred.evaluate_against_stats(&stats));
    }

    #[test]
    fn test_gt_predicate_pruning() {
        let stats = make_stats(100, 200);

        // All values > 300 (entire chunk below threshold) - prune
        let pred = ColumnPredicate::Gt("metric_name".to_string(), PredicateValue::Int64(300));
        assert!(!pred.evaluate_against_stats(&stats));

        // Some values > 150 (chunk spans threshold) - include
        let pred = ColumnPredicate::Gt("metric_name".to_string(), PredicateValue::Int64(150));
        assert!(pred.evaluate_against_stats(&stats));
    }

    #[test]
    fn test_between_predicate() {
        let stats = make_stats(100, 200);

        // Range overlaps - include
        let pred = ColumnPredicate::Between(
            "metric_name".to_string(),
            PredicateValue::Int64(150),
            PredicateValue::Int64(250),
        );
        assert!(pred.evaluate_against_stats(&stats));

        // Range completely below - prune
        let pred = ColumnPredicate::Between(
            "metric_name".to_string(),
            PredicateValue::Int64(10),
            PredicateValue::Int64(50),
        );
        assert!(!pred.evaluate_against_stats(&stats));

        // Range completely above - prune
        let pred = ColumnPredicate::Between(
            "metric_name".to_string(),
            PredicateValue::Int64(300),
            PredicateValue::Int64(400),
        );
        assert!(!pred.evaluate_against_stats(&stats));
    }

    #[test]
    fn test_and_predicate() {
        let stats = make_stats(100, 200);

        // Both satisfied - include
        let pred = ColumnPredicate::And(
            Box::new(ColumnPredicate::Gt(
                "metric_name".to_string(),
                PredicateValue::Int64(50),
            )),
            Box::new(ColumnPredicate::Lt(
                "metric_name".to_string(),
                PredicateValue::Int64(250),
            )),
        );
        assert!(pred.evaluate_against_stats(&stats));

        // One not satisfied - prune
        let pred = ColumnPredicate::And(
            Box::new(ColumnPredicate::Gt(
                "metric_name".to_string(),
                PredicateValue::Int64(300),
            )),
            Box::new(ColumnPredicate::Lt(
                "metric_name".to_string(),
                PredicateValue::Int64(250),
            )),
        );
        assert!(!pred.evaluate_against_stats(&stats));
    }

    #[test]
    fn test_in_predicate() {
        let stats = make_stats(100, 200);

        // At least one value in range - include
        let pred = ColumnPredicate::In(
            "metric_name".to_string(),
            vec![
                PredicateValue::Int64(50),
                PredicateValue::Int64(150),
                PredicateValue::Int64(300),
            ],
        );
        assert!(pred.evaluate_against_stats(&stats));

        // No values in range - prune
        let pred = ColumnPredicate::In(
            "metric_name".to_string(),
            vec![PredicateValue::Int64(50), PredicateValue::Int64(300)],
        );
        assert!(!pred.evaluate_against_stats(&stats));
    }
}
