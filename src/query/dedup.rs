//! Row-level deduplication for query results during shard split dual-write
//!
//! During a shard split's dual-write phase, the same data exists in both
//! the old shard and the new shards. This module provides deduplication
//! so queries return each row exactly once.

use arrow::compute::filter_record_batch;
use arrow_array::cast::AsArray;
use arrow_array::BooleanArray;
use arrow_array::{Array, RecordBatch};
use std::collections::HashSet;

use crate::Result;

/// Deduplicate rows across multiple record batches.
///
/// Uses (timestamp_nanos, metric_name) as the dedup key. The first occurrence
/// of each key is kept; subsequent duplicates are filtered out.
///
/// This is applied at query time when any shard involved in the result set
/// is in a dual-write split phase.
pub fn dedup_batches(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(batches);
    }

    let mut seen: HashSet<(i64, String)> = HashSet::new();
    let mut result = Vec::with_capacity(batches.len());

    for batch in &batches {
        let ts_col = batch.column_by_name("timestamp");
        let metric_col = batch.column_by_name("metric_name");

        // If the batch doesn't have both columns, we can't dedup — pass through
        let (ts_col, metric_col) = match (ts_col, metric_col) {
            (Some(t), Some(m)) => (t, m),
            _ => {
                result.push(batch.clone());
                continue;
            }
        };

        // Try to get timestamp as nanosecond or int64
        let ts_values: Vec<Option<i64>> = if let Some(ts_arr) =
            ts_col.as_primitive_opt::<arrow_array::types::TimestampNanosecondType>()
        {
            (0..batch.num_rows())
                .map(|i| {
                    if ts_arr.is_null(i) {
                        None
                    } else {
                        Some(ts_arr.value(i))
                    }
                })
                .collect()
        } else if let Some(ts_arr) = ts_col.as_primitive_opt::<arrow_array::types::Int64Type>() {
            (0..batch.num_rows())
                .map(|i| {
                    if ts_arr.is_null(i) {
                        None
                    } else {
                        Some(ts_arr.value(i))
                    }
                })
                .collect()
        } else {
            // Can't interpret timestamp column — pass through
            result.push(batch.clone());
            continue;
        };

        let metric_arr = match metric_col.as_string_opt::<i32>() {
            Some(arr) => arr,
            None => {
                result.push(batch.clone());
                continue;
            }
        };

        // Build keep mask
        let mut keep = vec![true; batch.num_rows()];
        let mut any_dropped = false;

        for i in 0..batch.num_rows() {
            let ts = match ts_values[i] {
                Some(v) => v,
                None => continue, // Keep nulls
            };
            let metric = if metric_arr.is_null(i) {
                String::new()
            } else {
                metric_arr.value(i).to_string()
            };

            if !seen.insert((ts, metric)) {
                keep[i] = false;
                any_dropped = true;
            }
        }

        if !any_dropped {
            result.push(batch.clone());
        } else {
            let mask = BooleanArray::from(keep);
            let filtered = filter_record_batch(batch, &mask)?;
            if filtered.num_rows() > 0 {
                result.push(filtered);
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(timestamps: &[i64], metrics: &[&str], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(StringArray::from(metrics.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_dedup_removes_duplicates() {
        let batch1 = make_batch(&[100, 200, 300], &["cpu", "mem", "cpu"], &[1.0, 2.0, 3.0]);
        let batch2 = make_batch(&[100, 400], &["cpu", "disk"], &[1.0, 4.0]);

        let result = dedup_batches(vec![batch1, batch2]).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 4,
            "Should keep 4 unique rows (100/cpu, 200/mem, 300/cpu, 400/disk)"
        );
    }

    #[test]
    fn test_dedup_no_duplicates_passthrough() {
        let batch = make_batch(&[100, 200, 300], &["cpu", "mem", "disk"], &[1.0, 2.0, 3.0]);

        let result = dedup_batches(vec![batch]).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_dedup_empty_batches() {
        let result = dedup_batches(vec![]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_dedup_all_duplicates() {
        let batch1 = make_batch(&[100, 200], &["cpu", "mem"], &[1.0, 2.0]);
        let batch2 = make_batch(&[100, 200], &["cpu", "mem"], &[1.0, 2.0]);

        let result = dedup_batches(vec![batch1, batch2]).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2,
            "Should keep only first occurrence of each key"
        );
    }

    #[test]
    fn test_dedup_same_timestamp_different_metrics() {
        let batch = make_batch(&[100, 100, 100], &["cpu", "mem", "disk"], &[1.0, 2.0, 3.0]);

        let result = dedup_batches(vec![batch]).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "Different metrics at same timestamp are not duplicates"
        );
    }
}
