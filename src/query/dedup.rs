//! Row-level deduplication for query results during shard split dual-write
//!
//! During a shard split's dual-write phase, the same data exists in both
//! the old shard and the new shards. This module provides deduplication
//! so queries return each row exactly once.

use arrow::compute::filter_record_batch;
use arrow::util::display::array_value_to_string;
use arrow_array::BooleanArray;
use arrow_array::{Array, RecordBatch};
use std::collections::HashSet;

use crate::Result;

/// Deduplicate rows across multiple record batches.
///
/// Uses full row identity as the dedup key, requiring `timestamp` and
/// `metric_name` to be present. This avoids false positives where rows share
/// timestamp+metric but differ by labels or value.
///
/// This is applied at query time when any shard involved in the result set
/// is in a dual-write split phase.
pub fn dedup_batches(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(batches);
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut result = Vec::with_capacity(batches.len());

    for batch in &batches {
        // If the batch doesn't have both columns, we can't dedup — pass through
        match (
            batch.column_by_name("timestamp"),
            batch.column_by_name("metric_name"),
        ) {
            (Some(_), Some(_)) => {}
            _ => {
                result.push(batch.clone());
                continue;
            }
        }

        // Sort by column name to keep key construction stable even if projected
        // column order differs across batches.
        let mut key_columns: Vec<(String, usize)> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| (field.name().to_string(), idx))
            .collect();
        key_columns.sort_by(|a, b| a.0.cmp(&b.0));

        // Build keep mask
        let mut keep = vec![true; batch.num_rows()];
        let mut any_dropped = false;

        for (i, keep_row) in keep.iter_mut().enumerate() {
            let key = build_row_key(batch, i, &key_columns);
            if !seen.insert(key) {
                *keep_row = false;
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

fn build_row_key(batch: &RecordBatch, row: usize, key_columns: &[(String, usize)]) -> String {
    let mut key = String::new();
    key.reserve(key_columns.len() * 32);

    for (column_name, column_idx) in key_columns {
        let value = cell_value_for_key(batch.column(*column_idx).as_ref(), row);
        key.push_str(&column_name.len().to_string());
        key.push(':');
        key.push_str(column_name);
        key.push('=');
        key.push_str(&value.len().to_string());
        key.push(':');
        key.push_str(&value);
        key.push('|');
    }

    key
}

fn cell_value_for_key(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "<null>".to_string();
    }

    array_value_to_string(array, row)
        .unwrap_or_else(|_| format!("<unprintable:{:?}>", array.data_type()))
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

    fn make_labeled_batch(
        timestamps: &[i64],
        metrics: &[&str],
        hosts: &[&str],
        values: &[f64],
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(StringArray::from(metrics.to_vec())),
                Arc::new(StringArray::from(hosts.to_vec())),
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

    #[test]
    fn test_dedup_same_timestamp_metric_different_labels() {
        let batch = make_labeled_batch(
            &[100, 100],
            &["cpu", "cpu"],
            &["host-a", "host-b"],
            &[1.0, 1.0],
        );

        let result = dedup_batches(vec![batch]).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2,
            "Rows with same timestamp+metric but different labels are distinct series"
        );
    }

    #[test]
    fn test_dedup_same_timestamp_metric_labels_different_values() {
        let batch = make_labeled_batch(
            &[100, 100],
            &["cpu", "cpu"],
            &["host-a", "host-a"],
            &[1.0, 2.0],
        );

        let result = dedup_batches(vec![batch]).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2,
            "Rows with same timestamp+metric+labels but different values are distinct rows"
        );
    }

    #[test]
    fn test_dedup_exact_rows_with_different_column_order() {
        let batch1 = make_labeled_batch(&[100], &["cpu"], &["host-a"], &[1.0]);

        let reordered_schema = Arc::new(Schema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch2 = RecordBatch::try_new(
            reordered_schema,
            vec![
                Arc::new(StringArray::from(vec!["cpu"])),
                Arc::new(StringArray::from(vec!["host-a"])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();

        let result = dedup_batches(vec![batch1, batch2]).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 1,
            "Exact duplicates should be removed even if column order differs across batches"
        );
    }
}
