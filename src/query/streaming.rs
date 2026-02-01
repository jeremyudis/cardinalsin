//! Streaming query support (historical + live)

use super::QueryEngine;
use crate::metadata::MetadataClient;
use crate::Result;

use arrow_array::{RecordBatch, BooleanArray, StringArray, Int64Array, Float64Array};
use arrow_array::cast::AsArray;
use arrow::compute::filter_record_batch;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

/// Streaming query that merges historical and live data
pub struct StreamingQuery {
    /// SQL query
    pub sql: String,
    /// Merge timestamp (where historical ends and live begins)
    pub merge_timestamp: i64,
}

/// Executor for streaming queries
pub struct StreamingQueryExecutor {
    engine: QueryEngine,
    metadata: Arc<dyn MetadataClient>,
    broadcast_rx: broadcast::Receiver<RecordBatch>,
}

impl StreamingQueryExecutor {
    /// Create a new streaming query executor
    pub fn new(
        engine: QueryEngine,
        metadata: Arc<dyn MetadataClient>,
        broadcast_rx: broadcast::Receiver<RecordBatch>,
    ) -> Self {
        Self {
            engine,
            metadata,
            broadcast_rx,
        }
    }

    /// Execute a streaming query returning a channel receiver
    pub async fn execute(
        mut self,
        sql: &str,
    ) -> Result<mpsc::Receiver<Result<RecordBatch>>> {
        let (tx, rx) = mpsc::channel(100);

        // Extract time range and set merge point to now
        let time_range = self.engine.extract_time_range(sql).await?;
        let merge_timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Parse SQL to extract predicates for live filtering
        let query_filter = QueryFilter::from_sql(sql);

        // Get historical chunks
        let chunks = self.metadata.get_chunks(time_range).await?;

        // Register chunks
        for chunk in &chunks {
            self.engine.register_chunk(&chunk.chunk_path).await?;
        }

        let sql_owned = sql.to_string();
        let engine = self.engine.clone();
        let mut broadcast_rx = self.broadcast_rx;

        // Spawn task to stream results
        tokio::spawn(async move {
            // First, stream historical data
            match engine.execute(&sql_owned).await {
                Ok(batches) => {
                    for batch in batches {
                        if tx.send(Ok(batch)).await.is_err() {
                            return; // Receiver dropped
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }

            // Then stream live data with filtering
            loop {
                match broadcast_rx.recv().await {
                    Ok(batch) => {
                        // Apply query filter to live data
                        match query_filter.apply(&batch, merge_timestamp) {
                            Ok(Some(filtered)) => {
                                if tx.send(Ok(filtered)).await.is_err() {
                                    break; // Receiver dropped
                                }
                            }
                            Ok(None) => {
                                // No matching rows, skip this batch
                                continue;
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Subscriber lagged, continue
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        Ok(rx)
    }
}

/// Filter for live data based on query predicates
#[derive(Clone, Debug)]
pub struct QueryFilter {
    /// Parsed predicates from SQL
    predicates: Vec<Predicate>,
}

/// A single predicate condition
#[derive(Clone, Debug)]
pub struct Predicate {
    pub column: String,
    pub op: PredicateOp,
    pub value: PredicateValue,
}

#[derive(Clone, Debug)]
pub enum PredicateOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug)]
pub enum PredicateValue {
    String(String),
    Int(i64),
    Float(f64),
}

impl QueryFilter {
    /// Parse SQL to extract predicates
    pub fn from_sql(sql: &str) -> Self {
        let mut predicates = Vec::new();

        // Simple regex-based extraction for common patterns
        // In production, use sqlparser-rs for proper AST parsing
        let sql_lower = sql.to_lowercase();

        // Look for simple equality patterns: column = 'value'
        if let Ok(re) = regex::Regex::new(r"(\w+)\s*=\s*'([^']*)'") {
            for cap in re.captures_iter(&sql_lower) {
                if let (Some(col), Some(val)) = (cap.get(1), cap.get(2)) {
                    predicates.push(Predicate {
                        column: col.as_str().to_string(),
                        op: PredicateOp::Eq,
                        value: PredicateValue::String(val.as_str().to_string()),
                    });
                }
            }
        }

        // Look for numeric equality: column = 123
        if let Ok(re) = regex::Regex::new(r"(\w+)\s*=\s*(\d+)") {
            for cap in re.captures_iter(&sql_lower) {
                if let (Some(col), Some(val)) = (cap.get(1), cap.get(2)) {
                    if let Ok(num) = val.as_str().parse::<i64>() {
                        predicates.push(Predicate {
                            column: col.as_str().to_string(),
                            op: PredicateOp::Eq,
                            value: PredicateValue::Int(num),
                        });
                    }
                }
            }
        }

        Self { predicates }
    }

    /// Apply filter to batch, returning only matching rows
    /// Returns None if no rows match
    pub fn apply(&self, batch: &RecordBatch, merge_timestamp: i64) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        // Start with all rows selected
        let mut mask = vec![true; batch.num_rows()];

        // Apply timestamp filter - only include rows after merge point
        if let Some(ts_col) = batch.column_by_name("timestamp") {
            if let Some(ts_array) = ts_col.as_primitive_opt::<arrow_array::types::TimestampNanosecondType>() {
                for (i, val) in ts_array.iter().enumerate() {
                    if let Some(ts) = val {
                        if ts < merge_timestamp {
                            mask[i] = false;
                        }
                    }
                }
            } else if let Some(ts_array) = ts_col.as_primitive_opt::<arrow_array::types::Int64Type>() {
                for (i, val) in ts_array.iter().enumerate() {
                    if let Some(ts) = val {
                        if ts < merge_timestamp {
                            mask[i] = false;
                        }
                    }
                }
            }
        }

        // Apply predicate filters
        for pred in &self.predicates {
            if let Some(col) = batch.column_by_name(&pred.column) {
                match &pred.value {
                    PredicateValue::String(expected) => {
                        if let Some(string_array) = col.as_string_opt::<i32>() {
                            for (i, val) in string_array.iter().enumerate() {
                                if mask[i] {
                                    match (&pred.op, val) {
                                        (PredicateOp::Eq, Some(v)) => mask[i] = v == expected,
                                        (PredicateOp::Ne, Some(v)) => mask[i] = v != expected,
                                        _ => mask[i] = false,
                                    }
                                }
                            }
                        }
                    }
                    PredicateValue::Int(expected) => {
                        if let Some(int_array) = col.as_primitive_opt::<arrow_array::types::Int64Type>() {
                            for (i, val) in int_array.iter().enumerate() {
                                if mask[i] {
                                    match (&pred.op, val) {
                                        (PredicateOp::Eq, Some(v)) => mask[i] = v == *expected,
                                        (PredicateOp::Ne, Some(v)) => mask[i] = v != *expected,
                                        (PredicateOp::Lt, Some(v)) => mask[i] = v < *expected,
                                        (PredicateOp::Le, Some(v)) => mask[i] = v <= *expected,
                                        (PredicateOp::Gt, Some(v)) => mask[i] = v > *expected,
                                        (PredicateOp::Ge, Some(v)) => mask[i] = v >= *expected,
                                        _ => mask[i] = false,
                                    }
                                }
                            }
                        }
                    }
                    PredicateValue::Float(expected) => {
                        if let Some(float_array) = col.as_primitive_opt::<arrow_array::types::Float64Type>() {
                            for (i, val) in float_array.iter().enumerate() {
                                if mask[i] {
                                    match (&pred.op, val) {
                                        (PredicateOp::Eq, Some(v)) => mask[i] = (v - expected).abs() < f64::EPSILON,
                                        (PredicateOp::Ne, Some(v)) => mask[i] = (v - expected).abs() >= f64::EPSILON,
                                        (PredicateOp::Lt, Some(v)) => mask[i] = v < *expected,
                                        (PredicateOp::Le, Some(v)) => mask[i] = v <= *expected,
                                        (PredicateOp::Gt, Some(v)) => mask[i] = v > *expected,
                                        (PredicateOp::Ge, Some(v)) => mask[i] = v >= *expected,
                                        _ => mask[i] = false,
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Check if any rows match
        if !mask.iter().any(|&m| m) {
            return Ok(None);
        }

        // Apply the filter mask to the batch
        let bool_array = BooleanArray::from(mask);
        let filtered = filter_record_batch(batch, &bool_array)?;

        if filtered.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(filtered))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_filter_parsing() {
        let filter = QueryFilter::from_sql("SELECT * FROM metrics WHERE service = 'api'");
        assert!(!filter.predicates.is_empty());
        assert_eq!(filter.predicates[0].column, "service");
    }

    #[test]
    fn test_empty_filter() {
        let filter = QueryFilter::from_sql("SELECT * FROM metrics");
        assert!(filter.predicates.is_empty());
    }
}
