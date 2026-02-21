//! Streaming query support (historical + live)

use super::QueryEngine;
use crate::ingester::FilteredReceiver;
use crate::metadata::predicates::{ColumnPredicate, PredicateValue};
use crate::metadata::MetadataClient;
use crate::Result;

use arrow::compute::filter_record_batch;
use arrow_array::cast::AsArray;
use arrow_array::{Array, BooleanArray, RecordBatch};
use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
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

/// Receiver type for streaming queries
pub enum StreamReceiver {
    /// Legacy broadcast receiver (no filtering)
    Broadcast(broadcast::Receiver<RecordBatch>),
    /// Topic-filtered receiver (90% less bandwidth)
    Filtered(FilteredReceiver),
}

/// Executor for streaming queries
pub struct StreamingQueryExecutor {
    engine: QueryEngine,
    metadata: Arc<dyn MetadataClient>,
    receiver: StreamReceiver,
}

impl StreamingQueryExecutor {
    /// Create a new streaming query executor with legacy broadcast
    pub fn new(
        engine: QueryEngine,
        metadata: Arc<dyn MetadataClient>,
        broadcast_rx: broadcast::Receiver<RecordBatch>,
    ) -> Self {
        Self {
            engine,
            metadata,
            receiver: StreamReceiver::Broadcast(broadcast_rx),
        }
    }

    /// Create a new streaming query executor with filtered receiver
    ///
    /// This is the recommended approach as it reduces bandwidth waste from 90% to near-zero
    /// by only receiving data matching the subscription filter.
    pub fn new_filtered(
        engine: QueryEngine,
        metadata: Arc<dyn MetadataClient>,
        filtered_rx: FilteredReceiver,
    ) -> Self {
        Self {
            engine,
            metadata,
            receiver: StreamReceiver::Filtered(filtered_rx),
        }
    }

    /// Execute a streaming query returning a channel receiver
    pub async fn execute(self, sql: &str) -> Result<mpsc::Receiver<Result<RecordBatch>>> {
        let (tx, rx) = mpsc::channel(100);

        // Extract time range and set merge point to now
        let time_range = self.engine.extract_time_range(sql).await?;
        let merge_timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Parse SQL to extract predicates for live filtering
        let query_filter = QueryFilter::from_sql(sql);

        // Extract column predicates for metadata-level filtering
        let predicates = self.engine.extract_column_predicates(sql).await?;

        // Get historical chunks with predicate pushdown
        let chunks = self
            .metadata
            .get_chunks_with_predicates(time_range, &predicates)
            .await?;

        // Register chunks
        for chunk in &chunks {
            self.engine.register_chunk(&chunk.chunk_path).await?;
        }

        let sql_owned = sql.to_string();
        let engine = self.engine.clone();
        let receiver = self.receiver;

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
            match receiver {
                StreamReceiver::Broadcast(mut broadcast_rx) => {
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
                }
                StreamReceiver::Filtered(mut filtered_rx) => {
                    // Filtered receiver already applies topic filtering at ingester level
                    // We still need to apply timestamp and other query filters
                    loop {
                        match filtered_rx.recv().await {
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
                }
            }
        });

        Ok(rx)
    }
}

/// Filter for live data based on query predicates.
///
/// Uses `ColumnPredicate` from the metadata predicates module (unified type system).
#[derive(Clone, Debug)]
pub struct QueryFilter {
    /// Parsed predicates from SQL
    pub predicates: Vec<ColumnPredicate>,
}

impl QueryFilter {
    /// Parse SQL to extract predicates using proper AST parsing.
    ///
    /// Produces `ColumnPredicate` values from the unified predicate type system.
    pub fn from_sql(sql: &str) -> Self {
        let mut predicates = Vec::new();

        let dialect = GenericDialect {};
        let ast = match Parser::parse_sql(&dialect, sql) {
            Ok(stmts) => stmts,
            Err(_) => {
                return Self { predicates: vec![] };
            }
        };

        for stmt in ast {
            if let Statement::Query(query) = stmt {
                Self::extract_predicates_from_set_expr(&query.body, &mut predicates);
            }
        }

        Self { predicates }
    }

    /// Extract predicates from a SetExpr (SELECT body)
    fn extract_predicates_from_set_expr(body: &SetExpr, predicates: &mut Vec<ColumnPredicate>) {
        if let SetExpr::Select(select) = body {
            if let Some(selection) = &select.selection {
                Self::extract_predicates_from_expr(selection, predicates);
            }
        }
    }

    /// Extract predicates from an expression recursively
    fn extract_predicates_from_expr(expr: &Expr, predicates: &mut Vec<ColumnPredicate>) {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                if let Some(pred) = Self::try_extract_comparison(left, op, right) {
                    predicates.push(pred);
                }
                if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    Self::extract_predicates_from_expr(left, predicates);
                    Self::extract_predicates_from_expr(right, predicates);
                }
            }
            Expr::Nested(inner) => {
                Self::extract_predicates_from_expr(inner, predicates);
            }
            _ => {}
        }
    }

    /// Try to extract a ColumnPredicate from a binary comparison
    fn try_extract_comparison(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Option<ColumnPredicate> {
        // Try column op value
        if let Some(pred) = Self::try_column_op_value(left, op, right) {
            return Some(pred);
        }
        // Try value op column (reversed)
        let reversed_op = match op {
            BinaryOperator::Lt => Some(BinaryOperator::Gt),
            BinaryOperator::LtEq => Some(BinaryOperator::GtEq),
            BinaryOperator::Gt => Some(BinaryOperator::Lt),
            BinaryOperator::GtEq => Some(BinaryOperator::LtEq),
            other => Some(other.clone()),
        };
        if let Some(rev) = reversed_op {
            if let Some(pred) = Self::try_column_op_value(right, &rev, left) {
                return Some(pred);
            }
        }
        None
    }

    /// Try to extract column name and value, producing a ColumnPredicate
    fn try_column_op_value(
        col_expr: &Expr,
        op: &BinaryOperator,
        val_expr: &Expr,
    ) -> Option<ColumnPredicate> {
        let column = match col_expr {
            Expr::Identifier(ident) => ident.value.to_lowercase(),
            Expr::CompoundIdentifier(idents) => idents.last()?.value.to_lowercase(),
            _ => return None,
        };

        let value = Self::parse_sql_value(val_expr)?;

        match op {
            BinaryOperator::Eq => Some(ColumnPredicate::Eq(column, value)),
            BinaryOperator::NotEq => Some(ColumnPredicate::NotEq(column, value)),
            BinaryOperator::Lt => Some(ColumnPredicate::Lt(column, value)),
            BinaryOperator::LtEq => Some(ColumnPredicate::LtEq(column, value)),
            BinaryOperator::Gt => Some(ColumnPredicate::Gt(column, value)),
            BinaryOperator::GtEq => Some(ColumnPredicate::GtEq(column, value)),
            _ => None,
        }
    }

    /// Parse a SQL AST value expression into a PredicateValue
    fn parse_sql_value(val_expr: &Expr) -> Option<PredicateValue> {
        match val_expr {
            Expr::Value(v) => match v {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    Some(PredicateValue::String(s.to_string()))
                }
                Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Some(PredicateValue::Int64(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Some(PredicateValue::Float64(f))
                    } else {
                        None
                    }
                }
                Value::Boolean(b) => Some(PredicateValue::Boolean(*b)),
                Value::Null => Some(PredicateValue::Null),
                _ => None,
            },
            _ => None,
        }
    }

    /// Apply filter to batch, returning only matching rows.
    /// Returns None if no rows match.
    pub fn apply(&self, batch: &RecordBatch, merge_timestamp: i64) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let mut mask = vec![true; batch.num_rows()];

        // Apply timestamp filter - only include rows after merge point
        if let Some(ts_col) = batch.column_by_name("timestamp") {
            if let Some(ts_array) =
                ts_col.as_primitive_opt::<arrow_array::types::TimestampNanosecondType>()
            {
                for (i, val) in ts_array.iter().enumerate() {
                    if let Some(ts) = val {
                        if ts < merge_timestamp {
                            mask[i] = false;
                        }
                    }
                }
            } else if let Some(ts_array) =
                ts_col.as_primitive_opt::<arrow_array::types::Int64Type>()
            {
                for (i, val) in ts_array.iter().enumerate() {
                    if let Some(ts) = val {
                        if ts < merge_timestamp {
                            mask[i] = false;
                        }
                    }
                }
            }
        }

        // Apply predicate filters using the unified ColumnPredicate type
        for pred in &self.predicates {
            Self::apply_predicate_to_mask(pred, batch, &mut mask);
        }

        if !mask.iter().any(|&m| m) {
            return Ok(None);
        }

        let bool_array = BooleanArray::from(mask);
        let filtered = filter_record_batch(batch, &bool_array)?;

        if filtered.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(filtered))
        }
    }

    /// Apply a single ColumnPredicate to a row mask
    fn apply_predicate_to_mask(pred: &ColumnPredicate, batch: &RecordBatch, mask: &mut [bool]) {
        match pred {
            ColumnPredicate::Eq(col, val)
            | ColumnPredicate::NotEq(col, val)
            | ColumnPredicate::Lt(col, val)
            | ColumnPredicate::LtEq(col, val)
            | ColumnPredicate::Gt(col, val)
            | ColumnPredicate::GtEq(col, val) => {
                if let Some(column) = batch.column_by_name(col) {
                    Self::apply_comparison(pred, column, val, mask);
                }
            }
            ColumnPredicate::And(left, right) => {
                Self::apply_predicate_to_mask(left, batch, mask);
                Self::apply_predicate_to_mask(right, batch, mask);
            }
            ColumnPredicate::Or(left, right) => {
                let mut left_mask = mask.to_vec();
                let mut right_mask = mask.to_vec();
                Self::apply_predicate_to_mask(left, batch, &mut left_mask);
                Self::apply_predicate_to_mask(right, batch, &mut right_mask);
                for i in 0..mask.len() {
                    mask[i] = left_mask[i] || right_mask[i];
                }
            }
            ColumnPredicate::Not(inner) => {
                let mut inner_mask = vec![true; mask.len()];
                Self::apply_predicate_to_mask(inner, batch, &mut inner_mask);
                for i in 0..mask.len() {
                    if mask[i] {
                        mask[i] = !inner_mask[i];
                    }
                }
            }
            ColumnPredicate::In(col, values) => {
                if let Some(column) = batch.column_by_name(col) {
                    for (i, m) in mask.iter_mut().enumerate() {
                        if *m {
                            *m = values.iter().any(|v| Self::row_matches_value(column, i, v));
                        }
                    }
                }
            }
            ColumnPredicate::NotIn(col, values) => {
                if let Some(column) = batch.column_by_name(col) {
                    for (i, m) in mask.iter_mut().enumerate() {
                        if *m {
                            *m = !values.iter().any(|v| Self::row_matches_value(column, i, v));
                        }
                    }
                }
            }
            ColumnPredicate::Between(col, low, high) => {
                if let Some(column) = batch.column_by_name(col) {
                    for (i, m) in mask.iter_mut().enumerate() {
                        if *m {
                            *m = Self::row_gte_value(column, i, low)
                                && Self::row_lte_value(column, i, high);
                        }
                    }
                }
            }
        }
    }

    /// Apply a comparison predicate to a column
    fn apply_comparison(
        pred: &ColumnPredicate,
        column: &dyn arrow_array::Array,
        val: &PredicateValue,
        mask: &mut [bool],
    ) {
        match val {
            PredicateValue::String(expected) => {
                if let Some(arr) = column.as_string_opt::<i32>() {
                    for (i, m) in mask.iter_mut().enumerate() {
                        if *m {
                            *m = match (pred, arr.is_null(i)) {
                                (_, true) => false,
                                (ColumnPredicate::Eq(..), _) => arr.value(i) == expected.as_str(),
                                (ColumnPredicate::NotEq(..), _) => {
                                    arr.value(i) != expected.as_str()
                                }
                                (ColumnPredicate::Lt(..), _) => arr.value(i) < expected.as_str(),
                                (ColumnPredicate::LtEq(..), _) => arr.value(i) <= expected.as_str(),
                                (ColumnPredicate::Gt(..), _) => arr.value(i) > expected.as_str(),
                                (ColumnPredicate::GtEq(..), _) => arr.value(i) >= expected.as_str(),
                                _ => false,
                            };
                        }
                    }
                }
            }
            PredicateValue::Int64(expected) => {
                if let Some(arr) = column.as_primitive_opt::<arrow_array::types::Int64Type>() {
                    for (i, val_opt) in arr.iter().enumerate() {
                        if mask[i] {
                            mask[i] = match (pred, val_opt) {
                                (ColumnPredicate::Eq(..), Some(v)) => v == *expected,
                                (ColumnPredicate::NotEq(..), Some(v)) => v != *expected,
                                (ColumnPredicate::Lt(..), Some(v)) => v < *expected,
                                (ColumnPredicate::LtEq(..), Some(v)) => v <= *expected,
                                (ColumnPredicate::Gt(..), Some(v)) => v > *expected,
                                (ColumnPredicate::GtEq(..), Some(v)) => v >= *expected,
                                _ => false,
                            };
                        }
                    }
                }
            }
            PredicateValue::Float64(expected) => {
                if let Some(arr) = column.as_primitive_opt::<arrow_array::types::Float64Type>() {
                    for (i, val_opt) in arr.iter().enumerate() {
                        if mask[i] {
                            mask[i] = match (pred, val_opt) {
                                (ColumnPredicate::Eq(..), Some(v)) => {
                                    (v - expected).abs() < f64::EPSILON
                                }
                                (ColumnPredicate::NotEq(..), Some(v)) => {
                                    (v - expected).abs() >= f64::EPSILON
                                }
                                (ColumnPredicate::Lt(..), Some(v)) => v < *expected,
                                (ColumnPredicate::LtEq(..), Some(v)) => v <= *expected,
                                (ColumnPredicate::Gt(..), Some(v)) => v > *expected,
                                (ColumnPredicate::GtEq(..), Some(v)) => v >= *expected,
                                _ => false,
                            };
                        }
                    }
                }
            }
            _ => {} // Boolean/Null: no-op for streaming filters
        }
    }

    /// Check if a row's value matches a PredicateValue (for IN/NOT IN)
    fn row_matches_value(
        column: &dyn arrow_array::Array,
        row: usize,
        val: &PredicateValue,
    ) -> bool {
        match val {
            PredicateValue::String(expected) => column
                .as_string_opt::<i32>()
                .map(|arr| !arr.is_null(row) && arr.value(row) == expected.as_str())
                .unwrap_or(false),
            PredicateValue::Int64(expected) => column
                .as_primitive_opt::<arrow_array::types::Int64Type>()
                .map(|arr| {
                    arr.iter()
                        .nth(row)
                        .flatten()
                        .map(|v| v == *expected)
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            PredicateValue::Float64(expected) => column
                .as_primitive_opt::<arrow_array::types::Float64Type>()
                .map(|arr| {
                    arr.iter()
                        .nth(row)
                        .flatten()
                        .map(|v| (v - expected).abs() < f64::EPSILON)
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            _ => false,
        }
    }

    /// Check if row value >= predicate value (for BETWEEN)
    fn row_gte_value(column: &dyn arrow_array::Array, row: usize, val: &PredicateValue) -> bool {
        match val {
            PredicateValue::Int64(expected) => column
                .as_primitive_opt::<arrow_array::types::Int64Type>()
                .map(|arr| {
                    arr.iter()
                        .nth(row)
                        .flatten()
                        .map(|v| v >= *expected)
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            PredicateValue::Float64(expected) => column
                .as_primitive_opt::<arrow_array::types::Float64Type>()
                .map(|arr| {
                    arr.iter()
                        .nth(row)
                        .flatten()
                        .map(|v| v >= *expected)
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            PredicateValue::String(expected) => column
                .as_string_opt::<i32>()
                .map(|arr| !arr.is_null(row) && arr.value(row) >= expected.as_str())
                .unwrap_or(false),
            _ => false,
        }
    }

    /// Check if row value <= predicate value (for BETWEEN)
    fn row_lte_value(column: &dyn arrow_array::Array, row: usize, val: &PredicateValue) -> bool {
        match val {
            PredicateValue::Int64(expected) => column
                .as_primitive_opt::<arrow_array::types::Int64Type>()
                .map(|arr| {
                    arr.iter()
                        .nth(row)
                        .flatten()
                        .map(|v| v <= *expected)
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            PredicateValue::Float64(expected) => column
                .as_primitive_opt::<arrow_array::types::Float64Type>()
                .map(|arr| {
                    arr.iter()
                        .nth(row)
                        .flatten()
                        .map(|v| v <= *expected)
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            PredicateValue::String(expected) => column
                .as_string_opt::<i32>()
                .map(|arr| !arr.is_null(row) && arr.value(row) <= expected.as_str())
                .unwrap_or(false),
            _ => false,
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
        match &filter.predicates[0] {
            ColumnPredicate::Eq(col, PredicateValue::String(val)) => {
                assert_eq!(col, "service");
                assert_eq!(val, "api");
            }
            other => panic!("Expected Eq predicate, got {:?}", other),
        }
    }

    #[test]
    fn test_empty_filter() {
        let filter = QueryFilter::from_sql("SELECT * FROM metrics");
        assert!(filter.predicates.is_empty());
    }
}
