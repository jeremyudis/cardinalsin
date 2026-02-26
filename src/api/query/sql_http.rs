//! SQL HTTP API
//!
//! REST API for executing SQL queries and retrieving results.

use crate::api::ApiState;

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// SQL query request
#[derive(Debug, Deserialize)]
pub struct SqlRequest {
    pub query: String,
    #[serde(default)]
    pub format: Option<String>,
}

/// SQL query response
#[derive(Debug, Serialize)]
pub struct SqlResponse {
    pub columns: Vec<String>,
    pub data: Vec<Vec<serde_json::Value>>,
    pub stats: QueryStats,
}

/// Query execution statistics
#[derive(Debug, Serialize)]
pub struct QueryStats {
    pub rows_read: u64,
    pub bytes_read: u64,
    pub execution_time_ms: u64,
}

/// Execute SQL query via POST
pub async fn execute_sql(
    State(state): State<ApiState>,
    Json(request): Json<SqlRequest>,
) -> Response {
    execute_query(state, request).await
}

/// Execute SQL query via GET (URL parameters)
pub async fn execute_sql_get(
    State(state): State<ApiState>,
    Query(request): Query<SqlRequest>,
) -> Response {
    execute_query(state, request).await
}

async fn execute_query(state: ApiState, request: SqlRequest) -> Response {
    let start = Instant::now();

    // Execute query
    let results = match state.query_node.query(&request.query).await {
        Ok(results) => results,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": e.to_string()
                })),
            )
                .into_response();
        }
    };

    let execution_time_ms = start.elapsed().as_millis() as u64;

    // Format response based on requested format
    match request.format.as_deref().unwrap_or("json") {
        "json" => format_json_response(results, execution_time_ms),
        "arrow" => format_arrow_response(results),
        "csv" => format_csv_response(results),
        _ => (
            StatusCode::BAD_REQUEST,
            "Invalid format. Use: json, arrow, or csv",
        )
            .into_response(),
    }
}

fn format_json_response(batches: Vec<RecordBatch>, execution_time_ms: u64) -> Response {
    if batches.is_empty() {
        return Json(SqlResponse {
            columns: Vec::new(),
            data: Vec::new(),
            stats: QueryStats {
                rows_read: 0,
                bytes_read: 0,
                execution_time_ms,
            },
        })
        .into_response();
    }

    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut data = Vec::new();
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;

    for batch in &batches {
        total_rows += batch.num_rows() as u64;
        total_bytes += batch.get_array_memory_size() as u64;

        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let value = column_value_to_json(batch.column(col_idx), row_idx);
                row.push(value);
            }
            data.push(row);
        }
    }

    Json(SqlResponse {
        columns,
        data,
        stats: QueryStats {
            rows_read: total_rows,
            bytes_read: total_bytes,
            execution_time_ms,
        },
    })
    .into_response()
}

fn format_arrow_response(batches: Vec<RecordBatch>) -> Response {
    if batches.is_empty() {
        return (StatusCode::OK, Vec::<u8>::new()).into_response();
    }

    let schema = batches[0].schema();
    let mut buffer = Vec::new();

    {
        let mut writer = match StreamWriter::try_new(&mut buffer, &schema) {
            Ok(w) => w,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to create Arrow writer: {}", e),
                )
                    .into_response();
            }
        };

        for batch in &batches {
            if let Err(e) = writer.write(batch) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to write batch: {}", e),
                )
                    .into_response();
            }
        }

        if let Err(e) = writer.finish() {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to finish Arrow stream: {}", e),
            )
                .into_response();
        }
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")],
        buffer,
    )
        .into_response()
}

fn format_csv_response(batches: Vec<RecordBatch>) -> Response {
    use arrow::csv::WriterBuilder;

    if batches.is_empty() {
        return (StatusCode::OK, "").into_response();
    }

    let mut buffer = Vec::new();

    {
        let mut writer = WriterBuilder::new().with_header(true).build(&mut buffer);

        for batch in &batches {
            if let Err(e) = writer.write(batch) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to write CSV: {}", e),
                )
                    .into_response();
            }
        }
    }

    (StatusCode::OK, [(header::CONTENT_TYPE, "text/csv")], buffer).into_response()
}

fn column_value_to_json(array: &dyn arrow_array::Array, row: usize) -> serde_json::Value {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_schema::{DataType, TimeUnit};

    if array.is_null(row) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Null => serde_json::Value::Null,
        DataType::Boolean => serde_json::Value::Bool(array.as_boolean().value(row)),
        DataType::Int8 => {
            serde_json::Value::Number(array.as_primitive::<Int8Type>().value(row).into())
        }
        DataType::Int16 => {
            serde_json::Value::Number(array.as_primitive::<Int16Type>().value(row).into())
        }
        DataType::Int32 => {
            serde_json::Value::Number(array.as_primitive::<Int32Type>().value(row).into())
        }
        DataType::Int64 => {
            serde_json::Value::Number(array.as_primitive::<Int64Type>().value(row).into())
        }
        DataType::UInt8 => {
            serde_json::Value::Number(array.as_primitive::<UInt8Type>().value(row).into())
        }
        DataType::UInt16 => {
            serde_json::Value::Number(array.as_primitive::<UInt16Type>().value(row).into())
        }
        DataType::UInt32 => {
            serde_json::Value::Number(array.as_primitive::<UInt32Type>().value(row).into())
        }
        DataType::UInt64 => {
            serde_json::Value::Number(array.as_primitive::<UInt64Type>().value(row).into())
        }
        DataType::Float32 => {
            let val = array.as_primitive::<Float32Type>().value(row);
            serde_json::Number::from_f64(val as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Float64 => {
            let val = array.as_primitive::<Float64Type>().value(row);
            serde_json::Number::from_f64(val)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Utf8 => {
            serde_json::Value::String(array.as_string::<i32>().value(row).to_string())
        }
        DataType::LargeUtf8 => {
            serde_json::Value::String(array.as_string::<i64>().value(row).to_string())
        }
        DataType::Utf8View => {
            serde_json::Value::String(array.as_string_view().value(row).to_string())
        }
        DataType::Dictionary(_, value_type) => dictionary_value_to_json(array, row, value_type)
            .unwrap_or_else(|| {
                serde_json::Value::String(format!("Unsupported type: {:?}", array.data_type()))
            }),
        DataType::Timestamp(unit, _) => {
            // Return all timestamps as unix-nanoseconds for stable JSON output.
            let val = match unit {
                TimeUnit::Second => array
                    .as_primitive::<TimestampSecondType>()
                    .value(row)
                    .saturating_mul(1_000_000_000),
                TimeUnit::Millisecond => array
                    .as_primitive::<TimestampMillisecondType>()
                    .value(row)
                    .saturating_mul(1_000_000),
                TimeUnit::Microsecond => array
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(row)
                    .saturating_mul(1_000),
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
            };
            serde_json::Value::Number(val.into())
        }
        _ => serde_json::Value::String(format!("Unsupported type: {:?}", array.data_type())),
    }
}

fn dictionary_value_to_json(
    array: &dyn arrow_array::Array,
    row: usize,
    value_type: &arrow_schema::DataType,
) -> Option<serde_json::Value> {
    use arrow_schema::DataType;

    let string_value = match value_type {
        DataType::Utf8 => extract_dictionary_string::<i32>(array, row),
        DataType::LargeUtf8 => extract_dictionary_string::<i64>(array, row),
        DataType::Utf8View => extract_dictionary_string_view(array, row),
        _ => None,
    }?;

    Some(serde_json::Value::String(string_value))
}

fn extract_dictionary_string<O: arrow_array::OffsetSizeTrait>(
    array: &dyn arrow_array::Array,
    row: usize,
) -> Option<String> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_array::Array;

    macro_rules! extract_for_key_type {
        ($key_type:ty) => {
            if let Some(dict) = array
                .as_any()
                .downcast_ref::<arrow_array::DictionaryArray<$key_type>>()
            {
                let key = dict.keys().value(row) as usize;
                let values = dict.values().as_string::<O>();
                if key < values.len() {
                    return Some(values.value(key).to_string());
                }
                return None;
            }
        };
    }

    extract_for_key_type!(Int8Type);
    extract_for_key_type!(Int16Type);
    extract_for_key_type!(Int32Type);
    extract_for_key_type!(Int64Type);
    extract_for_key_type!(UInt8Type);
    extract_for_key_type!(UInt16Type);
    extract_for_key_type!(UInt32Type);
    extract_for_key_type!(UInt64Type);

    None
}

fn extract_dictionary_string_view(array: &dyn arrow_array::Array, row: usize) -> Option<String> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_array::Array;

    macro_rules! extract_for_key_type {
        ($key_type:ty) => {
            if let Some(dict) = array
                .as_any()
                .downcast_ref::<arrow_array::DictionaryArray<$key_type>>()
            {
                let key = dict.keys().value(row) as usize;
                let values = dict.values().as_string_view();
                if key < values.len() {
                    return Some(values.value(key).to_string());
                }
                return None;
            }
        };
    }

    extract_for_key_type!(Int8Type);
    extract_for_key_type!(Int16Type);
    extract_for_key_type!(Int32Type);
    extract_for_key_type!(Int64Type);
    extract_for_key_type!(UInt8Type);
    extract_for_key_type!(UInt16Type);
    extract_for_key_type!(UInt32Type);
    extract_for_key_type!(UInt64Type);

    None
}

#[cfg(test)]
mod tests {
    use super::column_value_to_json;
    use arrow_array::types::{Int16Type, Int32Type};
    use arrow_array::{
        ArrayRef, DictionaryArray, Int16Array, Int32Array, StringArray, StringViewArray,
    };
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn test_column_value_to_json_utf8_view() {
        let array = StringViewArray::from(vec!["cardinalsin-query", "cardinalsin-ingester"]);
        assert_eq!(column_value_to_json(&array, 0), json!("cardinalsin-query"));
        assert_eq!(
            column_value_to_json(&array, 1),
            json!("cardinalsin-ingester")
        );
    }

    #[test]
    fn test_column_value_to_json_dictionary_utf8() {
        let keys = Int32Array::from(vec![0, 1, 0]);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["svc-a", "svc-b"]));
        let array = DictionaryArray::<Int32Type>::new(keys, values);

        assert_eq!(column_value_to_json(&array, 0), json!("svc-a"));
        assert_eq!(column_value_to_json(&array, 1), json!("svc-b"));
        assert_eq!(column_value_to_json(&array, 2), json!("svc-a"));
    }

    #[test]
    fn test_column_value_to_json_dictionary_utf8_view() {
        let keys = Int16Array::from(vec![1, 0]);
        let values: ArrayRef = Arc::new(StringViewArray::from(vec!["metric-a", "metric-b"]));
        let array = DictionaryArray::<Int16Type>::new(keys, values);

        assert_eq!(column_value_to_json(&array, 0), json!("metric-b"));
        assert_eq!(column_value_to_json(&array, 1), json!("metric-a"));
    }
}
