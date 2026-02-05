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
            ).into_response();
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
        ).into_response(),
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
        }).into_response();
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
    }).into_response()
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
                ).into_response();
            }
        };

        for batch in &batches {
            if let Err(e) = writer.write(batch) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to write batch: {}", e),
                ).into_response();
            }
        }

        if let Err(e) = writer.finish() {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to finish Arrow stream: {}", e),
            ).into_response();
        }
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")],
        buffer,
    ).into_response()
}

fn format_csv_response(batches: Vec<RecordBatch>) -> Response {
    use arrow::csv::WriterBuilder;

    if batches.is_empty() {
        return (StatusCode::OK, "").into_response();
    }

    let mut buffer = Vec::new();

    {
        let mut writer = WriterBuilder::new()
            .with_header(true)
            .build(&mut buffer);

        for batch in &batches {
            if let Err(e) = writer.write(batch) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to write CSV: {}", e),
                ).into_response();
            }
        }
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/csv")],
        buffer,
    ).into_response()
}

fn column_value_to_json(array: &dyn arrow_array::Array, row: usize) -> serde_json::Value {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_schema::DataType;

    if array.is_null(row) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Null => serde_json::Value::Null,
        DataType::Boolean => {
            serde_json::Value::Bool(array.as_boolean().value(row))
        }
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
        DataType::Timestamp(_, _) => {
            // Return timestamp as integer (nanoseconds)
            let val = array.as_primitive::<TimestampNanosecondType>().value(row);
            serde_json::Value::Number(val.into())
        }
        _ => serde_json::Value::String(format!("Unsupported type: {:?}", array.data_type())),
    }
}
