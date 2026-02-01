//! Prometheus Remote Write receiver
//!
//! Implements the Prometheus Remote Write protocol with Snappy compression.

use crate::api::ApiState;
use crate::schema::{TIMESTAMP_FIELD, METRIC_NAME_FIELD, VALUE_F64_FIELD};
use crate::Result;

use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Handle Prometheus Remote Write requests
///
/// POST /api/v1/write
/// Content-Encoding: snappy
/// Content-Type: application/x-protobuf
pub async fn handle_remote_write(
    State(state): State<ApiState>,
    body: Bytes,
) -> impl IntoResponse {
    // 1. Decompress Snappy
    let decompressed = match snap::raw::Decoder::new().decompress_vec(&body) {
        Ok(data) => data,
        Err(_) => return StatusCode::BAD_REQUEST,
    };

    // 2. Parse the protobuf (simplified - in production use prost-generated types)
    let write_request = match parse_write_request(&decompressed) {
        Ok(req) => req,
        Err(_) => return StatusCode::BAD_REQUEST,
    };

    // 3. Convert to Arrow RecordBatch
    let batch = match convert_prom_to_arrow(&write_request) {
        Ok(batch) => batch,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    // 4. Ingest
    match state.ingester.write(batch).await {
        Ok(_) => StatusCode::NO_CONTENT, // 204 = success
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Prometheus write request (simplified representation)
#[derive(Debug, Clone)]
pub struct WriteRequest {
    pub timeseries: Vec<TimeSeries>,
}

/// Time series in Prometheus format
#[derive(Debug, Clone)]
pub struct TimeSeries {
    pub labels: Vec<Label>,
    pub samples: Vec<Sample>,
}

/// Label key-value pair
#[derive(Debug, Clone)]
pub struct Label {
    pub name: String,
    pub value: String,
}

/// Sample with timestamp and value
#[derive(Debug, Clone)]
pub struct Sample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// Parse a write request from protobuf bytes
///
/// This is a simplified parser. In production, use prost-generated types.
fn parse_write_request(_data: &[u8]) -> Result<WriteRequest> {
    // In production, this would use prost to decode the protobuf
    // For now, return empty request for compilation
    Ok(WriteRequest {
        timeseries: Vec::new(),
    })
}

/// Convert Prometheus write request to Arrow RecordBatch
fn convert_prom_to_arrow(req: &WriteRequest) -> Result<RecordBatch> {
    if req.timeseries.is_empty() {
        return Err(crate::Error::InvalidSchema("No timeseries data".into()));
    }

    // Collect all unique label names (except __name__)
    let mut label_names: HashSet<String> = HashSet::new();
    for ts in &req.timeseries {
        for label in &ts.labels {
            if label.name != "__name__" {
                label_names.insert(label.name.clone());
            }
        }
    }

    // Pre-calculate total sample count
    let total_samples: usize = req.timeseries.iter().map(|ts| ts.samples.len()).sum();

    let mut timestamps = Vec::with_capacity(total_samples);
    let mut metric_names = Vec::with_capacity(total_samples);
    let mut values = Vec::with_capacity(total_samples);
    let mut label_values: HashMap<String, Vec<Option<String>>> = label_names
        .iter()
        .map(|name| (name.clone(), Vec::with_capacity(total_samples)))
        .collect();

    for ts in &req.timeseries {
        // Extract metric name and labels
        let metric_name = ts.labels
            .iter()
            .find(|l| l.name == "__name__")
            .map(|l| l.value.clone())
            .unwrap_or_default();

        let ts_labels: HashMap<&str, &str> = ts.labels
            .iter()
            .filter(|l| l.name != "__name__")
            .map(|l| (l.name.as_str(), l.value.as_str()))
            .collect();

        for sample in &ts.samples {
            // Convert milliseconds to nanoseconds
            timestamps.push(sample.timestamp_ms * 1_000_000);
            metric_names.push(metric_name.clone());
            values.push(sample.value);

            // Add label values
            for name in &label_names {
                let value = ts_labels.get(name.as_str()).map(|v| v.to_string());
                label_values.get_mut(name).unwrap().push(value);
            }
        }
    }

    // Build schema
    let mut fields = vec![
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new(METRIC_NAME_FIELD, DataType::Utf8, false),
        Field::new(VALUE_F64_FIELD, DataType::Float64, true),
    ];

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
        Arc::new(StringArray::from(metric_names)),
        Arc::new(Float64Array::from(values)),
    ];

    // Add label columns (sorted for consistency)
    let mut sorted_labels: Vec<_> = label_names.into_iter().collect();
    sorted_labels.sort();

    for name in sorted_labels {
        fields.push(Field::new(&name, DataType::Utf8, true));
        let values = label_values.remove(&name).unwrap();
        columns.push(Arc::new(StringArray::from(values)));
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)?;

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_prom_to_arrow() {
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![
                        Label { name: "__name__".to_string(), value: "cpu_usage".to_string() },
                        Label { name: "host".to_string(), value: "server1".to_string() },
                    ],
                    samples: vec![
                        Sample { timestamp_ms: 1000, value: 0.85 },
                        Sample { timestamp_ms: 2000, value: 0.90 },
                    ],
                },
            ],
        };

        let batch = convert_prom_to_arrow(&req).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.schema().field_with_name("host").is_ok());
    }
}
