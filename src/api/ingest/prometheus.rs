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
/// Implements manual protobuf parsing for the Prometheus Remote Write format.
/// The wire format follows proto3 conventions:
/// - WriteRequest: field 1 = repeated TimeSeries (length-delimited)
/// - TimeSeries: field 1 = repeated Label, field 2 = repeated Sample
/// - Label: field 1 = name (string), field 2 = value (string)
/// - Sample: field 1 = value (double), field 2 = timestamp (int64)
fn parse_write_request(data: &[u8]) -> Result<WriteRequest> {
    let mut timeseries = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        // Read field tag (varint)
        let (tag, new_pos) = read_varint(data, pos)?;
        pos = new_pos;

        let field_number = tag >> 3;
        let wire_type = tag & 0x7;

        match (field_number, wire_type) {
            // Field 1: timeseries (repeated, length-delimited)
            (1, 2) => {
                let (length, new_pos) = read_varint(data, pos)?;
                pos = new_pos;
                let end = pos + length as usize;
                if end > data.len() {
                    return Err(crate::Error::InvalidSchema("Truncated timeseries".into()));
                }
                let ts = parse_timeseries(&data[pos..end])?;
                timeseries.push(ts);
                pos = end;
            }
            // Skip unknown fields
            (_, 0) => {
                // Varint
                let (_, new_pos) = read_varint(data, pos)?;
                pos = new_pos;
            }
            (_, 1) => {
                // 64-bit
                pos += 8;
            }
            (_, 2) => {
                // Length-delimited
                let (length, new_pos) = read_varint(data, pos)?;
                pos = new_pos + length as usize;
            }
            (_, 5) => {
                // 32-bit
                pos += 4;
            }
            _ => {
                return Err(crate::Error::InvalidSchema(format!(
                    "Unknown wire type {} for field {}",
                    wire_type, field_number
                )));
            }
        }
    }

    Ok(WriteRequest { timeseries })
}

/// Parse a TimeSeries message from protobuf bytes
fn parse_timeseries(data: &[u8]) -> Result<TimeSeries> {
    let mut labels = Vec::new();
    let mut samples = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        let (tag, new_pos) = read_varint(data, pos)?;
        pos = new_pos;

        let field_number = tag >> 3;
        let wire_type = tag & 0x7;

        match (field_number, wire_type) {
            // Field 1: labels (repeated, length-delimited)
            (1, 2) => {
                let (length, new_pos) = read_varint(data, pos)?;
                pos = new_pos;
                let end = pos + length as usize;
                if end > data.len() {
                    return Err(crate::Error::InvalidSchema("Truncated label".into()));
                }
                let label = parse_label(&data[pos..end])?;
                labels.push(label);
                pos = end;
            }
            // Field 2: samples (repeated, length-delimited)
            (2, 2) => {
                let (length, new_pos) = read_varint(data, pos)?;
                pos = new_pos;
                let end = pos + length as usize;
                if end > data.len() {
                    return Err(crate::Error::InvalidSchema("Truncated sample".into()));
                }
                let sample = parse_sample(&data[pos..end])?;
                samples.push(sample);
                pos = end;
            }
            // Skip unknown fields
            (_, 0) => {
                let (_, new_pos) = read_varint(data, pos)?;
                pos = new_pos;
            }
            (_, 1) => {
                pos += 8;
            }
            (_, 2) => {
                let (length, new_pos) = read_varint(data, pos)?;
                pos = new_pos + length as usize;
            }
            (_, 5) => {
                pos += 4;
            }
            _ => {
                return Err(crate::Error::InvalidSchema(format!(
                    "Unknown wire type in timeseries: {}",
                    wire_type
                )));
            }
        }
    }

    Ok(TimeSeries { labels, samples })
}

/// Parse a Label message from protobuf bytes
fn parse_label(data: &[u8]) -> Result<Label> {
    let mut name = String::new();
    let mut value = String::new();
    let mut pos = 0;

    while pos < data.len() {
        let (tag, new_pos) = read_varint(data, pos)?;
        pos = new_pos;

        let field_number = tag >> 3;
        let wire_type = tag & 0x7;

        match (field_number, wire_type) {
            // Field 1: name (string, length-delimited)
            (1, 2) => {
                let (length, new_pos) = read_varint(data, pos)?;
                pos = new_pos;
                let end = pos + length as usize;
                if end > data.len() {
                    return Err(crate::Error::InvalidSchema("Truncated label name".into()));
                }
                name = String::from_utf8_lossy(&data[pos..end]).to_string();
                pos = end;
            }
            // Field 2: value (string, length-delimited)
            (2, 2) => {
                let (length, new_pos) = read_varint(data, pos)?;
                pos = new_pos;
                let end = pos + length as usize;
                if end > data.len() {
                    return Err(crate::Error::InvalidSchema("Truncated label value".into()));
                }
                value = String::from_utf8_lossy(&data[pos..end]).to_string();
                pos = end;
            }
            // Skip unknown fields
            (_, 0) => {
                let (_, new_pos) = read_varint(data, pos)?;
                pos = new_pos;
            }
            (_, 1) => {
                pos += 8;
            }
            (_, 2) => {
                let (length, new_pos) = read_varint(data, pos)?;
                pos = new_pos + length as usize;
            }
            (_, 5) => {
                pos += 4;
            }
            _ => {
                return Err(crate::Error::InvalidSchema(format!(
                    "Unknown wire type in label: {}",
                    wire_type
                )));
            }
        }
    }

    Ok(Label { name, value })
}

/// Parse a Sample message from protobuf bytes
fn parse_sample(data: &[u8]) -> Result<Sample> {
    let mut value = 0.0f64;
    let mut timestamp_ms = 0i64;
    let mut pos = 0;

    while pos < data.len() {
        let (tag, new_pos) = read_varint(data, pos)?;
        pos = new_pos;

        let field_number = tag >> 3;
        let wire_type = tag & 0x7;

        match (field_number, wire_type) {
            // Field 1: value (double, 64-bit fixed)
            (1, 1) => {
                if pos + 8 > data.len() {
                    return Err(crate::Error::InvalidSchema("Truncated sample value".into()));
                }
                let bytes: [u8; 8] = data[pos..pos + 8].try_into().unwrap();
                value = f64::from_le_bytes(bytes);
                pos += 8;
            }
            // Field 2: timestamp (int64, varint)
            (2, 0) => {
                let (ts, new_pos) = read_varint(data, pos)?;
                timestamp_ms = ts as i64;
                pos = new_pos;
            }
            // Skip unknown fields
            (_, 0) => {
                let (_, new_pos) = read_varint(data, pos)?;
                pos = new_pos;
            }
            (_, 1) => {
                pos += 8;
            }
            (_, 2) => {
                let (length, new_pos) = read_varint(data, pos)?;
                pos = new_pos + length as usize;
            }
            (_, 5) => {
                pos += 4;
            }
            _ => {
                return Err(crate::Error::InvalidSchema(format!(
                    "Unknown wire type in sample: {}",
                    wire_type
                )));
            }
        }
    }

    Ok(Sample { timestamp_ms, value })
}

/// Read a varint from the buffer, returning (value, new_position)
fn read_varint(data: &[u8], start: usize) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut pos = start;

    loop {
        if pos >= data.len() {
            return Err(crate::Error::InvalidSchema("Truncated varint".into()));
        }
        let byte = data[pos];
        pos += 1;

        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, pos));
        }
        shift += 7;
        if shift >= 64 {
            return Err(crate::Error::InvalidSchema("Varint too long".into()));
        }
    }
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
