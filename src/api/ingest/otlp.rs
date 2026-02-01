//! OpenTelemetry Protocol (OTLP) receiver
//!
//! Supports both gRPC and HTTP endpoints for OTLP metrics ingestion.

use crate::ingester::Ingester;
use crate::schema::{TIMESTAMP_FIELD, METRIC_NAME_FIELD, VALUE_F64_FIELD};
use crate::Result;

use arrow_array::{
    ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

/// OTLP metrics receiver
pub struct OtlpReceiver {
    ingester: Arc<Ingester>,
    schema: Arc<Schema>,
}

impl OtlpReceiver {
    /// Create a new OTLP receiver
    pub fn new(ingester: Arc<Ingester>) -> Self {
        // Build schema for converted metrics
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new(METRIC_NAME_FIELD, DataType::Utf8, false),
            Field::new(VALUE_F64_FIELD, DataType::Float64, true),
            // Dynamic label columns would be added based on actual data
        ]));

        Self { ingester, schema }
    }

    /// Convert OTLP metrics to Arrow RecordBatch
    ///
    /// This is a simplified implementation. In production, would use
    /// opentelemetry-proto for proper protobuf parsing.
    pub fn convert_to_arrow(
        &self,
        timestamps: Vec<i64>,
        metric_names: Vec<String>,
        values: Vec<f64>,
        labels: HashMap<String, Vec<String>>,
    ) -> Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(metric_names)),
            Arc::new(Float64Array::from(values)),
        ];

        // Add label columns
        let mut fields = vec![
            Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new(METRIC_NAME_FIELD, DataType::Utf8, false),
            Field::new(VALUE_F64_FIELD, DataType::Float64, true),
        ];

        for (name, values) in labels {
            fields.push(Field::new(&name, DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(values)));
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)?;

        Ok(batch)
    }

    /// Ingest metrics
    pub async fn ingest(&self, batch: RecordBatch) -> Result<()> {
        self.ingester.write(batch).await
    }
}

/// Metrics data point for OTLP conversion
#[derive(Debug, Clone)]
pub struct MetricDataPoint {
    pub timestamp_nanos: i64,
    pub metric_name: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

/// Convert a batch of data points to Arrow
pub fn data_points_to_arrow(points: Vec<MetricDataPoint>) -> Result<RecordBatch> {
    if points.is_empty() {
        return Err(crate::Error::InvalidSchema("No data points".into()));
    }

    // Collect all unique label keys
    let mut label_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    for point in &points {
        for key in point.labels.keys() {
            label_keys.insert(key.clone());
        }
    }

    let timestamps: Vec<i64> = points.iter().map(|p| p.timestamp_nanos).collect();
    let metric_names: Vec<String> = points.iter().map(|p| p.metric_name.clone()).collect();
    let values: Vec<f64> = points.iter().map(|p| p.value).collect();

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

    // Add label columns
    for key in label_keys {
        let label_values: Vec<Option<String>> = points
            .iter()
            .map(|p| p.labels.get(&key).cloned())
            .collect();

        fields.push(Field::new(&key, DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(label_values)));
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)?;

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_points_to_arrow() {
        let points = vec![
            MetricDataPoint {
                timestamp_nanos: 1000000000,
                metric_name: "cpu_usage".to_string(),
                value: 0.85,
                labels: [("host".to_string(), "server1".to_string())]
                    .into_iter()
                    .collect(),
            },
            MetricDataPoint {
                timestamp_nanos: 2000000000,
                metric_name: "cpu_usage".to_string(),
                value: 0.90,
                labels: [("host".to_string(), "server2".to_string())]
                    .into_iter()
                    .collect(),
            },
        ];

        let batch = data_points_to_arrow(points).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.schema().field_with_name("host").is_ok());
    }
}
