//! OpenTelemetry Protocol (OTLP) receiver
//!
//! Supports both gRPC and HTTP endpoints for OTLP metrics ingestion.

use crate::ingester::Ingester;
use crate::schema::{METRIC_NAME_FIELD, TIMESTAMP_FIELD, VALUE_F64_FIELD};
use crate::Result;

use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use base64::Engine;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{metric::Data, number_data_point, NumberDataPoint};
use std::collections::HashMap;
use std::sync::Arc;

/// OTLP metrics receiver
pub struct OtlpReceiver {
    ingester: Arc<Ingester>,
    _schema: Arc<Schema>,
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

        Self {
            ingester,
            _schema: schema,
        }
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

/// Convert OTLP export request payload into internal metric points.
pub fn export_request_to_data_points(
    request: &ExportMetricsServiceRequest,
) -> Vec<MetricDataPoint> {
    let mut out = Vec::new();

    for resource_metrics in &request.resource_metrics {
        let resource_labels = resource_metrics
            .resource
            .as_ref()
            .map(|r| key_values_to_labels(&r.attributes))
            .unwrap_or_default();

        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                match &metric.data {
                    Some(Data::Gauge(g)) => {
                        for point in &g.data_points {
                            out.push(number_point_to_metric_point(
                                &metric.name,
                                point,
                                &resource_labels,
                            ));
                        }
                    }
                    Some(Data::Sum(s)) => {
                        for point in &s.data_points {
                            out.push(number_point_to_metric_point(
                                &metric.name,
                                point,
                                &resource_labels,
                            ));
                        }
                    }
                    Some(Data::Histogram(h)) => {
                        for point in &h.data_points {
                            let value = point.sum.unwrap_or(point.count as f64);
                            out.push(MetricDataPoint {
                                timestamp_nanos: point.time_unix_nano as i64,
                                metric_name: metric.name.clone(),
                                value,
                                labels: merge_labels(
                                    &resource_labels,
                                    &key_values_to_labels(&point.attributes),
                                ),
                            });
                        }
                    }
                    Some(Data::ExponentialHistogram(h)) => {
                        for point in &h.data_points {
                            let value = point.sum.unwrap_or(point.count as f64);
                            out.push(MetricDataPoint {
                                timestamp_nanos: point.time_unix_nano as i64,
                                metric_name: metric.name.clone(),
                                value,
                                labels: merge_labels(
                                    &resource_labels,
                                    &key_values_to_labels(&point.attributes),
                                ),
                            });
                        }
                    }
                    Some(Data::Summary(s)) => {
                        for point in &s.data_points {
                            out.push(MetricDataPoint {
                                timestamp_nanos: point.time_unix_nano as i64,
                                metric_name: metric.name.clone(),
                                value: point.sum,
                                labels: merge_labels(
                                    &resource_labels,
                                    &key_values_to_labels(&point.attributes),
                                ),
                            });
                        }
                    }
                    None => {}
                }
            }
        }
    }

    out
}

/// Convert OTLP export request directly to Arrow RecordBatch.
pub fn export_request_to_arrow(request: &ExportMetricsServiceRequest) -> Result<RecordBatch> {
    let points = export_request_to_data_points(request);
    data_points_to_arrow(points)
}

fn number_point_to_metric_point(
    metric_name: &str,
    point: &NumberDataPoint,
    resource_labels: &HashMap<String, String>,
) -> MetricDataPoint {
    let value = match point.value {
        Some(number_data_point::Value::AsDouble(v)) => v,
        Some(number_data_point::Value::AsInt(v)) => v as f64,
        None => f64::NAN,
    };

    MetricDataPoint {
        timestamp_nanos: point.time_unix_nano as i64,
        metric_name: metric_name.to_string(),
        value,
        labels: merge_labels(resource_labels, &key_values_to_labels(&point.attributes)),
    }
}

fn key_values_to_labels(values: &[KeyValue]) -> HashMap<String, String> {
    values
        .iter()
        .map(|kv| {
            (
                kv.key.clone(),
                kv.value
                    .as_ref()
                    .and_then(any_value_to_string)
                    .unwrap_or_default(),
            )
        })
        .collect()
}

fn any_value_to_string(value: &AnyValue) -> Option<String> {
    match value.value.as_ref()? {
        any_value::Value::StringValue(v) => Some(v.clone()),
        any_value::Value::BoolValue(v) => Some(v.to_string()),
        any_value::Value::IntValue(v) => Some(v.to_string()),
        any_value::Value::DoubleValue(v) => Some(v.to_string()),
        any_value::Value::BytesValue(v) => {
            Some(base64::engine::general_purpose::STANDARD.encode(v))
        }
        any_value::Value::ArrayValue(v) => Some(format!("{:?}", v.values)),
        any_value::Value::KvlistValue(v) => Some(format!("{:?}", v.values)),
    }
}

fn merge_labels(
    a: &HashMap<String, String>,
    b: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = a.clone();
    for (k, v) in b {
        merged.insert(k.clone(), v.clone());
    }
    merged
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
        let label_values: Vec<Option<String>> =
            points.iter().map(|p| p.labels.get(&key).cloned()).collect();

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
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::{
        metric::Data, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics,
    };

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

    #[test]
    fn test_export_request_to_arrow_gauge_points() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "cpu_usage".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: vec![],
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "host".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(
                                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                                "server1".to_string(),
                                            ),
                                        ),
                                    }),
                                }],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_000_000_000,
                                exemplars: vec![],
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(0.42)),
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let batch = export_request_to_arrow(&request).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.schema().field_with_name("host").is_ok());
    }
}
