//! Metric schema definitions
//!
//! Defines the Arrow schema for metrics with label-as-columns model.
//! Low-cardinality labels use dictionary encoding, high-cardinality labels
//! are stored as plain strings without indexing.

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;

/// Standard field names
pub const TIMESTAMP_FIELD: &str = "timestamp";
pub const METRIC_NAME_FIELD: &str = "metric_name";
pub const VALUE_F64_FIELD: &str = "value_f64";
pub const VALUE_I64_FIELD: &str = "value_i64";
pub const VALUE_U64_FIELD: &str = "value_u64";

/// Metric types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// Point-in-time value (e.g., CPU usage, memory)
    Gauge,
    /// Monotonically increasing counter
    Counter,
    /// Distribution of values (histogram buckets)
    Histogram,
    /// Pre-computed quantiles
    Summary,
}

impl MetricType {
    /// Returns the primary value field for this metric type
    pub fn primary_value_field(&self) -> &'static str {
        match self {
            MetricType::Gauge => VALUE_F64_FIELD,
            MetricType::Counter => VALUE_U64_FIELD,
            MetricType::Histogram => VALUE_F64_FIELD,
            MetricType::Summary => VALUE_F64_FIELD,
        }
    }
}

/// Label cardinality classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelCardinality {
    /// Low cardinality (<1K unique values) - uses dictionary encoding + bloom filter
    Low,
    /// Medium cardinality (1K-100K unique values) - uses dictionary encoding
    Medium,
    /// High cardinality (>100K unique values) - plain string, no indexing
    High,
}

impl LabelCardinality {
    /// Estimate cardinality from unique value count
    pub fn from_count(count: u64) -> Self {
        match count {
            0..=1_000 => LabelCardinality::Low,
            1_001..=100_000 => LabelCardinality::Medium,
            _ => LabelCardinality::High,
        }
    }

    /// Returns the dictionary key type for this cardinality level
    pub fn dictionary_key_type(&self) -> Option<DataType> {
        match self {
            LabelCardinality::Low => Some(DataType::UInt16),
            LabelCardinality::Medium => Some(DataType::UInt32),
            LabelCardinality::High => None, // No dictionary for high cardinality
        }
    }
}

/// Column definition for a label
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    /// Column name (label key)
    pub name: String,
    /// Expected cardinality
    pub cardinality: LabelCardinality,
    /// Whether this column is nullable
    pub nullable: bool,
    /// Description of the column
    pub description: Option<String>,
}

impl ColumnDefinition {
    /// Create a new column definition
    pub fn new(name: impl Into<String>, cardinality: LabelCardinality) -> Self {
        Self {
            name: name.into(),
            cardinality,
            nullable: true,
            description: None,
        }
    }

    /// Set nullable
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set description
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Convert to Arrow field
    pub fn to_field(&self) -> Field {
        let data_type = match self.cardinality.dictionary_key_type() {
            Some(key_type) => DataType::Dictionary(
                Box::new(key_type),
                Box::new(DataType::Utf8),
            ),
            None => DataType::Utf8,
        };

        let mut field = Field::new(&self.name, data_type, self.nullable);
        if let Some(ref desc) = self.description {
            let mut metadata = HashMap::new();
            metadata.insert("description".to_string(), desc.clone());
            field = field.with_metadata(metadata);
        }
        field
    }
}

/// Schema for metric data
///
/// This schema follows the "labels as columns" model where each label key
/// becomes a column in the schema. This eliminates the need for per-tag-combination
/// indexing and allows for efficient columnar scans.
#[derive(Debug, Clone)]
pub struct MetricSchema {
    /// The Arrow schema
    schema: SchemaRef,
    /// Label columns by name
    label_columns: HashMap<String, ColumnDefinition>,
    /// Whether to include all value types
    _multi_value: bool,
}

impl MetricSchema {
    /// Create a new schema builder
    pub fn builder() -> MetricSchemaBuilder {
        MetricSchemaBuilder::new()
    }

    /// Get the Arrow schema
    pub fn arrow_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get all label column names
    pub fn label_columns(&self) -> impl Iterator<Item = &str> {
        self.label_columns.keys().map(|s| s.as_str())
    }

    /// Get a label column definition by name
    pub fn get_label(&self, name: &str) -> Option<&ColumnDefinition> {
        self.label_columns.get(name)
    }

    /// Check if a column exists
    pub fn has_column(&self, name: &str) -> bool {
        self.schema.field_with_name(name).is_ok()
    }

    /// Get the default schema for metrics
    pub fn default_metrics() -> Self {
        Self::builder()
            .with_label(ColumnDefinition::new("host", LabelCardinality::Medium)
                .description("Host name or IP"))
            .with_label(ColumnDefinition::new("service", LabelCardinality::Low)
                .description("Service name"))
            .with_label(ColumnDefinition::new("env", LabelCardinality::Low)
                .description("Environment (prod, staging, dev)"))
            .with_label(ColumnDefinition::new("region", LabelCardinality::Low)
                .description("Cloud region"))
            .with_label(ColumnDefinition::new("instance", LabelCardinality::Medium)
                .description("Instance ID"))
            .with_label(ColumnDefinition::new("pod", LabelCardinality::High)
                .description("Kubernetes pod ID"))
            .with_label(ColumnDefinition::new("trace_id", LabelCardinality::High)
                .description("Distributed trace ID"))
            .build()
    }
}

/// Builder for MetricSchema
#[derive(Debug, Default)]
pub struct MetricSchemaBuilder {
    labels: Vec<ColumnDefinition>,
    multi_value: bool,
}

impl MetricSchemaBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a label column
    pub fn with_label(mut self, column: ColumnDefinition) -> Self {
        self.labels.push(column);
        self
    }

    /// Enable multiple value types (f64, i64, u64)
    pub fn multi_value(mut self, enabled: bool) -> Self {
        self.multi_value = enabled;
        self
    }

    /// Build the schema
    pub fn build(self) -> MetricSchema {
        let mut fields = Vec::new();

        // Timestamp field (nanosecond precision)
        fields.push(Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ));

        // Metric name field (dictionary encoded, low cardinality)
        fields.push(Field::new(
            METRIC_NAME_FIELD,
            DataType::Dictionary(
                Box::new(DataType::UInt16),
                Box::new(DataType::Utf8),
            ),
            false,
        ));

        // Label columns
        let mut label_columns = HashMap::new();
        for column in self.labels {
            fields.push(column.to_field());
            label_columns.insert(column.name.clone(), column);
        }

        // Value fields
        fields.push(Field::new(VALUE_F64_FIELD, DataType::Float64, true));

        if self.multi_value {
            fields.push(Field::new(VALUE_I64_FIELD, DataType::Int64, true));
            fields.push(Field::new(VALUE_U64_FIELD, DataType::UInt64, true));
        }

        let schema = Arc::new(Schema::new(fields));

        MetricSchema {
            schema,
            label_columns,
            _multi_value: self.multi_value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_schema() {
        let schema = MetricSchema::default_metrics();
        let arrow_schema = schema.arrow_schema();

        // Check required fields exist
        assert!(arrow_schema.field_with_name(TIMESTAMP_FIELD).is_ok());
        assert!(arrow_schema.field_with_name(METRIC_NAME_FIELD).is_ok());
        assert!(arrow_schema.field_with_name(VALUE_F64_FIELD).is_ok());

        // Check label fields
        assert!(schema.has_column("host"));
        assert!(schema.has_column("service"));
        assert!(schema.has_column("pod"));
        assert!(schema.has_column("trace_id"));
    }

    #[test]
    fn test_cardinality_classification() {
        assert_eq!(LabelCardinality::from_count(100), LabelCardinality::Low);
        assert_eq!(LabelCardinality::from_count(1000), LabelCardinality::Low);
        assert_eq!(LabelCardinality::from_count(1001), LabelCardinality::Medium);
        assert_eq!(LabelCardinality::from_count(50000), LabelCardinality::Medium);
        assert_eq!(LabelCardinality::from_count(100001), LabelCardinality::High);
    }

    #[test]
    fn test_dictionary_encoding() {
        let low = ColumnDefinition::new("env", LabelCardinality::Low);
        let field = low.to_field();
        assert!(matches!(field.data_type(), DataType::Dictionary(_, _)));

        let high = ColumnDefinition::new("trace_id", LabelCardinality::High);
        let field = high.to_field();
        assert!(matches!(field.data_type(), DataType::Utf8));
    }

    #[test]
    fn test_custom_schema() {
        let schema = MetricSchema::builder()
            .with_label(ColumnDefinition::new("custom_label", LabelCardinality::Medium))
            .multi_value(true)
            .build();

        assert!(schema.has_column("custom_label"));
        assert!(schema.has_column(VALUE_I64_FIELD));
        assert!(schema.has_column(VALUE_U64_FIELD));
    }
}
