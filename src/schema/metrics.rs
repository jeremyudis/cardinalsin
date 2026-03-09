//! Metric schema definitions
//!
//! Defines the Arrow schema for metrics with tag-as-columns model.
//! Low-cardinality tags use dictionary encoding, high-cardinality tags
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

/// Tag cardinality classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagCardinality {
    /// Low cardinality (<1K unique values) - uses dictionary encoding + bloom filter
    Low,
    /// Medium cardinality (1K-100K unique values) - uses dictionary encoding
    Medium,
    /// High cardinality (>100K unique values) - plain string, no indexing
    High,
}

impl TagCardinality {
    /// Estimate cardinality from unique value count
    pub fn from_count(count: u64) -> Self {
        match count {
            0..=1_000 => TagCardinality::Low,
            1_001..=100_000 => TagCardinality::Medium,
            _ => TagCardinality::High,
        }
    }

    /// Returns the dictionary key type for this cardinality level
    pub fn dictionary_key_type(&self) -> Option<DataType> {
        match self {
            TagCardinality::Low => Some(DataType::UInt16),
            TagCardinality::Medium => Some(DataType::UInt32),
            TagCardinality::High => None, // No dictionary for high cardinality
        }
    }
}

/// Column definition for a tag
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    /// Column name (tag key)
    pub name: String,
    /// Expected cardinality
    pub cardinality: TagCardinality,
    /// Whether this column is nullable
    pub nullable: bool,
    /// Description of the column
    pub description: Option<String>,
}

impl ColumnDefinition {
    /// Create a new column definition
    pub fn new(name: impl Into<String>, cardinality: TagCardinality) -> Self {
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
            Some(key_type) => DataType::Dictionary(Box::new(key_type), Box::new(DataType::Utf8)),
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
/// This schema follows the "tags as columns" model where each tag key
/// becomes a column in the schema. This eliminates the need for per-tag-combination
/// indexing and allows for efficient columnar scans.
#[derive(Debug, Clone)]
pub struct MetricSchema {
    /// The Arrow schema
    schema: SchemaRef,
    /// Tag columns by name
    tag_columns: HashMap<String, ColumnDefinition>,
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

    /// Get all tag column names
    pub fn tag_columns(&self) -> impl Iterator<Item = &str> {
        self.tag_columns.keys().map(|s| s.as_str())
    }

    /// Get a tag column definition by name
    pub fn get_tag(&self, name: &str) -> Option<&ColumnDefinition> {
        self.tag_columns.get(name)
    }

    /// Check if a column exists
    pub fn has_column(&self, name: &str) -> bool {
        self.schema.field_with_name(name).is_ok()
    }

    /// Get the default schema for metrics
    pub fn default_metrics() -> Self {
        Self::builder()
            .with_tag(
                ColumnDefinition::new("host", TagCardinality::Medium)
                    .description("Host name or IP"),
            )
            .with_tag(
                ColumnDefinition::new("service", TagCardinality::Low).description("Service name"),
            )
            .with_tag(
                ColumnDefinition::new("env", TagCardinality::Low)
                    .description("Environment (prod, staging, dev)"),
            )
            .with_tag(
                ColumnDefinition::new("region", TagCardinality::Low).description("Cloud region"),
            )
            .with_tag(
                ColumnDefinition::new("instance", TagCardinality::Medium)
                    .description("Instance ID"),
            )
            .with_tag(
                ColumnDefinition::new("pod", TagCardinality::High).description("Kubernetes pod ID"),
            )
            .with_tag(
                ColumnDefinition::new("trace_id", TagCardinality::High)
                    .description("Distributed trace ID"),
            )
            .build()
    }
}

/// Builder for MetricSchema
#[derive(Debug)]
pub struct MetricSchemaBuilder {
    tags: Vec<ColumnDefinition>,
    multi_value: bool,
}

impl Default for MetricSchemaBuilder {
    fn default() -> Self {
        Self {
            tags: Vec::new(),
            multi_value: true, // Enable multi-value columns by default
        }
    }
}

impl MetricSchemaBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a tag column
    pub fn with_tag(mut self, column: ColumnDefinition) -> Self {
        self.tags.push(column);
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
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            false,
        ));

        // Tag columns
        let mut tag_columns = HashMap::new();
        for column in self.tags {
            fields.push(column.to_field());
            tag_columns.insert(column.name.clone(), column);
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
            tag_columns,
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

        // Check tag fields
        assert!(schema.has_column("host"));
        assert!(schema.has_column("service"));
        assert!(schema.has_column("pod"));
        assert!(schema.has_column("trace_id"));
    }

    #[test]
    fn test_cardinality_classification() {
        assert_eq!(TagCardinality::from_count(100), TagCardinality::Low);
        assert_eq!(TagCardinality::from_count(1000), TagCardinality::Low);
        assert_eq!(TagCardinality::from_count(1001), TagCardinality::Medium);
        assert_eq!(TagCardinality::from_count(50000), TagCardinality::Medium);
        assert_eq!(TagCardinality::from_count(100001), TagCardinality::High);
    }

    #[test]
    fn test_dictionary_encoding() {
        let low = ColumnDefinition::new("env", TagCardinality::Low);
        let field = low.to_field();
        assert!(matches!(field.data_type(), DataType::Dictionary(_, _)));

        let high = ColumnDefinition::new("trace_id", TagCardinality::High);
        let field = high.to_field();
        assert!(matches!(field.data_type(), DataType::Utf8));
    }

    #[test]
    fn test_custom_schema() {
        let schema = MetricSchema::builder()
            .with_tag(ColumnDefinition::new("custom_tag", TagCardinality::Medium))
            .multi_value(true)
            .build();

        assert!(schema.has_column("custom_tag"));
        assert!(schema.has_column(VALUE_I64_FIELD));
        assert!(schema.has_column(VALUE_U64_FIELD));
    }

    #[test]
    fn test_tag_columns_and_get_tag() {
        let schema = MetricSchema::default_metrics();
        let tag_names: Vec<&str> = schema.tag_columns().collect();
        assert!(tag_names.contains(&"host"));
        assert!(tag_names.contains(&"service"));

        let host_def = schema.get_tag("host").unwrap();
        assert_eq!(host_def.cardinality, TagCardinality::Medium);

        assert!(schema.get_tag("nonexistent").is_none());
    }
}
