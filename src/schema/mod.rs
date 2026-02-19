//! Schema definitions for CardinalSin metrics
//!
//! This module defines the Arrow schema for storing time-series metrics in a
//! columnar format. The key insight is that labels are stored as columns, not
//! as tag sets, which eliminates the cardinality explosion problem.

mod metrics;

pub use metrics::{
    ColumnDefinition, LabelCardinality, MetricSchema, MetricSchemaBuilder, MetricType,
    METRIC_NAME_FIELD, TIMESTAMP_FIELD, VALUE_F64_FIELD, VALUE_I64_FIELD, VALUE_U64_FIELD,
};
