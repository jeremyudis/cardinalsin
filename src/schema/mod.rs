//! Schema definitions for CardinalSin metrics
//!
//! This module defines the Arrow schema for storing time-series metrics in a
//! columnar format. The key insight is that labels are stored as columns, not
//! as tag sets, which eliminates the cardinality explosion problem.

mod metrics;

pub use metrics::{
    MetricSchema,
    MetricSchemaBuilder,
    MetricType,
    LabelCardinality,
    ColumnDefinition,
    TIMESTAMP_FIELD,
    METRIC_NAME_FIELD,
    VALUE_F64_FIELD,
    VALUE_I64_FIELD,
    VALUE_U64_FIELD,
};
