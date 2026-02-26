//! Prometheus HTTP API compatibility layer
//!
//! Implements the Prometheus HTTP API for query compatibility with
//! existing tools like Grafana.

use crate::api::ApiState;

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const PROM_VALUE_EXPR: &str =
    "COALESCE(value_f64, CAST(value_i64 AS DOUBLE), CAST(value_u64 AS DOUBLE))";

/// Instant query parameters
#[derive(Debug, Deserialize)]
pub struct InstantQueryParams {
    pub query: String,
    #[serde(default)]
    pub time: Option<f64>,
}

/// Range query parameters
#[derive(Debug, Deserialize)]
pub struct RangeQueryParams {
    pub query: String,
    pub start: f64,
    pub end: f64,
    pub step: f64,
}

/// Prometheus response format
#[derive(Debug, Serialize)]
pub struct PrometheusResponse {
    pub status: String,
    pub data: PrometheusData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

/// Prometheus data payload
#[derive(Debug, Serialize)]
pub struct PrometheusData {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub result: Vec<PrometheusResult>,
}

/// Individual result in Prometheus format
#[derive(Debug, Serialize)]
pub struct PrometheusResult {
    pub metric: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<(f64, String)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<(f64, String)>>,
}

/// Labels response
#[derive(Debug, Serialize)]
pub struct LabelsResponse {
    pub status: String,
    pub data: Vec<String>,
}

/// Label values response
#[derive(Debug, Serialize)]
pub struct LabelValuesResponse {
    pub status: String,
    pub data: Vec<String>,
}

/// Instant query endpoint
///
/// GET/POST /api/v1/query
pub async fn instant_query(
    State(state): State<ApiState>,
    Query(params): Query<InstantQueryParams>,
) -> Json<PrometheusResponse> {
    // Transpile PromQL to SQL
    let sql = transpile_promql_instant(&params.query, params.time);

    // Execute query
    let results = match state.query_node.query(&sql).await {
        Ok(results) => results,
        Err(e) => {
            // Note: metrics table now always exists (registered at startup, issue #97).
            // This error handling is kept as defensive fallback for edge cases.
            let error_msg = e.to_string();
            if error_msg.contains("table") && error_msg.contains("not found") {
                return Json(PrometheusResponse {
                    status: "success".to_string(),
                    data: PrometheusData {
                        result_type: "vector".to_string(),
                        result: Vec::new(),
                    },
                    warnings: Some(vec![format!("No data available: {}", error_msg)]),
                });
            }
            return Json(PrometheusResponse {
                status: "error".to_string(),
                data: PrometheusData {
                    result_type: "vector".to_string(),
                    result: Vec::new(),
                },
                warnings: Some(vec![e.to_string()]),
            });
        }
    };

    // Convert to Prometheus format
    let prometheus_results = convert_to_prometheus_vector(&results);

    Json(PrometheusResponse {
        status: "success".to_string(),
        data: PrometheusData {
            result_type: "vector".to_string(),
            result: prometheus_results,
        },
        warnings: None,
    })
}

/// Range query endpoint
///
/// GET/POST /api/v1/query_range
pub async fn range_query(
    State(state): State<ApiState>,
    Query(params): Query<RangeQueryParams>,
) -> Json<PrometheusResponse> {
    // Transpile PromQL to SQL with time bucketing
    let sql = transpile_promql_range(&params.query, params.start, params.end, params.step);

    // Execute query
    let results = match state.query_node.query(&sql).await {
        Ok(results) => results,
        Err(e) => {
            // Note: metrics table now always exists (registered at startup, issue #97).
            // This error handling is kept as defensive fallback for edge cases.
            let error_msg = e.to_string();
            if error_msg.contains("table") && error_msg.contains("not found") {
                return Json(PrometheusResponse {
                    status: "success".to_string(),
                    data: PrometheusData {
                        result_type: "matrix".to_string(),
                        result: Vec::new(),
                    },
                    warnings: Some(vec![format!("No data available: {}", error_msg)]),
                });
            }
            return Json(PrometheusResponse {
                status: "error".to_string(),
                data: PrometheusData {
                    result_type: "matrix".to_string(),
                    result: Vec::new(),
                },
                warnings: Some(vec![e.to_string()]),
            });
        }
    };

    // Convert to Prometheus format
    let prometheus_results = convert_to_prometheus_matrix(&results);

    Json(PrometheusResponse {
        status: "success".to_string(),
        data: PrometheusData {
            result_type: "matrix".to_string(),
            result: prometheus_results,
        },
        warnings: None,
    })
}

/// Get all label names
///
/// GET /api/v1/labels
pub async fn labels(State(state): State<ApiState>) -> Json<LabelsResponse> {
    // Query for distinct label names
    let sql =
        "SELECT DISTINCT column_name FROM information_schema.columns WHERE table_name = 'metrics'";

    let results = match state.query_node.query(sql).await {
        Ok(results) => results,
        Err(_) => {
            return Json(LabelsResponse {
                status: "success".to_string(),
                data: Vec::new(),
            });
        }
    };

    // Extract label names from results
    let mut labels = Vec::new();
    for batch in &results {
        if let Some(col) = batch.column_by_name("column_name") {
            use arrow_array::cast::AsArray;
            use arrow_array::Array;
            let string_array = col.as_string::<i32>();
            for i in 0..string_array.len() {
                if !string_array.is_null(i) {
                    labels.push(string_array.value(i).to_string());
                }
            }
        }
    }

    Json(LabelsResponse {
        status: "success".to_string(),
        data: labels,
    })
}

/// Get values for a specific label
///
/// GET /api/v1/label/{name}/values
pub async fn label_values(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Json<LabelValuesResponse> {
    // Query for distinct values of the label
    let sql = format!("SELECT DISTINCT {} FROM metrics", name);

    let results = match state.query_node.query(&sql).await {
        Ok(results) => results,
        Err(_) => {
            return Json(LabelValuesResponse {
                status: "success".to_string(),
                data: Vec::new(),
            });
        }
    };

    // Extract values from results
    let mut values = Vec::new();
    for batch in &results {
        if batch.num_columns() > 0 {
            use arrow_array::cast::AsArray;
            use arrow_array::Array;
            let col = batch.column(0);
            let string_array = col.as_string::<i32>();
            for i in 0..string_array.len() {
                if !string_array.is_null(i) {
                    values.push(string_array.value(i).to_string());
                }
            }
        }
    }

    Json(LabelValuesResponse {
        status: "success".to_string(),
        data: values,
    })
}

/// Parsed PromQL query components
#[derive(Debug, Clone)]
struct ParsedPromQL {
    /// Metric name
    metric_name: String,
    /// Label matchers {label="value"}
    label_matchers: Vec<LabelMatcher>,
    /// Aggregation function (sum, avg, count, etc.)
    aggregation: Option<String>,
    /// Aggregation grouping (by/without)
    group_by: Vec<String>,
    /// Range duration in seconds (for rate, increase, etc.)
    range_seconds: Option<f64>,
    /// Function being applied (rate, increase, histogram_quantile, etc.)
    function: Option<String>,
}

#[derive(Debug, Clone)]
struct LabelMatcher {
    label: String,
    op: LabelMatchOp,
    value: String,
}

#[derive(Debug, Clone)]
enum LabelMatchOp {
    Eq,  // =
    Ne,  // !=
    Re,  // =~
    Nre, // !~
}

impl LabelMatchOp {
    fn to_sql(&self, label: &str, value: &str) -> String {
        match self {
            LabelMatchOp::Eq => format!("{} = '{}'", label, value.replace('\'', "''")),
            LabelMatchOp::Ne => format!("{} != '{}'", label, value.replace('\'', "''")),
            LabelMatchOp::Re => format!("{} ~ '{}'", label, value.replace('\'', "''")),
            LabelMatchOp::Nre => format!("NOT ({} ~ '{}')", label, value.replace('\'', "''")),
        }
    }
}

/// Parse a PromQL query into components
fn parse_promql(promql: &str) -> ParsedPromQL {
    let promql = promql.trim();
    let mut result = ParsedPromQL {
        metric_name: String::new(),
        label_matchers: Vec::new(),
        aggregation: None,
        group_by: Vec::new(),
        range_seconds: None,
        function: None,
    };

    // Check for aggregation functions: sum(...), avg(...), etc.
    let agg_re = regex::Regex::new(
        r"^(sum|avg|count|min|max|stddev|stdvar|topk|bottomk)\s*(?:by\s*\(([^)]+)\))?\s*\((.+)\)$",
    )
    .ok();
    if let Some(re) = &agg_re {
        if let Some(caps) = re.captures(promql) {
            result.aggregation = Some(caps.get(1).unwrap().as_str().to_string());
            if let Some(group) = caps.get(2) {
                result.group_by = group
                    .as_str()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
            }
            // Parse inner expression
            let inner = caps.get(3).unwrap().as_str();
            let inner_parsed = parse_promql(inner);
            result.metric_name = inner_parsed.metric_name;
            result.label_matchers = inner_parsed.label_matchers;
            result.range_seconds = inner_parsed.range_seconds;
            result.function = inner_parsed.function;
            return result;
        }
    }

    // Check for rate/increase functions: rate(metric[5m])
    let rate_re = regex::Regex::new(
        r"^(rate|increase|irate|delta|idelta|deriv)\s*\((.+)\[(\d+)([smhd])\]\)$",
    )
    .ok();
    if let Some(re) = &rate_re {
        if let Some(caps) = re.captures(promql) {
            result.function = Some(caps.get(1).unwrap().as_str().to_string());
            let inner = caps.get(2).unwrap().as_str();
            let duration_val: f64 = caps.get(3).unwrap().as_str().parse().unwrap_or(1.0);
            let duration_unit = caps.get(4).unwrap().as_str();
            result.range_seconds = Some(match duration_unit {
                "s" => duration_val,
                "m" => duration_val * 60.0,
                "h" => duration_val * 3600.0,
                "d" => duration_val * 86400.0,
                _ => duration_val,
            });
            // Parse inner metric selector
            let inner_parsed = parse_promql(inner);
            result.metric_name = inner_parsed.metric_name;
            result.label_matchers = inner_parsed.label_matchers;
            return result;
        }
    }

    // Parse metric selector: metric_name{label="value", ...}
    let selector_re = regex::Regex::new(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{([^}]*)\})?$").ok();
    if let Some(re) = &selector_re {
        if let Some(caps) = re.captures(promql) {
            result.metric_name = caps.get(1).unwrap().as_str().to_string();

            // Parse label matchers
            if let Some(labels_str) = caps.get(2) {
                result.label_matchers = parse_label_matchers(labels_str.as_str());
            }
        }
    }

    // Fallback: treat entire string as metric name
    if result.metric_name.is_empty() {
        result.metric_name = promql.to_string();
    }

    result
}

/// Parse label matchers from string: label="value", label2!="value2"
fn parse_label_matchers(s: &str) -> Vec<LabelMatcher> {
    let mut matchers = Vec::new();

    // Match patterns like: label="value" or label!="value" or label=~"regex"
    let matcher_re = regex::Regex::new(r#"(\w+)\s*(=~|!=|!~|=)\s*"([^"]*)""#).ok();
    if let Some(re) = matcher_re {
        for cap in re.captures_iter(s) {
            let label = cap.get(1).unwrap().as_str().to_string();
            let op_str = cap.get(2).unwrap().as_str();
            let value = cap.get(3).unwrap().as_str().to_string();

            let op = match op_str {
                "=" => LabelMatchOp::Eq,
                "!=" => LabelMatchOp::Ne,
                "=~" => LabelMatchOp::Re,
                "!~" => LabelMatchOp::Nre,
                _ => LabelMatchOp::Eq,
            };

            matchers.push(LabelMatcher { label, op, value });
        }
    }

    matchers
}

/// Transpile PromQL instant query to SQL
fn transpile_promql_instant(promql: &str, time: Option<f64>) -> String {
    let parsed = parse_promql(promql);

    let time_clause = time
        .map(|t| format!("AND timestamp <= {}", (t * 1e9) as i64))
        .unwrap_or_default();

    // Build WHERE clause
    let mut where_clauses = vec![format!(
        "metric_name = '{}'",
        parsed.metric_name.replace('\'', "''")
    )];
    for matcher in &parsed.label_matchers {
        where_clauses.push(matcher.op.to_sql(&matcher.label, &matcher.value));
    }
    if !time_clause.is_empty() {
        where_clauses.push(time_clause.trim_start_matches("AND ").to_string());
    }

    let where_sql = where_clauses.join(" AND ");

    // Handle aggregations
    if let Some(agg) = &parsed.aggregation {
        let sql_agg = match agg.as_str() {
            "sum" => "SUM(PROM_VALUE_EXPR)",
            "avg" => "AVG(PROM_VALUE_EXPR)",
            "count" => "COUNT(*)",
            "min" => "MIN(PROM_VALUE_EXPR)",
            "max" => "MAX(PROM_VALUE_EXPR)",
            "stddev" => "STDDEV(PROM_VALUE_EXPR)",
            _ => "AVG(PROM_VALUE_EXPR)",
        };
        let sql_agg = sql_agg.replace("PROM_VALUE_EXPR", PROM_VALUE_EXPR);

        let group_clause = if parsed.group_by.is_empty() {
            "metric_name".to_string()
        } else {
            format!("metric_name, {}", parsed.group_by.join(", "))
        };

        return format!(
            "SELECT {}, {} as value FROM metrics WHERE {} GROUP BY {} ORDER BY value DESC",
            group_clause, sql_agg, where_sql, group_clause
        );
    }

    format!(
        "SELECT * FROM metrics WHERE {} ORDER BY timestamp DESC LIMIT 1",
        where_sql
    )
}

/// Transpile PromQL range query to SQL
fn transpile_promql_range(promql: &str, start: f64, end: f64, step: f64) -> String {
    let parsed = parse_promql(promql);
    let start_ns = (start * 1e9) as i64;
    let end_ns = (end * 1e9) as i64;
    let step_ns = (step * 1e9) as i64;

    // Build WHERE clause
    let mut where_clauses = vec![
        format!("metric_name = '{}'", parsed.metric_name.replace('\'', "''")),
        format!("timestamp >= {}", start_ns),
        format!("timestamp <= {}", end_ns),
    ];
    for matcher in &parsed.label_matchers {
        where_clauses.push(matcher.op.to_sql(&matcher.label, &matcher.value));
    }
    let where_sql = where_clauses.join(" AND ");

    // Handle rate/increase functions
    if let Some(func) = &parsed.function {
        let _range_ns = parsed.range_seconds.unwrap_or(step) as i64 * 1_000_000_000;

        return match func.as_str() {
            "rate" | "irate" => format!(
                "SELECT
                    (timestamp / {step}) * {step} as time_bucket,
                    metric_name,
                    (MAX({value_expr}) - MIN({value_expr})) / ({range_secs}) as value
                FROM metrics
                WHERE {where_sql}
                GROUP BY time_bucket, metric_name
                ORDER BY time_bucket",
                step = step_ns,
                value_expr = PROM_VALUE_EXPR,
                range_secs = parsed.range_seconds.unwrap_or(step),
                where_sql = where_sql
            ),
            "increase" | "delta" => format!(
                "SELECT
                    (timestamp / {step}) * {step} as time_bucket,
                    metric_name,
                    MAX({value_expr}) - MIN({value_expr}) as value
                FROM metrics
                WHERE {where_sql}
                GROUP BY time_bucket, metric_name
                ORDER BY time_bucket",
                step = step_ns,
                value_expr = PROM_VALUE_EXPR,
                where_sql = where_sql
            ),
            _ => format!(
                "SELECT
                    (timestamp / {step}) * {step} as time_bucket,
                    metric_name,
                    AVG({value_expr}) as value
                FROM metrics
                WHERE {where_sql}
                GROUP BY time_bucket, metric_name
                ORDER BY time_bucket",
                step = step_ns,
                value_expr = PROM_VALUE_EXPR,
                where_sql = where_sql
            ),
        };
    }

    // Handle aggregations
    if let Some(agg) = &parsed.aggregation {
        let sql_agg = match agg.as_str() {
            "sum" => "SUM(PROM_VALUE_EXPR)",
            "avg" => "AVG(PROM_VALUE_EXPR)",
            "count" => "COUNT(*)",
            "min" => "MIN(PROM_VALUE_EXPR)",
            "max" => "MAX(PROM_VALUE_EXPR)",
            _ => "AVG(PROM_VALUE_EXPR)",
        };
        let sql_agg = sql_agg.replace("PROM_VALUE_EXPR", PROM_VALUE_EXPR);

        let group_clause = if parsed.group_by.is_empty() {
            "time_bucket, metric_name".to_string()
        } else {
            format!("time_bucket, metric_name, {}", parsed.group_by.join(", "))
        };

        return format!(
            "SELECT
                (timestamp / {step}) * {step} as time_bucket,
                metric_name,
                {agg} as value
            FROM metrics
            WHERE {where_sql}
            GROUP BY {group_clause}
            ORDER BY time_bucket",
            step = step_ns,
            agg = sql_agg,
            where_sql = where_sql,
            group_clause = group_clause
        );
    }

    // Default: simple average
    format!(
        "SELECT
            (timestamp / {step}) * {step} as time_bucket,
            metric_name,
            AVG({value_expr}) as value
        FROM metrics
        WHERE {where_sql}
        GROUP BY time_bucket, metric_name
        ORDER BY time_bucket",
        step = step_ns,
        value_expr = PROM_VALUE_EXPR,
        where_sql = where_sql
    )
}

fn extract_prometheus_value(batch: &arrow_array::RecordBatch, row: usize) -> String {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type, UInt64Type};

    if let Some(col) = batch.column_by_name("value") {
        let values = col.as_primitive::<Float64Type>();
        if !values.is_null(row) {
            return values.value(row).to_string();
        }
    }

    if let Some(col) = batch.column_by_name("value_f64") {
        let values = col.as_primitive::<Float64Type>();
        if !values.is_null(row) {
            return values.value(row).to_string();
        }
    }

    if let Some(col) = batch.column_by_name("value_i64") {
        let values = col.as_primitive::<Int64Type>();
        if !values.is_null(row) {
            return values.value(row).to_string();
        }
    }

    if let Some(col) = batch.column_by_name("value_u64") {
        let values = col.as_primitive::<UInt64Type>();
        if !values.is_null(row) {
            return values.value(row).to_string();
        }
    }

    "0".to_string()
}

/// Convert Arrow results to Prometheus vector format
fn convert_to_prometheus_vector(batches: &[arrow_array::RecordBatch]) -> Vec<PrometheusResult> {
    let mut results = Vec::new();

    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut metric = HashMap::new();

            // Extract metric name
            if let Some(col) = batch.column_by_name("metric_name") {
                use arrow_array::cast::AsArray;
                let name = col.as_string::<i32>().value(row);
                metric.insert("__name__".to_string(), name.to_string());
            }

            // Extract timestamp and value
            let timestamp = batch
                .column_by_name("timestamp")
                .map(|c| {
                    use arrow_array::cast::AsArray;
                    use arrow_array::types::TimestampNanosecondType;
                    c.as_primitive::<TimestampNanosecondType>().value(row) as f64 / 1e9
                })
                .unwrap_or(0.0);

            let value = extract_prometheus_value(batch, row);

            results.push(PrometheusResult {
                metric,
                value: Some((timestamp, value)),
                values: None,
            });
        }
    }

    results
}

/// Convert Arrow results to Prometheus matrix format
fn convert_to_prometheus_matrix(batches: &[arrow_array::RecordBatch]) -> Vec<PrometheusResult> {
    type SeriesSamples = Vec<(f64, String)>;
    type SeriesEntry = (HashMap<String, String>, SeriesSamples);

    let mut series_map: HashMap<String, SeriesEntry> = HashMap::new();

    for batch in batches {
        for row in 0..batch.num_rows() {
            // Get metric name as series key
            let series_key = batch
                .column_by_name("metric_name")
                .map(|c| {
                    use arrow_array::cast::AsArray;
                    c.as_string::<i32>().value(row).to_string()
                })
                .unwrap_or_default();

            let entry = series_map.entry(series_key.clone()).or_insert_with(|| {
                let mut metric = HashMap::new();
                metric.insert("__name__".to_string(), series_key);
                (metric, Vec::new())
            });

            // Extract timestamp and value
            let timestamp = batch
                .column_by_name("time_bucket")
                .map(|c| {
                    use arrow_array::cast::AsArray;
                    use arrow_array::types::Int64Type;
                    c.as_primitive::<Int64Type>().value(row) as f64 / 1e9
                })
                .unwrap_or(0.0);

            let value = extract_prometheus_value(batch, row);

            entry.1.push((timestamp, value));
        }
    }

    series_map
        .into_values()
        .map(|(metric, values)| PrometheusResult {
            metric,
            value: None,
            values: Some(values),
        })
        .collect()
}
