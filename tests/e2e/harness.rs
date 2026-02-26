//! E2E Test Harness for CardinalSin
//!
//! Provides utilities for orchestrating end-to-end tests against
//! a running CardinalSin deployment (typically via docker-compose).

#![allow(dead_code)]

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// E2E test harness for interacting with deployed CardinalSin services
#[derive(Clone)]
pub struct E2EHarness {
    /// HTTP client for API requests
    pub http_client: Client,
    /// Ingester service URL (typically http://localhost:8081)
    pub ingester_url: String,
    /// Query node service URL (typically http://localhost:8080)
    pub query_url: String,
}

impl Default for E2EHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl E2EHarness {
    /// Create a new harness pointing to default local endpoints
    pub fn new() -> Self {
        Self::with_urls(
            "http://localhost:8081".to_string(),
            "http://localhost:8080".to_string(),
        )
    }

    /// Create a harness with custom URLs
    pub fn with_urls(ingester_url: String, query_url: String) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            http_client,
            ingester_url,
            query_url,
        }
    }

    /// Create harness from environment variables
    pub fn from_env() -> Self {
        let ingester_url = std::env::var("CARDINALSIN_INGESTER_URL")
            .unwrap_or_else(|_| "http://localhost:8081".to_string());
        let query_url = std::env::var("CARDINALSIN_QUERY_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());

        Self::with_urls(ingester_url, query_url)
    }

    /// Check if a service endpoint is healthy
    async fn check_health(&self, url: &str) -> bool {
        self.http_client
            .get(format!("{}/health", url))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    /// Check if a service endpoint is ready
    async fn check_ready(&self, url: &str) -> Option<String> {
        match self.http_client.get(format!("{}/ready", url)).send().await {
            Ok(resp) if resp.status().is_success() => resp.text().await.ok(),
            _ => None,
        }
    }

    /// Wait for all services to be healthy
    ///
    /// Polls health endpoints until all return success or timeout expires.
    pub async fn wait_healthy(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            let ingester_ok = self.check_health(&self.ingester_url).await;
            let query_ok = self.check_health(&self.query_url).await;

            if ingester_ok && query_ok {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Err(anyhow!("Services not healthy within timeout"))
    }

    /// Wait for ingester to be healthy
    pub async fn wait_ingester_healthy(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            if self.check_health(&self.ingester_url).await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Err(anyhow!("Ingester not healthy within timeout"))
    }

    /// Wait for query node to be healthy
    pub async fn wait_query_healthy(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            if self.check_health(&self.query_url).await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Err(anyhow!("Query node not healthy within timeout"))
    }

    /// Write samples via Prometheus Remote Write protocol
    pub async fn write_samples(&self, samples: Vec<Sample>) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        // Convert samples to Prometheus Remote Write format
        let write_request = self.samples_to_write_request(&samples);
        let proto_bytes = self.encode_write_request(&write_request)?;

        // Compress with snappy
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&proto_bytes)
            .map_err(|e| anyhow!("Snappy compression failed: {}", e))?;

        // Send to ingester
        let resp = self
            .http_client
            .post(format!("{}/api/v1/write", self.ingester_url))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .body(compressed)
            .send()
            .await?;

        if resp.status().is_success() || resp.status().as_u16() == 204 {
            Ok(())
        } else {
            Err(anyhow!(
                "Failed to write samples: {} - {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ))
        }
    }

    /// Execute a SQL query and return results
    pub async fn query_sql(&self, sql: &str) -> Result<QueryResult> {
        let request = SqlRequest {
            query: sql.to_string(),
            format: Some("json".to_string()),
        };

        let resp = self
            .http_client
            .post(format!("{}/api/v1/sql", self.query_url))
            .json(&request)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Query failed: {} - {}", status, text));
        }

        let sql_response: SqlResponse = resp.json().await?;

        Ok(QueryResult {
            columns: sql_response.columns,
            rows: sql_response.data,
            stats: sql_response.stats,
        })
    }

    /// Query via Prometheus instant query API
    pub async fn query_prom(&self, query: &str) -> Result<PromResult> {
        let resp = self
            .http_client
            .get(format!("{}/api/v1/query", self.query_url))
            .query(&[("query", query)])
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Prometheus query failed: {} - {}", status, text));
        }

        let prom_response: PrometheusResponse = resp.json().await?;

        Ok(PromResult {
            status: prom_response.status,
            data: prom_response.data.result,
        })
    }

    /// Query via Prometheus range query API
    pub async fn query_prom_range(
        &self,
        query: &str,
        start: f64,
        end: f64,
        step: f64,
    ) -> Result<PromResult> {
        let resp = self
            .http_client
            .get(format!("{}/api/v1/query_range", self.query_url))
            .query(&[
                ("query", query),
                ("start", &start.to_string()),
                ("end", &end.to_string()),
                ("step", &step.to_string()),
            ])
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Prometheus range query failed: {} - {}",
                status,
                text
            ));
        }

        let prom_response: PrometheusResponse = resp.json().await?;

        Ok(PromResult {
            status: prom_response.status,
            data: prom_response.data.result,
        })
    }

    /// Query via Prometheus instant query API (POST with form body)
    pub async fn query_prom_post(&self, query: &str) -> Result<PromResult> {
        let resp = self
            .http_client
            .post(format!("{}/api/v1/query", self.query_url))
            .form(&[("query", query)])
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Prometheus POST query failed: {} - {}",
                status,
                text
            ));
        }

        let prom_response: PrometheusResponse = resp.json().await?;

        Ok(PromResult {
            status: prom_response.status,
            data: prom_response.data.result,
        })
    }

    /// Query via Prometheus range query API (POST with form body)
    pub async fn query_prom_range_post(
        &self,
        query: &str,
        start: f64,
        end: f64,
        step: f64,
    ) -> Result<PromResult> {
        let resp = self
            .http_client
            .post(format!("{}/api/v1/query_range", self.query_url))
            .form(&[
                ("query", query),
                ("start", &start.to_string()),
                ("end", &end.to_string()),
                ("step", &step.to_string()),
            ])
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Prometheus POST range query failed: {} - {}",
                status,
                text
            ));
        }

        let prom_response: PrometheusResponse = resp.json().await?;

        Ok(PromResult {
            status: prom_response.status,
            data: prom_response.data.result,
        })
    }

    /// Get all label names via Prometheus API
    pub async fn get_labels(&self) -> Result<Vec<String>> {
        let resp = self
            .http_client
            .get(format!("{}/api/v1/labels", self.query_url))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Labels query failed: {} - {}", status, text));
        }

        let labels_response: LabelsResponse = resp.json().await?;
        Ok(labels_response.data)
    }

    /// Get label values via Prometheus API
    pub async fn get_label_values(&self, label_name: &str) -> Result<Vec<String>> {
        let resp = self
            .http_client
            .get(format!(
                "{}/api/v1/label/{}/values",
                self.query_url, label_name
            ))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Label values query failed: {} - {}", status, text));
        }

        let values_response: LabelValuesResponse = resp.json().await?;
        Ok(values_response.data)
    }

    /// Assert that a SQL query returns the expected row count
    pub async fn assert_row_count(&self, sql: &str, expected: usize) -> Result<()> {
        let result = self.query_sql(sql).await?;
        if result.rows.len() != expected {
            return Err(anyhow!(
                "Expected {} rows, got {}. Query: {}",
                expected,
                result.rows.len(),
                sql
            ));
        }
        Ok(())
    }

    /// Assert that a SQL query returns at least the expected row count
    pub async fn assert_min_row_count(&self, sql: &str, min_expected: usize) -> Result<()> {
        let result = self.query_sql(sql).await?;
        if result.rows.len() < min_expected {
            return Err(anyhow!(
                "Expected at least {} rows, got {}. Query: {}",
                min_expected,
                result.rows.len(),
                sql
            ));
        }
        Ok(())
    }

    /// Convert samples to a simplified write request
    fn samples_to_write_request(&self, samples: &[Sample]) -> WriteRequest {
        // Group samples by metric name + labels
        let mut series_map: HashMap<String, Vec<&Sample>> = HashMap::new();

        for sample in samples {
            let mut key_parts: Vec<String> = vec![sample.metric_name.clone()];
            let mut label_items: Vec<_> = sample.labels.iter().collect();
            label_items.sort_by_key(|(k, _)| *k);
            for (k, v) in label_items {
                key_parts.push(format!("{}={}", k, v));
            }
            let key = key_parts.join("|");
            series_map.entry(key).or_default().push(sample);
        }

        // Convert to timeseries
        let timeseries: Vec<TimeSeries> = series_map
            .into_values()
            .map(|samples| {
                let first = samples[0];
                let mut labels = vec![Label {
                    name: "__name__".to_string(),
                    value: first.metric_name.clone(),
                }];
                for (k, v) in &first.labels {
                    labels.push(Label {
                        name: k.clone(),
                        value: v.clone(),
                    });
                }

                let proto_samples: Vec<ProtoSample> = samples
                    .iter()
                    .map(|s| ProtoSample {
                        timestamp_ms: s.timestamp_ns / 1_000_000,
                        value: s.value,
                    })
                    .collect();

                TimeSeries {
                    labels,
                    samples: proto_samples,
                }
            })
            .collect();

        WriteRequest { timeseries }
    }

    /// Encode write request to protobuf bytes
    ///
    /// Implements manual protobuf encoding matching the Prometheus Remote Write format.
    fn encode_write_request(&self, req: &WriteRequest) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        for ts in &req.timeseries {
            // Encode TimeSeries as field 1 (length-delimited)
            let ts_bytes = self.encode_timeseries(ts);
            // Field 1, wire type 2 (length-delimited) = (1 << 3) | 2 = 10
            buf.push(10);
            self.write_varint(&mut buf, ts_bytes.len() as u64);
            buf.extend(ts_bytes);
        }

        Ok(buf)
    }

    /// Encode a TimeSeries to protobuf bytes
    fn encode_timeseries(&self, ts: &TimeSeries) -> Vec<u8> {
        let mut buf = Vec::new();

        // Field 1: labels (repeated, length-delimited)
        for label in &ts.labels {
            let label_bytes = self.encode_label(label);
            // Field 1, wire type 2 = (1 << 3) | 2 = 10
            buf.push(10);
            self.write_varint(&mut buf, label_bytes.len() as u64);
            buf.extend(label_bytes);
        }

        // Field 2: samples (repeated, length-delimited)
        for sample in &ts.samples {
            let sample_bytes = self.encode_sample(sample);
            // Field 2, wire type 2 = (2 << 3) | 2 = 18
            buf.push(18);
            self.write_varint(&mut buf, sample_bytes.len() as u64);
            buf.extend(sample_bytes);
        }

        buf
    }

    /// Encode a Label to protobuf bytes
    fn encode_label(&self, label: &Label) -> Vec<u8> {
        let mut buf = Vec::new();

        // Field 1: name (string)
        if !label.name.is_empty() {
            // Field 1, wire type 2 = (1 << 3) | 2 = 10
            buf.push(10);
            self.write_varint(&mut buf, label.name.len() as u64);
            buf.extend(label.name.as_bytes());
        }

        // Field 2: value (string)
        if !label.value.is_empty() {
            // Field 2, wire type 2 = (2 << 3) | 2 = 18
            buf.push(18);
            self.write_varint(&mut buf, label.value.len() as u64);
            buf.extend(label.value.as_bytes());
        }

        buf
    }

    /// Encode a Sample to protobuf bytes
    fn encode_sample(&self, sample: &ProtoSample) -> Vec<u8> {
        let mut buf = Vec::new();

        // Field 1: value (double, 64-bit fixed)
        // Field 1, wire type 1 = (1 << 3) | 1 = 9
        buf.push(9);
        buf.extend(sample.value.to_le_bytes());

        // Field 2: timestamp (int64, varint)
        // Field 2, wire type 0 = (2 << 3) | 0 = 16
        buf.push(16);
        self.write_varint(&mut buf, sample.timestamp_ms as u64);

        buf
    }

    /// Write a varint to the buffer
    fn write_varint(&self, buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if value == 0 {
                break;
            }
        }
    }
}

/// A metric sample for ingestion
#[derive(Debug, Clone)]
pub struct Sample {
    /// Timestamp in nanoseconds since epoch
    pub timestamp_ns: i64,
    /// Metric name
    pub metric_name: String,
    /// Metric value
    pub value: f64,
    /// Labels (key-value pairs)
    pub labels: HashMap<String, String>,
}

impl Sample {
    /// Create a new sample with the current timestamp
    pub fn now(metric_name: &str, value: f64) -> Self {
        Self {
            timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            metric_name: metric_name.to_string(),
            value,
            labels: HashMap::new(),
        }
    }

    /// Add a label to the sample
    pub fn with_label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.to_string(), value.to_string());
        self
    }

    /// Set the timestamp
    pub fn with_timestamp(mut self, timestamp_ns: i64) -> Self {
        self.timestamp_ns = timestamp_ns;
        self
    }
}

/// Query result from SQL API
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub stats: QueryStats,
}

/// Query execution statistics
#[derive(Debug, Deserialize)]
pub struct QueryStats {
    pub rows_read: u64,
    pub bytes_read: u64,
    pub execution_time_ms: u64,
}

/// Prometheus query result
#[derive(Debug)]
pub struct PromResult {
    pub status: String,
    pub data: Vec<PrometheusResultItem>,
}

// Internal types for serialization

#[derive(Debug, Serialize)]
struct SqlRequest {
    query: String,
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SqlResponse {
    columns: Vec<String>,
    data: Vec<Vec<serde_json::Value>>,
    stats: QueryStats,
}

#[derive(Debug, Deserialize)]
struct PrometheusResponse {
    status: String,
    data: PrometheusData,
}

#[derive(Debug, Deserialize)]
struct PrometheusData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PrometheusResultItem>,
}

/// Individual result item from Prometheus API
#[derive(Debug, Deserialize)]
pub struct PrometheusResultItem {
    pub metric: HashMap<String, String>,
    pub value: Option<(f64, String)>,
    pub values: Option<Vec<(f64, String)>>,
}

#[derive(Debug, Deserialize)]
struct LabelsResponse {
    status: String,
    data: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct LabelValuesResponse {
    status: String,
    data: Vec<String>,
}

// Write request types (for Prometheus Remote Write)

#[derive(Debug)]
struct WriteRequest {
    timeseries: Vec<TimeSeries>,
}

#[derive(Debug)]
struct TimeSeries {
    labels: Vec<Label>,
    samples: Vec<ProtoSample>,
}

#[derive(Debug)]
struct Label {
    name: String,
    value: String,
}

#[derive(Debug)]
struct ProtoSample {
    timestamp_ms: i64,
    value: f64,
}

/// Generate test samples for testing
pub fn generate_test_samples(count: usize, num_metrics: usize) -> Vec<Sample> {
    let metric_names: Vec<String> = (0..num_metrics)
        .map(|i| format!("test_metric_{}", i))
        .collect();

    let hosts = ["host-001", "host-002", "host-003"];
    let regions = ["us-east-1", "eu-west-1"];
    let envs = ["prod", "staging"];

    let base_ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    (0..count)
        .map(|i| {
            let metric_idx = i % num_metrics;
            let host_idx = i % hosts.len();
            let region_idx = (i / hosts.len()) % regions.len();
            let env_idx = (i / (hosts.len() * regions.len())) % envs.len();

            Sample {
                timestamp_ns: base_ts + (i as i64 * 1_000_000), // 1ms apart
                metric_name: metric_names[metric_idx].clone(),
                value: (i as f64) * 0.1 + ((i as f64).sin() * 10.0),
                labels: [
                    ("host".to_string(), hosts[host_idx].to_string()),
                    ("region".to_string(), regions[region_idx].to_string()),
                    ("env".to_string(), envs[env_idx].to_string()),
                ]
                .into(),
            }
        })
        .collect()
}

/// Generate CPU-like samples with sine wave pattern
pub fn generate_cpu_samples(count: usize, hosts: &[&str], interval_ms: i64) -> Vec<Sample> {
    let base_ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    hosts
        .iter()
        .flat_map(|host| {
            (0..count).map(move |i| {
                // Sine wave between 20% and 80%
                let value = 50.0 + 30.0 * ((i as f64 * 0.1).sin());

                Sample {
                    timestamp_ns: base_ts + (i as i64 * interval_ms * 1_000_000),
                    metric_name: "cpu_usage".to_string(),
                    value,
                    labels: [("host".to_string(), host.to_string())].into(),
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sample_builder() {
        let sample = Sample::now("test_metric", 42.0)
            .with_label("host", "test-host")
            .with_label("region", "us-east-1");

        assert_eq!(sample.metric_name, "test_metric");
        assert_eq!(sample.value, 42.0);
        assert_eq!(sample.labels.get("host"), Some(&"test-host".to_string()));
        assert_eq!(sample.labels.get("region"), Some(&"us-east-1".to_string()));
    }

    #[test]
    fn test_generate_test_samples() {
        let samples = generate_test_samples(100, 5);
        assert_eq!(samples.len(), 100);

        // Check we have 5 distinct metric names
        let metric_names: std::collections::HashSet<_> =
            samples.iter().map(|s| s.metric_name.clone()).collect();
        assert_eq!(metric_names.len(), 5);
    }

    #[test]
    fn test_generate_cpu_samples() {
        let samples = generate_cpu_samples(10, &["host-1", "host-2"], 1000);
        assert_eq!(samples.len(), 20); // 10 samples * 2 hosts

        // All should be cpu_usage
        assert!(samples.iter().all(|s| s.metric_name == "cpu_usage"));

        // Values should be in range
        assert!(samples.iter().all(|s| s.value >= 20.0 && s.value <= 80.0));
    }
}
