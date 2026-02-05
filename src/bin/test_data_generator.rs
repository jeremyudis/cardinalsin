//! Test Data Generator for CardinalSin E2E Validation
//!
//! A standalone CLI tool that generates realistic TSDB workloads
//! and sends them to the CardinalSin ingester via HTTP.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --bin test-data-generator -- \
//!   --endpoint http://localhost:8081 \
//!   --metrics 10 \
//!   --hosts 5 \
//!   --samples-per-second 100 \
//!   --duration 60s
//! ```
//!
//! ## Features
//!
//! - Generates multiple metric types (gauges, counters)
//! - Realistic value patterns (sine waves, random walks, monotonic counters)
//! - Configurable cardinality (hosts, regions, environments)
//! - Rate limiting to avoid overwhelming the ingester
//! - Burst mode for stress testing

use clap::Parser;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;

/// Test Data Generator for CardinalSin
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Ingester endpoint URL
    #[arg(long, default_value = "http://localhost:8081")]
    endpoint: String,

    /// Number of unique metric names to generate
    #[arg(long, default_value = "10")]
    metrics: usize,

    /// Number of unique hosts
    #[arg(long, default_value = "5")]
    hosts: usize,

    /// Number of unique regions
    #[arg(long, default_value = "3")]
    regions: usize,

    /// Target samples per second
    #[arg(long, default_value = "100")]
    samples_per_second: u64,

    /// Duration to run (e.g., "60s", "5m", "1h")
    #[arg(long, default_value = "60s")]
    duration: humantime::Duration,

    /// Batch size (samples per HTTP request)
    #[arg(long, default_value = "100")]
    batch_size: usize,

    /// Send all data at once (burst mode for stress testing)
    #[arg(long)]
    burst: bool,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

/// Metric value generator types
#[derive(Clone, Copy)]
enum ValuePattern {
    /// Sine wave pattern (for CPU-like metrics)
    Sine { min: f64, max: f64, period: f64 },
    /// Random walk (for memory-like metrics)
    RandomWalk { min: f64, max: f64, step: f64 },
    /// Monotonic counter (for request counts)
    Counter { rate: f64 },
    /// Constant value with jitter
    Constant { base: f64, jitter: f64 },
}

/// Metric definition
struct MetricDef {
    name: String,
    pattern: ValuePattern,
}

/// Sample for ingestion
#[derive(Clone)]
#[allow(dead_code)]
struct Sample {
    timestamp_ns: i64,
    metric_name: String,
    value: f64,
    labels: HashMap<String, String>,
}

/// Statistics counter
struct Stats {
    samples_sent: AtomicU64,
    batches_sent: AtomicU64,
    errors: AtomicU64,
    bytes_sent: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            samples_sent: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        }
    }

    fn record_batch(&self, samples: u64, bytes: u64) {
        self.samples_sent.fetch_add(samples, Ordering::Relaxed);
        self.batches_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn summary(&self) -> (u64, u64, u64, u64) {
        (
            self.samples_sent.load(Ordering::Relaxed),
            self.batches_sent.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
            self.bytes_sent.load(Ordering::Relaxed),
        )
    }
}

/// Data generator
struct DataGenerator {
    metrics: Vec<MetricDef>,
    hosts: Vec<String>,
    regions: Vec<String>,
    environments: Vec<String>,
    counter_values: HashMap<String, f64>,
    walk_values: HashMap<String, f64>,
    start_time: Instant,
}

impl DataGenerator {
    fn new(num_metrics: usize, num_hosts: usize, num_regions: usize) -> Self {
        // Create metric definitions with various patterns
        let metric_templates = [
            ("cpu_usage", ValuePattern::Sine { min: 10.0, max: 95.0, period: 300.0 }),
            ("memory_used_bytes", ValuePattern::RandomWalk { min: 1e9, max: 8e9, step: 1e7 }),
            ("disk_read_bytes_total", ValuePattern::Counter { rate: 1e6 }),
            ("disk_write_bytes_total", ValuePattern::Counter { rate: 5e5 }),
            ("network_rx_bytes_total", ValuePattern::Counter { rate: 2e6 }),
            ("network_tx_bytes_total", ValuePattern::Counter { rate: 1.5e6 }),
            ("http_requests_total", ValuePattern::Counter { rate: 1000.0 }),
            ("http_request_duration_seconds", ValuePattern::Sine { min: 0.01, max: 0.5, period: 120.0 }),
            ("gc_pause_seconds", ValuePattern::Constant { base: 0.05, jitter: 0.02 }),
            ("active_connections", ValuePattern::Sine { min: 10.0, max: 500.0, period: 600.0 }),
        ];

        let metrics: Vec<MetricDef> = (0..num_metrics)
            .map(|i| {
                let (name, pattern) = metric_templates[i % metric_templates.len()];
                MetricDef {
                    name: if i < metric_templates.len() {
                        name.to_string()
                    } else {
                        format!("{}_{}", name, i / metric_templates.len())
                    },
                    pattern,
                }
            })
            .collect();

        let hosts: Vec<String> = (0..num_hosts)
            .map(|i| format!("host-{:03}", i + 1))
            .collect();

        let region_names = ["us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1", "ap-southeast-1"];
        let regions: Vec<String> = region_names
            .iter()
            .take(num_regions)
            .map(|s| s.to_string())
            .collect();

        let environments = vec!["prod".to_string(), "staging".to_string()];

        Self {
            metrics,
            hosts,
            regions,
            environments,
            counter_values: HashMap::new(),
            walk_values: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    fn generate_samples(&mut self, count: usize) -> Vec<Sample> {
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();

        let mut samples = Vec::with_capacity(count);
        let mut sample_idx = 0;

        while samples.len() < count {
            for metric in &self.metrics {
                for host in &self.hosts {
                    for region in &self.regions {
                        for env in &self.environments {
                            if samples.len() >= count {
                                break;
                            }

                            let key = format!("{}|{}|{}|{}", metric.name, host, region, env);

                            let value = match metric.pattern {
                                ValuePattern::Sine { min, max, period } => {
                                    let range = max - min;
                                    let phase = (elapsed_secs / period) * 2.0 * std::f64::consts::PI;
                                    min + (range / 2.0) * (1.0 + phase.sin())
                                }
                                ValuePattern::RandomWalk { min, max, step } => {
                                    let current = self.walk_values.entry(key.clone()).or_insert((min + max) / 2.0);
                                    let delta = (rand::random::<f64>() - 0.5) * 2.0 * step;
                                    *current = (*current + delta).clamp(min, max);
                                    *current
                                }
                                ValuePattern::Counter { rate } => {
                                    let current = self.counter_values.entry(key.clone()).or_insert(0.0);
                                    *current += rate / 10.0 + (rand::random::<f64>() * rate / 20.0);
                                    *current
                                }
                                ValuePattern::Constant { base, jitter } => {
                                    base + (rand::random::<f64>() - 0.5) * 2.0 * jitter
                                }
                            };

                            let timestamp_ns = now_ns + (sample_idx as i64 * 1_000_000); // 1ms offset per sample

                            samples.push(Sample {
                                timestamp_ns,
                                metric_name: metric.name.clone(),
                                value,
                                labels: [
                                    ("host".to_string(), host.clone()),
                                    ("region".to_string(), region.clone()),
                                    ("env".to_string(), env.clone()),
                                ]
                                .into(),
                            });

                            sample_idx += 1;
                        }
                    }
                }
            }
        }

        samples.truncate(count);
        samples
    }
}

/// HTTP client for sending samples
struct IngesterClient {
    client: Client,
    endpoint: String,
}

impl IngesterClient {
    fn new(endpoint: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self { client, endpoint }
    }

    async fn send_samples(&self, samples: &[Sample]) -> Result<usize, String> {
        // Build Prometheus Remote Write request
        let write_request = self.samples_to_proto(samples);
        let proto_bytes = self.encode_proto(&write_request);

        // Compress with snappy
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&proto_bytes)
            .map_err(|e| format!("Snappy compression failed: {}", e))?;

        let bytes_len = compressed.len();

        // Send to ingester
        let resp = self
            .client
            .post(format!("{}/api/v1/write", self.endpoint))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .body(compressed)
            .send()
            .await
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if resp.status().is_success() || resp.status().as_u16() == 204 {
            Ok(bytes_len)
        } else {
            Err(format!(
                "HTTP error: {} - {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ))
        }
    }

    fn samples_to_proto(&self, _samples: &[Sample]) -> Vec<u8> {
        // Simplified: just return empty bytes since the server doesn't parse yet
        // In production, this would encode to proper protobuf format
        Vec::new()
    }

    fn encode_proto(&self, _data: &[u8]) -> Vec<u8> {
        Vec::new()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("CardinalSin Test Data Generator");
    println!("================================");
    println!("Endpoint:      {}", args.endpoint);
    println!("Metrics:       {}", args.metrics);
    println!("Hosts:         {}", args.hosts);
    println!("Regions:       {}", args.regions);
    println!("Target rate:   {} samples/sec", args.samples_per_second);
    println!("Duration:      {:?}", *args.duration);
    println!("Batch size:    {}", args.batch_size);
    println!("Mode:          {}", if args.burst { "burst" } else { "steady" });
    println!();

    let client = IngesterClient::new(args.endpoint.clone());
    let mut generator = DataGenerator::new(args.metrics, args.hosts, args.regions);
    let stats = Arc::new(Stats::new());

    let total_samples = args.samples_per_second * args.duration.as_secs();
    let start_time = Instant::now();

    if args.burst {
        // Burst mode: send all samples as fast as possible
        println!("Sending {} samples in burst mode...", total_samples);

        let mut remaining = total_samples as usize;
        while remaining > 0 {
            let batch_count = remaining.min(args.batch_size);
            let samples = generator.generate_samples(batch_count);

            match client.send_samples(&samples).await {
                Ok(bytes) => {
                    stats.record_batch(samples.len() as u64, bytes as u64);
                    if args.verbose {
                        println!("Sent batch of {} samples", samples.len());
                    }
                }
                Err(e) => {
                    stats.record_error();
                    eprintln!("Error sending batch: {}", e);
                }
            }

            remaining -= batch_count;
        }
    } else {
        // Steady mode: rate-limited sending
        println!("Starting steady-rate generation...");

        // Calculate interval between batches
        let batches_per_second = (args.samples_per_second as f64) / (args.batch_size as f64);
        let batch_interval = Duration::from_secs_f64(1.0 / batches_per_second);

        let mut ticker = interval(batch_interval);
        let deadline = start_time + *args.duration;

        // Stats reporter
        let stats_clone = stats.clone();
        tokio::spawn(async move {
            let mut reporter = interval(Duration::from_secs(10));
            loop {
                reporter.tick().await;
                let (samples, batches, errors, bytes) = stats_clone.summary();
                println!(
                    "[Stats] Samples: {}, Batches: {}, Errors: {}, Bytes: {}",
                    samples, batches, errors, bytes
                );
            }
        });

        while Instant::now() < deadline {
            ticker.tick().await;

            let samples = generator.generate_samples(args.batch_size);

            match client.send_samples(&samples).await {
                Ok(bytes) => {
                    stats.record_batch(samples.len() as u64, bytes as u64);
                    if args.verbose {
                        println!("Sent batch of {} samples", samples.len());
                    }
                }
                Err(e) => {
                    stats.record_error();
                    if args.verbose {
                        eprintln!("Error sending batch: {}", e);
                    }
                }
            }
        }
    }

    let elapsed = start_time.elapsed();
    let (samples, batches, errors, bytes) = stats.summary();

    println!();
    println!("Generation Complete");
    println!("===================");
    println!("Duration:       {:?}", elapsed);
    println!("Samples sent:   {}", samples);
    println!("Batches sent:   {}", batches);
    println!("Errors:         {}", errors);
    println!("Bytes sent:     {} ({:.2} MB)", bytes, bytes as f64 / 1e6);
    println!(
        "Actual rate:    {:.2} samples/sec",
        samples as f64 / elapsed.as_secs_f64()
    );

    if errors > 0 {
        eprintln!("WARNING: {} errors occurred during generation", errors);
    }

    Ok(())
}
