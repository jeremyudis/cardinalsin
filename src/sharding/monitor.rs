//! Hot shard detection and monitoring

use super::ShardId;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Configuration for hot shard detection
#[derive(Debug, Clone)]
pub struct HotShardConfig {
    /// Detection window duration
    pub detection_window: Duration,
    /// Write QPS threshold
    pub write_qps_threshold: u64,
    /// Bytes per second threshold
    pub bytes_threshold: u64,
    /// CPU utilization threshold (0.0-1.0)
    pub cpu_threshold: f64,
    /// P99 latency threshold
    pub latency_threshold: Duration,
}

impl Default for HotShardConfig {
    fn default() -> Self {
        Self {
            detection_window: Duration::from_secs(60),
            write_qps_threshold: 50_000,
            bytes_threshold: 500 * 1024 * 1024, // 500 MiB/sec
            cpu_threshold: 0.75,
            latency_threshold: Duration::from_millis(100),
        }
    }
}

/// Rolling average calculator
#[derive(Debug, Clone)]
pub struct RollingAverage {
    samples: VecDeque<(Instant, f64)>,
    window: Duration,
}

impl RollingAverage {
    pub fn new(window: Duration) -> Self {
        Self {
            samples: VecDeque::new(),
            window,
        }
    }

    pub fn add_sample(&mut self, value: f64) {
        let now = Instant::now();
        self.samples.push_back((now, value));

        // Remove old samples
        while let Some((time, _)) = self.samples.front() {
            if now.duration_since(*time) > self.window {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn avg(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.samples.iter().map(|(_, v)| v).sum();
        sum / self.samples.len() as f64
    }

    /// Get the count of samples per second (for rate calculations)
    pub fn rate_per_second(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        let first_time = self.samples.front().map(|(t, _)| *t).unwrap();
        let last_time = self.samples.back().map(|(t, _)| *t).unwrap();
        let duration = last_time.duration_since(first_time);
        if duration.is_zero() {
            return 0.0;
        }
        self.samples.len() as f64 / duration.as_secs_f64()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

/// Metrics for a shard
#[derive(Debug, Clone)]
pub struct ShardMetrics {
    pub write_qps: RollingAverage,
    pub bytes_per_sec: RollingAverage,
    pub cpu_utilization: RollingAverage,
    pub p99_latency: RollingAverage,
    hot_since: Option<Instant>,
}

impl ShardMetrics {
    pub fn new(window: Duration) -> Self {
        Self {
            write_qps: RollingAverage::new(window),
            bytes_per_sec: RollingAverage::new(window),
            cpu_utilization: RollingAverage::new(window),
            p99_latency: RollingAverage::new(window),
            hot_since: None,
        }
    }

    pub fn record_write(&mut self, bytes: usize, latency: Duration) {
        self.write_qps.add_sample(1.0);
        self.bytes_per_sec.add_sample(bytes as f64);
        self.p99_latency.add_sample(latency.as_secs_f64());
    }

    pub fn record_cpu(&mut self, utilization: f64) {
        self.cpu_utilization.add_sample(utilization);
    }

    pub fn duration_hot(&self) -> Duration {
        self.hot_since
            .map(|t| t.elapsed())
            .unwrap_or(Duration::ZERO)
    }

    pub fn mark_hot(&mut self) {
        if self.hot_since.is_none() {
            self.hot_since = Some(Instant::now());
        }
    }

    pub fn mark_cool(&mut self) {
        self.hot_since = None;
    }
}

/// Action to take for a shard
#[derive(Debug, Clone)]
pub enum ShardAction {
    /// Split the shard
    Split(ShardId),
    /// Transfer lease to another replica
    TransferLease(ShardId, String),
    /// Move replica to another node
    MoveReplica(ShardId, String, String),
}

/// Monitors shards for hotness
pub struct ShardMonitor {
    config: HotShardConfig,
    metrics: DashMap<ShardId, ShardMetrics>,
}

impl ShardMonitor {
    /// Create a new shard monitor
    pub fn new(config: HotShardConfig) -> Self {
        Self {
            config,
            metrics: DashMap::new(),
        }
    }

    /// Record a write operation
    pub fn record_write(&self, shard_id: &ShardId, bytes: usize, latency: Duration) {
        let mut entry = self
            .metrics
            .entry(shard_id.clone())
            .or_insert_with(|| ShardMetrics::new(self.config.detection_window));
        entry.record_write(bytes, latency);
    }

    /// Record CPU utilization
    pub fn record_cpu(&self, shard_id: &ShardId, utilization: f64) {
        let mut entry = self
            .metrics
            .entry(shard_id.clone())
            .or_insert_with(|| ShardMetrics::new(self.config.detection_window));
        entry.record_cpu(utilization);
    }

    /// Evaluate shards and return recommended actions
    pub fn evaluate_shards(&self) -> Vec<ShardAction> {
        let mut actions = Vec::new();

        for mut entry in self.metrics.iter_mut() {
            let shard_id = entry.key().clone();
            let metrics = entry.value_mut();

            // Use rate_per_second for QPS, avg for other metrics
            let is_hot = metrics.write_qps.rate_per_second()
                > self.config.write_qps_threshold as f64
                || metrics.bytes_per_sec.avg() > self.config.bytes_threshold as f64
                || metrics.cpu_utilization.avg() > self.config.cpu_threshold
                || metrics.p99_latency.avg() > self.config.latency_threshold.as_secs_f64();

            if is_hot {
                metrics.mark_hot();

                if metrics.duration_hot() > self.config.detection_window {
                    actions.push(ShardAction::Split(shard_id));
                }
            } else {
                metrics.mark_cool();
            }
        }

        actions
    }

    /// Get metrics for a shard
    pub fn get_metrics(&self, shard_id: &ShardId) -> Option<ShardMetrics> {
        self.metrics.get(shard_id).map(|e| e.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rolling_average() {
        let mut avg = RollingAverage::new(Duration::from_secs(60));

        avg.add_sample(10.0);
        avg.add_sample(20.0);
        avg.add_sample(30.0);

        assert_eq!(avg.avg(), 20.0);
    }

    #[test]
    fn test_hot_shard_detection() {
        let config = HotShardConfig {
            detection_window: Duration::from_millis(10),
            write_qps_threshold: 10,
            ..Default::default()
        };

        let monitor = ShardMonitor::new(config);
        let shard_id = "shard-1".to_string();

        // Record many writes quickly to generate high QPS
        for _ in 0..100 {
            monitor.record_write(&shard_id, 1000, Duration::from_millis(1));
        }

        // First evaluation marks the shard as hot
        let actions1 = monitor.evaluate_shards();
        assert!(actions1.is_empty()); // Not hot long enough yet

        // Wait for detection window to pass while shard stays hot
        std::thread::sleep(Duration::from_millis(15));

        // Record more writes to keep QPS high
        for _ in 0..100 {
            monitor.record_write(&shard_id, 1000, Duration::from_millis(1));
        }

        // Second evaluation should now see that shard has been hot long enough
        let actions2 = monitor.evaluate_shards();
        // Should recommend split due to sustained high QPS
        assert!(!actions2.is_empty());
    }
}
