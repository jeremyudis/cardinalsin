//! Query statistics collection for adaptive indexing

use super::TenantId;
use dashmap::DashMap;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Collects query statistics per tenant
pub struct QueryStatsCollector {
    /// Stats per tenant
    stats: DashMap<TenantId, TenantQueryStats>,
    /// Window duration for rolling statistics
    window_duration: Duration,
}

/// Per-tenant query statistics
#[derive(Debug, Clone)]
pub struct TenantQueryStats {
    pub tenant_id: TenantId,
    /// Column usage in WHERE clauses
    pub column_filter_stats: HashMap<String, ColumnStats>,
    /// GROUP BY usage
    pub groupby_stats: HashMap<String, u64>,
    /// Window start time
    pub window_start: Instant,
}

/// Statistics for a specific column
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Number of times used in WHERE
    pub filter_count: u64,
    /// Median selectivity (0.0-1.0)
    pub selectivity_p50: f64,
    /// P99 selectivity
    pub selectivity_p99: f64,
    /// Estimated time saved if indexed
    pub query_time_saved_est: Duration,
    /// Estimated unique values
    pub cardinality_estimate: u64,
    /// Selectivity samples for percentile calculation
    selectivity_samples: Vec<f64>,
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self {
            filter_count: 0,
            selectivity_p50: 0.0,
            selectivity_p99: 0.0,
            query_time_saved_est: Duration::ZERO,
            cardinality_estimate: 0,
            selectivity_samples: Vec::new(),
        }
    }
}

impl ColumnStats {
    /// Create a new ColumnStats with the specified cardinality
    pub fn with_cardinality(cardinality: u64) -> Self {
        Self {
            cardinality_estimate: cardinality,
            ..Default::default()
        }
    }

    /// Create ColumnStats for testing with specific values
    pub fn new_for_test(
        filter_count: u64,
        selectivity_p50: f64,
        selectivity_p99: f64,
        query_time_saved_est: Duration,
        cardinality_estimate: u64,
    ) -> Self {
        Self {
            filter_count,
            selectivity_p50,
            selectivity_p99,
            query_time_saved_est,
            cardinality_estimate,
            selectivity_samples: Vec::new(),
        }
    }
}

impl QueryStatsCollector {
    /// Create a new stats collector
    pub fn new(window_duration: Duration) -> Self {
        Self {
            stats: DashMap::new(),
            window_duration,
        }
    }

    /// Record a filter usage
    pub fn record_filter(
        &self,
        tenant_id: &TenantId,
        column_name: &str,
        selectivity: f64,
        query_time: Duration,
    ) {
        let mut entry = self.stats
            .entry(tenant_id.clone())
            .or_insert_with(|| TenantQueryStats::new(tenant_id.clone()));

        // Check if we need to reset the window
        if entry.window_start.elapsed() > self.window_duration {
            *entry = TenantQueryStats::new(tenant_id.clone());
        }

        // Update column stats
        let col_stats = entry.column_filter_stats
            .entry(column_name.to_string())
            .or_default();

        col_stats.filter_count += 1;
        col_stats.selectivity_samples.push(selectivity);
        col_stats.query_time_saved_est += query_time;

        // Update percentiles (simple rolling calculation)
        col_stats.update_percentiles();
    }

    /// Record a GROUP BY usage
    pub fn record_groupby(&self, tenant_id: &TenantId, columns: &[String]) {
        let mut entry = self.stats
            .entry(tenant_id.clone())
            .or_insert_with(|| TenantQueryStats::new(tenant_id.clone()));

        for col in columns {
            *entry.groupby_stats.entry(col.clone()).or_default() += 1;
        }
    }

    /// Update cardinality estimate for a column
    pub fn update_cardinality(
        &self,
        tenant_id: &TenantId,
        column_name: &str,
        cardinality: u64,
    ) {
        if let Some(mut entry) = self.stats.get_mut(tenant_id) {
            if let Some(col_stats) = entry.column_filter_stats.get_mut(column_name) {
                col_stats.cardinality_estimate = cardinality;
            }
        }
    }

    /// Get stats for a tenant
    pub fn get_tenant_stats(&self, tenant_id: &TenantId) -> Option<TenantQueryStats> {
        self.stats.get(tenant_id).map(|e| e.clone())
    }

    /// Get all tenant IDs with stats
    pub fn get_all_tenants(&self) -> Vec<TenantId> {
        self.stats.iter().map(|e| e.key().clone()).collect()
    }

    /// Clear stats for a tenant
    pub fn clear_tenant(&self, tenant_id: &TenantId) {
        self.stats.remove(tenant_id);
    }
}

impl TenantQueryStats {
    fn new(tenant_id: TenantId) -> Self {
        Self {
            tenant_id,
            column_filter_stats: HashMap::new(),
            groupby_stats: HashMap::new(),
            window_start: Instant::now(),
        }
    }
}

impl ColumnStats {
    fn update_percentiles(&mut self) {
        if self.selectivity_samples.is_empty() {
            return;
        }

        let mut sorted = self.selectivity_samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let len = sorted.len();

        // P50
        self.selectivity_p50 = if len % 2 == 0 {
            (sorted[len / 2 - 1] + sorted[len / 2]) / 2.0
        } else {
            sorted[len / 2]
        };

        // P99
        let p99_idx = ((len as f64 * 0.99) as usize).min(len - 1);
        self.selectivity_p99 = sorted[p99_idx];

        // Trim samples to prevent unbounded growth
        if self.selectivity_samples.len() > 1000 {
            self.selectivity_samples = self.selectivity_samples[500..].to_vec();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_filter() {
        let collector = QueryStatsCollector::new(Duration::from_secs(3600));

        collector.record_filter(
            &"tenant1".to_string(),
            "service",
            0.1,
            Duration::from_millis(100),
        );

        let stats = collector.get_tenant_stats(&"tenant1".to_string()).unwrap();
        assert_eq!(stats.column_filter_stats["service"].filter_count, 1);
    }

    #[test]
    fn test_percentile_calculation() {
        let collector = QueryStatsCollector::new(Duration::from_secs(3600));
        let tenant = "tenant1".to_string();

        // Record multiple samples
        for i in 0..100 {
            collector.record_filter(
                &tenant,
                "host",
                i as f64 / 100.0,
                Duration::from_millis(10),
            );
        }

        let stats = collector.get_tenant_stats(&tenant).unwrap();
        let col_stats = &stats.column_filter_stats["host"];

        // P50 should be around 0.5
        assert!(col_stats.selectivity_p50 >= 0.45 && col_stats.selectivity_p50 <= 0.55);

        // P99 should be around 0.99
        assert!(col_stats.selectivity_p99 >= 0.95);
    }
}
