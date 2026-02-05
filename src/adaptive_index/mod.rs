//! Adaptive indexing system
//!
//! Automatically detects tenant query patterns and promotes common dimensions
//! to dedicated indexed columns.

mod stats_collector;
mod recommender;
mod lifecycle;
mod column_promoter;

pub use stats_collector::{QueryStatsCollector, TenantQueryStats, ColumnStats};
pub use recommender::{IndexRecommendationEngine, IndexRecommendation, IndexType, TenantIndexConfig};
pub use lifecycle::{IndexLifecycleManager, IndexVisibility, IndexMetadata};
pub use column_promoter::ColumnPromoter;

use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Configuration for adaptive indexing
#[derive(Debug, Clone)]
pub struct AdaptiveIndexConfig {
    /// Minimum queries/day to consider for indexing
    pub min_filter_threshold: u64,
    /// Score threshold for creating an index
    pub score_threshold: f64,
    /// Maximum recommendations per cycle
    pub max_recommendations_per_cycle: usize,
    /// Visibility check delay after index creation
    pub visibility_check_delay: Duration,
    /// Days of inactivity before marking index as deprecated
    pub unused_days_threshold: u32,
    /// Grace period before removing deprecated index
    pub removal_grace_period: Duration,
    /// Statistics window duration
    pub stats_window: Duration,
}

impl Default for AdaptiveIndexConfig {
    fn default() -> Self {
        Self {
            min_filter_threshold: 100,
            score_threshold: 1.0,
            max_recommendations_per_cycle: 5,
            visibility_check_delay: Duration::from_secs(48 * 3600), // 48 hours
            unused_days_threshold: 30,
            removal_grace_period: Duration::from_secs(7 * 24 * 3600), // 7 days
            stats_window: Duration::from_secs(48 * 3600), // 48 hours
        }
    }
}

/// Tenant ID type
pub type TenantId = String;

/// Adaptive Index Controller - orchestrates the adaptive indexing system
///
/// This is the runtime that ties together:
/// - Stats collection (from query patterns)
/// - Index recommendations (based on collected stats)
/// - Index lifecycle management (invisible → visible → deprecated)
pub struct AdaptiveIndexController {
    _config: AdaptiveIndexConfig,
    stats_collector: Arc<QueryStatsCollector>,
    recommendation_engine: IndexRecommendationEngine,
    pub lifecycle_manager: IndexLifecycleManager,
    /// Tenant-specific configurations
    tenant_configs: std::sync::RwLock<std::collections::HashMap<TenantId, TenantIndexConfig>>,
    /// Check interval for the controller loop
    check_interval: Duration,
}

impl AdaptiveIndexController {
    /// Create a new adaptive index controller
    pub fn new(config: AdaptiveIndexConfig) -> Self {
        let stats_collector = Arc::new(QueryStatsCollector::new(config.stats_window));
        let recommendation_engine = IndexRecommendationEngine::new(config.clone());
        let lifecycle_manager = IndexLifecycleManager::new(config.clone());

        Self {
            _config: config,
            stats_collector,
            recommendation_engine,
            lifecycle_manager,
            tenant_configs: std::sync::RwLock::new(std::collections::HashMap::new()),
            check_interval: Duration::from_secs(300), // Check every 5 minutes
        }
    }

    /// Get the stats collector for recording query statistics
    pub fn stats_collector(&self) -> Arc<QueryStatsCollector> {
        Arc::clone(&self.stats_collector)
    }

    /// Register a tenant with custom configuration
    pub fn register_tenant(&self, tenant_id: TenantId, config: TenantIndexConfig) {
        let mut configs = self.tenant_configs.write().unwrap();
        configs.insert(tenant_id, config);
    }

    /// Run the adaptive indexing loop
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(self.check_interval);

        loop {
            interval.tick().await;

            if let Err(e) = self.run_cycle().await {
                warn!("Adaptive indexing cycle failed: {}", e);
            }
        }
    }

    /// Run a single adaptive indexing cycle
    pub async fn run_cycle(&self) -> crate::Result<()> {
        debug!("Starting adaptive indexing cycle");

        // Get all tenants with stats
        let tenants = self.stats_collector.get_all_tenants();

        for tenant_id in tenants {
            self.process_tenant(&tenant_id).await?;
        }

        // Check index visibility (promote invisible → visible)
        self.lifecycle_manager.check_visibility().await;

        // Check for unused indexes (mark as deprecated)
        self.lifecycle_manager.check_unused().await;

        debug!("Adaptive indexing cycle completed");
        Ok(())
    }

    /// Process a single tenant
    async fn process_tenant(&self, tenant_id: &TenantId) -> crate::Result<()> {
        // Get tenant stats
        let stats = match self.stats_collector.get_tenant_stats(tenant_id) {
            Some(s) => s,
            None => return Ok(()),
        };

        // Get tenant config (or use default)
        let tenant_config = {
            let configs = self.tenant_configs.read().unwrap();
            configs.get(tenant_id).cloned().unwrap_or_default()
        };

        // Generate recommendations
        let recommendations = self.recommendation_engine.recommend(
            tenant_id.clone(),
            &stats.column_filter_stats,
            &tenant_config,
        );

        if recommendations.is_empty() {
            return Ok(());
        }

        info!(
            tenant = %tenant_id,
            count = recommendations.len(),
            "Generated index recommendations"
        );

        // Create indexes as invisible
        for rec in recommendations {
            match self.lifecycle_manager.create_invisible_index(
                rec.tenant_id.clone(),
                rec.column_name.clone(),
                rec.index_type.clone(),
            ).await {
                Ok(index_id) => {
                    info!(
                        tenant = %rec.tenant_id,
                        column = %rec.column_name,
                        index_type = ?rec.index_type,
                        index_id = %index_id,
                        "Created invisible index"
                    );
                }
                Err(e) => {
                    warn!(
                        tenant = %rec.tenant_id,
                        column = %rec.column_name,
                        error = %e,
                        "Failed to create index"
                    );
                }
            }
        }

        Ok(())
    }

    /// Record a filter usage (called by query engine)
    pub fn record_filter(
        &self,
        tenant_id: &TenantId,
        column_name: &str,
        selectivity: f64,
        query_time: Duration,
    ) {
        self.stats_collector.record_filter(tenant_id, column_name, selectivity, query_time);
    }

    /// Record a GROUP BY usage (called by query engine)
    pub fn record_groupby(&self, tenant_id: &TenantId, columns: &[String]) {
        self.stats_collector.record_groupby(tenant_id, columns);
    }

    /// Get statistics for a tenant
    pub fn get_tenant_stats(&self, tenant_id: &TenantId) -> Option<TenantQueryStats> {
        self.stats_collector.get_tenant_stats(tenant_id)
    }

    /// Get all active indexes for a tenant
    pub fn get_tenant_indexes(&self, tenant_id: &TenantId) -> Vec<String> {
        self.lifecycle_manager.get_visible_indexes(tenant_id)
    }
}
