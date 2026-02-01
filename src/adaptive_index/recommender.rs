//! Index recommendation engine

use super::{AdaptiveIndexConfig, TenantId, ColumnStats};

/// Index recommendation
#[derive(Debug, Clone)]
pub struct IndexRecommendation {
    pub tenant_id: TenantId,
    pub column_name: String,
    pub index_type: IndexType,
    pub score: f64,
    pub estimated_benefit: IndexBenefit,
}

/// Type of index to create
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// Inverted index for low-cardinality equality filters
    Inverted,
    /// Range index for range queries
    Range,
    /// Bloom filter for high-cardinality equality (probabilistic)
    BloomFilter,
    /// Promote to dedicated column with dictionary encoding
    Dictionary,
}

/// Estimated benefit of an index
#[derive(Debug, Clone)]
pub struct IndexBenefit {
    pub query_speedup_factor: f64,
    pub estimated_storage_bytes: u64,
    pub write_overhead_factor: f64,
}

/// Tenant configuration for index limits
#[derive(Debug, Clone)]
pub struct TenantIndexConfig {
    pub max_indexes: u32,
    pub max_storage_bytes: u64,
    pub current_index_count: u32,
    pub current_storage_bytes: u64,
    pub write_rate: f64,
}

impl Default for TenantIndexConfig {
    fn default() -> Self {
        Self {
            max_indexes: 50,
            max_storage_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            current_index_count: 0,
            current_storage_bytes: 0,
            write_rate: 1000.0, // 1000 events/sec
        }
    }
}

/// Index recommendation engine
pub struct IndexRecommendationEngine {
    config: AdaptiveIndexConfig,
}

impl IndexRecommendationEngine {
    /// Create a new recommendation engine
    pub fn new(config: AdaptiveIndexConfig) -> Self {
        Self { config }
    }

    /// Generate recommendations for a tenant
    pub fn recommend(
        &self,
        tenant_id: TenantId,
        column_stats: &std::collections::HashMap<String, ColumnStats>,
        tenant_config: &TenantIndexConfig,
    ) -> Vec<IndexRecommendation> {
        // Check if at quota
        if tenant_config.current_index_count >= tenant_config.max_indexes {
            return Vec::new();
        }

        let mut recommendations: Vec<IndexRecommendation> = column_stats
            .iter()
            .filter(|(_, stats)| stats.filter_count >= self.config.min_filter_threshold)
            .filter_map(|(col, stats)| {
                let index_type = self.choose_index_type(stats);
                let score = self.score_candidate(stats, tenant_config);

                if score > self.config.score_threshold {
                    Some(IndexRecommendation {
                        tenant_id: tenant_id.clone(),
                        column_name: col.clone(),
                        index_type,
                        score,
                        estimated_benefit: self.estimate_benefit(stats, index_type),
                    })
                } else {
                    None
                }
            })
            .collect();

        // Sort by score descending
        recommendations.sort_by(|a, b| {
            b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Take top N recommendations
        recommendations.truncate(self.config.max_recommendations_per_cycle);

        recommendations
    }

    /// Score a candidate column for indexing
    fn score_candidate(
        &self,
        stats: &ColumnStats,
        tenant_config: &TenantIndexConfig,
    ) -> f64 {
        // Cost-benefit formula
        let query_benefit = stats.filter_count as f64
            * (1.0 - stats.selectivity_p50) // Higher benefit for selective filters
            * stats.query_time_saved_est.as_secs_f64();

        let storage_cost = self.estimate_index_size(stats.cardinality_estimate) as f64;
        let write_overhead = tenant_config.write_rate * 0.01; // 1% write overhead

        if storage_cost + write_overhead == 0.0 {
            return 0.0;
        }

        query_benefit / (storage_cost + write_overhead)
    }

    /// Choose the best index type based on cardinality
    fn choose_index_type(&self, stats: &ColumnStats) -> IndexType {
        match stats.cardinality_estimate {
            0..=1_000 => IndexType::Inverted,
            1_001..=100_000 => IndexType::Range,
            _ => IndexType::BloomFilter,
        }
    }

    /// Estimate index storage size
    fn estimate_index_size(&self, cardinality: u64) -> u64 {
        // Rough estimate: ~10 bytes per unique value for inverted index
        // Bloom filter: ~10 bits per element at 1% FPR
        cardinality * 10
    }

    /// Estimate benefit of creating an index
    fn estimate_benefit(&self, stats: &ColumnStats, index_type: IndexType) -> IndexBenefit {
        let query_speedup = match index_type {
            IndexType::Inverted => 10.0 / stats.selectivity_p50.max(0.01),
            IndexType::Range => 5.0 / stats.selectivity_p50.max(0.01),
            IndexType::BloomFilter => 2.0,
            IndexType::Dictionary => 3.0,
        };

        IndexBenefit {
            query_speedup_factor: query_speedup.min(100.0),
            estimated_storage_bytes: self.estimate_index_size(stats.cardinality_estimate),
            write_overhead_factor: 0.01,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_index_type_selection() {
        let engine = IndexRecommendationEngine::new(AdaptiveIndexConfig::default());

        let low_card = ColumnStats::with_cardinality(100);
        assert_eq!(engine.choose_index_type(&low_card), IndexType::Inverted);

        let medium_card = ColumnStats::with_cardinality(50_000);
        assert_eq!(engine.choose_index_type(&medium_card), IndexType::Range);

        let high_card = ColumnStats::with_cardinality(1_000_000);
        assert_eq!(engine.choose_index_type(&high_card), IndexType::BloomFilter);
    }

    #[test]
    fn test_recommendations() {
        let engine = IndexRecommendationEngine::new(AdaptiveIndexConfig {
            min_filter_threshold: 10,
            score_threshold: 0.0,
            ..Default::default()
        });

        let mut column_stats = std::collections::HashMap::new();
        column_stats.insert(
            "service".to_string(),
            ColumnStats::new_for_test(
                100,                          // filter_count
                0.1,                          // selectivity_p50
                0.2,                          // selectivity_p99
                Duration::from_secs(10),      // query_time_saved_est
                100,                          // cardinality_estimate
            ),
        );

        let tenant_config = TenantIndexConfig::default();

        let recommendations = engine.recommend(
            "tenant1".to_string(),
            &column_stats,
            &tenant_config,
        );

        assert_eq!(recommendations.len(), 1);
        assert_eq!(recommendations[0].column_name, "service");
    }
}
