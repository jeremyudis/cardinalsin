//! Index lifecycle management (Invisible → Visible → Deprecated)

use super::{AdaptiveIndexConfig, IndexRecommendation, IndexType, TenantId};
use crate::Result;
use dashmap::DashMap;
use std::time::Instant;
use tracing::{debug, info};

/// Index visibility state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexVisibility {
    /// Created but not used by query planner
    Invisible,
    /// Active, used by query planner
    Visible,
    /// Marked for removal (unused 30+ days)
    Deprecated,
}

/// Index metadata
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    pub id: String,
    pub tenant_id: TenantId,
    pub column_name: String,
    pub visibility: IndexVisibility,
    pub created_at: Instant,
    pub last_used: Option<Instant>,
    pub usage_count: u64,
    pub would_have_helped: u64,
}

/// Index lifecycle manager
pub struct IndexLifecycleManager {
    config: AdaptiveIndexConfig,
    indexes: DashMap<String, IndexMetadata>,
}

impl IndexLifecycleManager {
    /// Create a new lifecycle manager
    pub fn new(config: AdaptiveIndexConfig) -> Self {
        Self {
            config,
            indexes: DashMap::new(),
        }
    }

    /// Create a new index as invisible
    pub async fn create_index(&self, rec: &IndexRecommendation) -> Result<String> {
        self.create_invisible_index(
            rec.tenant_id.clone(),
            rec.column_name.clone(),
            rec.index_type,
        )
        .await
    }

    /// Create an invisible index with explicit parameters
    pub async fn create_invisible_index(
        &self,
        tenant_id: TenantId,
        column_name: String,
        _index_type: IndexType,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();

        let metadata = IndexMetadata {
            id: id.clone(),
            tenant_id,
            column_name,
            visibility: IndexVisibility::Invisible,
            created_at: Instant::now(),
            last_used: None,
            usage_count: 0,
            would_have_helped: 0,
        };

        self.indexes.insert(id.clone(), metadata);

        debug!(index_id = %id, "Created invisible index");

        Ok(id)
    }

    /// Check all invisible indexes for visibility promotion
    pub async fn check_visibility(&self) {
        let invisible_ids: Vec<String> = self
            .indexes
            .iter()
            .filter(|e| e.visibility == IndexVisibility::Invisible)
            .map(|e| e.id.clone())
            .collect();

        for id in invisible_ids {
            match self.visibility_check(&id).await {
                Ok(true) => {
                    info!(index_id = %id, "Promoted index to visible");
                }
                Ok(false) => {
                    debug!(index_id = %id, "Index not ready for promotion");
                }
                Err(e) => {
                    debug!(index_id = %id, error = %e, "Visibility check failed");
                }
            }
        }
    }

    /// Check for unused indexes and mark as deprecated
    pub async fn check_unused(&self) {
        let deprecated = self.retirement_check().await;
        if !deprecated.is_empty() {
            info!(count = deprecated.len(), "Marked indexes as deprecated");
        }
    }

    /// Check if an invisible index should be promoted to visible
    pub async fn visibility_check(&self, index_id: &str) -> Result<bool> {
        let mut should_promote = false;

        if let Some(mut entry) = self.indexes.get_mut(index_id) {
            if entry.visibility != IndexVisibility::Invisible {
                return Ok(false);
            }

            // Check if index would have helped queries
            if entry.would_have_helped >= 100 {
                entry.visibility = IndexVisibility::Visible;
                should_promote = true;
            } else if entry.created_at.elapsed() > self.config.visibility_check_delay {
                // Index didn't help, remove it
                drop(entry);
                self.indexes.remove(index_id);
                return Ok(false);
            }
        }

        Ok(should_promote)
    }

    /// Record that an index was used
    pub fn record_usage(&self, index_id: &str) {
        if let Some(mut entry) = self.indexes.get_mut(index_id) {
            entry.usage_count += 1;
            entry.last_used = Some(Instant::now());
        }
    }

    /// Record that an index would have helped a query
    pub fn record_would_have_helped(&self, index_id: &str) {
        if let Some(mut entry) = self.indexes.get_mut(index_id) {
            entry.would_have_helped += 1;
        }
    }

    /// Check for indexes to deprecate
    pub async fn retirement_check(&self) -> Vec<String> {
        let mut to_deprecate = Vec::new();
        let threshold =
            std::time::Duration::from_secs(self.config.unused_days_threshold as u64 * 24 * 3600);

        for entry in self.indexes.iter() {
            if entry.visibility == IndexVisibility::Visible {
                let unused_duration = entry
                    .last_used
                    .map(|t| t.elapsed())
                    .unwrap_or(entry.created_at.elapsed());

                if unused_duration > threshold {
                    to_deprecate.push(entry.id.clone());
                }
            }
        }

        // Mark as deprecated
        for id in &to_deprecate {
            if let Some(mut entry) = self.indexes.get_mut(id) {
                entry.visibility = IndexVisibility::Deprecated;
            }
        }

        to_deprecate
    }

    /// Get visible indexes for a tenant (returns index IDs)
    pub fn get_visible_indexes(&self, tenant_id: &TenantId) -> Vec<String> {
        self.indexes
            .iter()
            .filter(|e| e.tenant_id == *tenant_id && e.visibility == IndexVisibility::Visible)
            .map(|e| e.id.clone())
            .collect()
    }

    /// Get visible index metadata for a tenant
    pub fn get_visible_index_metadata(&self, tenant_id: &TenantId) -> Vec<IndexMetadata> {
        self.indexes
            .iter()
            .filter(|e| e.tenant_id == *tenant_id && e.visibility == IndexVisibility::Visible)
            .map(|e| e.clone())
            .collect()
    }

    /// Get invisible indexes for a tenant (for testing)
    pub fn get_invisible_indexes(&self, tenant_id: &TenantId) -> Vec<IndexMetadata> {
        self.indexes
            .iter()
            .filter(|e| e.tenant_id == *tenant_id && e.visibility == IndexVisibility::Invisible)
            .map(|e| e.clone())
            .collect()
    }

    /// Remove an index
    pub fn remove_index(&self, index_id: &str) {
        self.indexes.remove(index_id);
    }
}

#[cfg(test)]
mod tests {
    use super::super::IndexType;
    use super::*;

    #[tokio::test]
    async fn test_create_invisible_index() {
        let manager = IndexLifecycleManager::new(AdaptiveIndexConfig::default());

        let rec = IndexRecommendation {
            tenant_id: "tenant1".to_string(),
            column_name: "service".to_string(),
            index_type: IndexType::Inverted,
            score: 10.0,
            estimated_benefit: super::super::recommender::IndexBenefit {
                query_speedup_factor: 10.0,
                estimated_storage_bytes: 1000,
                write_overhead_factor: 0.01,
            },
        };

        let id = manager.create_index(&rec).await.unwrap();

        let invisible = manager.get_invisible_indexes(&"tenant1".to_string());
        assert_eq!(invisible.len(), 1);
        assert_eq!(invisible[0].id, id);
    }

    #[tokio::test]
    async fn test_promote_to_visible() {
        let manager = IndexLifecycleManager::new(AdaptiveIndexConfig {
            visibility_check_delay: std::time::Duration::from_secs(0),
            ..Default::default()
        });

        let rec = IndexRecommendation {
            tenant_id: "tenant1".to_string(),
            column_name: "service".to_string(),
            index_type: IndexType::Inverted,
            score: 10.0,
            estimated_benefit: super::super::recommender::IndexBenefit {
                query_speedup_factor: 10.0,
                estimated_storage_bytes: 1000,
                write_overhead_factor: 0.01,
            },
        };

        let id = manager.create_index(&rec).await.unwrap();

        // Simulate 100 queries that would have been helped
        for _ in 0..100 {
            manager.record_would_have_helped(&id);
        }

        let promoted = manager.visibility_check(&id).await.unwrap();
        assert!(promoted);

        let visible = manager.get_visible_index_metadata(&"tenant1".to_string());
        assert_eq!(visible.len(), 1);
    }
}
