//! Column promotion (label → dedicated column)

use super::{IndexType, TenantId};
use crate::Result;

/// Source of label data
#[derive(Debug, Clone)]
pub enum LabelSource {
    /// From generic labels column
    GenericLabels(String),
    /// Already a dedicated column
    Dedicated(String),
}

/// Target for column promotion
#[derive(Debug, Clone)]
pub struct ColumnTarget {
    pub column_name: String,
    pub index_type: IndexType,
}

/// Backfill job for column promotion
#[derive(Debug, Clone)]
pub struct BackfillJob {
    pub id: String,
    pub tenant_id: TenantId,
    pub source: LabelSource,
    pub target: ColumnTarget,
    pub status: BackfillStatus,
    pub progress: BackfillProgress,
}

/// Backfill job status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// Backfill progress tracking
#[derive(Debug, Clone, Default)]
pub struct BackfillProgress {
    pub total_chunks: u64,
    pub processed_chunks: u64,
    pub total_rows: u64,
    pub processed_rows: u64,
}

impl BackfillProgress {
    pub fn percentage(&self) -> f64 {
        if self.total_chunks == 0 {
            return 0.0;
        }
        (self.processed_chunks as f64 / self.total_chunks as f64) * 100.0
    }
}

/// Column promoter for migrating labels to dedicated columns
pub struct ColumnPromoter {
    /// Active backfill jobs
    jobs: dashmap::DashMap<String, BackfillJob>,
}

impl ColumnPromoter {
    /// Create a new column promoter
    pub fn new() -> Self {
        Self {
            jobs: dashmap::DashMap::new(),
        }
    }

    /// Queue a column promotion
    pub async fn promote_column(
        &self,
        tenant_id: TenantId,
        label_name: &str,
        index_type: IndexType,
    ) -> Result<String> {
        let job_id = uuid::Uuid::new_v4().to_string();

        let job = BackfillJob {
            id: job_id.clone(),
            tenant_id,
            source: LabelSource::GenericLabels(label_name.to_string()),
            target: ColumnTarget {
                column_name: label_name.to_string(),
                index_type,
            },
            status: BackfillStatus::Pending,
            progress: BackfillProgress::default(),
        };

        self.jobs.insert(job_id.clone(), job);

        // In production, this would:
        // 1. Update schema to add new column
        // 2. Enable dual-write for new data
        // 3. Queue background backfill job

        Ok(job_id)
    }

    /// Get job status
    pub fn get_job(&self, job_id: &str) -> Option<BackfillJob> {
        self.jobs.get(job_id).map(|e| e.clone())
    }

    /// Update job progress
    pub fn update_progress(&self, job_id: &str, progress: BackfillProgress) {
        if let Some(mut entry) = self.jobs.get_mut(job_id) {
            entry.progress = progress;
        }
    }

    /// Mark job as completed
    pub fn complete_job(&self, job_id: &str) {
        if let Some(mut entry) = self.jobs.get_mut(job_id) {
            entry.status = BackfillStatus::Completed;
        }
    }

    /// Mark job as failed
    pub fn fail_job(&self, job_id: &str) {
        if let Some(mut entry) = self.jobs.get_mut(job_id) {
            entry.status = BackfillStatus::Failed;
        }
    }

    /// Get all pending jobs
    pub fn get_pending_jobs(&self) -> Vec<BackfillJob> {
        self.jobs
            .iter()
            .filter(|e| e.status == BackfillStatus::Pending)
            .map(|e| e.clone())
            .collect()
    }
}

impl Default for ColumnPromoter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_promote_column() {
        let promoter = ColumnPromoter::new();

        let job_id = promoter
            .promote_column("tenant1".to_string(), "service", IndexType::Dictionary)
            .await
            .unwrap();

        let job = promoter.get_job(&job_id).unwrap();
        assert_eq!(job.status, BackfillStatus::Pending);
        assert!(matches!(job.source, LabelSource::GenericLabels(_)));
    }

    #[test]
    fn test_progress_percentage() {
        let progress = BackfillProgress {
            total_chunks: 100,
            processed_chunks: 50,
            total_rows: 1000,
            processed_rows: 500,
        };

        assert_eq!(progress.percentage(), 50.0);
    }
}
