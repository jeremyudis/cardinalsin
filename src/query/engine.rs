//! DataFusion query engine integration

use super::cache::TieredCache;
use crate::metadata::TimeRange;
use crate::{Error, Result};

use arrow_array::RecordBatch;
use datafusion::prelude::*;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl, ListingOptions};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::{LogicalPlan, Expr, Operator};
use datafusion::scalar::ScalarValue;
use object_store::ObjectStore;
use std::sync::Arc;
use std::collections::HashSet;
use parking_lot::RwLock;

/// Query engine powered by DataFusion
#[derive(Clone)]
pub struct QueryEngine {
    /// DataFusion session context
    ctx: SessionContext,
    /// Object store reference
    object_store: Arc<dyn ObjectStore>,
    /// Tiered cache
    cache: Arc<TieredCache>,
    /// Registered table paths
    registered_paths: Arc<RwLock<HashSet<String>>>,
}

impl QueryEngine {
    /// Create a new query engine
    pub async fn new(
        object_store: Arc<dyn ObjectStore>,
        cache: Arc<TieredCache>,
    ) -> Result<Self> {
        // Configure runtime environment
        let runtime_config = RuntimeConfig::new();
        let runtime_env = Arc::new(RuntimeEnv::try_new(runtime_config)?);

        // Create session config with optimizations
        let session_config = SessionConfig::new()
            .with_batch_size(8192)
            .with_target_partitions(num_cpus::get())
            .with_information_schema(true)
            .with_parquet_pruning(true)
            .with_collect_statistics(true);

        // Create session state and context
        let state = SessionState::new_with_config_rt(session_config, runtime_env);
        let ctx = SessionContext::new_with_state(state);

        Ok(Self {
            ctx,
            object_store,
            cache,
            registered_paths: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    /// Register a chunk file as a table
    pub async fn register_chunk(&self, path: &str) -> Result<()> {
        // Check if already registered
        {
            let registered = self.registered_paths.read();
            if registered.contains(path) {
                return Ok(());
            }
        }

        // Generate a unique table name from the path
        let table_name = Self::path_to_table_name(path);

        // Create listing table configuration
        let file_format = ParquetFormat::default()
            .with_enable_pruning(true);

        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_collect_stat(true);

        // Register the file as a table
        // Note: In production, we'd use the object_store directly
        // For now, we create a simple table config
        let url = format!("file://{}", path);
        if let Ok(table_url) = ListingTableUrl::parse(&url) {
            let config = ListingTableConfig::new(table_url)
                .with_listing_options(listing_options);

            if let Ok(config) = config.infer_schema(&self.ctx.state()).await {
                let table = ListingTable::try_new(config)?;
                self.ctx.register_table(&table_name, Arc::new(table))?;

                // Mark as registered
                let mut registered = self.registered_paths.write();
                registered.insert(path.to_string());
            }
        }

        Ok(())
    }

    /// Execute a SQL query
    pub async fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;
        Ok(batches)
    }

    /// Execute a SQL query and return a stream
    pub async fn execute_stream(
        &self,
        sql: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let df = self.ctx.sql(sql).await?;
        let stream = df.execute_stream().await?;
        Ok(stream)
    }

    /// Extract time range from a SQL query by analyzing the logical plan
    pub async fn extract_time_range(&self, sql: &str) -> Result<TimeRange> {
        let df = self.ctx.sql(sql).await?;
        let plan = df.logical_plan();

        // Extract time predicates from the plan
        let mut min_time: Option<i64> = None;
        let mut max_time: Option<i64> = None;

        Self::extract_time_bounds(plan, &mut min_time, &mut max_time);

        // Default to last hour if no time bounds found
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let hour_ago = now - 3_600_000_000_000;

        Ok(TimeRange::new(
            min_time.unwrap_or(hour_ago),
            max_time.unwrap_or(now),
        ))
    }

    /// Recursively extract time bounds from a logical plan
    fn extract_time_bounds(plan: &LogicalPlan, min_time: &mut Option<i64>, max_time: &mut Option<i64>) {
        match plan {
            LogicalPlan::Filter(filter) => {
                Self::extract_time_from_expr(&filter.predicate, min_time, max_time);
                Self::extract_time_bounds(&filter.input, min_time, max_time);
            }
            LogicalPlan::Projection(proj) => {
                Self::extract_time_bounds(&proj.input, min_time, max_time);
            }
            LogicalPlan::Sort(sort) => {
                Self::extract_time_bounds(&sort.input, min_time, max_time);
            }
            LogicalPlan::Limit(limit) => {
                Self::extract_time_bounds(&limit.input, min_time, max_time);
            }
            LogicalPlan::Aggregate(agg) => {
                Self::extract_time_bounds(&agg.input, min_time, max_time);
            }
            _ => {}
        }
    }

    /// Extract time bounds from a filter expression
    fn extract_time_from_expr(expr: &Expr, min_time: &mut Option<i64>, max_time: &mut Option<i64>) {
        match expr {
            Expr::BinaryExpr(binary) => {
                // Check if this is a timestamp comparison
                if let Expr::Column(col) = binary.left.as_ref() {
                    if col.name == "timestamp" || col.name == "time" {
                        if let Some(value) = Self::extract_timestamp_value(&binary.right) {
                            match binary.op {
                                Operator::Gt | Operator::GtEq => {
                                    *min_time = Some(min_time.unwrap_or(i64::MAX).min(value));
                                }
                                Operator::Lt | Operator::LtEq => {
                                    *max_time = Some(max_time.unwrap_or(i64::MIN).max(value));
                                }
                                Operator::Eq => {
                                    *min_time = Some(value);
                                    *max_time = Some(value);
                                }
                                _ => {}
                            }
                        }
                    }
                }
                // Handle reversed comparison (literal on left)
                if let Expr::Column(col) = binary.right.as_ref() {
                    if col.name == "timestamp" || col.name == "time" {
                        if let Some(value) = Self::extract_timestamp_value(&binary.left) {
                            match binary.op {
                                Operator::Lt | Operator::LtEq => {
                                    *min_time = Some(min_time.unwrap_or(i64::MAX).min(value));
                                }
                                Operator::Gt | Operator::GtEq => {
                                    *max_time = Some(max_time.unwrap_or(i64::MIN).max(value));
                                }
                                _ => {}
                            }
                        }
                    }
                }
                // Recurse into AND/OR expressions
                if matches!(binary.op, Operator::And | Operator::Or) {
                    Self::extract_time_from_expr(&binary.left, min_time, max_time);
                    Self::extract_time_from_expr(&binary.right, min_time, max_time);
                }
            }
            Expr::Between(between) => {
                if let Expr::Column(col) = between.expr.as_ref() {
                    if col.name == "timestamp" || col.name == "time" {
                        if let Some(low) = Self::extract_timestamp_value(&between.low) {
                            *min_time = Some(min_time.unwrap_or(i64::MAX).min(low));
                        }
                        if let Some(high) = Self::extract_timestamp_value(&between.high) {
                            *max_time = Some(max_time.unwrap_or(i64::MIN).max(high));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Extract timestamp value from an expression
    fn extract_timestamp_value(expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Literal(ScalarValue::Int64(Some(v))) => Some(*v),
            Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), _)) => Some(*v),
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(v), _)) => Some(*v * 1000),
            Expr::Literal(ScalarValue::TimestampMillisecond(Some(v), _)) => Some(*v * 1_000_000),
            Expr::Literal(ScalarValue::TimestampSecond(Some(v), _)) => Some(*v * 1_000_000_000),
            _ => None
        }
    }

    /// Analyze a query without executing
    pub async fn analyze(&self, sql: &str) -> Result<datafusion::logical_expr::LogicalPlan> {
        let df = self.ctx.sql(sql).await?;
        Ok(df.logical_plan().clone())
    }

    /// Create a prepared statement for repeated queries
    pub async fn prepare(&self, sql: &str) -> Result<String> {
        // Generate a handle for the prepared statement
        let handle = uuid::Uuid::new_v4().to_string();

        // In a full implementation, we'd cache the logical plan
        // For now, just validate the SQL
        let _ = self.ctx.sql(sql).await?;

        Ok(handle)
    }

    /// Get the session context
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Convert a file path to a valid table name
    fn path_to_table_name(path: &str) -> String {
        path.replace(['/', '.', '-'], "_")
            .trim_start_matches('_')
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_engine_creation() {
        let object_store = Arc::new(InMemory::new());
        let cache = Arc::new(TieredCache::new(super::super::CacheConfig::default()).unwrap());

        let engine = QueryEngine::new(object_store, cache).await;
        assert!(engine.is_ok());
    }

    #[test]
    fn test_path_to_table_name() {
        assert_eq!(
            QueryEngine::path_to_table_name("data/2024/chunk.parquet"),
            "data_2024_chunk_parquet"
        );
    }
}
