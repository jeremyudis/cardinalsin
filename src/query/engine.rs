//! DataFusion query engine integration

use super::cache::TieredCache;
use super::cached_store::CachedObjectStore;
use crate::metadata::TimeRange;
use crate::{Error, Result};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use object_store::ObjectStore;
use parking_lot::RwLock;
use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Query engine powered by DataFusion
#[derive(Clone)]
pub struct QueryEngine {
    /// DataFusion session context
    ctx: SessionContext,
    /// Registered table paths
    registered_paths: Arc<RwLock<HashSet<String>>>,
    /// Paths currently backing the logical "metrics" table
    registered_metrics_paths: Arc<RwLock<BTreeSet<String>>>,
    /// Serialize operations that mutate and query the logical `metrics` table
    metrics_table_query_lock: Arc<Mutex<()>>,
}

impl QueryEngine {
    /// Create a new query engine
    pub async fn new(object_store: Arc<dyn ObjectStore>, cache: Arc<TieredCache>) -> Result<Self> {
        // Wrap object store with caching layer
        let cached_store = Arc::new(CachedObjectStore::new(object_store.clone(), cache.clone()));

        // Configure runtime environment
        let runtime_env = RuntimeEnvBuilder::new()
            .build_arc()
            .map_err(|e| Error::Config(format!("Failed to build runtime env: {}", e)))?;

        // Register object store with DataFusion for s3:// URLs
        // DataFusion uses the URL scheme to look up the appropriate ObjectStore
        let s3_url = url::Url::parse("s3://bucket")
            .map_err(|e| Error::Config(format!("Failed to parse S3 URL: {}", e)))?;
        runtime_env.register_object_store(&s3_url, cached_store.clone());

        // Create session config with optimizations
        let session_config = SessionConfig::new()
            .with_batch_size(8192)
            .with_target_partitions(num_cpus::get())
            .with_information_schema(true)
            .with_parquet_pruning(true)
            .with_collect_statistics(true);

        // Create session state and context
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime_env)
            .build();
        let ctx = SessionContext::new_with_state(state);

        // object_store and cache are used indirectly through CachedObjectStore
        let _ = (&object_store, &cache);

        let engine = Self {
            ctx,
            registered_paths: Arc::new(RwLock::new(HashSet::new())),
            registered_metrics_paths: Arc::new(RwLock::new(BTreeSet::new())),
            metrics_table_query_lock: Arc::new(Mutex::new(())),
        };

        // Register empty metrics table at startup for Grafana compatibility (issue #97).
        // This ensures `SELECT ... FROM metrics` returns empty results instead of
        // "table not found" errors before any data has been ingested.
        engine.register_empty_metrics_table().await?;

        Ok(engine)
    }

    /// Execute an operation while `metrics` is bound to the provided chunk paths.
    ///
    /// This prevents concurrent requests from racing on the shared SessionContext
    /// table mapping.
    pub async fn with_metrics_table<F, Fut, T>(
        &self,
        chunk_paths: &[String],
        operation: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let _guard = self.metrics_table_query_lock.lock().await;
        self.register_metrics_table_for_chunks_locked(chunk_paths)
            .await?;
        operation().await
    }

    /// Register the logical `metrics` table over a set of chunk paths.
    ///
    /// This resolves the model mismatch between SQL queries (`FROM metrics`) and
    /// metadata-selected chunk files by mapping the selected chunks into one table.
    pub async fn register_metrics_table_for_chunks(&self, chunk_paths: &[String]) -> Result<()> {
        let _guard = self.metrics_table_query_lock.lock().await;
        self.register_metrics_table_for_chunks_locked(chunk_paths)
            .await
    }

    async fn register_metrics_table_for_chunks_locked(&self, chunk_paths: &[String]) -> Result<()> {
        let normalized_paths: BTreeSet<String> = chunk_paths
            .iter()
            .map(|p| p.trim_start_matches('/').to_string())
            .collect();

        if normalized_paths.is_empty() {
            self.register_empty_metrics_table().await?;
            *self.registered_metrics_paths.write() = normalized_paths;
            return Ok(());
        }

        {
            let registered = self.registered_metrics_paths.read();
            if *registered == normalized_paths {
                return Ok(());
            }
        }

        let table_urls: Result<Vec<_>> = normalized_paths
            .iter()
            .map(|path| {
                let url = Self::path_to_object_store_url(path);
                ListingTableUrl::parse(&url).map_err(|e| {
                    Error::Config(format!(
                        "Failed to parse metrics table URL '{}': {}",
                        url, e
                    ))
                })
            })
            .collect();
        let table_urls = table_urls?;

        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_collect_stat(true);

        let config = ListingTableConfig::new_with_multi_paths(table_urls)
            .with_listing_options(listing_options)
            .infer_schema(&self.ctx.state())
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to infer schema for metrics table: {}", e))
            })?;

        let table = ListingTable::try_new(config)?;

        let _ = self.ctx.deregister_table("metrics");
        self.ctx.register_table("metrics", Arc::new(table))?;

        *self.registered_metrics_paths.write() = normalized_paths;

        Ok(())
    }

    async fn register_empty_metrics_table(&self) -> Result<()> {
        let schema = self.metrics_table_schema().await;
        let empty_table = EmptyTable::new(schema);

        let _ = self.ctx.deregister_table("metrics");
        self.ctx
            .register_table("metrics", Arc::new(empty_table))
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn metrics_table_schema(&self) -> SchemaRef {
        match self.ctx.table("metrics").await {
            Ok(df) => df.schema().inner().clone(),
            Err(_) => crate::schema::MetricSchema::default_metrics().arrow_schema(),
        }
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
        let file_format = ParquetFormat::default().with_enable_pruning(true);

        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_collect_stat(true);

        // Normalize chunk paths to the object-store URL registered in QueryEngine::new
        let url = Self::path_to_object_store_url(path);

        let table_url = ListingTableUrl::parse(&url)
            .map_err(|e| Error::Config(format!("Failed to parse table URL '{}': {}", url, e)))?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .infer_schema(&self.ctx.state())
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to infer schema for '{}': {}", path, e))
            })?;

        let table = ListingTable::try_new(config)?;
        self.ctx.register_table(&table_name, Arc::new(table))?;

        // Mark as registered
        let mut registered = self.registered_paths.write();
        registered.insert(path.to_string());

        Ok(())
    }

    /// Execute a SQL query
    pub async fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;
        Ok(batches)
    }

    /// Execute a SQL query with index awareness for adaptive indexing
    pub async fn execute_with_indexes(
        &self,
        sql: &str,
        tenant_id: &str,
        index_controller: Arc<crate::adaptive_index::AdaptiveIndexController>,
    ) -> Result<Vec<RecordBatch>> {
        // 1. Analyze query for filter predicates
        let df = self.ctx.sql(sql).await?;
        let plan = df.logical_plan();
        let filter_columns = Self::extract_filter_columns(plan);

        // 2. Get visible indexes for this tenant
        let tenant_string = tenant_id.to_string();
        let available_indexes = index_controller
            .lifecycle_manager
            .get_visible_index_metadata(&tenant_string);

        // 3. Match indexes to query predicates and record usage
        for index in &available_indexes {
            if filter_columns.contains(&index.column_name) {
                index_controller.lifecycle_manager.record_usage(&index.id);
            }
        }

        // 4. Execute query (DataFusion handles predicate pushdown automatically)
        let batches = df.collect().await?;

        // 5. Track would-have-helped for invisible indexes
        let invisible_indexes = index_controller
            .lifecycle_manager
            .get_invisible_indexes(&tenant_string);

        for index in invisible_indexes {
            if filter_columns.contains(&index.column_name) {
                index_controller
                    .lifecycle_manager
                    .record_would_have_helped(&index.id);
            }
        }

        Ok(batches)
    }

    /// Extract filter columns from a logical plan
    fn extract_filter_columns(plan: &LogicalPlan) -> Vec<String> {
        let mut columns = Vec::new();
        Self::extract_filter_columns_recursive(plan, &mut columns);
        columns.sort();
        columns.dedup();
        columns
    }

    /// Recursively extract filter columns from a logical plan
    fn extract_filter_columns_recursive(plan: &LogicalPlan, columns: &mut Vec<String>) {
        match plan {
            LogicalPlan::Filter(filter) => {
                Self::extract_columns_from_expr(&filter.predicate, columns);
                Self::extract_filter_columns_recursive(&filter.input, columns);
            }
            LogicalPlan::Projection(proj) => {
                Self::extract_filter_columns_recursive(&proj.input, columns);
            }
            LogicalPlan::Sort(sort) => {
                Self::extract_filter_columns_recursive(&sort.input, columns);
            }
            LogicalPlan::Limit(limit) => {
                Self::extract_filter_columns_recursive(&limit.input, columns);
            }
            LogicalPlan::Aggregate(agg) => {
                Self::extract_filter_columns_recursive(&agg.input, columns);
            }
            _ => {}
        }
    }

    /// Extract column names from an expression
    fn extract_columns_from_expr(expr: &Expr, columns: &mut Vec<String>) {
        match expr {
            Expr::Column(col) => {
                columns.push(col.name.clone());
            }
            Expr::BinaryExpr(binary) => {
                Self::extract_columns_from_expr(&binary.left, columns);
                Self::extract_columns_from_expr(&binary.right, columns);
            }
            Expr::Between(between) => {
                Self::extract_columns_from_expr(&between.expr, columns);
            }
            Expr::InList(in_list) => {
                Self::extract_columns_from_expr(&in_list.expr, columns);
            }
            Expr::Not(not) => {
                Self::extract_columns_from_expr(not, columns);
            }
            _ => {}
        }
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
    fn extract_time_bounds(
        plan: &LogicalPlan,
        min_time: &mut Option<i64>,
        max_time: &mut Option<i64>,
    ) {
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
            _ => None,
        }
    }

    /// Extract column predicates from a SQL query for metadata-level filtering
    ///
    /// This enables predicate pushdown to the metadata layer, dramatically
    /// reducing S3 read costs by pruning chunks before DataFusion sees them.
    pub async fn extract_column_predicates(
        &self,
        sql: &str,
    ) -> Result<Vec<crate::metadata::predicates::ColumnPredicate>> {
        let df = self.ctx.sql(sql).await?;
        let plan = df.logical_plan();

        let mut predicates = Vec::new();
        Self::extract_predicates_from_plan(plan, &mut predicates);

        Ok(predicates)
    }

    /// Recursively extract predicates from a logical plan
    fn extract_predicates_from_plan(
        plan: &LogicalPlan,
        predicates: &mut Vec<crate::metadata::predicates::ColumnPredicate>,
    ) {
        match plan {
            LogicalPlan::Filter(filter) => {
                if let Some(pred) = Self::convert_expr_to_predicate(&filter.predicate) {
                    predicates.push(pred);
                }
                Self::extract_predicates_from_plan(&filter.input, predicates);
            }
            LogicalPlan::Projection(proj) => {
                Self::extract_predicates_from_plan(&proj.input, predicates);
            }
            LogicalPlan::Sort(sort) => {
                Self::extract_predicates_from_plan(&sort.input, predicates);
            }
            LogicalPlan::Limit(limit) => {
                Self::extract_predicates_from_plan(&limit.input, predicates);
            }
            LogicalPlan::Aggregate(agg) => {
                Self::extract_predicates_from_plan(&agg.input, predicates);
            }
            _ => {}
        }
    }

    /// Convert a DataFusion expression to a ColumnPredicate
    fn convert_expr_to_predicate(
        expr: &Expr,
    ) -> Option<crate::metadata::predicates::ColumnPredicate> {
        use crate::metadata::predicates::ColumnPredicate;

        match expr {
            Expr::BinaryExpr(binary) => {
                // Skip timestamp predicates (already handled by time range extraction)
                if let Expr::Column(col) = binary.left.as_ref() {
                    if col.name == "timestamp" || col.name == "time" {
                        return None;
                    }

                    let value = Self::convert_scalar_to_predicate_value(&binary.right)?;

                    match binary.op {
                        Operator::Eq => Some(ColumnPredicate::Eq(col.name.clone(), value)),
                        Operator::NotEq => Some(ColumnPredicate::NotEq(col.name.clone(), value)),
                        Operator::Lt => Some(ColumnPredicate::Lt(col.name.clone(), value)),
                        Operator::LtEq => Some(ColumnPredicate::LtEq(col.name.clone(), value)),
                        Operator::Gt => Some(ColumnPredicate::Gt(col.name.clone(), value)),
                        Operator::GtEq => Some(ColumnPredicate::GtEq(col.name.clone(), value)),
                        Operator::And => {
                            let left = Self::convert_expr_to_predicate(&binary.left)?;
                            let right = Self::convert_expr_to_predicate(&binary.right)?;
                            Some(ColumnPredicate::And(Box::new(left), Box::new(right)))
                        }
                        Operator::Or => {
                            let left = Self::convert_expr_to_predicate(&binary.left)?;
                            let right = Self::convert_expr_to_predicate(&binary.right)?;
                            Some(ColumnPredicate::Or(Box::new(left), Box::new(right)))
                        }
                        _ => None,
                    }
                } else {
                    // Handle AND/OR without column reference
                    match binary.op {
                        Operator::And => {
                            let left = Self::convert_expr_to_predicate(&binary.left)?;
                            let right = Self::convert_expr_to_predicate(&binary.right)?;
                            Some(ColumnPredicate::And(Box::new(left), Box::new(right)))
                        }
                        Operator::Or => {
                            let left = Self::convert_expr_to_predicate(&binary.left)?;
                            let right = Self::convert_expr_to_predicate(&binary.right)?;
                            Some(ColumnPredicate::Or(Box::new(left), Box::new(right)))
                        }
                        _ => None,
                    }
                }
            }
            Expr::Between(between) => {
                if let Expr::Column(col) = between.expr.as_ref() {
                    if col.name == "timestamp" || col.name == "time" {
                        return None;
                    }

                    let low = Self::convert_scalar_to_predicate_value(&between.low)?;
                    let high = Self::convert_scalar_to_predicate_value(&between.high)?;
                    Some(ColumnPredicate::Between(col.name.clone(), low, high))
                } else {
                    None
                }
            }
            Expr::InList(in_list) => {
                if let Expr::Column(col) = in_list.expr.as_ref() {
                    if col.name == "timestamp" || col.name == "time" {
                        return None;
                    }

                    let values: Option<Vec<_>> = in_list
                        .list
                        .iter()
                        .map(Self::convert_scalar_to_predicate_value)
                        .collect();
                    let values = values?;

                    if in_list.negated {
                        Some(ColumnPredicate::NotIn(col.name.clone(), values))
                    } else {
                        Some(ColumnPredicate::In(col.name.clone(), values))
                    }
                } else {
                    None
                }
            }
            Expr::Not(inner) => {
                let inner_pred = Self::convert_expr_to_predicate(inner)?;
                Some(ColumnPredicate::Not(Box::new(inner_pred)))
            }
            _ => None,
        }
    }

    /// Convert a DataFusion scalar expression to a PredicateValue
    fn convert_scalar_to_predicate_value(
        expr: &Expr,
    ) -> Option<crate::metadata::predicates::PredicateValue> {
        use crate::metadata::predicates::PredicateValue;

        match expr {
            Expr::Literal(scalar) => match scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                    Some(PredicateValue::String(s.clone()))
                }
                ScalarValue::Int64(Some(i)) => Some(PredicateValue::Int64(*i)),
                ScalarValue::Int32(Some(i)) => Some(PredicateValue::Int64(*i as i64)),
                ScalarValue::Float64(Some(f)) => Some(PredicateValue::Float64(*f)),
                ScalarValue::Float32(Some(f)) => Some(PredicateValue::Float64(*f as f64)),
                ScalarValue::Boolean(Some(b)) => Some(PredicateValue::Boolean(*b)),
                ScalarValue::Null => Some(PredicateValue::Null),
                _ => None,
            },
            _ => None,
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

    /// Normalize a chunk path to an object-store URL for DataFusion
    fn path_to_object_store_url(path: &str) -> String {
        if path.starts_with("s3://") {
            path.to_string()
        } else {
            format!("s3://bucket/{}", path.trim_start_matches('/'))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_engine_creation() {
        let object_store = Arc::new(InMemory::new());
        let dir = tempdir().unwrap();
        let config = super::super::CacheConfig {
            l1_size: 10 * 1024 * 1024,
            l2_size: 50 * 1024 * 1024,
            l2_dir: Some(dir.path().to_str().unwrap().to_string()),
        };
        let cache = Arc::new(TieredCache::new(config).await.unwrap());

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

    #[tokio::test]
    async fn test_empty_metrics_table_query() {
        let object_store = Arc::new(InMemory::new());
        let dir = tempdir().unwrap();
        let config = super::super::CacheConfig {
            l1_size: 10 * 1024 * 1024,
            l2_size: 50 * 1024 * 1024,
            l2_dir: Some(dir.path().to_str().unwrap().to_string()),
        };
        let cache = Arc::new(TieredCache::new(config).await.unwrap());

        let engine = QueryEngine::new(object_store, cache).await.unwrap();

        // Metrics table should be registered at startup - query should return empty, not error
        let results = engine
            .execute("SELECT * FROM metrics WHERE metric_name = 'cpu_usage'")
            .await
            .unwrap();

        assert!(
            results.is_empty() || results[0].num_rows() == 0,
            "Empty metrics table should return 0 rows"
        );
    }

    #[tokio::test]
    async fn test_register_metrics_table_empty_paths() {
        let object_store = Arc::new(InMemory::new());
        let dir = tempdir().unwrap();
        let config = super::super::CacheConfig {
            l1_size: 10 * 1024 * 1024,
            l2_size: 50 * 1024 * 1024,
            l2_dir: Some(dir.path().to_str().unwrap().to_string()),
        };
        let cache = Arc::new(TieredCache::new(config).await.unwrap());

        let engine = QueryEngine::new(object_store, cache).await.unwrap();

        // Re-register with empty paths should succeed and preserve empty table
        engine.register_metrics_table_for_chunks(&[]).await.unwrap();

        let results = engine.execute("SELECT * FROM metrics").await.unwrap();

        assert!(
            results.is_empty() || results[0].num_rows() == 0,
            "Should return 0 rows after re-registering with empty paths"
        );
    }

    #[test]
    fn test_path_to_object_store_url() {
        // Bare path gets s3://bucket/ prefix
        assert_eq!(
            QueryEngine::path_to_object_store_url("tenant/data/chunk.parquet"),
            "s3://bucket/tenant/data/chunk.parquet"
        );
        // Leading slash is stripped
        assert_eq!(
            QueryEngine::path_to_object_store_url("/tenant/data/chunk.parquet"),
            "s3://bucket/tenant/data/chunk.parquet"
        );
        // Already has s3:// prefix - returned as-is
        assert_eq!(
            QueryEngine::path_to_object_store_url("s3://mybucket/data/chunk.parquet"),
            "s3://mybucket/data/chunk.parquet"
        );
    }
}
