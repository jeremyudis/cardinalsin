use arrow_array::{Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cardinalsin::ingester::{Ingester, IngesterConfig};
use cardinalsin::metadata::{LocalMetadataClient, MetadataClient};
use cardinalsin::query::{QueryConfig, QueryNode};
use cardinalsin::schema::MetricSchema;
use cardinalsin::StorageConfig;
use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};
use std::sync::Arc;
use tempfile::tempdir;

fn create_metric_batch(metric_name: &str, rows: usize, start_ts: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value_f64", DataType::Float64, true),
        Field::new("host", DataType::Utf8, true),
    ]));

    let timestamps: Vec<i64> = (0..rows as i64).map(|i| start_ts + i * 1_000_000).collect();
    let metrics: Vec<&str> = (0..rows).map(|_| metric_name).collect();
    let values: Vec<f64> = (0..rows).map(|i| i as f64).collect();
    let hosts: Vec<&str> = (0..rows).map(|_| "host-a").collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(metrics)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(hosts)),
        ],
    )
    .expect("test batch must be valid")
}

async fn ingest_and_create_query_node(object_store: Arc<dyn ObjectStore>) -> QueryNode {
    let metadata: Arc<dyn MetadataClient> = Arc::new(LocalMetadataClient::new());
    let storage_config = StorageConfig::default();
    let metric_schema = MetricSchema::default_metrics();

    let ingester = Ingester::new(
        IngesterConfig {
            flush_row_count: 4,
            ..Default::default()
        },
        object_store.clone(),
        metadata.clone(),
        storage_config.clone(),
        metric_schema,
    );

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    ingester
        .write(create_metric_batch("cpu", 4, now))
        .await
        .expect("cpu batch write should succeed");
    ingester
        .write(create_metric_batch("mem", 4, now + 10_000_000))
        .await
        .expect("mem batch write should succeed");

    let chunks = metadata
        .list_chunks()
        .await
        .expect("metadata list should succeed");
    assert!(
        !chunks.is_empty(),
        "ingestion should flush and register at least one chunk"
    );

    QueryNode::new(
        QueryConfig::default(),
        object_store,
        metadata,
        storage_config,
    )
    .await
    .expect("query node creation should succeed")
}

#[tokio::test]
async fn test_query_node_query_over_flushed_chunks_inmemory() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let query_node = ingest_and_create_query_node(object_store).await;

    let cpu = query_node
        .query("SELECT metric_name FROM metrics WHERE metric_name = 'cpu'")
        .await
        .expect("cpu query should succeed");
    let cpu_rows: usize = cpu.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(cpu_rows, 4);

    let mem = query_node
        .query("SELECT metric_name FROM metrics WHERE metric_name = 'mem'")
        .await
        .expect("mem query should succeed");
    let mem_rows: usize = mem.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(mem_rows, 4);
}

#[tokio::test]
async fn test_query_node_query_over_flushed_chunks_filesystem_store() {
    let temp = tempdir().expect("tempdir should be created");
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(temp.path()).expect("local fs store"));
    let query_node = ingest_and_create_query_node(object_store).await;

    let total = query_node
        .query("SELECT metric_name FROM metrics")
        .await
        .expect("total query should succeed");
    let total_rows: usize = total.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 8);
}
