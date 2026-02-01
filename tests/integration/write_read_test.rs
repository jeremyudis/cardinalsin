//! Integration tests for write and read path

use cardinalsin::ingester::{Ingester, IngesterConfig};
use cardinalsin::metadata::LocalMetadataClient;
use cardinalsin::schema::MetricSchema;
use cardinalsin::StorageConfig;

use arrow_array::{Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use object_store::memory::InMemory;
use std::sync::Arc;

fn create_test_batch(rows: usize) -> RecordBatch {
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

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let timestamps: Vec<i64> = (0..rows as i64).map(|i| now + i * 1_000_000).collect();
    let names: Vec<&str> = (0..rows).map(|_| "cpu_usage").collect();
    let values: Vec<f64> = (0..rows).map(|i| (i as f64 % 100.0) / 100.0).collect();
    let hosts: Vec<&str> = (0..rows).map(|_| "server-1").collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(hosts)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_ingester_write() {
    let object_store = Arc::new(InMemory::new());
    let metadata: Arc<dyn cardinalsin::metadata::MetadataClient> =
        Arc::new(LocalMetadataClient::new());

    let config = IngesterConfig {
        flush_row_count: 100, // Low threshold for testing
        ..Default::default()
    };

    let storage_config = StorageConfig::default();
    let schema = MetricSchema::default_metrics();

    let ingester = Ingester::new(config, object_store, metadata, storage_config, schema);

    // Write a batch
    let batch = create_test_batch(50);
    ingester.write(batch).await.unwrap();

    // Check buffer stats
    let stats = ingester.buffer_stats().await;
    assert_eq!(stats.row_count, 50);

    // Write more to trigger flush
    let batch2 = create_test_batch(60);
    ingester.write(batch2).await.unwrap();

    // Buffer should be empty after flush
    let stats = ingester.buffer_stats().await;
    assert_eq!(stats.row_count, 0);
}

#[tokio::test]
async fn test_broadcast_channel() {
    let object_store = Arc::new(InMemory::new());
    let metadata: Arc<dyn cardinalsin::metadata::MetadataClient> =
        Arc::new(LocalMetadataClient::new());

    let config = IngesterConfig::default();
    let storage_config = StorageConfig::default();
    let schema = MetricSchema::default_metrics();

    let ingester = Ingester::new(config, object_store, metadata, storage_config, schema);

    // Subscribe to broadcast
    let mut rx = ingester.subscribe();

    // No data yet
    assert!(rx.try_recv().is_err());
}
