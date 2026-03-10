use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cardinalsin::ingester::{ChunkMetadata, Ingester, IngesterConfig};
use cardinalsin::metadata::{LocalMetadataClient, MetadataClient};
use cardinalsin::schema::MetricSchema;
use cardinalsin::sharding::{HotShardConfig, ShardKey};
use cardinalsin::{CloudProvider, StorageConfig};
use object_store::memory::InMemory;

fn make_batch(rows: &[(i64, &str)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let timestamps = Int64Array::from(rows.iter().map(|(ts, _)| *ts).collect::<Vec<_>>());
    let metrics = StringArray::from(rows.iter().map(|(_, metric)| *metric).collect::<Vec<_>>());
    let values = Float64Array::from((0..rows.len()).map(|i| i as f64).collect::<Vec<_>>());

    RecordBatch::try_new(
        schema,
        vec![Arc::new(timestamps), Arc::new(metrics), Arc::new(values)],
    )
    .unwrap()
}

fn make_ingester(
    metadata: Arc<LocalMetadataClient>,
    tenant_id: u32,
    flush_row_count: usize,
) -> Ingester {
    let config = IngesterConfig {
        flush_interval: Duration::from_secs(3600),
        flush_row_count,
        flush_size_bytes: usize::MAX,
        ..Default::default()
    };
    let store = Arc::new(InMemory::new());
    let storage_config = StorageConfig {
        provider: CloudProvider::Memory,
        container: "test-bucket".to_string(),
        tenant_id: "tenant-a".to_string(),
    };
    let metadata_client: Arc<dyn MetadataClient> = metadata;

    Ingester::with_shard_config(
        config,
        store,
        metadata_client,
        storage_config,
        MetricSchema::default_metrics(),
        HotShardConfig::default(),
        tenant_id,
    )
}

#[tokio::test]
async fn mixed_batch_flushes_to_separate_shard_paths_and_metadata() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let ingester = make_ingester(metadata.clone(), 7, 1);

    let ts_a = 1_700_000_000_000_000_000i64;
    let ts_b = ts_a + (5 * 60 * 1_000_000_000i64);
    let batch = make_batch(&[(ts_a, "cpu_usage"), (ts_b, "cpu_usage")]);

    ingester.write(batch).await.unwrap();

    let chunks = metadata.list_chunks().await.unwrap();
    assert_eq!(
        chunks.len(),
        2,
        "mixed-shard writes should flush separately"
    );

    let expected_a = ShardKey::new(7, "cpu_usage", ts_a).shard_id();
    let expected_b = ShardKey::new(7, "cpu_usage", ts_b).shard_id();
    let expected: HashSet<_> = [expected_a.clone(), expected_b.clone()]
        .into_iter()
        .collect();
    let actual: HashSet<_> = chunks
        .iter()
        .map(|chunk| {
            chunk
                .shard_id
                .clone()
                .expect("chunk should record shard_id")
        })
        .collect();
    assert_eq!(actual, expected);

    for chunk in &chunks {
        let shard_id = chunk.shard_id.as_deref().unwrap();
        assert!(
            chunk.chunk_path.contains(&format!("shard={shard_id}")),
            "chunk path should include its shard id: {}",
            chunk.chunk_path
        );
    }

    assert_eq!(
        metadata
            .get_chunks_for_shard(&expected_a)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        metadata
            .get_chunks_for_shard(&expected_b)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn local_metadata_prefers_explicit_shard_id_over_path_matching() {
    let metadata = LocalMetadataClient::new();
    let chunk = ChunkMetadata {
        path: "tenant-a/data/year=2026/month=03/day=10/hour=00/chunk.parquet".to_string(),
        min_timestamp: 100,
        max_timestamp: 200,
        row_count: 2,
        size_bytes: 128,
        shard_id: Some("shard-explicit".to_string()),
    };

    metadata.register_chunk(&chunk.path, &chunk).await.unwrap();

    let matching = metadata
        .get_chunks_for_shard("shard-explicit")
        .await
        .unwrap();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].chunk_path, chunk.path);

    let non_matching = metadata.get_chunks_for_shard("shard-other").await.unwrap();
    assert!(non_matching.is_empty());
}
