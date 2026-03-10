use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use cardinalsin::api;
use cardinalsin::api::ingest::{IngestDispatcher, RoutingContext, ShardWriteRouter};
use cardinalsin::cluster::{
    AssignmentStrategy, DistributedWriteRouter, NodeInfo, NodeRegistry, NodeType, ShardAssignment,
};
use cardinalsin::ingester::{Ingester, IngesterConfig};
use cardinalsin::metadata::{LocalMetadataClient, MetadataClient};
use cardinalsin::query::{QueryConfig, QueryNode};
use cardinalsin::schema::MetricSchema;
use cardinalsin::sharding::{HotShardConfig, ShardKey};
use cardinalsin::{CloudProvider, Result, StorageConfig};
use object_store::memory::InMemory;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn make_batch(rows: &[(i64, &str)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, metric)| *metric).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..rows.len()).map(|idx| idx as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn make_ingester(
    metadata: Arc<LocalMetadataClient>,
    tenant_id: u32,
    flush_row_count: usize,
) -> Arc<Ingester> {
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
    Arc::new(Ingester::with_shard_config(
        config,
        store,
        metadata_client,
        storage_config,
        MetricSchema::default_metrics(),
        HotShardConfig::default(),
        tenant_id,
    ))
}

struct FakeRouter {
    routes: HashMap<String, Option<NodeInfo>>,
    forwarded: Mutex<Vec<(String, usize, String)>>,
}

#[async_trait]
impl ShardWriteRouter for FakeRouter {
    async fn route_write(&self, shard_id: &str) -> Result<Option<NodeInfo>> {
        Ok(self.routes.get(shard_id).cloned().flatten())
    }

    async fn forward_write(
        &self,
        target_node: &NodeInfo,
        batch: &RecordBatch,
        tenant_id: &str,
    ) -> Result<()> {
        self.forwarded.lock().await.push((
            target_node.id.clone(),
            batch.num_rows(),
            tenant_id.to_string(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn dispatcher_preserves_local_behavior_when_routing_disabled() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let ingester = make_ingester(metadata.clone(), 11, 1);
    let dispatcher = IngestDispatcher::new(ingester, None);

    let ts = 1_700_000_000_000_000_000i64;
    dispatcher
        .dispatch(make_batch(&[(ts, "cpu_usage"), (ts, "cpu_usage")]))
        .await
        .unwrap();

    let chunks = metadata.list_chunks().await.unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].row_count, 2);
}

#[tokio::test]
async fn dispatcher_routes_local_and_remote_shards_separately() {
    let metadata = Arc::new(LocalMetadataClient::new());
    let ingester = make_ingester(metadata.clone(), 11, 1);

    let ts_a = 1_700_000_000_000_000_000i64;
    let ts_b = ts_a + 5 * 60 * 1_000_000_000i64;
    let shard_a = ShardKey::new(11, "cpu_usage", ts_a).shard_id();
    let shard_b = ShardKey::new(11, "cpu_usage", ts_b).shard_id();

    let local_node = NodeInfo::new(
        "node-local".to_string(),
        "127.0.0.1:19090".parse().unwrap(),
        NodeType::Ingester,
    );
    let remote_node = NodeInfo::new(
        "node-remote".to_string(),
        "127.0.0.1:19091".parse().unwrap(),
        NodeType::Ingester,
    );
    let router = Arc::new(FakeRouter {
        routes: HashMap::from([
            (shard_a.clone(), Some(local_node)),
            (shard_b.clone(), Some(remote_node.clone())),
        ]),
        forwarded: Mutex::new(Vec::new()),
    });
    let dispatcher = IngestDispatcher::new(
        ingester,
        Some(RoutingContext::new(
            router.clone(),
            "node-local".to_string(),
            "tenant-a".to_string(),
        )),
    );

    dispatcher
        .dispatch(make_batch(&[(ts_a, "cpu_usage"), (ts_b, "cpu_usage")]))
        .await
        .unwrap();

    let chunks = metadata.list_chunks().await.unwrap();
    assert_eq!(
        chunks.len(),
        1,
        "only the local shard should be ingested locally"
    );
    assert_eq!(chunks[0].shard_id.as_deref(), Some(shard_a.as_str()));

    let forwarded = router.forwarded.lock().await.clone();
    assert_eq!(
        forwarded,
        vec![("node-remote".to_string(), 1, "tenant-a".to_string())]
    );
}

async fn make_query_node() -> Arc<QueryNode> {
    let store = Arc::new(InMemory::new());
    let metadata: Arc<dyn MetadataClient> = Arc::new(LocalMetadataClient::new());
    let storage_config = StorageConfig {
        provider: CloudProvider::Memory,
        container: "test-bucket".to_string(),
        tenant_id: "tenant-a".to_string(),
    };
    Arc::new(
        QueryNode::new(QueryConfig::default(), store, metadata, storage_config)
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn distributed_write_router_forwards_arrow_batches_to_internal_endpoint() {
    let remote_metadata = Arc::new(LocalMetadataClient::new());
    let remote_ingester = make_ingester(remote_metadata.clone(), 3, 1);
    let query_node = make_query_node().await;
    let dispatcher = Arc::new(IngestDispatcher::new(remote_ingester.clone(), None));
    let router: Router =
        api::build_http_router(Some(remote_ingester), Some(dispatcher), query_node);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let nodes = Arc::new(NodeRegistry::new(30));
    let assignments = Arc::new(ShardAssignment::new(
        nodes.clone(),
        AssignmentStrategy::ConsistentHash,
    ));
    let write_router = DistributedWriteRouter::new(assignments, nodes);
    let target_node = NodeInfo::new("node-remote".to_string(), addr, NodeType::Ingester);

    let ts = 1_700_000_000_000_000_000i64;
    write_router
        .forward_write(
            &target_node,
            &make_batch(&[(ts, "cpu_usage"), (ts, "cpu_usage")]),
            "tenant-a",
        )
        .await
        .unwrap();

    let chunks = remote_metadata.list_chunks().await.unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].row_count, 2);

    server.abort();
}
