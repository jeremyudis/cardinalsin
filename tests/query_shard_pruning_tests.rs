use std::sync::Arc;

use async_trait::async_trait;
use cardinalsin::ingester::ChunkMetadata;
use cardinalsin::metadata::predicates::ColumnPredicate;
use cardinalsin::metadata::{
    CompactionJob, CompactionLease, CompactionLeases, CompactionStatus, LeaseStatus,
    MetadataClient, SplitState, TimeIndexEntry, TimeRange,
};
use cardinalsin::query::{
    QueryConfig, QueryEngine, QueryNode, StreamingQueryExecutor, TieredCache,
};
use cardinalsin::sharding::{ShardKey, ShardMetadata, ShardState, SplitPhase};
use cardinalsin::{CloudProvider, Error, Result, StorageConfig};
use object_store::memory::InMemory;
use tokio::sync::broadcast;
use tokio::sync::Mutex;

#[derive(Default)]
struct RecordingMetadata {
    shards: Vec<ShardMetadata>,
    requested_shards: Mutex<Vec<String>>,
}

impl RecordingMetadata {
    fn new(shards: Vec<ShardMetadata>) -> Self {
        Self {
            shards,
            requested_shards: Mutex::new(Vec::new()),
        }
    }

    async fn requested_shards(&self) -> Vec<String> {
        self.requested_shards.lock().await.clone()
    }
}

#[async_trait]
impl MetadataClient for RecordingMetadata {
    async fn register_chunk(&self, _path: &str, _metadata: &ChunkMetadata) -> Result<()> {
        Ok(())
    }

    async fn get_chunks(&self, _range: TimeRange) -> Result<Vec<TimeIndexEntry>> {
        Ok(Vec::new())
    }

    async fn get_chunks_with_predicates(
        &self,
        _range: TimeRange,
        _predicates: &[ColumnPredicate],
    ) -> Result<Vec<TimeIndexEntry>> {
        Err(Error::Internal(
            "expected shard-scoped metadata lookup".to_string(),
        ))
    }

    async fn get_chunks_with_predicates_for_shards(
        &self,
        _range: TimeRange,
        _predicates: &[ColumnPredicate],
        shard_ids: &[String],
    ) -> Result<Vec<TimeIndexEntry>> {
        *self.requested_shards.lock().await = shard_ids.to_vec();
        Ok(Vec::new())
    }

    async fn get_chunk(&self, _path: &str) -> Result<Option<ChunkMetadata>> {
        Ok(None)
    }

    async fn delete_chunk(&self, _path: &str) -> Result<()> {
        Ok(())
    }

    async fn list_chunks(&self) -> Result<Vec<TimeIndexEntry>> {
        Ok(Vec::new())
    }

    async fn get_l0_candidates(&self, _min_count: usize) -> Result<Vec<Vec<String>>> {
        Ok(Vec::new())
    }

    async fn get_level_candidates(
        &self,
        _level: usize,
        _target_size: usize,
    ) -> Result<Vec<Vec<String>>> {
        Ok(Vec::new())
    }

    async fn create_compaction_job(&self, _job: CompactionJob) -> Result<()> {
        Ok(())
    }

    async fn complete_compaction(
        &self,
        _source_chunks: &[String],
        _target_chunk: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn update_compaction_status(
        &self,
        _job_id: &str,
        _status: CompactionStatus,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_pending_compaction_jobs(&self) -> Result<Vec<CompactionJob>> {
        Ok(Vec::new())
    }

    async fn start_split(
        &self,
        _old_shard: &str,
        _new_shards: Vec<String>,
        _split_point: Vec<u8>,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_split_state(&self, _shard_id: &str) -> Result<Option<SplitState>> {
        Ok(None)
    }

    async fn update_split_progress(
        &self,
        _shard_id: &str,
        _progress: f64,
        _phase: SplitPhase,
    ) -> Result<()> {
        Ok(())
    }

    async fn complete_split(&self, _old_shard: &str) -> Result<()> {
        Ok(())
    }

    async fn get_chunks_for_shard(&self, _shard_id: &str) -> Result<Vec<TimeIndexEntry>> {
        Ok(Vec::new())
    }

    async fn get_shard_metadata(&self, shard_id: &str) -> Result<Option<ShardMetadata>> {
        Ok(self
            .shards
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .cloned())
    }

    async fn update_shard_metadata(
        &self,
        _shard_id: &str,
        _metadata: &ShardMetadata,
        _expected_generation: u64,
    ) -> Result<()> {
        Ok(())
    }

    async fn list_shards(&self) -> Result<Vec<ShardMetadata>> {
        Ok(self.shards.clone())
    }

    async fn acquire_lease(
        &self,
        node_id: &str,
        chunks: &[String],
        level: u32,
    ) -> Result<CompactionLease> {
        let now = chrono::Utc::now();
        Ok(CompactionLease {
            lease_id: "lease".to_string(),
            holder_id: node_id.to_string(),
            chunks: chunks.to_vec(),
            acquired_at: now,
            expires_at: now,
            level,
            status: LeaseStatus::Active,
        })
    }

    async fn complete_lease(&self, _lease_id: &str) -> Result<()> {
        Ok(())
    }

    async fn fail_lease(&self, _lease_id: &str) -> Result<()> {
        Ok(())
    }

    async fn renew_lease(&self, _lease_id: &str) -> Result<()> {
        Ok(())
    }

    async fn load_leases(&self) -> Result<CompactionLeases> {
        Ok(CompactionLeases::default())
    }

    async fn scavenge_leases(&self) -> Result<usize> {
        Ok(0)
    }

    async fn has_active_split(&self) -> Result<bool> {
        Ok(false)
    }
}

fn storage_config() -> StorageConfig {
    StorageConfig {
        provider: CloudProvider::Memory,
        container: "test-bucket".to_string(),
        tenant_id: "tenant-a".to_string(),
    }
}

async fn make_query_node(metadata: Arc<dyn MetadataClient>) -> QueryNode {
    let store = Arc::new(InMemory::new());
    QueryNode::new(QueryConfig::default(), store, metadata, storage_config())
        .await
        .unwrap()
}

async fn make_engine() -> QueryEngine {
    let store = Arc::new(InMemory::new());
    let cache = Arc::new(TieredCache::new(Default::default()).await.unwrap());
    QueryEngine::new(store, cache, &storage_config())
        .await
        .unwrap()
}

fn shard_for(metric_name: &str, ts: i64) -> ShardMetadata {
    let key = ShardKey::new(0, metric_name, ts);
    let start = key.to_bytes();
    let mut end = start.clone();
    *end.last_mut().unwrap() += 1;
    ShardMetadata {
        shard_id: key.shard_id(),
        generation: 1,
        key_range: (start, end),
        replicas: Vec::new(),
        state: ShardState::Active,
        min_time: ts,
        max_time: ts + 300_000_000_000,
    }
}

#[tokio::test]
async fn query_node_uses_shard_scoped_metadata_lookup() {
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let shard = shard_for("cpu_usage", ts);
    let metadata = Arc::new(RecordingMetadata::new(vec![shard.clone()]));
    let node = make_query_node(metadata.clone()).await;

    node.query_for_tenant("SELECT * FROM metrics WHERE metric_name = 'cpu_usage'", "0")
        .await
        .unwrap();

    assert_eq!(metadata.requested_shards().await, vec![shard.shard_id]);
}

#[tokio::test]
async fn streaming_query_uses_shard_scoped_metadata_lookup() {
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let shard = shard_for("cpu_usage", ts);
    let metadata = Arc::new(RecordingMetadata::new(vec![shard.clone()]));
    let engine = make_engine().await;
    let (_tx, rx) = broadcast::channel(8);
    let executor = StreamingQueryExecutor::new(engine, metadata.clone(), rx);

    let _receiver = executor
        .execute("SELECT * FROM metrics WHERE metric_name = 'cpu_usage'")
        .await
        .unwrap();

    assert_eq!(metadata.requested_shards().await, vec![shard.shard_id]);
}
