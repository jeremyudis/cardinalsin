use cardinalsin::ingester::ChunkMetadata;
use cardinalsin::metadata::{MetadataClient, S3MetadataClient, S3MetadataConfig};
use object_store::memory::InMemory;
use std::sync::Arc;

#[tokio::test]
async fn local_metadata_rejects_empty_shard_id() {
    let metadata = cardinalsin::metadata::LocalMetadataClient::new();
    let chunk = ChunkMetadata {
        path: "tenant-a/data/shard=path-shard/year=2026/month=03/day=09/hour=00/chunk.parquet"
            .to_string(),
        min_timestamp: 100,
        max_timestamp: 200,
        row_count: 2,
        size_bytes: 128,
        shard_id: String::new(),
    };

    let err = metadata
        .register_chunk(&chunk.path, &chunk)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("shard_id"),
        "registration should fail when shard_id is missing: {err}"
    );
}

#[tokio::test]
async fn object_store_metadata_rejects_empty_shard_id() {
    let store = Arc::new(InMemory::new());
    let client = S3MetadataClient::new(
        store,
        S3MetadataConfig {
            bucket: "test-bucket".to_string(),
            metadata_prefix: "test/".to_string(),
            enable_cache: true,
            allow_unsafe_overwrite: false,
            ..Default::default()
        },
    );

    let chunk = ChunkMetadata {
        path: "tenant-a/data/shard=path-shard/year=2026/month=03/day=09/hour=00/chunk.parquet"
            .to_string(),
        min_timestamp: 100,
        max_timestamp: 200,
        row_count: 2,
        size_bytes: 128,
        shard_id: String::new(),
    };

    let err = client
        .register_chunk(&chunk.path, &chunk)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("shard_id"),
        "registration should fail when shard_id is missing: {err}"
    );
}
