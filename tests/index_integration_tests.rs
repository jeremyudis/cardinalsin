//! End-to-end integration tests for the inverted index.
//!
//! These tests exercise the real ingester → metadata → prefilter path:
//! - Chunks are registered through `MetadataClient::register_chunk` (populating
//!   `shard_id` through the real code path).
//! - Segments are built through `IndexBuilder::build_and_publish_segment`.
//! - Pruning goes through `IndexPrefilter::prune` against `TimeIndexEntry`s
//!   materialized from `ChunkMetadata`.
//!
//! This closes the gap where PR #185's unit tests hand-constructed
//! `TimeIndexEntry { shard_id: Some("shard-0"), .. }` — production callers
//! never did, so the prefilter was a silent no-op.

use cardinalsin::index::{IndexBuilder, IndexConfig, IndexPrefilter};
use cardinalsin::ingester::ChunkMetadata;
use cardinalsin::metadata::predicates::{ColumnPredicate, PredicateValue};
use cardinalsin::metadata::{LocalMetadataClient, MetadataClient, TimeIndexEntry, TimeRange};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use object_store::memory::InMemory;
use std::sync::Arc;

const SHARD: &str = "shard-a";
const TENANT: &str = "tenant-a";

fn make_batch(hosts: &[&str], regions: &[&str]) -> RecordBatch {
    assert_eq!(hosts.len(), regions.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let host = StringArray::from(hosts.to_vec());
    let region = StringArray::from(regions.to_vec());
    let metric = StringArray::from(vec!["m"; hosts.len()]);
    let ts = Int64Array::from(vec![1_000i64; hosts.len()]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(host),
            Arc::new(region),
            Arc::new(metric),
            Arc::new(ts),
        ],
    )
    .unwrap()
}

/// Register a chunk through the real metadata path + build a segment for it.
async fn ingest_chunk(
    metadata: &LocalMetadataClient,
    builder: &IndexBuilder,
    chunk_path: &str,
    batch: &RecordBatch,
    time_range: (i64, i64),
) {
    let chunk = ChunkMetadata {
        path: chunk_path.to_string(),
        min_timestamp: time_range.0,
        max_timestamp: time_range.1,
        row_count: batch.num_rows() as u64,
        size_bytes: 1_024,
        shard_id: Some(SHARD.to_string()),
    };
    metadata.register_chunk(chunk_path, &chunk).await.unwrap();
    builder
        .build_and_publish_segment(batch, chunk_path, SHARD, 0, time_range)
        .await
        .unwrap();
}

async fn get_all_chunks(metadata: &LocalMetadataClient) -> Vec<TimeIndexEntry> {
    metadata
        .get_chunks(TimeRange::new(0, i64::MAX))
        .await
        .unwrap()
}

#[tokio::test]
async fn prune_by_single_eq_predicate() {
    let object_store = Arc::new(InMemory::new());
    let metadata = LocalMetadataClient::new();
    let builder = IndexBuilder::new(object_store.clone(), TENANT, IndexConfig::default());

    ingest_chunk(
        &metadata,
        &builder,
        "chunk_a.parquet",
        &make_batch(&["a", "a"], &["us", "us"]),
        (0, 1_000),
    )
    .await;
    ingest_chunk(
        &metadata,
        &builder,
        "chunk_b.parquet",
        &make_batch(&["b"], &["us"]),
        (1_001, 2_000),
    )
    .await;
    ingest_chunk(
        &metadata,
        &builder,
        "chunk_c.parquet",
        &make_batch(&["c"], &["eu"]),
        (2_001, 3_000),
    )
    .await;

    let chunks = get_all_chunks(&metadata).await;
    assert_eq!(chunks.len(), 3);
    for c in &chunks {
        assert_eq!(
            c.shard_id.as_deref(),
            Some(SHARD),
            "shard_id must survive register_chunk → TimeIndexEntry"
        );
    }

    let prefilter = IndexPrefilter::new(object_store, TENANT);

    let preds = [ColumnPredicate::Eq(
        "host".into(),
        PredicateValue::String("a".into()),
    )];
    let pruned = prefilter.prune(&chunks, &preds).await;
    let paths: Vec<&str> = pruned.iter().map(|c| c.chunk_path.as_str()).collect();
    assert_eq!(paths, vec!["chunk_a.parquet"]);
}

#[tokio::test]
async fn prune_by_compound_and_predicate() {
    let object_store = Arc::new(InMemory::new());
    let metadata = LocalMetadataClient::new();
    let builder = IndexBuilder::new(object_store.clone(), TENANT, IndexConfig::default());

    // host=a region=us — should match
    ingest_chunk(
        &metadata,
        &builder,
        "chunk_aus.parquet",
        &make_batch(&["a"], &["us"]),
        (0, 1_000),
    )
    .await;
    // host=a region=eu — host matches but region doesn't
    ingest_chunk(
        &metadata,
        &builder,
        "chunk_aeu.parquet",
        &make_batch(&["a"], &["eu"]),
        (1_001, 2_000),
    )
    .await;
    // host=b region=us — region matches but host doesn't
    ingest_chunk(
        &metadata,
        &builder,
        "chunk_bus.parquet",
        &make_batch(&["b"], &["us"]),
        (2_001, 3_000),
    )
    .await;

    let chunks = get_all_chunks(&metadata).await;
    assert_eq!(chunks.len(), 3);

    let prefilter = IndexPrefilter::new(object_store, TENANT);

    let pred = ColumnPredicate::And(
        Box::new(ColumnPredicate::Eq(
            "host".into(),
            PredicateValue::String("a".into()),
        )),
        Box::new(ColumnPredicate::Eq(
            "region".into(),
            PredicateValue::String("us".into()),
        )),
    );
    let pruned = prefilter.prune(&chunks, &[pred]).await;
    let paths: Vec<&str> = pruned.iter().map(|c| c.chunk_path.as_str()).collect();
    assert_eq!(
        paths,
        vec!["chunk_aus.parquet"],
        "compound AND must intersect host + region bitmaps"
    );
}

#[tokio::test]
async fn pruning_pass_through_when_no_index() {
    // Register chunks WITHOUT building segments. Prefilter must pass all
    // chunks through rather than prune them silently.
    let object_store = Arc::new(InMemory::new());
    let metadata = LocalMetadataClient::new();

    for (path, host) in [
        ("chunk_a.parquet", "a"),
        ("chunk_b.parquet", "b"),
        ("chunk_c.parquet", "c"),
    ] {
        let chunk = ChunkMetadata {
            path: path.to_string(),
            min_timestamp: 0,
            max_timestamp: 1_000,
            row_count: 1,
            size_bytes: 1_024,
            shard_id: Some(SHARD.to_string()),
        };
        metadata.register_chunk(path, &chunk).await.unwrap();
        let _ = host; // intentionally no segment built
    }

    let chunks = get_all_chunks(&metadata).await;
    let prefilter = IndexPrefilter::new(object_store, TENANT);
    let preds = [ColumnPredicate::Eq(
        "host".into(),
        PredicateValue::String("a".into()),
    )];
    let pruned = prefilter.prune(&chunks, &preds).await;
    assert_eq!(
        pruned.len(),
        3,
        "no index ⇒ must pass all chunks through (safety)"
    );
}

#[tokio::test]
async fn batcher_coalesces_chunks_into_single_segment() {
    // Enqueue N chunks below the chunk threshold, then force flush_all.
    // Only one segment must land in the manifest, and pruning must still
    // resolve ordinals back to individual chunk paths.
    let object_store = Arc::new(InMemory::new());
    let metadata = LocalMetadataClient::new();
    let config = IndexConfig {
        batch_max_chunks: 100,
        batch_max_rows: 10_000_000,
        batch_max_age_secs: 60,
        ..IndexConfig::default()
    };
    let builder = IndexBuilder::new(object_store.clone(), TENANT, config);

    let batches = [
        ("c_a.parquet", "a", "us", (0, 100)),
        ("c_b.parquet", "b", "us", (101, 200)),
        ("c_c.parquet", "c", "eu", (201, 300)),
    ];
    for (path, host, region, (lo, hi)) in batches {
        let chunk = ChunkMetadata {
            path: path.to_string(),
            min_timestamp: lo,
            max_timestamp: hi,
            row_count: 1,
            size_bytes: 1_024,
            shard_id: Some(SHARD.to_string()),
        };
        metadata.register_chunk(path, &chunk).await.unwrap();
        builder
            .enqueue_chunk(make_batch(&[host], &[region]), path, SHARD, 0, (lo, hi))
            .await
            .unwrap();
    }

    // Nothing flushed yet — all under thresholds.
    let chunks = get_all_chunks(&metadata).await;
    let prefilter = IndexPrefilter::new(object_store.clone(), TENANT);
    let preds = [ColumnPredicate::Eq(
        "host".into(),
        PredicateValue::String("a".into()),
    )];
    let pruned_before = prefilter.prune(&chunks, &preds).await;
    assert_eq!(
        pruned_before.len(),
        3,
        "no segments published yet ⇒ passthrough"
    );

    builder.flush_all(0).await.unwrap();

    // One segment now covers all three chunks — pruning must resolve to
    // the single matching chunk path.
    let prefilter = IndexPrefilter::new(object_store, TENANT);
    let pruned = prefilter.prune(&chunks, &preds).await;
    let paths: Vec<&str> = pruned.iter().map(|c| c.chunk_path.as_str()).collect();
    assert_eq!(
        paths,
        vec!["c_a.parquet"],
        "batched segment must still resolve ordinals back to individual chunks"
    );
}
