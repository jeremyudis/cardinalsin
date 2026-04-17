//! Tests for the CSI inverted index: segment serialization, FST construction,
//! roaring bitmap operations, segment merge, manifest CAS, and query correctness.

use cardinalsin::index::config::IndexConfig;
use cardinalsin::index::fst_builder::FstTermBuilder;
use cardinalsin::index::manifest::{IndexManifest, ManifestClient, SegmentEntry};
use cardinalsin::index::merge::SegmentMerger;
use cardinalsin::index::postings::PostingsList;
use cardinalsin::index::segment::{ChunkOrdinalTable, SegmentReader, SegmentWriter};
use cardinalsin::index::{IndexBuilder, IndexPrefilter};
use cardinalsin::metadata::predicates::{ColumnPredicate, PredicateValue};
use cardinalsin::metadata::TimeIndexEntry;

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use object_store::memory::InMemory;
use std::sync::Arc;

// ────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────

fn make_batch(hosts: &[&str], metrics: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value_f64", DataType::Float64, false),
    ]));
    let host_arr = StringArray::from(hosts.to_vec());
    let metric_arr = StringArray::from(metrics.to_vec());
    let ts_arr = arrow_array::Int64Array::from(vec![1_000_000_000i64; hosts.len()]);
    let val_arr = arrow_array::Float64Array::from(vec![42.0; hosts.len()]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(host_arr),
            Arc::new(metric_arr),
            Arc::new(ts_arr),
            Arc::new(val_arr),
        ],
    )
    .unwrap()
}

fn make_fst_and_segment(hosts: &[&str], metrics: &[&str], chunk_path: &str) -> Vec<u8> {
    let batch = make_batch(hosts, metrics);
    let ordinals = vec![0u32; batch.num_rows()];
    let config = IndexConfig::default();
    let columns =
        FstTermBuilder::build_all_columns(&batch, &ordinals, &config.skip_columns, 100_000)
            .unwrap();
    let ordinal_table = ChunkOrdinalTable::new(vec![(0, chunk_path.to_string())]);
    SegmentWriter::write_segment(&ordinal_table, &columns).unwrap()
}

// ────────────────────────────────────────────────────────────────────
// 1. Segment Serialization (8 tests)
// ────────────────────────────────────────────────────────────────────

#[test]
fn test_segment_roundtrip_single_column() {
    let data = make_fst_and_segment(&["web-01"], &["cpu"], "chunk_a.parquet");
    let reader = SegmentReader::open(data).unwrap();
    assert!(reader.indexed_columns().contains(&"host".to_string()));
    assert!(reader
        .indexed_columns()
        .contains(&"metric_name".to_string()));
}

#[test]
fn test_segment_roundtrip_multi_column() {
    let data = make_fst_and_segment(
        &["web-01", "web-02", "web-03"],
        &["cpu", "mem", "disk"],
        "chunk_b.parquet",
    );
    let reader = SegmentReader::open(data).unwrap();

    // Look up host=web-01 → should find ordinal 0
    let result = reader.lookup("host", "web-01").unwrap().unwrap();
    assert!(result.ordinals().contains(&0));

    // Look up host=web-02 → should find ordinal 0
    let result = reader.lookup("host", "web-02").unwrap().unwrap();
    assert!(result.ordinals().contains(&0));
}

#[test]
fn test_segment_header_footer_validation() {
    let data = make_fst_and_segment(&["web-01"], &["cpu"], "chunk.parquet");

    // Corrupt header magic — also update file CRC so the CRC check passes
    // but the header magic check catches the corruption.
    let mut bad = data.clone();
    bad[0] = b'Z'; // 'C' -> 'Z'
                   // Recompute file CRC (stored at len-8..len-4) so CRC check passes
    let file_crc = crc32fast::hash(&bad[..bad.len() - 8]);
    let len = bad.len();
    bad[len - 8..len - 4].copy_from_slice(&file_crc.to_le_bytes());
    assert!(SegmentReader::open(bad).is_err());

    // Corrupt footer magic — "CSIX" ends with 'X' so use a different byte
    let mut bad = data.clone();
    let len = bad.len();
    bad[len - 2] = b'Z'; // 'I' -> 'Z' in "CSIX"
    assert!(SegmentReader::open(bad).is_err());
}

#[test]
fn test_segment_corrupt_crc_detection() {
    let mut data = make_fst_and_segment(&["web-01"], &["cpu"], "chunk.parquet");
    // Flip a byte in the middle (section data)
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    // CRC mismatch should be caught on open
    assert!(SegmentReader::open(data).is_err());
}

#[test]
fn test_segment_empty_postings() {
    // Build a segment with no string columns (only skipped columns)
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value_f64", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1i64])),
            Arc::new(arrow_array::Float64Array::from(vec![1.0])),
        ],
    )
    .unwrap();
    let ordinals = vec![0u32];
    let config = IndexConfig::default();
    let columns =
        FstTermBuilder::build_all_columns(&batch, &ordinals, &config.skip_columns, 100_000)
            .unwrap();

    // No indexable columns, so columns should be empty
    assert!(columns.is_empty());
}

#[test]
fn test_segment_large_ordinal_table() {
    let mut entries = Vec::new();
    for i in 0..100 {
        entries.push((i as u32, format!("chunk_{i}.parquet")));
    }
    let ordinal_table = ChunkOrdinalTable::new(entries);
    let columns = vec![]; // No columns -- trivial segment
    let data = SegmentWriter::write_segment(&ordinal_table, &columns).unwrap();
    let reader = SegmentReader::open(data).unwrap();
    assert_eq!(reader.ordinal_table().entries.len(), 100);
    assert_eq!(reader.ordinal_table().resolve(50), Some("chunk_50.parquet"));
}

#[test]
fn test_segment_unicode_column_names() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("hôst_名前", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["web-01"])),
            Arc::new(arrow_array::Int64Array::from(vec![1i64])),
        ],
    )
    .unwrap();
    let ordinals = vec![0u32];
    let config = IndexConfig::default();
    let columns =
        FstTermBuilder::build_all_columns(&batch, &ordinals, &config.skip_columns, 100_000)
            .unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].column_name, "hôst_名前");

    let ordinal_table = ChunkOrdinalTable::new(vec![(0, "chunk.parquet".into())]);
    let data = SegmentWriter::write_segment(&ordinal_table, &columns).unwrap();
    let reader = SegmentReader::open(data).unwrap();
    assert!(reader.indexed_columns().contains(&"hôst_名前".to_string()));
}

#[test]
fn test_segment_too_short() {
    assert!(SegmentReader::open(vec![0u8; 4]).is_err());
}

// ────────────────────────────────────────────────────────────────────
// 2. FST Construction (6 tests)
// ────────────────────────────────────────────────────────────────────

#[test]
fn test_fst_sorted_keys() {
    let batch = make_batch(&["zebra", "alpha", "middle"], &["cpu", "cpu", "cpu"]);
    let ordinals = vec![0u32; 3];
    let fst_data = FstTermBuilder::build_column_fst("host", &batch, &ordinals).unwrap();

    // FST keys must be in sorted order (alpha, middle, zebra)
    let fst_map = fst::Map::new(fst_data.fst_bytes).unwrap();
    assert!(fst_map.get(b"alpha").is_some());
    assert!(fst_map.get(b"middle").is_some());
    assert!(fst_map.get(b"zebra").is_some());
    // Verify lexicographic order via output values
    let alpha_idx = fst_map.get(b"alpha").unwrap();
    let middle_idx = fst_map.get(b"middle").unwrap();
    let zebra_idx = fst_map.get(b"zebra").unwrap();
    assert!(alpha_idx < middle_idx);
    assert!(middle_idx < zebra_idx);
}

#[test]
fn test_fst_null_values_skipped() {
    let schema = Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)]));
    let host_arr = StringArray::from(vec![Some("web-01"), None, Some("web-02")]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(host_arr)]).unwrap();
    let ordinals = vec![0u32; 3];
    let fst_data = FstTermBuilder::build_column_fst("host", &batch, &ordinals).unwrap();

    let fst_map = fst::Map::new(fst_data.fst_bytes).unwrap();
    assert!(fst_map.get(b"web-01").is_some());
    assert!(fst_map.get(b"web-02").is_some());
    assert_eq!(fst_data.postings.len(), 2); // Only 2 unique non-null values
}

#[test]
fn test_fst_empty_column() {
    let schema = Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)]));
    let host_arr = StringArray::from(vec![None::<&str>, None, None]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(host_arr)]).unwrap();
    let ordinals = vec![0u32; 3];
    let fst_data = FstTermBuilder::build_column_fst("host", &batch, &ordinals).unwrap();
    assert!(fst_data.postings.is_empty());
}

#[test]
fn test_fst_cardinality_limit() {
    // Build batch with 10 distinct values
    let hosts: Vec<&str> = (0..10)
        .map(|i| match i {
            0 => "a",
            1 => "b",
            2 => "c",
            3 => "d",
            4 => "e",
            5 => "f",
            6 => "g",
            7 => "h",
            8 => "i",
            _ => "j",
        })
        .collect();
    let batch = make_batch(&hosts, &["cpu"; 10]);
    let ordinals = vec![0u32; 10];

    // With max_cardinality=5, the host column should be skipped
    let columns = FstTermBuilder::build_all_columns(
        &batch,
        &ordinals,
        &["timestamp".into(), "value_f64".into()],
        5,
    )
    .unwrap();
    // host has 10 distinct values > 5, so it should be excluded
    let host_col = columns.iter().find(|c| c.column_name == "host");
    assert!(host_col.is_none());
}

#[test]
fn test_fst_missing_column_error() {
    let batch = make_batch(&["web-01"], &["cpu"]);
    let ordinals = vec![0u32];
    let result = FstTermBuilder::build_column_fst("nonexistent", &batch, &ordinals);
    assert!(result.is_err());
}

#[test]
fn test_fst_prefix_sharing() {
    // FST should efficiently share prefixes for similar keys
    let hosts: Vec<&str> = vec!["web-server-001", "web-server-002", "web-server-003"];
    let batch = make_batch(&hosts, &["cpu"; 3]);
    let ordinals = vec![0u32; 3];
    let fst_data = FstTermBuilder::build_column_fst("host", &batch, &ordinals).unwrap();

    // FST should be smaller than naive key storage
    let total_key_bytes: usize = hosts.iter().map(|h| h.len()).sum();
    assert!(fst_data.fst_bytes.len() < total_key_bytes + 100);
}

// ────────────────────────────────────────────────────────────────────
// 3. Roaring Bitmap Operations (5 tests)
// ────────────────────────────────────────────────────────────────────

#[test]
fn test_roaring_and_intersection() {
    let mut a = PostingsList::new();
    a.add(1);
    a.add(2);
    a.add(3);
    let mut b = PostingsList::new();
    b.add(2);
    b.add(3);
    b.add(4);

    let result = a.intersect(&b);
    assert_eq!(result.ordinals(), vec![2, 3]);
}

#[test]
fn test_roaring_or_union() {
    let mut a = PostingsList::new();
    a.add(1);
    a.add(2);
    let mut b = PostingsList::new();
    b.add(2);
    b.add(3);

    let result = a.union(&b);
    assert_eq!(result.ordinals(), vec![1, 2, 3]);
}

#[test]
fn test_roaring_difference() {
    let mut a = PostingsList::new();
    a.add(1);
    a.add(2);
    a.add(3);
    let mut b = PostingsList::new();
    b.add(2);

    let result = a.difference(&b);
    assert_eq!(result.ordinals(), vec![1, 3]);
}

#[test]
fn test_roaring_serialize_deserialize() {
    let mut pl = PostingsList::new();
    pl.add(10);
    pl.add(20);
    pl.add(30);

    let bytes = pl.serialize();
    let deserialized = PostingsList::deserialize(&bytes).unwrap();
    assert_eq!(deserialized.ordinals(), vec![10, 20, 30]);
}

#[test]
fn test_roaring_empty_intersection() {
    let mut a = PostingsList::new();
    a.add(1);
    a.add(2);
    let mut b = PostingsList::new();
    b.add(3);
    b.add(4);

    let result = a.intersect(&b);
    assert!(result.is_empty());
    assert_eq!(result.len(), 0);
}

// ────────────────────────────────────────────────────────────────────
// 4. Segment Merge (6 tests)
// ────────────────────────────────────────────────────────────────────

#[test]
fn test_merge_two_single_chunk_segments() {
    let seg_a = make_fst_and_segment(&["web-01"], &["cpu"], "chunk_a.parquet");
    let seg_b = make_fst_and_segment(&["web-02"], &["mem"], "chunk_b.parquet");

    let reader_a = SegmentReader::open(seg_a).unwrap();
    let reader_b = SegmentReader::open(seg_b).unwrap();

    let merged =
        SegmentMerger::merge_segments(&[reader_a, reader_b], &["merged.parquet".into()]).unwrap();

    let merged_reader = SegmentReader::open(merged).unwrap();

    // Should have host and metric_name columns
    let cols = merged_reader.indexed_columns();
    assert!(cols.contains(&"host".to_string()));
    assert!(cols.contains(&"metric_name".to_string()));

    // All terms should resolve to ordinal 0 (single output chunk)
    let result = merged_reader.lookup("host", "web-01").unwrap().unwrap();
    assert!(result.ordinals().contains(&0));
    let result = merged_reader.lookup("host", "web-02").unwrap().unwrap();
    assert!(result.ordinals().contains(&0));
}

#[test]
fn test_merge_five_segments() {
    let segments: Vec<Vec<u8>> = (0..5)
        .map(|i| {
            make_fst_and_segment(
                &[&format!("host-{i}")],
                &["cpu"],
                &format!("chunk_{i}.parquet"),
            )
        })
        .collect();

    let readers: Vec<SegmentReader> = segments
        .into_iter()
        .map(|s| SegmentReader::open(s).unwrap())
        .collect();

    let merged = SegmentMerger::merge_segments(&readers, &["merged.parquet".into()]).unwrap();

    let reader = SegmentReader::open(merged).unwrap();
    for i in 0..5 {
        let result = reader
            .lookup("host", &format!("host-{i}"))
            .unwrap()
            .unwrap();
        assert!(!result.is_empty());
    }
}

#[test]
fn test_merge_ordinal_remapping() {
    let seg_a = make_fst_and_segment(&["web-01"], &["cpu"], "chunk_a.parquet");
    let seg_b = make_fst_and_segment(&["web-01"], &["cpu"], "chunk_b.parquet");

    let reader_a = SegmentReader::open(seg_a).unwrap();
    let reader_b = SegmentReader::open(seg_b).unwrap();

    // Merge into single output
    let merged =
        SegmentMerger::merge_segments(&[reader_a, reader_b], &["merged.parquet".into()]).unwrap();

    let reader = SegmentReader::open(merged).unwrap();

    // web-01 should resolve to ordinal 0
    let result = reader.lookup("host", "web-01").unwrap().unwrap();
    assert_eq!(result.ordinals(), vec![0]);
}

#[test]
fn test_merge_disjoint_columns() {
    // Segment A has column "host", segment B has column "region"
    let schema_a = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let batch_a = RecordBatch::try_new(
        schema_a,
        vec![
            Arc::new(StringArray::from(vec!["web-01"])),
            Arc::new(arrow_array::Int64Array::from(vec![1i64])),
        ],
    )
    .unwrap();

    let schema_b = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let batch_b = RecordBatch::try_new(
        schema_b,
        vec![
            Arc::new(StringArray::from(vec!["us-east-1"])),
            Arc::new(arrow_array::Int64Array::from(vec![2i64])),
        ],
    )
    .unwrap();

    let config = IndexConfig::default();

    let cols_a =
        FstTermBuilder::build_all_columns(&batch_a, &[0u32], &config.skip_columns, 100_000)
            .unwrap();
    let ot_a = ChunkOrdinalTable::new(vec![(0, "chunk_a.parquet".into())]);
    let seg_a_bytes = SegmentWriter::write_segment(&ot_a, &cols_a).unwrap();

    let cols_b =
        FstTermBuilder::build_all_columns(&batch_b, &[0u32], &config.skip_columns, 100_000)
            .unwrap();
    let ot_b = ChunkOrdinalTable::new(vec![(0, "chunk_b.parquet".into())]);
    let seg_b_bytes = SegmentWriter::write_segment(&ot_b, &cols_b).unwrap();

    let reader_a = SegmentReader::open(seg_a_bytes).unwrap();
    let reader_b = SegmentReader::open(seg_b_bytes).unwrap();

    let merged =
        SegmentMerger::merge_segments(&[reader_a, reader_b], &["merged.parquet".into()]).unwrap();

    let reader = SegmentReader::open(merged).unwrap();
    let cols = reader.indexed_columns();
    assert!(cols.contains(&"host".to_string()));
    assert!(cols.contains(&"region".to_string()));
}

#[test]
fn test_merge_overlapping_terms() {
    // Both segments have host=web-01
    let seg_a = make_fst_and_segment(&["web-01"], &["cpu"], "chunk_a.parquet");
    let seg_b = make_fst_and_segment(&["web-01", "web-02"], &["cpu", "mem"], "chunk_b.parquet");

    let reader_a = SegmentReader::open(seg_a).unwrap();
    let reader_b = SegmentReader::open(seg_b).unwrap();

    let merged =
        SegmentMerger::merge_segments(&[reader_a, reader_b], &["merged.parquet".into()]).unwrap();

    let reader = SegmentReader::open(merged).unwrap();

    // web-01 should exist and map to ordinal 0
    let result = reader.lookup("host", "web-01").unwrap().unwrap();
    assert!(!result.is_empty());

    // web-02 should also exist
    let result = reader.lookup("host", "web-02").unwrap().unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_merge_empty_segment_handling() {
    let seg = make_fst_and_segment(&["web-01"], &["cpu"], "chunk.parquet");
    let reader = SegmentReader::open(seg).unwrap();

    // Merge single segment (degenerate case)
    let merged = SegmentMerger::merge_segments(&[reader], &["merged.parquet".into()]).unwrap();

    let merged_reader = SegmentReader::open(merged).unwrap();
    let result = merged_reader.lookup("host", "web-01").unwrap().unwrap();
    assert!(!result.is_empty());
}

// ────────────────────────────────────────────────────────────────────
// 5. Manifest CAS (5 tests)
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_manifest_create_and_load() {
    let store = Arc::new(InMemory::new());
    let client = ManifestClient::new(store, "tenant-1");

    let mut manifest = IndexManifest::new("shard-0");
    manifest.segments.push(SegmentEntry {
        path: "seg_0.csi".into(),
        min_time_ns: 1000,
        max_time_ns: 2000,
        chunk_count: 1,
        level: 0,
        size_bytes: 512,
        created_at: "2026-01-01T00:00:00Z".into(),
    });
    client.create_manifest(&manifest).await.unwrap();

    let (loaded, _etag) = client.load_manifest("shard-0").await.unwrap().unwrap();
    assert_eq!(loaded.shard_id, "shard-0");
    assert_eq!(loaded.segments.len(), 1);
    assert_eq!(loaded.segments[0].path, "seg_0.csi");
}

#[tokio::test]
async fn test_manifest_missing_returns_none() {
    let store = Arc::new(InMemory::new());
    let client = ManifestClient::new(store, "tenant-1");

    let result = client.load_manifest("nonexistent").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_manifest_freeze_unfreeze() {
    let store = Arc::new(InMemory::new());
    let client = ManifestClient::new(store, "tenant-1");

    let manifest = IndexManifest::new("shard-0");
    client.create_manifest(&manifest).await.unwrap();

    client.freeze_manifest("shard-0").await.unwrap();
    let (loaded, _) = client.load_manifest("shard-0").await.unwrap().unwrap();
    assert!(loaded.frozen);

    client.unfreeze_manifest("shard-0").await.unwrap();
    let (loaded, _) = client.load_manifest("shard-0").await.unwrap().unwrap();
    assert!(!loaded.frozen);
}

#[tokio::test]
async fn test_manifest_empty_segments() {
    let store = Arc::new(InMemory::new());
    let client = ManifestClient::new(store, "tenant-1");

    let manifest = IndexManifest::new("shard-0");
    client.create_manifest(&manifest).await.unwrap();

    let (loaded, _) = client.load_manifest("shard-0").await.unwrap().unwrap();
    assert!(loaded.segments.is_empty());
    assert_eq!(loaded.generation, 0);
}

#[tokio::test]
async fn test_manifest_add_remove_entries() {
    let store = Arc::new(InMemory::new());
    let client = ManifestClient::new(store, "tenant-1");

    let mut manifest = IndexManifest::new("shard-0");
    manifest.segments.push(SegmentEntry {
        path: "seg_0.csi".into(),
        min_time_ns: 1000,
        max_time_ns: 2000,
        chunk_count: 1,
        level: 0,
        size_bytes: 512,
        created_at: "2026-01-01T00:00:00Z".into(),
    });
    manifest.segments.push(SegmentEntry {
        path: "seg_1.csi".into(),
        min_time_ns: 2000,
        max_time_ns: 3000,
        chunk_count: 1,
        level: 0,
        size_bytes: 256,
        created_at: "2026-01-01T00:01:00Z".into(),
    });
    client.create_manifest(&manifest).await.unwrap();

    let (mut loaded, etag) = client.load_manifest("shard-0").await.unwrap().unwrap();
    assert_eq!(loaded.segments.len(), 2);

    // Remove seg_0, add seg_2
    loaded.segments.retain(|s| s.path != "seg_0.csi");
    loaded.segments.push(SegmentEntry {
        path: "seg_2.csi".into(),
        min_time_ns: 1000,
        max_time_ns: 3000,
        chunk_count: 2,
        level: 1,
        size_bytes: 700,
        created_at: "2026-01-01T00:02:00Z".into(),
    });
    loaded.generation += 1;
    client.save_manifest(&loaded, &etag).await.unwrap();

    let (final_manifest, _) = client.load_manifest("shard-0").await.unwrap().unwrap();
    assert_eq!(final_manifest.segments.len(), 2);
    let paths: Vec<&str> = final_manifest
        .segments
        .iter()
        .map(|s| s.path.as_str())
        .collect();
    assert!(paths.contains(&"seg_1.csi"));
    assert!(paths.contains(&"seg_2.csi"));
}

// ────────────────────────────────────────────────────────────────────
// 6. Query Correctness & E2E (10 tests)
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_eq_predicate_pruning() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let builder = IndexBuilder::new(store.clone(), "tenant-1", IndexConfig::default());

    // Build segments for two chunks with different hosts
    let batch_a = make_batch(&["web-01", "web-01"], &["cpu", "cpu"]);
    builder
        .build_and_publish_segment(&batch_a, "chunk_a.parquet", "shard-0", 0, (1000, 2000))
        .await
        .unwrap();

    let batch_b = make_batch(&["web-02", "web-02"], &["mem", "mem"]);
    builder
        .build_and_publish_segment(&batch_b, "chunk_b.parquet", "shard-0", 0, (1000, 2000))
        .await
        .unwrap();

    // Query with host=web-01
    let prefilter = IndexPrefilter::new(store.clone(), "tenant-1");
    let chunks = vec![
        TimeIndexEntry {
            chunk_path: "chunk_a.parquet".into(),
            min_timestamp: 1000,
            max_timestamp: 2000,
            row_count: 2,
            size_bytes: 100,
            shard_id: Some("shard-0".into()),
        },
        TimeIndexEntry {
            chunk_path: "chunk_b.parquet".into(),
            min_timestamp: 1000,
            max_timestamp: 2000,
            row_count: 2,
            size_bytes: 100,
            shard_id: Some("shard-0".into()),
        },
    ];

    let predicates = vec![ColumnPredicate::Eq(
        "host".into(),
        PredicateValue::String("web-01".into()),
    )];

    let result = prefilter.prune(&chunks, &predicates).await;
    // Should only include chunk_a (which has web-01)
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].chunk_path, "chunk_a.parquet");
}

#[tokio::test]
async fn test_in_predicate_pruning() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let builder = IndexBuilder::new(store.clone(), "tenant-1", IndexConfig::default());

    let batch_a = make_batch(&["web-01"], &["cpu"]);
    builder
        .build_and_publish_segment(&batch_a, "chunk_a.parquet", "shard-0", 0, (1000, 2000))
        .await
        .unwrap();

    let batch_b = make_batch(&["web-02"], &["mem"]);
    builder
        .build_and_publish_segment(&batch_b, "chunk_b.parquet", "shard-0", 0, (1000, 2000))
        .await
        .unwrap();

    let batch_c = make_batch(&["web-03"], &["disk"]);
    builder
        .build_and_publish_segment(&batch_c, "chunk_c.parquet", "shard-0", 0, (1000, 2000))
        .await
        .unwrap();

    let prefilter = IndexPrefilter::new(store.clone(), "tenant-1");
    let chunks = vec![
        TimeIndexEntry {
            chunk_path: "chunk_a.parquet".into(),
            min_timestamp: 1000,
            max_timestamp: 2000,
            row_count: 1,
            size_bytes: 50,
            shard_id: Some("shard-0".into()),
        },
        TimeIndexEntry {
            chunk_path: "chunk_b.parquet".into(),
            min_timestamp: 1000,
            max_timestamp: 2000,
            row_count: 1,
            size_bytes: 50,
            shard_id: Some("shard-0".into()),
        },
        TimeIndexEntry {
            chunk_path: "chunk_c.parquet".into(),
            min_timestamp: 1000,
            max_timestamp: 2000,
            row_count: 1,
            size_bytes: 50,
            shard_id: Some("shard-0".into()),
        },
    ];

    let predicates = vec![ColumnPredicate::In(
        "host".into(),
        vec![
            PredicateValue::String("web-01".into()),
            PredicateValue::String("web-02".into()),
        ],
    )];

    let result = prefilter.prune(&chunks, &predicates).await;
    assert_eq!(result.len(), 2);
    let paths: Vec<&str> = result.iter().map(|r| r.chunk_path.as_str()).collect();
    assert!(paths.contains(&"chunk_a.parquet"));
    assert!(paths.contains(&"chunk_b.parquet"));
}

#[tokio::test]
async fn test_missing_manifest_passthrough() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let prefilter = IndexPrefilter::new(store.clone(), "tenant-1");

    let chunks = vec![TimeIndexEntry {
        chunk_path: "chunk.parquet".into(),
        min_timestamp: 1000,
        max_timestamp: 2000,
        row_count: 1,
        size_bytes: 50,
        shard_id: Some("shard-0".into()),
    }];

    let predicates = vec![ColumnPredicate::Eq(
        "host".into(),
        PredicateValue::String("web-01".into()),
    )];

    let result = prefilter.prune(&chunks, &predicates).await;
    // No manifest → all chunks pass through
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_frozen_manifest_passthrough() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let manifest_client = ManifestClient::new(store.clone(), "tenant-1");

    let manifest = IndexManifest::new("shard-0");
    manifest_client.create_manifest(&manifest).await.unwrap();
    manifest_client.freeze_manifest("shard-0").await.unwrap();

    let prefilter = IndexPrefilter::new(store.clone(), "tenant-1");
    let chunks = vec![TimeIndexEntry {
        chunk_path: "chunk.parquet".into(),
        min_timestamp: 1000,
        max_timestamp: 2000,
        row_count: 1,
        size_bytes: 50,
        shard_id: Some("shard-0".into()),
    }];

    let predicates = vec![ColumnPredicate::Eq(
        "host".into(),
        PredicateValue::String("web-01".into()),
    )];

    let result = prefilter.prune(&chunks, &predicates).await;
    // Frozen manifest → all chunks pass through
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_non_indexable_predicates_passthrough() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let prefilter = IndexPrefilter::new(store.clone(), "tenant-1");

    let chunks = vec![TimeIndexEntry {
        chunk_path: "chunk.parquet".into(),
        min_timestamp: 1000,
        max_timestamp: 2000,
        row_count: 1,
        size_bytes: 50,
        shard_id: Some("shard-0".into()),
    }];

    // Int64 predicates are not indexable by the inverted index
    let predicates = vec![ColumnPredicate::Gt(
        "value".into(),
        PredicateValue::Int64(100),
    )];

    let result = prefilter.prune(&chunks, &predicates).await;
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_unindexed_column_predicate_no_false_negatives() {
    // Regression test: when a predicate targets a column not indexed in a segment,
    // the segment must NOT falsely prune chunks. Previously, an unindexed column
    // caused the predicate to be silently skipped, which could lead to false
    // negatives when ANDed with an indexed predicate that matched fewer chunks.
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let builder = IndexBuilder::new(store.clone(), "t1", IndexConfig::default());

    // Build segment with only "host" and "metric_name" indexed (default skip_columns
    // excludes timestamp/value_f64). No "region" column → it won't be indexed.
    let batch = make_batch(&["web-01", "web-02"], &["cpu", "mem"]);
    builder
        .build_and_publish_segment(&batch, "chunk_a.parquet", "shard-0", 0, (100, 200))
        .await
        .unwrap();

    let prefilter = IndexPrefilter::new(store, "t1");
    let chunks = vec![TimeIndexEntry {
        chunk_path: "chunk_a.parquet".into(),
        min_timestamp: 100,
        max_timestamp: 200,
        row_count: 2,
        size_bytes: 64,
        shard_id: Some("shard-0".into()),
    }];

    // Predicate on "host" (indexed) AND "region" (not indexed).
    // "host=web-01" matches, and "region=us-east-1" is not in the index.
    // The chunk MUST still be returned -- region predicate cannot prune.
    let result = prefilter
        .prune(
            &chunks,
            &[
                ColumnPredicate::Eq("host".into(), PredicateValue::String("web-01".into())),
                ColumnPredicate::Eq("region".into(), PredicateValue::String("us-east-1".into())),
            ],
        )
        .await;
    assert_eq!(
        result.len(),
        1,
        "Chunk must not be pruned when a predicate targets a non-indexed column"
    );

    // Also test: ONLY a non-indexed column predicate. All chunks must pass through.
    let result2 = prefilter
        .prune(
            &chunks,
            &[ColumnPredicate::Eq(
                "region".into(),
                PredicateValue::String("us-east-1".into()),
            )],
        )
        .await;
    assert_eq!(
        result2.len(),
        1,
        "Chunk must not be pruned when all predicates target non-indexed columns"
    );
}

#[tokio::test]
async fn test_build_at_ingest_then_query() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let builder = IndexBuilder::new(store.clone(), "t1", IndexConfig::default());

    // Simulate ingest: build segment per chunk
    let batch = make_batch(&["web-01", "web-02", "web-03"], &["cpu", "cpu", "cpu"]);
    builder
        .build_and_publish_segment(&batch, "chunk_1.parquet", "shard-0", 0, (100, 300))
        .await
        .unwrap();

    // Query: prune using index
    let prefilter = IndexPrefilter::new(store, "t1");
    let chunks = vec![TimeIndexEntry {
        chunk_path: "chunk_1.parquet".into(),
        min_timestamp: 100,
        max_timestamp: 300,
        row_count: 3,
        size_bytes: 128,
        shard_id: Some("shard-0".into()),
    }];

    // Predicate that matches
    let hit = prefilter
        .prune(
            &chunks,
            &[ColumnPredicate::Eq(
                "host".into(),
                PredicateValue::String("web-02".into()),
            )],
        )
        .await;
    assert_eq!(hit.len(), 1);

    // Predicate that doesn't match any value in the chunk
    let miss = prefilter
        .prune(
            &chunks,
            &[ColumnPredicate::Eq(
                "host".into(),
                PredicateValue::String("web-99".into()),
            )],
        )
        .await;
    // web-99 doesn't exist, lookup returns empty bitmap → chunk NOT matched
    // BUT the chunk IS in the indexed set → it should be pruned
    // Actually: FST lookup for non-existent key returns empty PostingsList
    // So combined bitmap is empty → no ordinals matched → chunk_1 not in matching_paths
    // But chunk_1 IS in indexed_paths → it gets pruned!
    assert_eq!(miss.len(), 0);
}

#[tokio::test]
async fn test_multi_shard_pruning() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let builder = IndexBuilder::new(store.clone(), "t1", IndexConfig::default());

    // Shard 0: web-01
    let batch_a = make_batch(&["web-01"], &["cpu"]);
    builder
        .build_and_publish_segment(&batch_a, "s0_chunk.parquet", "shard-0", 0, (100, 200))
        .await
        .unwrap();

    // Shard 1: web-02
    let batch_b = make_batch(&["web-02"], &["mem"]);
    builder
        .build_and_publish_segment(&batch_b, "s1_chunk.parquet", "shard-1", 0, (100, 200))
        .await
        .unwrap();

    let prefilter = IndexPrefilter::new(store, "t1");
    let chunks = vec![
        TimeIndexEntry {
            chunk_path: "s0_chunk.parquet".into(),
            min_timestamp: 100,
            max_timestamp: 200,
            row_count: 1,
            size_bytes: 50,
            shard_id: Some("shard-0".into()),
        },
        TimeIndexEntry {
            chunk_path: "s1_chunk.parquet".into(),
            min_timestamp: 100,
            max_timestamp: 200,
            row_count: 1,
            size_bytes: 50,
            shard_id: Some("shard-1".into()),
        },
    ];

    let result = prefilter
        .prune(
            &chunks,
            &[ColumnPredicate::Eq(
                "host".into(),
                PredicateValue::String("web-01".into()),
            )],
        )
        .await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].chunk_path, "s0_chunk.parquet");
}

#[tokio::test]
async fn test_merge_then_query() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let builder = IndexBuilder::new(store.clone(), "t1", IndexConfig::default());

    // Build two segments
    let batch_a = make_batch(&["web-01"], &["cpu"]);
    builder
        .build_and_publish_segment(&batch_a, "chunk_a.parquet", "shard-0", 0, (100, 200))
        .await
        .unwrap();

    let batch_b = make_batch(&["web-02"], &["mem"]);
    builder
        .build_and_publish_segment(&batch_b, "chunk_b.parquet", "shard-0", 0, (100, 200))
        .await
        .unwrap();

    // Merge segments (simulating compaction)
    builder
        .merge_and_publish_segments(
            &["chunk_a.parquet".into(), "chunk_b.parquet".into()],
            "merged.parquet",
            "shard-0",
            1,
            (100, 200),
        )
        .await
        .unwrap();

    // Query with the merged segment
    let prefilter = IndexPrefilter::new(store, "t1");
    let chunks = vec![TimeIndexEntry {
        chunk_path: "merged.parquet".into(),
        min_timestamp: 100,
        max_timestamp: 200,
        row_count: 2,
        size_bytes: 100,
        shard_id: Some("shard-0".into()),
    }];

    let result = prefilter
        .prune(
            &chunks,
            &[ColumnPredicate::Eq(
                "host".into(),
                PredicateValue::String("web-01".into()),
            )],
        )
        .await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].chunk_path, "merged.parquet");
}

#[tokio::test]
async fn test_no_false_negatives() {
    // Ensure the index never removes chunks that should match
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let builder = IndexBuilder::new(store.clone(), "t1", IndexConfig::default());

    let hosts = ["web-01", "web-02", "web-03", "web-04", "web-05"];
    let metrics = ["cpu", "mem", "disk", "net", "io"];

    for (i, (host, metric)) in hosts.iter().zip(metrics.iter()).enumerate() {
        let batch = make_batch(&[host], &[metric]);
        builder
            .build_and_publish_segment(
                &batch,
                &format!("chunk_{i}.parquet"),
                "shard-0",
                0,
                (i as i64 * 100, (i + 1) as i64 * 100),
            )
            .await
            .unwrap();
    }

    let prefilter = IndexPrefilter::new(store, "t1");
    let all_chunks: Vec<TimeIndexEntry> = (0..5)
        .map(|i| TimeIndexEntry {
            chunk_path: format!("chunk_{i}.parquet"),
            min_timestamp: i as i64 * 100,
            max_timestamp: (i + 1) as i64 * 100,
            row_count: 1,
            size_bytes: 50,
            shard_id: Some("shard-0".into()),
        })
        .collect();

    // Each host should match exactly its own chunk
    for (i, host) in hosts.iter().enumerate() {
        let result = prefilter
            .prune(
                &all_chunks,
                &[ColumnPredicate::Eq(
                    "host".into(),
                    PredicateValue::String(host.to_string()),
                )],
            )
            .await;
        assert!(
            result
                .iter()
                .any(|c| c.chunk_path == format!("chunk_{i}.parquet")),
            "Expected chunk_{i}.parquet to be in results for host={host}",
        );
    }
}

#[tokio::test]
async fn test_corrupt_segment_passthrough() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let builder = IndexBuilder::new(store.clone(), "t1", IndexConfig::default());

    // Build a valid segment
    let batch = make_batch(&["web-01"], &["cpu"]);
    builder
        .build_and_publish_segment(&batch, "chunk.parquet", "shard-0", 0, (100, 200))
        .await
        .unwrap();

    // Corrupt the segment file in object store
    let manifest_client = ManifestClient::new(store.clone(), "t1");
    let (manifest, _) = manifest_client
        .load_manifest("shard-0")
        .await
        .unwrap()
        .unwrap();
    let seg_path = &manifest.segments[0].path;
    store
        .put(
            &seg_path.clone().into(),
            bytes::Bytes::from(vec![0u8; 100]).into(),
        )
        .await
        .unwrap();

    // Query should pass all chunks through (corrupt segment = no pruning)
    let prefilter = IndexPrefilter::new(store, "t1");
    let chunks = vec![TimeIndexEntry {
        chunk_path: "chunk.parquet".into(),
        min_timestamp: 100,
        max_timestamp: 200,
        row_count: 1,
        size_bytes: 50,
        shard_id: Some("shard-0".into()),
    }];

    let result = prefilter
        .prune(
            &chunks,
            &[ColumnPredicate::Eq(
                "host".into(),
                PredicateValue::String("web-01".into()),
            )],
        )
        .await;
    // Corrupt segment → pass all through
    assert_eq!(result.len(), 1);
}
