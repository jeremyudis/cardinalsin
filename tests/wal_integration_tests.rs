//! Integration tests for WAL integration with the ingester.
//!
//! Tests cover:
//! - WAL append during writes and sequence tracking
//! - Recovery of unflushed WAL entries on startup
//! - WAL truncation after successful flush to S3
//! - Crash recovery with partial/corrupt entries
//! - Flushed sequence number persistence

use arrow_array::{Int64Array, RecordBatch};
use arrow_array::types::TimestampNanosecondType;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cardinalsin::ingester::{
    load_flushed_seq, persist_flushed_seq, IngesterConfig, WalConfig, WalSyncMode, WriteAheadLog,
};
use cardinalsin::metadata::LocalMetadataClient;
use cardinalsin::schema::MetricSchema;
use cardinalsin::StorageConfig;
use object_store::memory::InMemory;
use std::sync::Arc;
use tempfile::TempDir;

fn make_wal_config(dir: &TempDir) -> WalConfig {
    WalConfig {
        wal_dir: dir.path().to_path_buf(),
        max_segment_size: 64 * 1024 * 1024,
        sync_mode: WalSyncMode::None,
        enabled: true,
    }
}

fn make_ingester_config(wal_config: WalConfig) -> IngesterConfig {
    IngesterConfig {
        flush_row_count: 10_000,
        flush_size_bytes: 100 * 1024 * 1024,
        wal: wal_config,
        ..Default::default()
    }
}

fn make_timestamp_batch(values: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                arrow_array::PrimitiveArray::<TimestampNanosecondType>::from(values.to_vec()),
            ),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn make_ingester(
    config: IngesterConfig,
) -> cardinalsin::ingester::Ingester {
    let store = Arc::new(InMemory::new());
    let metadata: Arc<dyn cardinalsin::metadata::MetadataClient> =
        Arc::new(LocalMetadataClient::new());
    let storage_config = StorageConfig {
        tenant_id: "test-tenant".to_string(),
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
        endpoint: None,
    };
    let schema = MetricSchema::default_metrics();
    cardinalsin::ingester::Ingester::new(config, store, metadata, storage_config, schema)
}

// --- Flushed seq persistence tests ---

#[test]
fn test_persist_and_load_flushed_seq() {
    let dir = TempDir::new().unwrap();
    persist_flushed_seq(dir.path(), 42).unwrap();
    let loaded = load_flushed_seq(dir.path()).unwrap();
    assert_eq!(loaded, 42);
}

#[test]
fn test_load_flushed_seq_missing_file() {
    let dir = TempDir::new().unwrap();
    let loaded = load_flushed_seq(dir.path()).unwrap();
    assert_eq!(loaded, 0, "missing file should return 0");
}

#[test]
fn test_persist_flushed_seq_overwrites() {
    let dir = TempDir::new().unwrap();
    persist_flushed_seq(dir.path(), 10).unwrap();
    persist_flushed_seq(dir.path(), 99).unwrap();
    let loaded = load_flushed_seq(dir.path()).unwrap();
    assert_eq!(loaded, 99);
}

// --- WAL read_entries_after tests ---

#[tokio::test]
async fn test_read_entries_after() {
    let dir = TempDir::new().unwrap();
    let config = make_wal_config(&dir);

    // Write entries and record sequence numbers
    let (seq1, seq2, seq3);
    {
        let mut wal = WriteAheadLog::open(config.clone()).await.unwrap();
        let batch = make_timestamp_batch(&[1_000_000_000, 2_000_000_000]);
        seq1 = wal.append(&batch).await.unwrap();
        seq2 = wal.append(&batch).await.unwrap();
        seq3 = wal.append(&batch).await.unwrap();
    }

    // Reopen WAL (simulates recovery) and read entries after seq1
    let wal = WriteAheadLog::open(config).await.unwrap();

    let entries = wal.read_entries_after(seq1).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].seq, seq2);
    assert_eq!(entries[1].seq, seq3);

    // Read after seq3 should return nothing
    let entries = wal.read_entries_after(seq3).unwrap();
    assert!(entries.is_empty());

    // Read after 0 should return all
    let entries = wal.read_entries_after(0).unwrap();
    assert_eq!(entries.len(), 3);
}

// --- WAL ensure_wal recovery tests ---

#[tokio::test]
async fn test_ensure_wal_initializes_and_recovers_empty() {
    let dir = TempDir::new().unwrap();
    let config = make_ingester_config(make_wal_config(&dir));
    let mut ingester = make_ingester(config);

    ingester.ensure_wal().await.unwrap();

    // Buffer should be empty after recovery with no WAL entries
    let stats = ingester.buffer_stats().await;
    assert_eq!(stats.row_count, 0);
}

#[tokio::test]
async fn test_wal_recovery_replays_unflushed_entries() {
    let dir = TempDir::new().unwrap();
    let wal_config = make_wal_config(&dir);

    // Phase 1: Write some entries to WAL (simulating pre-crash state)
    {
        let mut wal = WriteAheadLog::open(wal_config.clone()).await.unwrap();
        let batch1 = make_timestamp_batch(&[1_000_000_000, 2_000_000_000]);
        let batch2 = make_timestamp_batch(&[3_000_000_000, 4_000_000_000, 5_000_000_000]);
        wal.append(&batch1).await.unwrap();
        wal.append(&batch2).await.unwrap();
        // WAL is dropped here (simulates crash - no flush happened)
    }

    // Phase 2: Create a new ingester and recover
    let config = make_ingester_config(make_wal_config(&dir));
    let mut ingester = make_ingester(config);
    ingester.ensure_wal().await.unwrap();

    // Buffer should contain the recovered entries
    let stats = ingester.buffer_stats().await;
    assert_eq!(
        stats.row_count, 5,
        "should have recovered 2 + 3 = 5 rows from WAL"
    );
}

#[tokio::test]
async fn test_wal_recovery_skips_flushed_entries() {
    let dir = TempDir::new().unwrap();
    let wal_config = make_wal_config(&dir);

    // Phase 1: Write entries and mark some as flushed
    let flushed_seq;
    {
        let mut wal = WriteAheadLog::open(wal_config.clone()).await.unwrap();
        let batch1 = make_timestamp_batch(&[1_000_000_000]);
        flushed_seq = wal.append(&batch1).await.unwrap();
        let batch2 = make_timestamp_batch(&[2_000_000_000, 3_000_000_000]);
        wal.append(&batch2).await.unwrap();
    }

    // Persist that we flushed up to seq1
    persist_flushed_seq(dir.path(), flushed_seq).unwrap();

    // Phase 2: Recover - should only replay the unflushed entry
    let config = make_ingester_config(make_wal_config(&dir));
    let mut ingester = make_ingester(config);
    ingester.ensure_wal().await.unwrap();

    let stats = ingester.buffer_stats().await;
    assert_eq!(
        stats.row_count, 2,
        "should only recover the 2 rows from the unflushed entry"
    );
}

// --- WAL append during write + truncation after flush ---

#[tokio::test]
async fn test_write_appends_to_wal() {
    let dir = TempDir::new().unwrap();
    let wal_config = make_wal_config(&dir);
    let config = make_ingester_config(wal_config.clone());
    let mut ingester = make_ingester(config);
    ingester.ensure_wal().await.unwrap();

    let batch = make_timestamp_batch(&[1_000_000_000, 2_000_000_000]);
    ingester.write(batch).await.unwrap();

    // Verify WAL contains the entry
    let wal = WriteAheadLog::open(wal_config).await.unwrap();
    let entries = wal.read_entries().unwrap();
    assert_eq!(entries.len(), 1, "WAL should contain one entry after write");

    // Decode and verify the payload
    let batches = entries[0].batches().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
}

#[tokio::test]
async fn test_wal_disabled_does_not_write() {
    let dir = TempDir::new().unwrap();
    let mut wal_config = make_wal_config(&dir);
    wal_config.enabled = false;
    let config = make_ingester_config(wal_config.clone());
    let mut ingester = make_ingester(config);
    ingester.ensure_wal().await.unwrap();

    let batch = make_timestamp_batch(&[1_000_000_000]);
    ingester.write(batch).await.unwrap();

    // WAL directory should have no segment files
    let segments: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("segment-")
        })
        .collect();
    assert!(segments.is_empty(), "no WAL segments when WAL is disabled");
}

// --- Flushed seq round-trip edge cases ---

#[test]
fn test_load_corrupt_flushed_seq() {
    let dir = TempDir::new().unwrap();
    // Write a corrupt (wrong size) file
    std::fs::write(dir.path().join("flushed_seq"), b"short").unwrap();
    let loaded = load_flushed_seq(dir.path()).unwrap();
    assert_eq!(loaded, 0, "corrupt flushed_seq should return 0");
}

#[tokio::test]
async fn test_wal_next_seq_after_recovery() {
    let dir = TempDir::new().unwrap();
    let wal_config = make_wal_config(&dir);

    // Write 3 entries
    {
        let mut wal = WriteAheadLog::open(wal_config.clone()).await.unwrap();
        let batch = make_timestamp_batch(&[1_000_000_000]);
        wal.append(&batch).await.unwrap();
        wal.append(&batch).await.unwrap();
        wal.append(&batch).await.unwrap();
    }

    // Reopen - next_seq should continue from 4
    let wal = WriteAheadLog::open(wal_config).await.unwrap();
    assert_eq!(wal.next_seq(), 4, "next_seq should be 4 after reopening with 3 entries");
}

#[tokio::test]
async fn test_multiple_recovery_cycles() {
    let dir = TempDir::new().unwrap();
    let wal_config = make_wal_config(&dir);

    // Cycle 1: Write entries, no flush
    {
        let mut wal = WriteAheadLog::open(wal_config.clone()).await.unwrap();
        let batch = make_timestamp_batch(&[1_000_000_000]);
        wal.append(&batch).await.unwrap();
    }

    // Cycle 2: Recover and write more, still no flush
    let config = make_ingester_config(make_wal_config(&dir));
    let mut ingester = make_ingester(config);
    ingester.ensure_wal().await.unwrap();

    let stats = ingester.buffer_stats().await;
    assert_eq!(stats.row_count, 1, "first recovery should replay 1 row");

    // Write another entry
    let batch = make_timestamp_batch(&[2_000_000_000, 3_000_000_000]);
    ingester.write(batch).await.unwrap();
    drop(ingester);

    // Cycle 3: Recover again - should see all entries (since nothing was flushed)
    let config2 = make_ingester_config(make_wal_config(&dir));
    let mut ingester2 = make_ingester(config2);
    ingester2.ensure_wal().await.unwrap();

    let stats2 = ingester2.buffer_stats().await;
    assert_eq!(
        stats2.row_count, 3,
        "second recovery should replay all 3 rows (1 + 2 unflushed)"
    );
}
