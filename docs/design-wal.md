# Design: Write-Ahead Log (WAL) for CardinalSin Ingester

## Status

**Draft** - Design document for future implementation.

## Problem Statement

CardinalSin's ingester acknowledges writes after appending to an in-memory buffer (`src/ingester/mod.rs:159-201`) but before any durable persistence. The buffer is only flushed to S3/Parquet when a threshold is reached (1M rows, 100MB, or 5-minute timer). A process crash, OOM kill, or power failure between write acknowledgment and flush results in **silent data loss**.

This is the single most critical production-readiness gap identified in both the architecture audit and the TSDB research survey (`docs/tsdb-research-findings.md`, P0-1).

---

## 1. Industry Survey: How Other TSDBs Handle WAL

### 1.1 InfluxDB IOx / InfluxDB 3

**Source**: [InfluxDB IOx WAL](https://github.com/influxdata/influxdb_iox/tree/main/wal), [InfluxDB 3 WAL Design](https://github.com/influxdata/influxdb/issues/25144)

- **Format**: FlatBuffers for serialization. WAL entries are individual files identified by monotonically increasing `u64` sequence numbers.
- **Flush strategy**: Writes buffered on a flush interval (default 1 second), then a WAL file is written with its sequence number as the filename. This is a **group commit** strategy -- multiple writes within the 1s window are batched into a single WAL file.
- **Snapshot/truncation**: After `N + N/2` WAL files accumulate (default N=600, so 900 files at ~10 minute intervals), a snapshot is triggered. Once the snapshot completes, the first N files are deleted.
- **Recovery**: All WAL files are read and replayed in order on startup. Uses a pluggable replay interface.
- **Key insight**: InfluxDB 3 writes its WAL to **object storage** rather than local disk, which eliminates the local fsync bottleneck entirely but introduces network latency.

### 1.2 Prometheus TSDB

**Source**: [Prometheus WAL and Checkpoint](https://ganeshvernekar.com/blog/prometheus-tsdb-wal-and-checkpoint/)

- **Format**: Custom binary format with record types: Series (1), Samples (2), Tombstones (3), Exemplars (4), Histograms (5), Metadata (6). Each record is length-prefixed with CRC-32 checksums.
- **Segment management**: Default segment size 128 MB. Segments are numbered sequentially and cannot be deleted out of order (first N segments deleted together, no gaps).
- **Checkpoint**: Stored in a directory named `checkpoint.N` in the same segmented format as the WAL itself. When WAL is truncated, a checkpoint is created. Checkpoint creation and WAL segment deletion are not atomic.
- **fsync**: Called only after a full segment is written (128 MB), not per-entry. This means up to 128 MB of data can be lost on crash -- Prometheus accepts this tradeoff.
- **Recovery**: Read checkpoint, then replay WAL segments after the checkpoint.

### 1.3 VictoriaMetrics

**Source**: [VictoriaMetrics Durability](https://blog.damnever.com/en/2022/some-important-notes-about-victoriametrics), [WAL Critique](https://valyala.medium.com/wal-usage-looks-broken-in-modern-time-series-databases-b62a627ab704)

- **No WAL**. VictoriaMetrics intentionally does not use a WAL.
- **Rationale**: VictoriaMetrics' creator argues that WALs in TSDBs are "broken" because (a) OS page cache means data written to WAL files is not actually on disk unless fsync is called, (b) fsync is extremely slow (1K-10K ops/sec on SSD, ~100 ops/sec on HDD), and (c) the performance penalty of per-write fsync defeats the purpose of a high-throughput TSDB.
- **Tradeoff**: VictoriaMetrics accepts 1-5 seconds of data loss on hard crash (kill -9). Data in the inmemoryPart buffer that has not been flushed is lost.
- **Implication for CardinalSin**: We should seriously consider whether per-write fsync is appropriate for our use case, or whether Prometheus-style batched fsync (or even VictoriaMetrics-style no-WAL with fast flush) better matches our requirements.

### 1.4 Summary Comparison

| System | WAL Format | fsync Strategy | Data Loss Window | Recovery |
|--------|-----------|---------------|-----------------|----------|
| InfluxDB 3 | FlatBuffers files to object store | Per-file (1s batch) | ~1 second | Replay WAL files |
| Prometheus | Custom binary segments (128MB) | Per-segment | Up to 128MB | Checkpoint + WAL replay |
| VictoriaMetrics | None | N/A (batch fsync on flush) | 1-5 seconds | Re-scrape from sources |
| **CardinalSin (current)** | **None** | **N/A** | **Up to 5 minutes / 1M rows / 100MB** | **Data lost** |

---

## 2. Recommended WAL Design for CardinalSin

### 2.1 Design Goals

1. **Durability**: No acknowledged write should be silently lost on crash
2. **Throughput**: WAL overhead must not reduce ingestion below 500K samples/sec (50% of our 1M target)
3. **Simplicity**: Prefer a simple design over a feature-rich one
4. **Compatibility**: Use Arrow IPC format for WAL entries since our entire pipeline is Arrow-based
5. **Configurability**: Allow operators to tune the durability vs. performance tradeoff

### 2.2 WAL Format: Arrow IPC Streaming

**Recommendation**: Use Arrow IPC streaming format for WAL entries.

**Rationale**:
- Our entire write path already uses `RecordBatch` (`src/ingester/mod.rs:159`, `src/ingester/buffer.rs:28`)
- Arrow IPC streaming format serializes RecordBatch with minimal overhead and supports zero-copy reads on replay
- No schema conversion needed -- the WAL entry IS a RecordBatch, same as what goes into the buffer
- Well-tested, maintained by the Arrow project, available via the `arrow-ipc` crate already in our dependency tree

**Entry format**:

```
[WAL Entry]
+------------------+
| Magic: 4 bytes   |  "CSWA" (CardinalSin WAL)
| Version: 1 byte  |  0x01
| Flags: 1 byte    |  bit 0: compressed (0=no, 1=lz4)
| Seq No: 8 bytes  |  u64 little-endian, monotonically increasing
| Length: 4 bytes   |  u32 little-endian, payload length
| CRC-32: 4 bytes  |  CRC-32C of payload
| Payload: N bytes  |  Arrow IPC StreamWriter output (one or more RecordBatches)
+------------------+
```

**Why not FlatBuffers** (like InfluxDB IOx): FlatBuffers would require defining a schema and generating code. Since our data is already in RecordBatch format, Arrow IPC is zero-overhead whereas FlatBuffers would require serialization/deserialization between RecordBatch and FlatBuffer representations.

**Why not raw Parquet**: Parquet is optimized for large columnar files, not small append-only entries. The encoding overhead (dictionary building, statistics computation, row group metadata) would add significant latency to each WAL write. Parquet is the right format for S3 persistence; Arrow IPC is the right format for WAL entries.

### 2.3 Segment Management

WAL entries are written to **segment files** on local disk:

```
<wal_dir>/
  segment-000001.wal     # Active segment
  segment-000002.wal     # Sealed segment (pending flush)
  segment-000003.wal     # Sealed segment (pending flush)
```

**Segment lifecycle**:
1. **Active**: Receives new WAL entries via append. Only one active segment at a time.
2. **Sealed**: Segment is full (reached `max_segment_size`) or a flush has been triggered. No new entries appended.
3. **Flushed**: All entries in this segment have been successfully flushed to S3 and confirmed in metadata. Segment file is deleted.

**Configuration**:

```rust
pub struct WalConfig {
    /// Directory for WAL segment files
    pub wal_dir: PathBuf,
    /// Maximum segment file size before rotation (default: 64 MB)
    pub max_segment_size: usize,
    /// fsync strategy
    pub sync_mode: WalSyncMode,
    /// Whether to enable WAL (allows disabling for development)
    pub enabled: bool,
}

pub enum WalSyncMode {
    /// fsync after every write batch (strongest durability, lowest throughput)
    EveryWrite,
    /// fsync on a timer interval (default: 100ms). Balances durability and throughput.
    /// Data loss window equals the interval.
    Interval(Duration),
    /// fsync only on segment rotation (weakest durability, highest throughput).
    /// Prometheus-style: up to max_segment_size of data can be lost.
    OnRotation,
    /// No fsync (fastest, for development/testing only)
    None,
}
```

**Default**: `WalSyncMode::Interval(Duration::from_millis(100))` -- matches InfluxDB 3's 100ms recommendation. This provides a 100ms maximum data loss window while allowing group commit of all writes within each 100ms window into a single fsync.

### 2.4 Integration with Ingester

**Current write path** (`src/ingester/mod.rs:159-201`):

```
write(batch) -> check split state -> buffer.append(batch) -> ack -> [later] flush_batches()
```

**Proposed write path**:

```
write(batch) -> check split state -> wal.append(batch) -> buffer.append(batch) -> ack
                                                                                    |
                                                    [background] wal.sync()  <------+
                                                                                    |
                                     [threshold reached] flush_batches() -> S3 put  |
                                                              |                     |
                                              metadata.register_chunk()             |
                                                              |                     |
                                                       wal.truncate(seq_no)         |
```

**Key changes to `Ingester` struct** (`src/ingester/mod.rs:69-95`):

```rust
pub struct Ingester {
    // ... existing fields ...

    /// Write-ahead log for durability
    wal: Option<WriteAheadLog>,  // Option to allow disabling

    /// Last WAL sequence number that was flushed to S3
    last_flushed_seq: Arc<AtomicU64>,
}
```

**Modified `write()` method** (`src/ingester/mod.rs:159-201`):

```rust
pub async fn write(&self, batch: RecordBatch) -> Result<()> {
    // ... existing split check logic ...

    // Step 1: Write to WAL (durable)
    let seq_no = if let Some(ref wal) = self.wal {
        Some(wal.append(&batch).await?)
    } else {
        None
    };

    // Step 2: Append to in-memory buffer
    let mut buffer = self.buffer.write().await;
    if buffer.size_bytes() + batch_size > self.config.max_buffer_size_bytes {
        return Err(Error::BufferFull);
    }
    buffer.append(batch.clone())?;

    // Step 3: Record shard metrics (unchanged)
    self.shard_monitor.record_write(&shard_id, batch_size, write_latency);

    // Step 4: Check flush threshold (unchanged)
    if self.should_flush(&buffer) {
        let batches = buffer.take();
        drop(buffer);
        self.flush_batches(batches).await?;

        // Step 5: Truncate WAL after successful flush
        if let (Some(ref wal), Some(seq)) = (&self.wal, seq_no) {
            wal.truncate_before(seq).await?;
        }
    }

    Ok(())
}
```

**Note on the dual-write path** (`write_with_split_awareness`, line 205): This path also needs WAL integration. The WAL entry should contain the original batch (pre-split), and replay should re-execute the split logic. This avoids needing to WAL three separate writes.

### 2.5 Recovery Protocol

On ingester startup:

```rust
impl Ingester {
    pub async fn recover(wal_dir: &Path, /* ... other params ... */) -> Result<Self> {
        let wal = WriteAheadLog::open(wal_dir)?;

        // Step 1: Get the last flushed sequence number from metadata
        let last_flushed = metadata.get_last_flushed_wal_seq().await?
            .unwrap_or(0);

        // Step 2: Replay WAL entries after the last flush
        let mut buffer = WriteBuffer::new();
        let mut replayed = 0u64;

        for entry in wal.iter_from(last_flushed + 1)? {
            let entry = entry?;  // Validates CRC

            for batch in entry.batches() {
                buffer.append(batch)?;
                replayed += 1;
            }
        }

        if replayed > 0 {
            info!(
                replayed_entries = replayed,
                buffer_rows = buffer.row_count(),
                "WAL recovery complete"
            );
        }

        // Step 3: Truncate replayed entries that were already flushed
        wal.truncate_before(last_flushed).await?;

        // Step 4: Create ingester with recovered buffer
        let ingester = Self {
            buffer: Arc::new(RwLock::new(buffer)),
            wal: Some(wal),
            last_flushed_seq: Arc::new(AtomicU64::new(last_flushed)),
            // ... other fields ...
        };

        // Step 5: If buffer exceeds flush threshold, flush immediately
        if ingester.should_flush(&ingester.buffer.read().await) {
            let batches = ingester.buffer.write().await.take();
            ingester.flush_batches(batches).await?;
        }

        Ok(ingester)
    }
}
```

**Edge cases during recovery**:
- **Partial WAL entry (truncated write)**: Detected by CRC mismatch. Skip the entry -- it was never acknowledged to the client.
- **WAL entries for data already in S3**: Detected by comparing sequence numbers. Skip entries with `seq <= last_flushed`.
- **Corrupted segment file**: Log a warning, skip remaining entries in that segment. The data was either already flushed or never acknowledged.

---

## 3. Crate Recommendations

### 3.1 Option A: Custom Implementation (Recommended)

**Build a lightweight WAL module specific to CardinalSin.**

Rationale:
- Our WAL entries are Arrow IPC RecordBatches, which is a very specific format. No existing crate understands this natively.
- The WAL logic is straightforward: append to file, fsync on schedule, read for recovery, delete after flush.
- Total implementation is estimated at 400-600 lines of Rust.
- No external dependency to maintain or version-pin.

Key dependencies (already in our Cargo.toml):
- `arrow-ipc`: Serialize/deserialize RecordBatch to bytes
- `crc32fast`: CRC-32C checksums for entry validation
- `tokio::fs`: Async file I/O with fsync

### 3.2 Option B: OkayWAL

**Source**: [OkayWAL](https://github.com/khonsulabs/okaywal)

Pros:
- Mature fsync batching across threads (exactly what we need for group commit)
- CRC-32 checksums built in
- No unsafe code
- Good performance characteristics (sub-3ms for 256B-1MB entries)

Cons:
- We'd need to serialize RecordBatch to bytes before passing to OkayWAL, then deserialize on recovery. The OkayWAL entry format wraps our data in its own header, adding overhead.
- OkayWAL's `LogManager` trait is designed for key-value checkpointing, not time-series flush-to-S3. The abstraction mismatch would require adapter code.
- Additional dependency (though small and pure Rust).

### 3.3 Option C: InfluxDB 3 Approach (WAL to Object Storage)

Write WAL files directly to S3 instead of local disk.

Pros:
- No local disk requirement (truly stateless ingesters)
- Aligns with our S3-first architecture
- No fsync concerns

Cons:
- S3 PUT latency (50-200ms) on every WAL write, even with batching
- More complex: need to handle S3 PUT failures, retries, partial uploads
- Recovery requires listing and reading S3 objects (slow for many small files)
- Higher S3 cost from many small PUTs (each WAL file is a separate PUT)

**Not recommended** unless we are willing to accept 200ms+ write latency per batch. More suitable for a future "enhanced durability" mode.

### 3.4 Recommendation

**Option A (custom implementation)** for the following reasons:
1. The format is Arrow IPC specific -- no generic WAL crate handles this natively
2. Total scope is small (400-600 LOC) and well-understood
3. We control the fsync strategy and can tune it precisely
4. No abstraction mismatch with our flush-to-S3 lifecycle

---

## 4. Performance Impact Analysis

### 4.1 fsync Cost

The fundamental bottleneck of any WAL is fsync. Measured costs on modern hardware:

| Storage | fsync latency | Max fsyncs/sec |
|---------|--------------|----------------|
| NVMe SSD | 0.1-0.5 ms | 2,000-10,000 |
| SATA SSD | 1-5 ms | 200-1,000 |
| HDD | 5-20 ms | 50-200 |

With `WalSyncMode::EveryWrite`, each write batch requires one fsync. At 1,000 batches/sec of 1,000 samples each (our 1M samples/sec target), that's 1,000 fsyncs/sec, which is feasible on NVMe but not on SATA SSD or HDD.

### 4.2 Group Commit (Recommended Default)

With `WalSyncMode::Interval(100ms)`:
- All writes within a 100ms window are batched into a single WAL append
- One fsync per 100ms = 10 fsyncs/sec
- Even an HDD can handle 10 fsyncs/sec
- Maximum data loss window: 100ms (matching InfluxDB 3's recommendation)

**Expected throughput impact with group commit**:

```
Current (no WAL):     ~1M samples/sec (limited by buffer/Parquet/S3, not disk)
With WAL (100ms):     ~950K samples/sec (estimated 5% overhead for IPC serialization)
With WAL (per-write): ~500K samples/sec on NVMe, ~100K on SATA SSD
```

The 5% overhead estimate for 100ms group commit comes from:
- Arrow IPC serialization: ~1-2 microseconds per RecordBatch (negligible)
- File write (no fsync): ~0.1ms per batch (buffered I/O)
- fsync: amortized across all writes in the 100ms window

### 4.3 Mitigation Strategies

1. **Group commit** (described above): Default strategy. Batches fsync across all writes in a configurable time window.

2. **Separate WAL disk**: Common production practice. Put the WAL directory on a separate physical disk (or NVMe) from the OS and temporary files to avoid I/O contention. Configurable via `WalConfig::wal_dir`.

3. **Async fsync with write pipelining**: Accept the write into the buffer immediately, but delay the ack until the next fsync completes. This allows the ingester to continue buffering while the previous batch's fsync is in flight.

4. **Configurable durability level**: The `WalSyncMode` enum lets operators choose their tradeoff:
   - `EveryWrite`: No data loss, lower throughput. For regulated/financial use cases.
   - `Interval(100ms)`: Default. Up to 100ms of data loss, near-full throughput.
   - `OnRotation`: Up to 64MB of data loss, maximum throughput. Prometheus-style.
   - `None`: Development only. Equivalent to current behavior (no fsync).

### 4.4 Write Path Latency Breakdown (Estimated)

| Step | Current | With WAL (100ms group commit) |
|------|---------|------------------------------|
| Split state check | 0.01 ms | 0.01 ms |
| WAL IPC serialize | -- | 0.005 ms |
| WAL file write | -- | 0.1 ms (buffered) |
| WAL fsync | -- | 0 ms (amortized to background) |
| Buffer append | 0.01 ms | 0.01 ms |
| Shard monitor | 0.001 ms | 0.001 ms |
| **Total per-write** | **~0.02 ms** | **~0.13 ms** |
| **Ack latency** | **~0.02 ms** | **~0.13 ms** (or up to 100ms if waiting for fsync confirmation) |

The choice of whether to ack immediately after buffered write (accepting 100ms data loss window) or ack only after fsync (adding up to 100ms latency) is configurable. The recommended default is to ack after buffered write, matching InfluxDB 3 and Prometheus behavior.

---

## 5. Configuration

New environment variables and config fields:

```rust
// In src/ingester/mod.rs or src/config.rs
pub struct IngesterConfig {
    // ... existing fields ...

    /// WAL configuration
    pub wal: WalConfig,
}

pub struct WalConfig {
    /// Enable WAL (default: true in production, false in dev/memory mode)
    pub enabled: bool,
    /// WAL directory (default: /var/lib/cardinalsin/wal)
    pub dir: PathBuf,
    /// Maximum segment size before rotation (default: 64 MB)
    pub max_segment_size: usize,
    /// fsync mode (default: Interval(100ms))
    pub sync_mode: WalSyncMode,
}
```

Environment variables:
- `WAL_ENABLED`: "true" or "false" (default: "true" if STORAGE_BACKEND=s3, "false" if memory)
- `WAL_DIR`: Path to WAL directory (default: `/var/lib/cardinalsin/wal`)
- `WAL_SYNC_MODE`: "every_write", "interval_100ms", "interval_1s", "on_rotation", "none"
- `WAL_MAX_SEGMENT_SIZE`: Size in bytes (default: 67108864 = 64MB)

---

## 6. Testing Plan

### 6.1 Unit Tests

- Append entries, read back, verify CRC and contents
- Segment rotation at max size
- Truncation removes correct segments
- Corrupted entry detection (invalid CRC)
- Partial write detection (truncated file)

### 6.2 Integration Tests

- Write N batches, kill process, recover, verify all acknowledged batches are in buffer
- Write, flush to S3, truncate, verify WAL is empty
- Concurrent writers to WAL (verify serialization correctness)
- Recovery with mixed flushed/unflushed entries

### 6.3 Performance Tests

- Measure throughput with each `WalSyncMode`
- Compare latency distribution with and without WAL
- Measure recovery time for various WAL sizes (1MB, 100MB, 1GB)
- Benchmark Arrow IPC serialization overhead

### 6.4 Chaos Tests

- Kill ingester during WAL write (partial entry)
- Kill ingester during flush (WAL not yet truncated)
- Fill disk during WAL write (ENOSPC handling)
- Corrupt WAL file bytes and verify recovery skips bad entries

---

## 7. Open Questions

1. **Should WAL support compression?** LZ4 compression of Arrow IPC data could reduce disk I/O by 2-5x with minimal CPU overhead. The design includes a compression flag in the entry header but does not mandate it for v1.

2. **Should we support WAL-to-S3?** InfluxDB 3 writes WAL to object storage for truly stateless ingesters. This is architecturally cleaner but adds latency and cost. Consider as a v2 enhancement.

3. **How does WAL interact with dual-write during shard splits?** The proposed design WALs the original (pre-split) batch. On replay, the split logic re-executes. This is correct because the split state is also recoverable from metadata. However, if the split completes during the data loss window, replay could route data to the wrong shard. This edge case needs careful handling.

4. **What happens when the WAL disk fills up?** Options: (a) reject writes with `Error::WalFull`, (b) force an emergency flush to S3 and truncate, (c) switch to WAL-disabled mode with a warning. Recommend option (a) for safety.

---

## Sources

- [InfluxDB IOx WAL module](https://github.com/influxdata/influxdb_iox/tree/main/wal)
- [InfluxDB 3 WAL Design Issue #25144](https://github.com/influxdata/influxdb/issues/25144)
- [Prometheus TSDB WAL and Checkpoint](https://ganeshvernekar.com/blog/prometheus-tsdb-wal-and-checkpoint/)
- [VictoriaMetrics: WAL Usage Looks Broken](https://valyala.medium.com/wal-usage-looks-broken-in-modern-time-series-databases-b62a627ab704)
- [VictoriaMetrics Durability Notes](https://blog.damnever.com/en/2022/some-important-notes-about-victoriametrics)
- [OkayWAL: A Write Ahead Log for Rust](https://github.com/khonsulabs/okaywal)
- [Introducing OkayWAL](https://bonsaidb.io/blog/introducing-okaywal/)
- [Arrow IPC Streaming Format](https://arrow.apache.org/docs/python/ipc.html)
- [InfluxDB 3 Enterprise WAL Configuration](https://docs.influxdata.com/influxdb3/enterprise/reference/config-options/)
