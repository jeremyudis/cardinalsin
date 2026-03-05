# WAL Design and Implementation Audit (2026-03-05)

## Scope
This audit reviews the current write-ahead-log (WAL) implementation and its integration with the ingester write/flush/recovery paths.

Reviewed code:
- `src/ingester/wal.rs`
- `src/ingester/mod.rs`
- `src/bin/ingester.rs`
- `src/bin/query.rs`
- `src/api/mod.rs`
- `tests/wal_integration_tests.rs`

Executed validation:
- `cargo test --test wal_integration_tests` (13 passed)
- `cargo test wal --lib` (6 passed)

## Current WAL Design (As Implemented)

### Data model
- WAL entries are Arrow IPC payloads wrapped in a fixed 22-byte header.
- Header fields in `src/ingester/wal.rs`:
  - magic (`CSWA`), version (`1`), flags, sequence (`u64`), payload length (`u32`), payload CRC32 (`u32`).
- Sequence numbers are monotonically increasing (`next_seq`) across segment files.

### Segment lifecycle
- Segments are local files named `segment-<id>.wal` in `wal_dir`.
- Appends go to one active segment; rotation occurs when `current_size + entry_size > max_segment_size`.
- Truncation is segment-granular (`truncate_before(seq)`), never in-place within a segment.

### Recovery flow
- Ingester startup (`ensure_wal`) loads `flushed_seq`, replays entries with `seq > flushed_seq`, then truncates old segments.
- Recovery tolerates trailing truncation/corruption by stopping at first incomplete/corrupt tail entry.

### ACK semantics
- Ingestion ACK occurs after WAL append plus buffer enqueue, not after fsync.
- Default sync mode is `interval_100ms`.

## Intended Durability Invariants
The implementation depends on these invariants being true:
1. `flushed_seq` must never advance past data durably present in object storage + metadata.
2. WAL truncation must not remove entries that have not yet been flushed.
3. Replay must include all unflushed acknowledged writes after crash.
4. Sync mode documentation must match actual behavior under low traffic and high traffic.

## Findings

### [Critical] Flush watermark race can advance `flushed_seq` beyond flushed data (data loss)
Evidence:
- `last_wal_seq` is updated on every append in write path (`src/ingester/mod.rs:331`, `src/ingester/mod.rs:376`).
- `flush_batches` computes `flushed_up_to` by reading global `last_wal_seq` at flush completion (`src/ingester/mod.rs:691`).
- It then truncates WAL and persists `flushed_seq` using that global value (`src/ingester/mod.rs:693-703`).

Why this is a bug:
- `flush_batches` does not use a watermark tied to the specific batch set being flushed.
- Concurrent writes can append new WAL entries while a flush is in flight; those entries are not part of the flushed batch but can still raise `last_wal_seq`.
- If a crash occurs after `persist_flushed_seq` is advanced to that newer value, recovery (`read_entries_after(flushed_seq)`) skips entries that were never flushed.

Concrete failure sequence:
1. Flush A starts for batches up to seq=100.
2. Concurrent write appends seq=101 and sits in memory buffer.
3. Flush A finishes and reads global `last_wal_seq=101`.
4. System persists `flushed_seq=101` and may truncate.
5. Crash occurs before seq=101 is flushed.
6. Restart replays `seq > 101` only; seq=101 is permanently lost.

Recommendation:
- Track a flush watermark bound to buffered data, not a global append watermark.
- Practical fix: store `max_seq` in `WriteBuffer` (or per buffered batch), return it with `take()`, and pass that exact value into `flush_batches` for truncation/persist.

### [High] `interval_*` sync mode is not truly interval-based; no background fsync timer
Evidence:
- Interval sync only runs inside append path (`src/ingester/wal.rs:231-238`).
- No WAL background sync task exists.
- Documentation comment claims loss window equals sync interval and fsync runs asynchronously (`src/ingester/mod.rs:295-298`).

Why this is a bug:
- With low/bursty traffic, the last write can remain unsynced indefinitely until a later append or rotation.
- The actual loss window is not bounded by configured interval under idle conditions.

Recommendation:
- Add a WAL sync task that fsyncs active segment at configured interval while WAL is open.
- Alternatively, update docs/comments to explicitly state "interval checked on append" semantics (weaker durability), but this is not recommended for production defaults.

### [High] Query node exposes write endpoint with non-initialized WAL and no flush timer
Evidence:
- Query binary constructs a default `Ingester` (`src/bin/query.rs:107-113`).
- It does not call `ensure_wal()`.
- It does not start `run_flush_timer()`.
- Router still exposes Prometheus write route (`src/api/mod.rs:75`).

Impact:
- Misrouted writes to query nodes are acknowledged but are not WAL-durable.
- Data may remain buffered in memory until high thresholds are hit, with no timer flush.

Recommendation:
- Query node should not register write routes, or should reject writes with explicit 4xx/5xx.
- If writes must remain enabled, initialize WAL and run flush timer exactly as ingester binary does.

### [Medium] `flushed_seq` persistence is not crash-atomic or fsync-safe
Evidence:
- `persist_flushed_seq` uses direct `std::fs::write` (`src/ingester/wal.rs:453-456`) with no temp-file + rename + fsync sequence.

Impact:
- Power loss/crash can yield torn/rolled-back `flushed_seq` metadata.
- Current fallback resets to `0` on corruption (`src/ingester/wal.rs:465-467`), which is safe for loss but can cause replay duplication.

Recommendation:
- Use atomic write pattern: write temp file, `sync_data`, rename, fsync directory.

## Tradeoffs (Design Decisions and Consequences)

### ACK after append (not fsync)
- Benefit: higher ingest throughput and lower p50 latency.
- Cost: acknowledged writes can still be lost on host crash/power failure until fsync or successful object-store flush.
- Decision quality: reasonable for TSDB throughput goals, but only if risk is explicitly documented and operationally accepted.

### Segment-granular truncation
- Benefit: simple implementation, fast delete operations, no in-place rewriting.
- Cost: cannot reclaim prefix of active segment; temporary disk amplification until rotation.
- Decision quality: acceptable v1 tradeoff.

### Tail-corruption recovery policy (stop at first bad tail entry)
- Benefit: deterministic crash recovery for common torn-write cases.
- Cost: mid-segment corruption drops all subsequent entries in that segment.
- Decision quality: acceptable for crash-tolerance, weak for media-corruption resilience.

### Arrow IPC payload format
- Benefit: zero schema translation across ingest pipeline and easy replay into RecordBatch.
- Cost: no built-in WAL compression/indexing; larger disk footprint than compressed WALs.
- Decision quality: good fit for current architecture and implementation velocity.

## Test Coverage Gaps
Current tests are mostly single-threaded and do not cover the critical race.

Add these tests:
1. Concurrent writes during flush must not advance persisted `flushed_seq` past flushed data.
2. Idle-period durability test for `interval_*` mode (no subsequent writes).
3. Query-node write endpoint behavior test (should reject or fully initialize durability path).
4. Crash-atomicity test for `flushed_seq` file writes (fault-injected).

## Priority Fix Plan
1. Fix flush watermark race (Critical).
2. Implement true periodic WAL sync task (High).
3. Remove or hard-fail write paths on query nodes unless durability path is initialized (High).
4. Make `flushed_seq` persistence crash-atomic (Medium).
