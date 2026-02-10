# CardinalSin Architecture Audit

**Date**: 2026-02-06
**Auditor**: Distributed Systems Architect Agent
**Scope**: Complete review of `src/` for production readiness

---

## Executive Summary

CardinalSin has strong foundations -- columnar storage on object storage, DataFusion integration, and tiered caching are solid architectural choices. However, the current implementation has several **critical** and **high-severity** gaps in its distributed systems design that would cause data loss, corruption, or incorrect query results under production conditions. The most serious issues are: (1) non-atomic two-object metadata updates in the S3 CAS layer, (2) the MinIO fallback that silently disables all CAS protections, (3) crash-unsafe shard split phases, and (4) data loss on ingester crash due to purely in-memory buffering with no WAL.

**Summary by Severity**:
- Critical (data loss/corruption): 5 findings
- High (incorrect behavior under production conditions): 7 findings
- Medium (degraded performance or reliability): 6 findings
- Low (code quality, minor issues): 4 findings

---

## Finding 1: Non-Atomic Two-Object Metadata Update (CRITICAL)

**File**: `src/metadata/s3.rs:604-711`

**Description**: The `atomic_register_chunk()` method attempts to atomically update two separate S3 objects -- `chunks/metadata.json` and `time-index.json` -- but these are two independent PUT operations with independent ETags. If the first PUT succeeds but the second fails (due to conflict, network error, or process crash), the system ends up in an inconsistent state: the chunk exists in the metadata but is not indexed in the time index (or vice versa on retry).

**Specific code path**:
```
1. Load metadata with ETag A      (line 611)
2. Load time index with ETag B    (line 612)
3. Modify both in memory          (lines 615-635)
4. PUT metadata with ETag A       (line 641) -- SUCCESS
5. PUT time index with ETag B     (line 649) -- FAILS (conflict)
6. Retry from step 1              (line 663)
```

On the retry, step 1 loads the already-updated metadata (which now includes the chunk), and step 3 inserts the chunk again (idempotent for metadata, but not for time index where it would add duplicate entries). More critically, if the process crashes between steps 4 and 5, the chunk is registered in metadata but invisible to queries because it has no time index entry.

The same pattern exists in `delete_chunk()` (line 1076-1146) and `complete_compaction()` (line 1275-1372).

**Impact**: Data invisibility (registered chunks not queryable), duplicate time index entries, metadata/index divergence after crash recovery.

**Severity**: CRITICAL

**Recommended Fix**: Merge `metadata.json` and `time-index.json` into a single S3 object with a single ETag, or introduce a transaction log pattern. Alternatively, implement a reconciliation process that detects and repairs inconsistencies on startup.

---

## Finding 2: MinIO Fallback Silently Disables CAS (CRITICAL)

**File**: `src/metadata/s3.rs:354-384`

**Description**: When the S3 implementation detects a "not yet implemented" error from MinIO (which does not support conditional PUTs), it silently falls back to `PutMode::Overwrite`:

```rust
Err(e) if e.to_string().to_lowercase().contains("not yet implemented") => {
    warn!("Conditional PUT not supported, falling back to overwrite mode: {}", e);
    let fallback_opts = PutOptions {
        mode: PutMode::Overwrite,
        ..Default::default()
    };
    // ...overwrites without any concurrency protection
}
```

This completely destroys all atomicity guarantees. Any concurrent metadata operation will silently overwrite the other, leading to lost chunk registrations, lost compaction completions, and lost shard split state transitions. The fallback exists in both `atomic_save_chunk_metadata()` (line 354) and `atomic_save_time_index()` (line 508).

**Impact**: Complete loss of concurrency safety when using MinIO, which is the stated development/test environment. All CAS-protected operations become simple overwrites, meaning concurrent ingesters or compactors will silently lose each other's work.

**Severity**: CRITICAL

**Recommended Fix**: Either (a) require an S3-compatible store that supports conditional PUTs, (b) implement a locking mechanism on top of S3 (e.g., using DynamoDB, etcd, or a lock file with TTL), or (c) use per-chunk metadata files instead of a single shared `metadata.json` to eliminate contention.

---

## Finding 3: Data Loss on Ingester Crash -- No WAL (CRITICAL)

**File**: `src/ingester/mod.rs:159-201`, `src/ingester/buffer.rs`

**Description**: The ingester's `WriteBuffer` is entirely in-memory with no write-ahead log (WAL) or durability mechanism:

```rust
pub async fn write(&self, batch: RecordBatch) -> Result<()> {
    let mut buffer = self.buffer.write().await;
    buffer.append(batch.clone())?;

    if self.should_flush(&buffer) {
        let batches = buffer.take();
        drop(buffer);
        self.flush_batches(batches).await?;
    }
    Ok(())
}
```

If the ingester crashes after `write()` returns `Ok(())` but before `flush_batches()` completes (or if flush thresholds haven't been met yet), all buffered data is lost. With default settings of `flush_interval: 300s` (5 minutes) and `flush_row_count: 1_000_000`, this could mean up to 5 minutes of data loss.

Furthermore, `flush_batches()` first converts to Parquet, uploads to S3, and then registers metadata. A crash between S3 upload and metadata registration leaves an orphaned Parquet file on S3 that will never be queried.

**Impact**: Up to 5 minutes of data loss per ingester crash. Orphaned S3 objects consuming storage.

**Severity**: CRITICAL

**Recommended Fix**: Implement a WAL (write-ahead log) to local disk before acknowledging writes. On restart, replay the WAL. Alternatively, reduce flush intervals and implement a "pending uploads" manifest that can be reconciled on startup.

---

## Finding 4: Shard Split Is Not Crash-Safe (CRITICAL)

**File**: `src/sharding/splitter.rs:58-115`

**Description**: The 5-phase shard split protocol in `execute_split_with_monitoring()` is a long-running operation (involving network I/O, data copies, and a 300-second grace period) with no crash recovery mechanism. If the process crashes at any point, the split is abandoned in whatever intermediate state it was in:

- **Crash during Phase 2 (DualWrite)**: Split state is set to `DualWrite` but ingesters may or may not have started dual-writing. There is no mechanism to resume or abort the split.
- **Crash during Phase 3 (Backfill)**: Historical data is partially copied. There is no idempotency guarantee -- the backfill does not track which chunks have been copied, so resuming would duplicate data.
- **Crash during Phase 4 (Cutover)**: The cutover at lines 407-421 creates new shard metadata and marks the old shard as `PendingDeletion` in three separate non-atomic operations. If the process crashes between creating `new_shard_a` and `new_shard_b`, only one new shard exists.
- **Crash during Phase 5 (Cleanup)**: Chunk deletion is partially complete.

Additionally, Phase 2 relies on a fixed `tokio::time::sleep(Duration::from_secs(10))` (line 87) to "give ingesters time to discover the split state." This is not a reliable coordination mechanism -- ingesters poll split state on every write, but a 10-second sleep is neither necessary nor sufficient to guarantee all ingesters have discovered the split.

**Impact**: Split operations that fail partway leave the system in an unrecoverable state requiring manual intervention. Potential for data duplication during backfill or data loss during cleanup.

**Severity**: CRITICAL

**Recommended Fix**: Implement a state machine that persists progress at each phase transition. On startup, detect in-progress splits and either resume or roll back. Make the backfill idempotent by tracking which chunks have been successfully copied. Make the cutover truly atomic (all-or-nothing).

---

## Finding 5: Compaction Can Double-Count or Lose Data (CRITICAL)

**File**: `src/compactor/mod.rs:325-382`, `src/metadata/s3.rs:1275-1372`

**Description**: The compaction flow has a gap between the merged chunk being uploaded to S3 and the metadata being atomically updated via `complete_compaction()`. During this window:

1. The merged chunk exists on S3 but is not yet in the metadata.
2. All source chunks are still in the metadata and queryable.
3. If `complete_compaction()` succeeds, source chunks are removed from metadata.
4. But the source chunks are only *scheduled* for deletion from S3 (via `schedule_deletion` at line 363).

If the compactor crashes after uploading the merged chunk but before `complete_compaction()`, the merged chunk is orphaned on S3. If it crashes after `complete_compaction()` but before source chunks are deleted from S3, queries won't see the source chunks (correct behavior), but S3 storage is leaked.

More seriously, `complete_compaction()` does not verify that the target chunk was successfully registered before removing source chunks. At line 1313:

```rust
if let Some(meta) = all_metadata.get_mut(target_chunk) {
    meta.level = new_level;
}
```

If `target_chunk` is not in `all_metadata` (e.g., it was never registered because the registration happened through a different code path), the source chunks are still removed, causing data loss.

**Impact**: Data loss if target chunk registration fails but source chunks are still removed. Orphaned S3 objects on crash.

**Severity**: CRITICAL

**Recommended Fix**: Register the target chunk in the same atomic CAS operation as source chunk removal (which is already done since they share `all_metadata`). Add an explicit check that the target chunk exists in metadata before removing sources. Implement orphan cleanup on startup.

---

## Finding 6: Single-File Metadata Bottleneck (HIGH)

**File**: `src/metadata/s3.rs:150-180, 258-305`

**Description**: All chunk metadata is stored in a single JSON file (`metadata/chunks/metadata.json`). Every metadata operation (register, delete, compact, query) must:
1. Download the entire file from S3
2. Deserialize the full JSON
3. Modify in memory
4. Serialize back to JSON
5. Upload the entire file

For a system targeting >1B unique series and >1M samples/sec, this file will grow to hundreds of MB or even GB. Every operation will have O(n) latency where n is the total number of chunks. At scale:
- A single `register_chunk()` call requires downloading, parsing, modifying, serializing, and uploading a multi-MB JSON file.
- CAS contention will be extreme since every ingester and compactor contends on the same file.
- S3 GET/PUT costs will dominate operational costs.

**Impact**: System becomes unusable at scale. O(n) metadata operations where n is total chunks. Extreme CAS contention under concurrent writes.

**Severity**: HIGH

**Recommended Fix**: Partition metadata by shard, time range, or use per-chunk metadata files. Consider migrating to a proper metadata store (FoundationDB, etcd, DynamoDB) for production use. The CLAUDE.md already mentions "FoundationDB for prod" but the trait should be designed with this scalability in mind.

---

## Finding 7: Time Index Cache Can Serve Stale Data During Writes (HIGH)

**File**: `src/metadata/s3.rs:184-239`

**Description**: The time index has a 60-second TTL cache (`time_index_ttl: Duration::from_secs(60)`). When `get_chunks()` is called (for queries), it uses this cached time index. But the cache is only invalidated on write operations via `save_time_index()` and `atomic_save_time_index()`.

In a multi-node setup:
- Ingester A writes chunk C1 and updates the time index on S3.
- Query node Q has a cached time index from 30 seconds ago.
- A query for the time range containing C1 arrives at Q.
- Q uses its stale cache and does not see C1.
- C1 is invisible for up to 60 seconds.

This means queries can return incomplete results for up to 60 seconds after data ingestion.

**Impact**: Queries return incomplete/stale results for up to 60 seconds. In a monitoring/alerting context, this means alerts can fire 60 seconds late or not at all if the window is shorter than 60s.

**Severity**: HIGH

**Recommended Fix**: Add a cache-busting mechanism (e.g., S3 notification, poll with If-None-Match header, or reduce TTL for critical queries). For queries that need freshness guarantees, bypass the cache entirely.

---

## Finding 8: `get_chunks_for_shard()` Uses Path Substring Matching (HIGH)

**File**: `src/metadata/s3.rs:1610-1633`, `src/metadata/local.rs:321-332`

**Description**: Both S3 and Local metadata clients identify chunks belonging to a shard using substring matching on the path:

```rust
.filter(|(path, _)| path.contains(shard_id))
```

This is fragile and can produce false positives. For example, if `shard_id = "shard-1"`, this would also match paths containing `"shard-10"`, `"shard-11"`, `"shard-100"`, etc. During a shard split, the old shard's chunks could be misattributed to a new shard if the shard ID is a substring of the new shard's ID (though UUID-based shard IDs make this less likely in practice, it is still a correctness hazard).

This function is used during backfill (line 162 of splitter.rs) and cleanup (line 445 of splitter.rs), so false positives could cause data from unrelated shards to be copied or deleted.

**Impact**: Potential data corruption during shard split if shard IDs overlap as substrings.

**Severity**: HIGH

**Recommended Fix**: Store shard ID as a field in chunk metadata rather than encoding it in the path. Use exact matching on the metadata field.

---

## Finding 9: No Deduplication During Dual-Write (HIGH)

**File**: `src/ingester/mod.rs:205-241`

**Description**: During dual-write mode (shard split Phase 2/3), the ingester writes data to both the old shard's buffer and the new shard destinations:

```rust
async fn write_with_split_awareness(&self, batch: RecordBatch, shard_id: &str) -> Result<()> {
    // Write to old shard first
    let mut buffer = self.buffer.write().await;
    buffer.append(batch.clone())?;
    drop(buffer);

    // Split and write to new shards
    let (batch_a, batch_b) = self.split_batch_by_key(&batch, &split_state.split_point)?;
    self.write_to_shard(&batch_a, &split_state.new_shards[0]).await?;
    self.write_to_shard(&batch_b, &split_state.new_shards[1]).await?;
}
```

There is no deduplication mechanism. After the backfill completes and the cutover happens, the data written during dual-write exists in both the old shard (which hasn't been cleaned up yet) and the new shards. Queries during Phase 5 (cleanup) could return duplicate results if they scan both old and new shards.

Furthermore, `write_to_shard()` immediately flushes to S3 and registers metadata (line 244-278), bypassing the normal buffer and flush logic. This means dual-write data is not batched efficiently and creates many small Parquet files.

**Impact**: Duplicate query results during split transition. Inefficient S3 usage from many small files during dual-write.

**Severity**: HIGH

**Recommended Fix**: Mark old shard chunks as "superseded" during cutover so queries don't scan them. Add deduplication based on timestamp ranges. Buffer dual-write data before flushing.

---

## Finding 10: No Compaction Mutual Exclusion (HIGH)

**File**: `src/compactor/mod.rs:268-296, 325-382`

**Description**: The compactor has no mechanism to prevent two compactor instances from selecting and compacting the same set of chunks simultaneously. The `get_l0_candidates()` and `get_level_candidates()` calls return candidates based on current metadata state, but there is no "claim" or "lock" mechanism:

1. Compactor A calls `get_l0_candidates()` and gets group [chunk1, chunk2, chunk3].
2. Compactor B calls `get_l0_candidates()` and gets the same group.
3. Both compactors read, merge, and upload new chunks.
4. Both call `complete_compaction()`. One succeeds, the other gets a CAS conflict and retries.
5. On retry, the source chunks are gone (removed by the first compactor), so the second compactor's merged chunk is either orphaned or causes an error.

The `create_compaction_job()` call does track jobs, but it is not used to prevent duplicate work -- it merely logs the job.

**Impact**: Wasted compute and S3 bandwidth from duplicate compaction. Potential for orphaned chunks or errors.

**Severity**: HIGH

**Recommended Fix**: Implement a "claim" mechanism in metadata. Before starting compaction, atomically mark source chunks as "being compacted" with a lease/TTL. Only the holder of the claim can complete the compaction.

---

## Finding 11: Query Consistency During Compaction (HIGH)

**File**: `src/query/mod.rs:139-175`, `src/metadata/s3.rs:958-1060`

**Description**: When a query is executing:
1. It calls `get_chunks_with_predicates()` to get the list of relevant chunks.
2. It registers those chunks with DataFusion.
3. DataFusion executes the query.

But between steps 1 and 3, a compaction might complete, removing the source chunks from S3. DataFusion would then try to read a file that no longer exists, causing a query error. The 60-second GC grace period (`gc_grace_period`) in the compactor mitigates this, but:
- Long-running queries (>60s) would still fail.
- The grace period is configurable and set to only 60 seconds by default, which is too short for production.
- There is no mechanism for queries to "pin" chunks they are using.

**Impact**: Long-running queries can fail with S3 "Not Found" errors if compaction deletes their chunks.

**Severity**: HIGH

**Recommended Fix**: Implement reference counting or "pinning" for in-use chunks. Extend GC grace period to at least 5-10 minutes. Add retry logic to DataFusion's ObjectStore wrapper to handle transient "Not Found" errors during compaction.

---

## Finding 12: Ingester Does Not Check Backpressure (MEDIUM)

**File**: `src/ingester/mod.rs:159-201`, `src/compactor/mod.rs:146-165`

**Description**: The compactor exposes a `backpressure()` method that returns recommended write delays, but the ingester never calls it. The only backpressure mechanism is the `max_buffer_size_bytes` check:

```rust
if buffer.size_bytes() + batch_size > self.config.max_buffer_size_bytes {
    return Err(Error::BufferFull);
}
```

This is a hard cutoff (reject writes) rather than a gradual slowdown. There is no feedback loop between the compactor's L0 pile-up and the ingester's write rate. If compaction falls behind, L0 files accumulate indefinitely, increasing query latency and metadata size.

**Impact**: No coordination between ingestion rate and compaction capacity. Write amplification and query degradation when compaction falls behind.

**Severity**: MEDIUM

**Recommended Fix**: Wire the compactor's backpressure signal into the ingester. Implement gradual write throttling before hard rejection.

---

## Finding 13: Unbounded Compaction Job List (MEDIUM)

**File**: `src/metadata/s3.rs:714-789`, `src/compactor/mod.rs:344-350`

**Description**: Compaction jobs are stored in a single `compaction-jobs.json` file with no cleanup of completed or failed jobs. Every `create_compaction_job()` appends to this list, and `get_pending_compaction_jobs()` loads and filters the full list. Over time, this file grows unboundedly, causing the same scalability issues as the metadata file.

Additionally, the `CompactionStatus::Completed` and `CompactionStatus::Failed` jobs are never removed, making the file an ever-growing append-only log.

**Impact**: Increasing metadata operation latency over time. Unnecessary S3 costs for loading/storing large job files.

**Severity**: MEDIUM

**Recommended Fix**: Implement job cleanup -- remove completed/failed jobs after a retention period. Move to per-job files or a proper job queue.

---

## Finding 14: No Fencing During Shard Split Cutover (MEDIUM)

**File**: `src/sharding/splitter.rs:344-432`

**Description**: The cutover phase creates new shard metadata for both new shards and marks the old shard as `PendingDeletion` in three separate `update_shard_metadata()` calls (lines 407-421). These are not an atomic transaction:

```rust
// Three separate CAS operations -- not atomic together
self.metadata.update_shard_metadata(&new_shard_a.shard_id, &new_shard_a, 0).await?;
self.metadata.update_shard_metadata(&new_shard_b.shard_id, &new_shard_b, 0).await?;
self.metadata.update_shard_metadata(old_shard, &old_updated, current_generation).await?;
```

If the process crashes between creating `new_shard_a` and `new_shard_b`, the key space is only partially covered. Queries and writes for the uncovered range will fail.

Also, there is no fencing token to prevent a stale split process (e.g., one that was paused due to GC) from completing a cutover that has already been superseded.

**Impact**: Partial key space coverage on crash during cutover. Potential for stale split processes to corrupt state.

**Severity**: MEDIUM

**Recommended Fix**: Create both new shards atomically (e.g., in a single metadata file) before deactivating the old shard. Add a fencing token (e.g., split ID) that must match the current active split to proceed.

---

## Finding 15: Clock Skew Vulnerability in Time-Based Operations (MEDIUM)

**File**: `src/ingester/mod.rs:504-517`, `src/compactor/mod.rs:533-534`

**Description**: Multiple operations rely on `chrono::Utc::now()` for generating timestamps:
- Chunk path generation includes year/month/day/hour from local clock.
- Retention enforcement calculates cutoff based on local clock.
- Split timestamps use local clock.
- The `Instant::now()` used for GC grace periods is monotonic (good), but mixed with `chrono::Utc::now()`.

In a distributed deployment, clock skew between nodes can cause:
- Chunks being placed in wrong hour directories (minor -- path is cosmetic).
- Retention enforcement deleting chunks prematurely or failing to delete them.
- Split timestamps being inconsistent across nodes.

**Impact**: Premature data deletion or retention policy violations under clock skew. Cosmetic path inconsistencies.

**Severity**: MEDIUM

**Recommended Fix**: Use a single authoritative time source for critical operations (e.g., S3 object timestamps, or NTP-synced system clock with drift bounds). For retention, use chunk metadata timestamps rather than wall clock.

---

## Finding 16: Error Handling in read_chunk Silently Drops Errors (MEDIUM)

**File**: `src/compactor/merge.rs:48`

**Description**: The `read_chunk()` method uses `filter_map(|r| r.ok())` which silently drops Parquet read errors:

```rust
let batches: Vec<RecordBatch> = reader.filter_map(|r| r.ok()).collect();
```

If a Parquet file is partially corrupted, this will silently skip corrupted record groups. The compaction would then produce a merged file missing data, effectively causing silent data loss.

**Impact**: Silent data loss during compaction of corrupted Parquet files.

**Severity**: MEDIUM

**Recommended Fix**: Propagate errors instead of silently skipping: `reader.collect::<std::result::Result<Vec<_>, _>>()?`.

---

## Finding 17: Shard Router Cache Does Not Validate Generation (MEDIUM)

**File**: `src/sharding/router.rs:33-48`

**Description**: The `ShardRouter::get_shard()` method returns cached shard metadata based on key range matching, but does not validate the generation number against the current metadata. A stale cache entry with an old generation could route writes to a shard that has already been split:

```rust
pub fn get_shard(&self, key: &ShardKey) -> Option<ShardMetadata> {
    for entry in self.cache.iter() {
        if entry.cached_at.elapsed() > self.ttl {
            continue;  // TTL-based expiration only
        }
        let shard = &entry.shard;
        if key_bytes >= shard.key_range.0 && key_bytes < shard.key_range.1 {
            return Some(shard.clone());
        }
    }
    None
}
```

The only staleness protection is the 60-second TTL, during which writes could be misrouted.

**Impact**: Writes routed to stale shards during the TTL window after a split completes.

**Severity**: MEDIUM

**Recommended Fix**: On `StaleGeneration` error, invalidate the cache entry (which is already done in `route_query` but not in the general `get_shard()` path used by ingesters). Add generation validation in the routing layer.

---

## Finding 18: `NOT` Predicate Evaluation Is Incorrect (LOW)

**File**: `src/metadata/predicates.rs:138-148`

**Description**: The `ColumnPredicate::Not` evaluation returns the inner result directly instead of negating it:

```rust
ColumnPredicate::Not(inner) => {
    let inner_result = inner.evaluate_against_stats(column_stats);
    inner_result  // BUG: should be !inner_result or a conservative true
}
```

The comment says "Be conservative: only prune if we're certain" but the implementation does not negate the inner result. `NOT (col > 300)` would prune chunks where `col > 300` is possible, which is the opposite of correct behavior.

**Impact**: Incorrect chunk pruning for queries with NOT predicates, potentially returning incomplete results.

**Severity**: LOW (NOT predicates are uncommon in TSDB queries, and the current behavior errs on the side of including too many chunks rather than pruning too aggressively in most cases).

**Recommended Fix**: Return `true` (never prune on NOT predicates) for safety, or implement proper negation with conservative semantics.

---

## Finding 19: `update_split_progress` Is Silently No-Op for Missing State (LOW)

**File**: `src/metadata/s3.rs:1519-1574`, `src/metadata/local.rs:302-313`

**Description**: Both metadata clients silently succeed when `update_split_progress()` is called for a shard with no split state:

```rust
if !state_found {
    warn!("No split state found for shard {} during progress update", shard_id);
    return Ok(());  // Silent success
}
```

This masks bugs where the split state was prematurely cleaned up or never created. The caller has no way to know the progress update was not applied.

**Impact**: Masking of split orchestration bugs. Progress tracking becomes unreliable.

**Severity**: LOW

**Recommended Fix**: Return an error when the split state does not exist, so callers can detect and handle the situation.

---

## Finding 20: Pending Deletions List Is Not Persisted (LOW)

**File**: `src/compactor/mod.rs:103, 526-529`

**Description**: The compactor's `pending_deletions` list uses `std::sync::RwLock<Vec<(String, Instant)>>` -- an in-memory data structure. If the compactor restarts, all pending deletions are lost, leaving orphaned S3 objects that were removed from metadata but never deleted from storage.

**Impact**: Storage leaks on compactor restart. Orphaned S3 objects accumulate over time.

**Severity**: LOW

**Recommended Fix**: Persist pending deletions to S3 metadata. On startup, resume deletion of any pending items.

---

## Finding 21: ETag String Matching for Error Detection (LOW)

**File**: `src/metadata/s3.rs:172, 222-224, 295-296, and throughout`

**Description**: Error type detection throughout the S3 metadata client uses string matching on error messages:

```rust
Err(e) if e.to_string().contains("Not Found")
    || e.to_string().contains("not found")
    || e.to_string().contains("No data in memory") => { ... }
```

```rust
Err(e) if e.to_string().contains("Precondition")
    || e.to_string().contains("412")
    || e.to_string().to_lowercase().contains("conflict") => { ... }
```

This is fragile and depends on the exact error message format of the `object_store` crate, which could change between versions. The `"No data in memory"` pattern suggests this was added to handle a specific InMemory store behavior but could accidentally match unrelated errors.

**Impact**: Incorrect error classification if error message format changes. Potential for masking real errors.

**Severity**: LOW

**Recommended Fix**: Match on `object_store::Error` variants directly instead of string matching. The `object_store` crate provides specific error types for `NotFound` and `Precondition` failures.

---

## Architectural Recommendations

### Priority 1: Fix Metadata Atomicity (Addresses Findings 1, 2, 5, 6)

The single-file metadata approach with S3 ETags is fundamentally limited. Consider:

1. **Short-term**: Merge `metadata.json` and `time-index.json` into a single file to eliminate cross-file inconsistency.
2. **Medium-term**: Partition metadata by time range (e.g., one file per hour bucket) to reduce contention.
3. **Long-term**: Implement the FoundationDB/etcd-based metadata backend mentioned in CLAUDE.md. This eliminates all CAS limitations and provides true ACID transactions.

### Priority 2: Add Write Durability (Addresses Finding 3)

Implement a local WAL for the ingester:
- Write to a local file before acknowledging the write.
- On restart, replay the WAL.
- Delete WAL segments after successful S3 flush.

### Priority 3: Make Shard Split Crash-Safe (Addresses Findings 4, 9, 14)

Redesign the split protocol as a persistent state machine:
- Each phase transition is persisted to metadata before proceeding.
- On startup, detect in-progress splits and resume from the last completed phase.
- Make backfill idempotent by tracking per-chunk completion.
- Make cutover atomic by creating both new shards in a single metadata update.

### Priority 4: Add Compaction Coordination (Addresses Findings 10, 11, 13)

- Implement chunk "leasing" for compaction to prevent duplicate work.
- Add query-time chunk pinning to prevent GC of in-use chunks.
- Clean up completed compaction jobs periodically.

---

## Testing Gaps

*Cross-referenced with test engineer audit findings. The most critical architectural gaps (Findings 1-5) are also the least tested areas, meaning bugs are most likely to exist precisely where they would cause the most damage.*

### Gap 1: Metadata Concurrent Tests Only Use InMemory Store

**Reinforces**: Findings 1 (Non-Atomic Two-Object Update), 2 (MinIO Fallback), 6 (Single-File Bottleneck)

**Evidence**: All 6 tests in `tests/atomic_metadata_tests.rs` use `object_store::memory::InMemory`:
- `test_concurrent_chunk_registration()` (line 14): `Arc::new(InMemory::new())`
- `test_atomic_retry_on_conflict()` (line 62): `Arc::new(InMemory::new())`
- `test_heavy_concurrent_load()` (line 128): `Arc::new(InMemory::new())`
- `test_atomic_compaction_completion()` (line 201): `Arc::new(InMemory::new())`
- `test_concurrent_compactions()` (line 272): `Arc::new(InMemory::new())`

All 9 tests in `tests/generation_cas_tests.rs` use `LocalMetadataClient`:
- `test_generation_increments()` (line 31): `LocalMetadataClient::new()`
- `test_stale_generation_rejected()` (line 68): `LocalMetadataClient::new()`
- `test_concurrent_cas_updates()` (line 97): `LocalMetadataClient::new()`
- All remaining tests: same pattern.

**Why this matters**: The `InMemory` object store does not support conditional PUTs (S3 ETags). The `S3MetadataClient` code detects this and falls back to `PutMode::Overwrite` (Finding 2, `src/metadata/s3.rs:354-384`). This means:
- The tests *appear* to test concurrent CAS operations, but CAS is actually disabled during these tests.
- The real contention behavior (ETag conflicts, retry storms, partial two-object updates) has never been exercised.
- `test_heavy_concurrent_load()` (line 128) additionally silently swallows registration failures at line 156 (`if client.register_chunk(...).is_ok()`), masking any concurrent access issues.

### Gap 2: No Crash Recovery Tests Exist

**Reinforces**: Findings 3 (No WAL), 4 (Crash-Unsafe Shard Split), 5 (Compaction Data Loss), 20 (Pending Deletions Not Persisted)

**Evidence**: Zero tests in the entire test suite simulate process crash during:
- Ingester write (Finding 3: up to 5 minutes of data loss)
- Shard split at any phase (Finding 4: unrecoverable intermediate state)
- Compaction between S3 upload and metadata registration (Finding 5: orphaned chunks or data loss)
- GC between metadata removal and S3 deletion (Finding 20: storage leaks)

**Why this matters**: The crash recovery paths are the most dangerous code paths in a distributed system, and they are completely untested. The split state machine has no recovery mechanism, and no test validates that the system can recover from partial operations.

### Gap 3: Several Tests Accept Both Success AND Failure

**Reinforces**: Finding 4 (Crash-Unsafe Shard Split)

**Evidence**: `tests/shard_split_tests.rs:296-367`, `test_full_split_execution()`:
```rust
match result {
    Ok(_) => {
        // Split succeeded - verify split state is cleaned up
        let split_state = metadata.get_split_state(&shard.shard_id).await.unwrap();
        assert!(split_state.is_none(), "Split state should be cleaned up");
    }
    Err(e) => {
        // Expected to fail at cutover due to missing shard metadata
        // But phases 1-3 should have executed
        assert!(
            e.to_string().contains("shard") || e.to_string().contains("generation"),
            "Error should be related to shard metadata: {}",
            e
        );
    }
}
```

This test accepts *any* error containing "shard" or "generation" as a valid outcome. This is problematic because:
- It cannot distinguish between an expected limitation (missing shard metadata) and a real bug (e.g., the non-atomic cutover from Finding 14 failing partway).
- The assertion `e.to_string().contains("shard")` would match virtually any shard-related error, including errors from completely unrelated bugs.
- It provides false confidence that the full split execution is tested when in fact it only tests the failure path.

### Gap 4: No Tests for Concurrent Compaction+Write or Concurrent Split+Write

**Reinforces**: Findings 9 (No Deduplication During Dual-Write), 10 (No Compaction Mutual Exclusion), 11 (Query Consistency During Compaction)

**Evidence**: No test in the suite exercises:
- An ingester writing data while a compactor is running on the same chunks (Finding 11: queries can get S3 "Not Found" if compaction deletes chunks during a query).
- Two compactors selecting the same chunk group (Finding 10: no mutual exclusion mechanism).
- An ingester performing dual-write while compaction runs on the old shard's data (Findings 9+10 combined).
- A query executing while a split cutover changes the shard routing (Finding 17: stale router cache).

**Why this matters**: These are the exact production scenarios where race conditions and data corruption occur. The individual tests verify each component in isolation, but the interactions between components are untested. In a real deployment, these components run concurrently and their interactions are where most distributed systems bugs manifest.
