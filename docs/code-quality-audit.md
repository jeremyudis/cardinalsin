# CardinalSin Code Quality Audit

**Date**: 2026-02-06
**Scope**: Full codebase review of `/Users/jeremyudis/code/cardinalsin/`
**Focus**: Production readiness, anti-patterns, reinvented wheels, code quality

---

## Executive Summary

The codebase demonstrates solid domain understanding and good architectural decisions (labels-as-columns, ETag-based CAS, leveled compaction). However, it has several production-critical issues: **massive code duplication** in the S3 metadata layer, **hand-rolled retry/backoff logic** that should use well-maintained crates, **error context swallowing**, **string-based error matching** for control flow, **unbounded data structures**, and **silent data corruption on parse failure**. The following report categorizes findings by severity.

---

## P0: Critical Production Issues

### 1. String-based error matching for control flow (`src/metadata/s3.rs`)

**Problem**: The entire S3 metadata layer matches errors by searching for substrings in error messages. This is fragile and will break silently if the `object_store` crate changes its error messages.

**Locations**:
- `src/metadata/s3.rs:172` - `e.to_string().contains("Not Found")`
- `src/metadata/s3.rs:222-224` - `contains("Not Found") || contains("not found") || contains("No data in memory")`
- `src/metadata/s3.rs:293-295` - same triple-check pattern
- `src/metadata/s3.rs:347-349` - matching `"Precondition"`, `"412"`, `"conflict"`
- `src/metadata/s3.rs:354` - matching `"not yet implemented"` for MinIO fallback
- `src/metadata/s3.rs:504`, `src/metadata/s3.rs:783`, `src/metadata/s3.rs:861`, `src/metadata/s3.rs:942` - same pattern repeated

**Fix**: Match on `object_store::Error` variants directly:
```rust
match e {
    object_store::Error::NotFound { .. } => { /* handle */ }
    object_store::Error::Precondition { .. } => { /* conflict */ }
    object_store::Error::NotImplemented { .. } => { /* MinIO fallback */ }
    _ => Err(Error::ObjectStore(e))
}
```

### 2. Silent data corruption: `unwrap_or_else` on deserialization errors (`src/metadata/s3.rs`)

**Problem**: When JSON deserialization fails, the code silently returns an empty collection and logs a warning. This means **corrupted metadata files are silently ignored**, and the system will happily overwrite them with empty data on the next write, losing all metadata.

**Locations**:
- `src/metadata/s3.rs:164` - `serde_json::from_str(&content).unwrap_or_else(|e| { warn!(...); HashMap::new() })`
- `src/metadata/s3.rs:208-212` - same for time index
- `src/metadata/s3.rs:280-283` - same for chunk metadata with ETag
- `src/metadata/s3.rs:429-433` - same for time index with ETag
- `src/metadata/s3.rs:727-728` - same for compaction jobs
- `src/metadata/s3.rs:805-806` - same for split states
- `src/metadata/s3.rs:1426` - same for pending compaction jobs
- `src/metadata/s3.rs:1505` - same for split states in get_split_state

**Fix**: Return an error on deserialization failure. If recovery is needed, implement an explicit recovery mode, not a silent fallback:
```rust
let metadata: HashMap<String, ChunkMetadataExtended> =
    serde_json::from_str(&content)
    .map_err(|e| Error::Metadata(format!("Corrupt metadata at {}: {}", path, e)))?;
```

### 3. Non-atomic two-phase write in `atomic_register_chunk` (`src/metadata/s3.rs:604-711`)

**Problem**: `atomic_register_chunk` writes chunk metadata and time index as two separate CAS operations. If the chunk metadata CAS succeeds but the time index CAS fails, the system is left in an inconsistent state: the chunk exists in metadata but is not indexed. The retry loop only retries on Conflict, not on other failures.

**Location**: `src/metadata/s3.rs:639-670`

**Fix**: Either:
1. Store both in a single JSON file (simpler, less S3 calls)
2. Implement a write-ahead log pattern so partial failures can be recovered
3. At minimum, if time index save fails with a non-Conflict error, roll back the chunk metadata

### 4. Hand-rolled retry/backoff logic - reinventing the wheel (`src/metadata/s3.rs`)

**Problem**: The same retry-with-exponential-backoff pattern is copy-pasted ~10 times across the file. This is exactly what the `backoff` or `tokio-retry` crates provide, with additional features like jitter, configurable strategies, and proper instrumentation.

**Locations** (all in `src/metadata/s3.rs`):
- `atomic_register_chunk()` at line 609
- `delete_chunk()` at line 1077
- `create_compaction_job()` at line 1245
- `complete_compaction()` at line 1280
- `update_compaction_status()` at line 1375
- `start_split()` at line 1456
- `update_split_progress()` at line 1525
- `complete_split()` at line 1577
- `update_shard_metadata()` at line 1652

**Each instance** repeats:
```rust
for retry in 0..MAX_CAS_RETRIES {
    // load with etag
    // modify
    // attempt save
    match result {
        Ok(_) => return Ok(()),
        Err(Error::Conflict) => {
            let backoff_ms = BASE_BACKOFF_MS * 2_u64.pow(retry);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            continue;
        }
        Err(e) => return Err(e),
    }
}
Err(Error::TooManyRetries)
```

**Fix**: Add `backoff = "0.4"` to dependencies and extract a generic helper:
```rust
async fn with_cas_retry<F, Fut, T>(operation: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    backoff::future::retry(ExponentialBackoff::default(), || async {
        operation().await.map_err(|e| match e {
            Error::Conflict => backoff::Error::transient(e),
            other => backoff::Error::permanent(other),
        })
    }).await
}
```

---

## P1: Significant Code Quality Issues

### 5. Massive code duplication in S3 metadata layer

**Problem**: `src/metadata/s3.rs` is ~1700 lines long with enormous repetition. The load-with-etag and atomic-save patterns are duplicated for every data type: chunk metadata, time index, compaction jobs, split states, shard metadata. Each pair follows the identical pattern.

**Duplicated load-with-etag methods**:
- `load_chunk_metadata_with_etag()` (line 258)
- `load_time_index_with_etag()` (line 411)
- `load_compaction_jobs_with_etag()` (line 714)
- `load_split_states_with_etag()` (line 792)
- `load_shard_with_etag()` (line 870)

**Duplicated atomic-save methods**:
- `atomic_save_chunk_metadata()` (line 308)
- `atomic_save_time_index()` (line 458)
- `atomic_save_compaction_jobs()` (line 749)
- `atomic_save_split_states()` (line 827)
- `atomic_save_shard()` (line 905)

**Fix**: Extract a generic `AtomicJsonStore<T>` that handles load-with-etag, atomic-save, and not-found fallback for any `Serialize + DeserializeOwned` type. This would reduce the file by ~60%.

### 6. Duplicate `split_batch` implementations

**Problem**: The batch-splitting-by-timestamp logic is implemented twice with slightly different approaches:
- `src/ingester/mod.rs:282-327` (`split_batch_by_key`) - uses `as_primitive_opt::<Int64Type>` via `AsArray`
- `src/sharding/splitter.rs:236-285` (`split_batch`) - uses `as_any().downcast_ref::<Int64Array>()`

Both do the same thing: split a RecordBatch by comparing timestamps to a split point.

**Fix**: Extract to a shared utility function in `src/sharding/mod.rs` or a new `src/utils.rs`.

### 7. `hour_bucket` duplicated with a constant (`NANOS_PER_HOUR`)

**Problem**: The constant `3_600_000_000_000i64` appears as:
- `LocalMetadataClient::NANOS_PER_HOUR` (line 58) AND `hour_bucket()` function using a local variable (line 53)
- `S3MetadataClient::NANOS_PER_HOUR` (line 100) AND `hour_bucket()` (line 114)
- `get_chunks_with_predicates()` inlines it again at line 972
- `get_l0_candidates()` inlines it again at line 1178

**Fix**: Define `NANOS_PER_HOUR` once in `src/metadata/mod.rs` and use it everywhere.

### 8. Error type `Error::Shard(ShardError)` redundant with standalone variants

**Problem**: `src/error.rs` has both:
- `Error::Shard(ShardError)` with `ShardError::NotFound(String)`
- `Error::ShardNotFound(String)`

These are redundant. Also `ShardError` doesn't implement `Display` (only `Debug`), so `Error::Shard(e)` at line 98 prints `Shard error: {:?}` which produces ugly debug output in user-facing messages.

**Fix**: Either remove `Error::ShardNotFound` and use `Error::Shard(ShardError::NotFound(...))` consistently, or remove `ShardError` and use standalone variants. Implement `Display` for `ShardError` regardless.

### 9. Error type uses `thiserror` in `Cargo.toml` but doesn't use it

**Problem**: `thiserror = "1"` is in dependencies but `src/error.rs` manually implements `std::error::Error`, `Display`, and all `From` conversions. This is exactly what `thiserror` automates.

**Fix**: Use `#[derive(thiserror::Error)]` to reduce ~100 lines of boilerplate.

### 10. `serde_json::Error` loses type information (`src/error.rs:157-160`)

**Problem**: `From<serde_json::Error>` converts to `Error::Serialization(e.to_string())`, discarding the original error. This means `.source()` returns `None` for serialization errors, breaking error chains.

**Fix**: Either store `serde_json::Error` directly or use `thiserror` with `#[from]`.

### 11. Predicate `Not` is implemented incorrectly (`src/metadata/predicates.rs:138-148`)

**Problem**: The `Not` variant of `ColumnPredicate::evaluate_against_stats` just returns the inner result unchanged:
```rust
ColumnPredicate::Not(inner) => {
    let inner_result = inner.evaluate_against_stats(column_stats);
    inner_result  // This is wrong! NOT should invert or be conservative
}
```
This means `NOT (x = 5)` behaves identically to `x = 5` for pruning purposes, which will incorrectly prune chunks.

**Fix**: For safety, `Not` should always return `true` (never prune), since chunk statistics can't safely evaluate negation:
```rust
ColumnPredicate::Not(_) => true, // Can't safely prune on negated predicates
```

### 12. `NotEq` and `NotIn` have dead code branches (`src/metadata/predicates.rs:73-80, 110-117`)

**Problem**: Both `NotEq` and `NotIn` have `if/else` branches that both return `true`:
```rust
ColumnPredicate::NotEq(col, _val) => {
    if column_stats.contains_key(col) {
        true
    } else {
        true
    }
}
```

**Fix**: Simplify to just `true` and add a comment explaining why.

### 13. `read_chunk` silently drops parse errors (`src/compactor/merge.rs:48`)

**Problem**: `reader.filter_map(|r| r.ok())` silently drops any Parquet read errors during compaction merging. If a row group is corrupt, it will be silently skipped and the data lost.

**Fix**: Collect errors and propagate them:
```rust
let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
```

### 14. `once_cell` is unnecessary with Rust 2021 edition

**Problem**: `once_cell = "1"` is in dependencies. Since Rust 1.80+, `std::sync::LazyLock` and `std::sync::OnceLock` are stable. The crate appears unused anyway.

**Fix**: Remove from `Cargo.toml`.

### 15. Duplicate query predicate extraction (`src/query/streaming.rs` vs `src/query/engine.rs`)

**Problem**: `QueryFilter::from_sql()` in `src/query/streaming.rs:224-243` parses SQL using `sqlparser` to extract predicates. Meanwhile, `QueryEngine::extract_column_predicates()` in `src/query/engine.rs:420-431` does the same thing using DataFusion's logical plan analysis. Two completely different predicate extraction systems for the same purpose.

Also, `src/query/streaming.rs` defines its own `Predicate`, `PredicateOp`, `PredicateValue` types (lines 198-220), while `src/metadata/predicates.rs` defines `ColumnPredicate`, `PredicateValue` - another duplicate type system.

**Fix**: Use a single predicate extraction path. The DataFusion-based approach in `engine.rs` is more robust. The streaming module should reuse `ColumnPredicate` from `metadata/predicates.rs`.

---

## P2: Design and Maintainability Issues

### 16. `Config` struct in `src/lib.rs` is not used

**Problem**: `src/lib.rs:38-49` defines a `Config` struct, but it's never referenced anywhere in the codebase. Actual configuration is done through individual component configs (`IngesterConfig`, `QueryConfig`, `CompactorConfig`) and `ComponentFactory` environment variables.

### 17. `config` crate is in dependencies but unused

**Problem**: `config = "0.14"` is listed as a dependency, but configuration is done entirely through `std::env::var()` calls in `src/config.rs`. The `config` crate provides layered configuration (files, env vars, defaults) which would be better than ad-hoc env var reading.

### 18. `QueryEngine::register_chunk` silently ignores errors (`src/query/engine.rs:106-119`)

**Problem**: If `ListingTableUrl::parse()` fails or schema inference fails, the method returns `Ok(())` silently, meaning the chunk is never registered but no error is reported.

```rust
if let Ok(table_url) = ListingTableUrl::parse(&url) {
    // if this fails, Ok(()) is returned silently
    if let Ok(config) = config.infer_schema(&self.ctx.state()).await {
        // ...
    }
}
Ok(())  // Silent success even on failure
```

**Fix**: Propagate the errors or at least log them.

### 19. `QueryRouter::get_shard` is O(n) linear scan (`src/query/router.rs:42-56`)

**Problem**: Every shard lookup scans all entries, checking TTL and key range. With many shards this will be slow.

**Fix**: Use a sorted data structure (e.g., `BTreeMap` keyed by `min_key`) or an interval tree for O(log n) lookup.

### 20. Unbounded `pending_deletions` in Compactor (`src/compactor/mod.rs:103`)

**Problem**: `pending_deletions: std::sync::RwLock<Vec<(String, std::time::Instant)>>` grows without bound. If GC runs behind or is disabled, this vector will grow indefinitely.

**Fix**: Add a maximum size with an eviction policy for the oldest entries, or use a bounded data structure.

### 21. `CompactionStatus` derives `Clone` but should derive `Copy` (`src/metadata/mod.rs:77`)

**Problem**: `CompactionStatus` is an enum of 4 unit variants. It derives `Clone` but not `Copy`, requiring `.clone()` calls at `src/metadata/s3.rs:1381`.

**Fix**: Add `Copy` to the derive list.

### 22. `Level` enum has redundant `L0` variant

**Problem**: `Level::L0` and `Level::L(0)` represent the same thing, but the code must handle both. `Level::from(0)` returns `L0`, not `L(0)`, creating a subtle inconsistency.

**Fix**: Just use `Level(usize)` as a newtype, or document that `L(0)` is never constructed.

### 23. `TopicFilter` derives `Hash` but contains `Vec` with non-order-dependent semantics

**Problem**: `TopicFilter::Metrics(Vec<String>)` derives `Hash`, but `vec!["a", "b"]` and `vec!["b", "a"]` hash differently even though they represent the same filter. This could cause issues if used as a HashMap key.

**Fix**: Either use `BTreeSet<String>` or remove the `Hash` derive.

### 24. `Ingester::compute_shard_id` uses `DefaultHasher` which is not stable across runs

**Problem**: `src/sharding/mod.rs:59-63` uses `DefaultHasher` to hash metric names for shard keys. `DefaultHasher` is explicitly not guaranteed to be stable across Rust versions or platforms.

**Fix**: Use a stable hash like `xxhash` or `siphasher` with a fixed seed.

### 25. `from_be_bytes` panic potential in shard split (`src/sharding/splitter.rs:262-265`)

**Problem**: `split_point.try_into()` converts `&[u8]` to `[u8; 8]`. If the split point is not exactly 8 bytes, this returns an error that gets mapped to `Error::Internal("Invalid split point")`, but the same pattern at `src/sharding/splitter.rs:373-374` uses `unwrap_or([0u8; 8])` which silently produces a zero timestamp on malformed data.

### 26. `CachedObjectStore::get` returns fabricated `ObjectMeta` (`src/query/cached_store.rs:97-110`)

**Problem**: The cached `GetResult` fabricates metadata with `last_modified: chrono::Utc::now()` and `e_tag: None`. This means every cached read appears to be newly modified, which could break conditional GET logic and ETag-based caching in downstream code.

**Fix**: Store the original `ObjectMeta` alongside the cached bytes.

### 27. `from_sql` lowercases predicate values (`src/query/streaming.rs:315`)

**Problem**: `PredicateValue::String(s.to_lowercase())` lowercases the predicate value, meaning `WHERE metric_name = 'CPU'` will match `cpu` but not the actual value `CPU`. This is incorrect for case-sensitive data.

---

## P3: Minor Issues and Cleanup

### 28. Dead `#[allow(dead_code)]` markers

- `src/metadata/s3.rs:119` - `chunk_metadata_path` is `#[allow(dead_code)]`
- `src/ingester/mod.rs:87-88` - `schema` field is `#[allow(dead_code)]`
- `src/sharding/splitter.rs:36-37` - `PendingShard` is `#[allow(dead_code)]`

If these are truly unused, remove them. If they're planned, add a TODO comment.

### 29. Unnecessary dependencies

- `once_cell` - use `std::sync::OnceLock` (stable since 1.80)
- `config` - unused
- `hex` - grep finds no usage
- `pin-project-lite` - grep finds no usage (may be transitive)
- `humantime` - listed in both dependencies and dev-dependencies
- `lz4_flex` and `snap` compression crates - used only as Parquet feature flags, consider removing from direct deps

### 30. Missing `Default` impl for `LocalMetadataClient`

Already fixed - `Default` is implemented at `src/metadata/local.rs:61-65`.

### 31. `WriteBuffer::append` returns `Result` but never errors (`src/ingester/buffer.rs:28-37`)

**Problem**: The function signature is `pub fn append(&mut self, batch: RecordBatch) -> Result<()>` but it always returns `Ok(())`. This forces callers to use `.unwrap()` or `?` unnecessarily.

**Fix**: Change return type to `()`.

### 32. `l2_size` used for memory capacity in foyer builder (`src/query/cache.rs:88`)

**Problem**: `HybridCacheBuilder::new().memory(config.l1_size)` uses `l1_size` for memory, which is correct. But the field name confusion suggests the L2 builder's memory parameter was possibly intended to be different. The naming is at least confusing.

### 33. `prelude` module re-exports may be overly broad (`src/lib.rs:76-82`)

Minor: The prelude exports 8 types. Consider whether all are needed for typical consumers.

### 34. Missing `#[must_use]` on important return types

Types like `CompactionBackpressure`, `CacheStats`, `BufferStats` should have `#[must_use]` to prevent accidental discard.

### 35. `run_flush_timer` is an infinite loop with no shutdown signal (`src/ingester/mod.rs:477-501`)

**Problem**: The flush timer loop runs forever. There is no way to gracefully shut down the ingester. Same issue with `Compactor::run()` at `src/compactor/mod.rs:172-182`, `NodeRegistry::run_health_checks()` at `src/cluster/node_registry.rs:210-243`, and `AdaptiveIndexController::run()` at `src/adaptive_index/mod.rs:104-113`.

**Fix**: Accept a `tokio::sync::watch::Receiver<bool>` or `CancellationToken` for graceful shutdown.

### 36. `FilteredReceiver::recv` has a division-by-zero on stats logging (`src/ingester/topic_broadcast.rs:197-199`)

**Problem**: When `delivered_count + filtered_count` is 0 and `filtered_count % 100 == 0`, the percentage calculation divides by zero. The modulo check prevents this at `filtered_count == 0`, but technically the types allow it.

---

## Dependency Audit

### Good Choices
- `datafusion`, `arrow`, `parquet` - canonical Rust data processing stack
- `moka` for in-memory caching - well-maintained, battle-tested
- `foyer` for hybrid caching - actively maintained
- `dashmap` for concurrent maps - appropriate
- `tokio` with full features - standard

### Concerns
- **No `backoff` or `tokio-retry`** for retry logic despite extensive need
- **`once_cell`** - removable with modern Rust
- **`config`** - listed but unused, adds compile time
- **`hex`** - appears unused
- **`pin-project-lite`** - appears unused (may be transitive but should not be direct)
- **`humantime`** - duplicated in deps and dev-deps

### Missing Dependencies That Would Help
- `backoff` or `tokio-retry` - for retry/backoff patterns
- `tracing-error` or `color-eyre` - for better error context
- `static_assertions` - for compile-time invariant checking
- `tokio-util` - for `CancellationToken` graceful shutdown

---

## P1-A: Systemic Weak Test Assertions (Addendum)

This section documents a codebase-wide pattern of tests that provide false confidence by accepting any outcome, asserting trivially true conditions, or never actually exercising the code path they claim to test.

### 37. "Accept both Ok and Err" pattern in tests

**Problem**: Multiple tests use `match result { Ok(_) => { ... }, Err(e) => { println!(...) } }` where the Err branch just prints a message and the test passes regardless. These tests can NEVER fail.

**Instances in E2E tests** (`tests/e2e/smoke/`):

- `roundtrip_tests.rs:41-55` (`test_write_then_query_returns_data`): Writes data, queries it, and if the query fails, just prints "Query failed" and passes. The core roundtrip promise is never verified.
- `roundtrip_tests.rs:83-92` (`test_multiple_metrics_queryable`): Same pattern. Ok branch just prints, Err branch just prints. Zero assertions in either path.
- `roundtrip_tests.rs:116-131` (`test_sql_query_with_filters`): Ok branch asserts columns exist, but Err branch just prints and passes. The test name claims to verify filters but will pass with no data at all.
- `roundtrip_tests.rs:159-180` (`test_sql_aggregation_query`): Same. Good assertions in Ok path, but Err branch silently passes.
- `roundtrip_tests.rs:208-218` (`test_time_range_query`): Ok branch only prints, Err branch only prints. No assertions at all.

- `prometheus_api_tests.rs:31-43` (`test_prometheus_instant_query`): Ok branch asserts status, but Err branch prints and passes. A failing Prometheus API is accepted.
- `prometheus_api_tests.rs:94-101` (`test_prometheus_label_values_endpoint`): Both branches just print. Zero assertions.
- `prometheus_api_tests.rs:122-134` (`test_prometheus_range_query`): Same Ok-asserts/Err-passes pattern.
- `prometheus_api_tests.rs:161-172` (`test_prometheus_query_with_labels`): Same.
- `prometheus_api_tests.rs:188-199` (`test_prometheus_sum_aggregation`): Same.
- `prometheus_api_tests.rs:215-226` (`test_prometheus_sum_by_aggregation`): Same.

**Total: 11 E2E tests that can never fail on Err.**

**Mitigating factor**: These E2E tests are all `#[ignore]` (require docker-compose stack), so they don't run in CI by default. But this makes them even more dangerous -- when someone finally runs them manually to validate a deployment, they will report green regardless of actual behavior.

**Instances in integration tests**:

- `shard_split_tests.rs:296-366` (`test_full_split_execution`): The test explicitly accepts both `Ok` and `Err` outcomes. The comment says "This may fail at cutover because we haven't set up full shard metadata" -- this is not a test, it's an excuse. The test should either set up proper state or be marked as incomplete.

  The Err branch checks that the error message contains "shard" or "generation", which is slightly better than nothing but still means a completely unrelated error (e.g., a null pointer) with "shard" in its message chain would pass.

### 38. Tests that only verify initialization, not behavior

**Problem**: Several tests create components and assert they exist, without testing any actual behavior.

- `adaptive_indexing_tests.rs:15-22` (`test_extract_filter_columns_simple`): Named "test filter column extraction" but only asserts `Arc::strong_count(&index_controller) >= 1`. This is ALWAYS true -- you just created the Arc. The test literally verifies that memory allocation works, not filter column extraction.

- `adaptive_indexing_tests.rs:206-227` (`test_query_node_integration`): Creates a QueryNode with adaptive indexing and asserts `Arc::strong_count(&index_controller) >= 2`. The comment says "We can't easily test query execution without setting up full data" -- but that's exactly what the test should do. As written, it only tests that `.with_adaptive_indexing()` stores the Arc.

- `adaptive_indexing_tests.rs:408-427` (`test_graceful_degradation_without_indexes`): Creates a QueryNode without adaptive indexing and... that's it. No query execution, no assertions beyond `unwrap()` on creation. The comment "This test verifies compilation and initialization work" is honest but this should be a compile-time test (or just removed), not a runtime test providing false coverage.

### 39. Tests that are mostly `println!` statements

**Problem**: Tests that consist primarily of print statements with no meaningful assertions.

- `end_to_end_tests.rs:384-468` (`test_cost_optimization_full_stack`): This "test" prints a marketing-style cost optimization report (29 println! statements) and then calls three trivial operations with no assertions on their results. It reads like copy from a slide deck, not a test. The final `println!("All optimizations verified and working!")` is aspirational, not factual.

  The "verification" at the end is:
  - Calls `get_chunks_with_predicates` and assigns to `_` (discards result)
  - Calls `subscribe_filtered` and assigns to `_rx` (discards result)
  - Creates a NodeRegistry and registers a node

  None of these verify that the optimizations actually produce the claimed savings.

- `end_to_end_tests.rs:258-294` (`test_complete_query_pipeline`): Claims to test the "complete query pipeline" but never actually queries any data. It extracts a time range and predicates from SQL (parser-level operations), then has a comment block explaining what a "real test" would do. The actual query execution is entirely missing.

### 40. "Accept any outcome" pattern in production code (not just tests)

**Problem**: The "accept either result" anti-pattern also appears in production source code:

- `src/query/engine.rs:106-119` (already flagged as P2-18): `register_chunk` uses nested `if let Ok(...)` that silently succeeds on any failure. This is the production equivalent of the test pattern.

- `src/compactor/merge.rs:48`: `reader.filter_map(|r| r.ok())` silently drops corrupt Parquet row groups during compaction (already flagged as P1-13). This is particularly dangerous because corrupted data is silently lost during a background process with no operator visibility.

- `src/metadata/s3.rs` (6+ locations, already flagged as P0-2): `serde_json::from_str(...).unwrap_or_else(|_| empty_collection)` silently replaces corrupt metadata with empty data.

- `src/api/ingest/flight_ingest.rs:60`: `if let Ok(schema) = Schema::try_from(data)` silently ignores schema parse failures for Flight ingestion data. If the schema is malformed, the data is silently dropped with no error to the client.

### 41. Tests with weak assertions that look strong

**Problem**: Tests that have assertions, but the assertions are too weak to catch real regressions.

- `shard_split_tests.rs:393-396` (`test_split_point_calculation`): Asserts that `shard_a` and `shard_b` are `!is_empty()`. Since these are UUIDs, they will always be non-empty. The test name claims to verify split point "calculation" but never examines the actual split point value.

- `shard_split_tests.rs:206-209` (`test_phase3_backfill`): Asserts `!chunks_a.is_empty() || !chunks_b.is_empty()`. The OR makes this trivially satisfiable -- even if all data goes to one shard (a broken split), the test passes. It should verify BOTH shards have data when the split point is between the data ranges.

- `end_to_end_tests.rs:179-185` (streaming test): Uses `tokio::time::timeout(...).await.is_ok()` to count received messages, but a message that arrives but contains wrong data still counts as received. The test should verify the content of received batches, not just their arrival.

### 42. Pattern: Comments as substitutes for tests

**Problem**: Multiple tests have comments explaining what they SHOULD test, followed by no actual test code for those behaviors:

- `end_to_end_tests.rs:289-294`:
  ```
  // In a real test with data, we would:
  // 1. Ingest data with different metrics
  // 2. Query with predicate
  // 3. Verify only relevant chunks were fetched
  // 4. Verify correct results returned
  ```
  This is a TODO list inside a test that claims to be complete.

- `end_to_end_tests.rs:116`: `// In a real implementation, this would filter chunks` -- but this IS the real implementation, and the test doesn't verify filtering.

- `adaptive_indexing_tests.rs:224-226`: `// We can't easily test query execution without setting up full data` -- difficulty is not an excuse for false coverage.

### Summary: False Confidence Metrics

| Category | Count | Risk |
|----------|-------|------|
| Tests that accept both Ok and Err | 12 | High: mask failures |
| Tests that only verify initialization | 3 | Medium: false coverage |
| Tests that are mostly println! | 2 | Medium: waste of test budget |
| Production code with accept-any-outcome | 4+ | Critical: silent data loss |
| Tests with weak/trivial assertions | 3 | Medium: false confidence |
| Comment-as-test placeholders | 3+ | Medium: false coverage |

**Estimated false coverage**: ~20 tests out of ~38 integration tests provide weak or no meaningful verification. This means roughly **half the test suite is theater** -- it looks like coverage in reports but catches almost nothing.

**Recommendation**: Each of these tests should be either:
1. **Fixed** to assert specific, meaningful outcomes
2. **Marked as `#[ignore]` with `= "TODO: needs real assertions"`** so they don't inflate coverage metrics
3. **Deleted** if they add no value (compile-only tests, println-only tests)

The E2E tests specifically should use `unwrap()` or `expect()` on their Result values -- if the docker-compose stack is running (which is the precondition for these `#[ignore]` tests), errors ARE test failures.

---

## Summary of Recommendations (Priority Order)

| Priority | Issue | Effort | Impact |
|----------|-------|--------|--------|
| P0 | Fix string-based error matching | Medium | Prevents silent breakage on dep upgrades |
| P0 | Fix silent data corruption on parse failure | Low | Prevents metadata loss |
| P0 | Fix non-atomic two-phase metadata writes | High | Prevents inconsistent state |
| P0 | Extract retry/backoff to shared utility | Medium | Eliminates ~300 lines of duplication |
| P1 | Extract generic `AtomicJsonStore<T>` | High | Reduces s3.rs by ~60% |
| P1 | Fix `Not` predicate evaluation | Low | Correctness fix |
| P1 | Fix silent error swallowing in merge/register | Low | Prevents data loss |
| P1 | Use `thiserror` for error types | Medium | Reduces boilerplate, fixes error chains |
| P1 | Deduplicate split_batch implementations | Low | Code hygiene |
| P1 | Unify predicate type systems | Medium | Eliminates confusion |
| P2 | Add graceful shutdown to all service loops | Medium | Operational requirement |
| P2 | Use stable hash for shard keys | Low | Correctness across upgrades |
| P2 | Clean up unused dependencies | Low | Reduces compile time |
| P2 | Fix CachedObjectStore metadata fabrication | Low | Correctness |
| P2 | Fix case-sensitive predicate matching | Low | Correctness |
| P3 | Remove dead code and `#[allow(dead_code)]` | Low | Code hygiene |
| P3 | Fix `WriteBuffer::append` return type | Low | API clarity |
| P3 | Add `#[must_use]` to stat types | Low | API safety |
| P1 | Fix 12 tests that accept both Ok and Err | Medium | Eliminates false confidence |
| P1 | Fix 3 initialization-only tests | Low | Real coverage |
| P2 | Replace 2 println-only tests with real assertions | Low | Test quality |
| P2 | Fix 3 tests with trivially-true assertions | Low | Real regression detection |
