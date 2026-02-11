# Design: Merge metadata.json and time-index.json

**Date**: 2026-02-06
**Author**: Distributed Systems Architect Agent
**Status**: Proposal (Phase C)
**Addresses**: Architecture Audit Finding 1 (Non-Atomic Two-Object Metadata Update)

---

## 1. Problem Statement

Currently, `S3MetadataClient` maintains chunk metadata in two separate S3 objects:

1. **`metadata/chunks/metadata.json`** -- `HashMap<String, ChunkMetadataExtended>` containing chunk path, timestamps, row count, size, column stats, and compaction level.
2. **`metadata/time-index.json`** -- `BTreeMap<i64, Vec<String>>` mapping hour-bucket timestamps to lists of chunk paths.

Every mutating operation (`register_chunk`, `delete_chunk`, `complete_compaction`) must update both files using separate ETag-based CAS PUTs. This creates a non-atomic two-phase write where the system can enter an inconsistent state if:

- The first PUT succeeds but the second fails (ETag conflict or crash).
- A retry after partial success re-applies the first change (idempotent for metadata but not for time index, which can accumulate duplicate path entries).

See Architecture Audit Finding 1 (`docs/architecture-audit.md`) for the full analysis.

---

## 2. New Unified Schema

### 2.1 Unified File: `metadata/catalog.json`

The merged file combines chunk metadata and time index into a single JSON object with a single ETag:

```json
{
  "version": 2,
  "chunks": {
    "tenant/2026/02/06/10/chunk_abc123.parquet": {
      "path": "tenant/2026/02/06/10/chunk_abc123.parquet",
      "min_timestamp": 1738839600000000000,
      "max_timestamp": 1738843200000000000,
      "row_count": 500000,
      "size_bytes": 52428800,
      "level": 0,
      "shard_id": "shard-7a3f",
      "column_stats": {
        "metric_name": { "min": "api_latency", "max": "cpu_usage", "has_nulls": false },
        "status_code": { "min": 200, "max": 503, "has_nulls": false }
      }
    }
  },
  "time_index": {
    "1738839600000000000": ["tenant/2026/02/06/10/chunk_abc123.parquet"],
    "1738843200000000000": ["tenant/2026/02/06/10/chunk_abc123.parquet"]
  }
}
```

### 2.2 Rust Types

```rust
/// Unified metadata catalog -- single S3 object, single ETag
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetadataCatalog {
    /// Schema version for forward compatibility
    pub version: u32,
    /// All chunk metadata, keyed by chunk path
    pub chunks: HashMap<String, ChunkMetadataExtended>,
    /// Time index: hour-bucket timestamp -> list of chunk paths
    pub time_index: BTreeMap<i64, Vec<String>>,
}
```

### 2.3 Key Design Decisions

**Adding `shard_id` to `ChunkMetadataExtended`**: The current `get_chunks_for_shard()` uses fragile `path.contains(shard_id)` substring matching (Architecture Audit Finding 8). Adding an explicit `shard_id` field to `ChunkMetadataExtended` fixes this while we're changing the schema anyway.

**`version` field**: Enables future schema evolution without breaking deserialization. Version 1 is the legacy separate-file format; version 2 is the merged format.

**Time index remains denormalized**: The time index is a derived structure (it can be rebuilt from chunk metadata). Keeping it denormalized in the same file avoids the consistency bug while maintaining O(1) time-range lookups. The cost is a modest increase in file size (~30-50 bytes per chunk per hour bucket spanned).

---

## 3. Migration Path

### 3.1 Three-Phase Migration Strategy

**Phase M1: Dual-Read Support** (backwards compatible, no data migration needed)

Modify `S3MetadataClient` to attempt reading `catalog.json` first. If it does not exist, fall back to reading `metadata.json` + `time-index.json` separately and constructing a `MetadataCatalog` in memory:

```rust
async fn load_catalog_with_etag(&self) -> Result<(MetadataCatalog, String)> {
    // Try unified catalog first
    match self.load_unified_catalog().await {
        Ok(result) => return Ok(result),
        Err(_) => {
            // Fall back to legacy format
            let (chunks, _) = self.load_chunk_metadata_with_etag().await?;
            let time_index = self.load_time_index().await?;
            let catalog = MetadataCatalog {
                version: 1, // Legacy
                chunks,
                time_index,
            };
            Ok((catalog, "none".to_string()))
        }
    }
}
```

All write operations write to `catalog.json` exclusively. This means the first write after deployment automatically migrates to the new format.

**Phase M2: Migration Tool**

Add a `migrate-metadata` binary (or a flag on the existing `backfill-levels` binary) that:

1. Reads the legacy `metadata.json` and `time-index.json`.
2. Validates consistency (detects chunks missing from time index and vice versa).
3. Repairs any inconsistencies found.
4. Writes the unified `catalog.json`.
5. Optionally deletes the legacy files (with `--delete-legacy` flag).

```bash
cargo run --bin migrate-metadata -- \
  --bucket cardinalsin-metadata \
  --prefix metadata/ \
  --dry-run          # Preview changes
  --delete-legacy    # Remove old files after successful migration
  --repair           # Fix inconsistencies found during migration
```

**Phase M3: Remove Legacy Support**

After all deployments have been migrated and verified (tracked by the `version` field), remove the dual-read fallback code. This is a code-only change with no data migration.

### 3.2 Rollback Strategy

If a deployment needs to roll back to pre-merge code:

1. The legacy code ignores `catalog.json` (it only reads `metadata.json` and `time-index.json`).
2. Run the migration tool in reverse: `--split-catalog` flag reads `catalog.json` and writes back to the two separate files.
3. Roll back the code.

### 3.3 Zero-Downtime Deployment

During the transition window (Phase M1):
- Old code writes to `metadata.json` + `time-index.json`.
- New code writes to `catalog.json`.
- Both formats can coexist since they use different file paths.
- However, concurrent writes from old and new code will diverge. Therefore, **all nodes must be upgraded before any writes occur**, or a brief write pause is required during deployment.

**Recommended deployment sequence**:
1. Deploy new code to all query nodes first (they only read).
2. Pause ingesters and compactors briefly.
3. Run migration tool.
4. Deploy new code to ingesters and compactors.
5. Resume writes.

Total downtime: migration tool runtime (seconds for typical deployments) plus rolling restart time.

---

## 4. Performance Implications

### 4.1 File Size Analysis

Current separate files:
- `metadata.json`: ~300 bytes per chunk (path, timestamps, row_count, size_bytes, level, column_stats)
- `time-index.json`: ~80 bytes per hour-bucket entry (timestamp key + path string)

A typical chunk spans 1 hour, so each chunk adds ~380 bytes total across both files.

Merged `catalog.json`: ~380 bytes per chunk (same data, one file).

**At scale**:
| Chunks | Separate Files Total | Merged File |
|--------|---------------------|-------------|
| 1,000 | ~370 KB | ~370 KB |
| 10,000 | ~3.7 MB | ~3.7 MB |
| 100,000 | ~37 MB | ~37 MB |
| 1,000,000 | ~370 MB | ~370 MB |

The merged file is the same total size -- we're not adding data, just combining files. The concern is that every operation now downloads/uploads the full combined file instead of potentially only needing one.

### 4.2 Read Path Impact

**`get_chunks()` / `get_chunks_with_predicates()`**: Currently loads time index first, then uses chunk cache for metadata. With the merged file:
- First call loads the full catalog (same total bytes, one GET instead of potential two).
- Subsequent calls use the in-memory cache (no change).
- **Net impact**: Slight improvement -- one S3 GET instead of two.

**`list_chunks()`, `get_l0_candidates()`, `get_level_candidates()`**: Currently loads only `metadata.json`. With the merged file, they load the slightly larger catalog.
- **Net impact**: Marginal increase in bytes transferred, but already cached in memory.

### 4.3 Write Path Impact

**`register_chunk()`, `delete_chunk()`, `complete_compaction()`**: Currently performs 2 GET + 2 conditional PUT (4 S3 operations). With the merged file: 1 GET + 1 conditional PUT (2 S3 operations).
- **Net impact**: 50% reduction in S3 operations per write. Significant cost and latency improvement.
- The PUT payload is larger (includes both chunks and time_index), but the total bytes are the same since both were being uploaded anyway.

### 4.4 Maintaining Efficient Range Queries

The `BTreeMap<i64, Vec<String>>` time index is kept as a field in the catalog. After deserialization, it's the same in-memory BTreeMap with O(log n) range lookups. No change in query efficiency.

The cache structure changes from two separate caches to one:

```rust
/// Single cached catalog with TTL
catalog_cache: Arc<tokio::sync::RwLock<Option<(MetadataCatalog, Instant)>>>,
```

This simplifies cache invalidation -- one cache to invalidate instead of two, eliminating the current window where chunk cache and time index cache can be out of sync.

---

## 5. CAS Contention Analysis

### 5.1 The Tradeoff

**Before merge**: Two files, each with its own ETag. A conflict on either file requires full retry. But two concurrent writers that touch non-overlapping hour buckets can sometimes succeed on the metadata file even if they conflict on the time index (or vice versa).

**After merge**: One file, one ETag. Any concurrent write to any part of the catalog conflicts. But each operation is one GET + one PUT instead of two, so each retry is faster and the retry loop completes sooner.

### 5.2 Quantitative Analysis

**Contention probability model** (simplified):

With N concurrent writers each performing one operation during a time window T:
- P(conflict per operation) ~ (N-1) * t_write / T, where t_write is the duration of a single write operation.

Current system (two files):
- Each operation does 2 conditional PUTs, each taking ~50ms for S3.
- P(conflict per operation) ~ (N-1) * 100ms / T
- But if either PUT conflicts, the *entire* operation retries (both PUTs), so the effective conflict window is 100ms.

Merged system (one file):
- Each operation does 1 conditional PUT, taking ~50ms for S3.
- P(conflict per operation) ~ (N-1) * 50ms / T
- Effective conflict window is 50ms.

**Result**: The merged system actually has **lower** contention probability per operation because the conflict window is halved (50ms vs 100ms). Even though all writers contend on the same file, each individual operation is faster.

### 5.3 Expected Writer Counts

| Component | Concurrent Writers | Operations/sec | Contention with Merge |
|-----------|-------------------|-----------------|----------------------|
| Ingesters (3 nodes) | 3 | ~0.01/sec each (flush every ~5min) | Negligible |
| Compactors (1 node) | 1 | ~0.1/sec during compaction bursts | Low |
| Split orchestrator | 1 (rare) | Burst during split phases | Low |
| **Total typical** | **4-5** | **~0.15/sec** | **<1% conflict rate** |
| **Total burst** | **5-10** | **~1/sec** | **~5% conflict rate** |

With 5 retries and exponential backoff (100ms, 200ms, 400ms, 800ms, 1600ms), a 5% base conflict rate results in:
- P(all 5 retries fail) < 0.05^5 = 0.000003% (effectively zero)
- Average retries per operation: ~0.05 (1 retry per 20 operations)

### 5.4 When Contention Becomes a Problem

The single-file approach breaks down when:
- **>50 concurrent writers** sustained (possible with >50 ingesters, but at that scale you should be using FoundationDB anyway).
- **File size >100 MB** (>300K chunks), where serialization/deserialization time exceeds 100ms and the conflict window grows.

Both thresholds are well beyond the current architecture's intended operating range and would trigger the migration to partitioned metadata (see Section 6).

---

## 6. Future Partitioning Strategy

### 6.1 Design for Partitionability

The unified `MetadataCatalog` is designed to be partitioned in the future without changing the core CAS pattern. The key insight is that partitioning decomposes the single `catalog.json` into multiple independent catalog files, each with its own ETag and zero cross-file dependencies.

### 6.2 Partitioning by Shard

**File layout**: `metadata/shards/{shard_id}/catalog.json`

Each shard gets its own catalog file containing only chunks and time index entries for that shard. Since the `shard_id` field is now part of `ChunkMetadataExtended` (added in this design), routing a chunk to the correct partition is straightforward.

**Advantages**:
- Zero contention between shards (each shard has its own ETag).
- File size scales with chunks-per-shard, not total chunks.
- Shard splits naturally create new partition files.

**Implementation**:
```rust
async fn register_chunk(&self, path: &str, metadata: &ChunkMetadata) -> Result<()> {
    let shard_id = self.compute_shard_id(metadata);
    let catalog_path = format!("metadata/shards/{}/catalog.json", shard_id);
    // CAS on per-shard catalog only
    self.atomic_update_catalog(&catalog_path, |catalog| {
        catalog.insert_chunk(path, metadata);
    }).await
}
```

**Query path**: `get_chunks()` would need to query multiple shard catalogs. Two approaches:
1. **Scatter-gather**: Query all shard catalogs in parallel, merge results. O(num_shards) S3 GETs.
2. **Global time index**: Maintain a lightweight global index mapping time ranges to shard IDs (much smaller than the full time index). This is a read-only, eventually-consistent lookup.

Recommended: Option 2, since the number of shards is small (typically <100) and the global index would be <10KB.

### 6.3 Partitioning by Time Range

**File layout**: `metadata/hourly/{hour_bucket}/catalog.json`

Each hour gets its own catalog file. This naturally aligns with the existing hour-bucket time index and the ingester's time-based file organization.

**Advantages**:
- Hot (recent) hours have high contention but small files.
- Cold (old) hours are rarely written, so CAS conflicts are near-zero.
- Retention/deletion operates on entire partition files.

**Disadvantages**:
- Queries spanning many hours require loading many catalog files.
- Cross-hour compaction requires updating two partition files (the source-hour and target-hour catalogs). This reintroduces the two-file consistency problem unless compaction stays within hour boundaries.

### 6.4 Recommended Partitioning Strategy

**Two-level partitioning**: Shard first, then time range within each shard.

```
metadata/
  shards/
    shard-7a3f/
      catalog.json          # Active catalog for recent chunks
      archive/
        2026-02-05.json     # Archived daily catalogs (compacted, rarely updated)
        2026-02-04.json
    shard-9b2e/
      catalog.json
      archive/
        ...
  global-index.json         # Lightweight: shard_id -> time_ranges
```

**Why shard-first**:
1. Shard boundaries are the natural isolation unit -- writes to shard A never need to update shard B's metadata.
2. Compaction runs within a shard, so the single-file CAS pattern applies within each shard partition.
3. Shard splits create new partition directories naturally.

**Why archive by day (not hour)**:
1. Hourly partitions create too many small files for long time ranges.
2. Daily archival aligns with typical retention policies (30d, 90d, 1y).
3. Old daily catalogs are immutable after the day ends (no CAS needed), so they can use plain PUT.

### 6.5 Preparing the Unified Schema for Partitioning

The `MetadataCatalog` struct already supports partitioning with no schema changes:
- Each partition file is a valid `MetadataCatalog` with its own `version`, `chunks`, and `time_index`.
- The `shard_id` field on chunks enables correct routing during partition splits.
- The `version` field can signal whether this catalog is partition-aware.

The code changes needed for partitioning are:
1. Replace `catalog_path()` with a function that computes the partition-specific path.
2. Add a `get_catalog_for_shard()` method.
3. Add scatter-gather logic to `get_chunks()`.
4. Add a global index maintenance background task.

These are additive changes that do not affect the merged schema itself.

---

## 7. Implementation Checklist

### Phase C-1: Core Merge (Critical Path)

- [ ] Add `MetadataCatalog` struct to `src/metadata/s3.rs`
- [ ] Add `shard_id: Option<String>` field to `ChunkMetadataExtended`
- [ ] Implement `load_catalog_with_etag()` with dual-read fallback
- [ ] Implement `atomic_save_catalog()` (single-file CAS)
- [ ] Rewrite `atomic_register_chunk()` to use single catalog
- [ ] Rewrite `delete_chunk()` to use single catalog
- [ ] Rewrite `complete_compaction()` to use single catalog
- [ ] Replace `chunk_cache` + `time_index_cache` with single `catalog_cache`
- [ ] Update `get_chunks_with_predicates()` to use catalog
- [ ] Update `get_chunks_for_shard()` to use `shard_id` field instead of path substring matching

### Phase C-2: Migration Tooling

- [ ] Add `migrate-metadata` binary or `--migrate` flag to existing tooling
- [ ] Implement consistency validation (detect chunks in metadata but not in time index, and vice versa)
- [ ] Implement repair logic
- [ ] Add `--dry-run` and `--delete-legacy` flags

### Phase C-3: Cleanup

- [ ] Remove `load_time_index_with_etag()` and `atomic_save_time_index()` methods
- [ ] Remove `load_chunk_metadata_with_etag()` and `atomic_save_chunk_metadata()` methods
- [ ] Remove dual-read fallback
- [ ] Update `LocalMetadataClient` to match (optional -- it already uses in-memory structures)

### Phase C-4: Update Tests

- [ ] Update `atomic_metadata_tests.rs` to verify single-file atomicity
- [ ] Add test: crash between load and save produces consistent catalog
- [ ] Add test: concurrent register_chunk with merged catalog
- [ ] Add test: migration tool repairs inconsistent legacy files
- [ ] Fix `get_chunks_for_shard()` tests to use `shard_id` field

---

## 8. Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| Larger file increases serialization time | Low | At 10K chunks (~3.7MB), serde_json parses in <10ms. Only a concern at >100K chunks, at which point partitioning is the solution. |
| Migration tool corrupts data | High | `--dry-run` mode, backup of legacy files before migration, validation checksums. |
| Rolling deployment with mixed old/new code | Medium | Old code ignores `catalog.json`; new code with dual-read handles both. But concurrent writes diverge -- brief pause required. |
| Increased CAS retries from single-file contention | Low | Analysis in Section 5 shows conflict rate actually decreases due to shorter conflict window. |
| Breaking change to `MetadataClient` trait | None | The trait interface (`register_chunk`, `get_chunks`, etc.) does not change. This is purely an implementation detail of `S3MetadataClient`. |

---

## 9. Summary

Merging `metadata.json` and `time-index.json` into a single `catalog.json` eliminates the most critical consistency bug in the system (Architecture Audit Finding 1) while actually improving performance (2 S3 operations instead of 4 per write). The tradeoff of increased CAS contention on a single file is a net positive because the conflict window is halved.

The design preserves forward compatibility through the `version` field and prepares for future shard-based partitioning by adding a `shard_id` field to chunk metadata. The migration path is straightforward with a dual-read fallback and a dedicated migration tool.

This is the single highest-impact change for data consistency in CardinalSin's metadata layer.
