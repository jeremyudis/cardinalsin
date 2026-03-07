# Design: Production-Grade Indexing Architecture (Converged)

| Field       | Value                                           |
|-------------|------------------------------------------------|
| **Status**  | Converged (PR #158 + PR #159)                  |
| **Authors** | CardinalSin Team                                |
| **Issue**   | gh-157, cardinalsin-vf6                        |
| **Date**    | 2026-03-05 → 2026-03-07 (converged)            |
| **Refs**    | Adaptive Indexing Epic (gh-134)                |
| **Summary** | Shard-scoped immutable index segments with CAS manifests, compactor-first builds, explicit watermark freshness gate, 4-phase rollout. Supersedes separate `design-indexing-next-evolution.md`. |

---

## 1. Problem Statement

CardinalSin targets >1B unique time series stored as columnar Parquet on object storage. The current indexing relies on three mechanisms:

1. **Parquet row-group statistics** — min/max per column per row group (zone maps). DataFusion uses these for predicate pushdown automatically.
2. **Parquet bloom filters** — enabled for 7 hardcoded columns (`metric_name`, `host`, `region`, `env`, `service`, `job`, `instance`) with 1% FPP and NDV=1000.
3. **Naive inverted index** — `BTreeMap<String, BTreeMap<String, Vec<String>>>` serialized as JSON at `indexes/{tenant_id}/inverted.json`, updated via CAS.

### Why This Is Insufficient

| Gap | Impact |
|-----|--------|
| **No efficient label→chunk mapping** | Queries like `WHERE host='web-01'` must scan all chunk metadata, evaluate zone maps linearly. With 100K+ chunks this is seconds, not milliseconds. |
| **Inverted index doesn't scale** | The BTreeMap/JSON approach has O(n) deserialization, unbounded JSON size, and serializes the entire index on every CAS update. At 1M chunk×label entries the JSON exceeds 100MB. |
| **No series enumeration** | `/api/v1/series` and `/api/v1/label/*/values` require full data scans — no index maps label values to chunks without reading Parquet. |
| **Bloom filters are static** | Only 7 columns are bloom-filtered. High-cardinality columns like `trace_id`, `request_id`, or user-defined labels get no bloom filter support. |
| **No cross-chunk label awareness** | The adaptive index controller tracks query patterns but has no physical index to build. Lifecycle state (`Invisible→Visible→Deprecated`) exists only in memory. |
| **Cardinality cap silently degrades** | `INVERTED_INDEX_CARDINALITY_LIMIT = 10,000` — columns above this are silently excluded from the inverted index with no fallback. |

### Goals

1. Sub-millisecond label→chunk resolution for equality and IN predicates
2. Efficient series enumeration without full Parquet scans
3. Index storage that scales to billions of label×chunk entries
4. Zero coordination between stateless nodes — immutable indexes, no distributed locks
5. Pluggable index types driven by the adaptive index controller

---

## 2. Index Types

### 2a. Inverted Index (Label→Chunk Mapping)

**Purpose**: Given a predicate like `host = 'web-01'`, return the set of chunk IDs containing that value — without scanning metadata.

**Current state**: No production inverted index. The prior `BTreeMap<String, BTreeMap<String, Vec<String>>>` JSON design was reverted in #156.

**Production approach**: Shard-scoped immutable `.csi` segments (FST term dictionary + roaring bitmap postings) published via CAS-protected manifests.

#### Architecture

```
{tenant_id}/indexes/
  shard={shard_id}/
    manifest.json  (CAS + indexed_through_ns watermark)
    segments/
      seg_{uuid}_{level}.csi   (immutable)

Each `.csi` segment contains:
  [chunk ordinal table]
  [section directory]
  [inverted section: FST(term -> postings offset) + roaring postings]
```

**Compactor-built segments** (Phase 1, production):
- Compactor builds one segment per compaction output (level-tagged), then CAS-updates `manifest.json`
- FST maps `column_name=value` terms to postings offsets
- Roaring postings contain segment-local chunk ordinals, resolved through the segment's chunk ordinal table
- Query planner unions postings across all manifest-listed segments, then applies watermark fallback for frontier chunks

**Optional ingester-built segments** (Phase 3, future):
- Ingester may publish single-chunk `.csi` segments to reduce freshness lag
- Same file format and manifest semantics; no mutable shared index

#### Why FST + Roaring Bitmaps

| Approach | Lookup | Storage | Update | Used By |
|----------|--------|---------|--------|---------|
| BTreeMap + JSON (current) | O(log n) after O(n) deser | Unbounded, no compression | Full rewrite via CAS | — |
| HashMap + MessagePack | O(1) after O(n) deser | Better than JSON, still O(n) | Full rewrite | — |
| **FST + Roaring** | O(k) key length, mmap-friendly | ~1 byte/key (sorted), bitmap compression | Immutable (rebuild) | Tantivy, Quickwit, Meilisearch |
| SQLite | O(log n) | Good | Mutable but needs coordination | InfluxDB IOx (catalog only) |

FST + roaring is the standard for search/TSDB systems because:
- **FST** shares prefixes across sorted keys → `host=web-01`, `host=web-02` share `host=web-0` prefix
- **Roaring bitmaps** compress sorted integer sets to ~1-2 bits/entry for dense sets, with efficient AND/OR/XOR operations
- Both are **immutable after construction** — perfect for CardinalSin's stateless architecture
- Both support **memory-mapped access** — query nodes can mmap index files from NVMe cache

#### Comparison with Industry

| System | Term Dictionary | Postings | Scope |
|--------|----------------|----------|-------|
| **VictoriaMetrics** | Custom `mergeset` (LSM-based) | Custom uint64 sets | Global mutable |
| **InfluxDB IOx** | Parquet + DataFusion catalog | Per-file, DataFusion handles | Per-file |
| **Quickwit** | FST (Tantivy) | Roaring bitmaps | Per-split (immutable) |
| **ClickHouse** | Per-granule skip index | Bloom/minmax/set | Per-part |
| **CardinalSin (proposed)** | FST (`fst` crate) | Roaring bitmaps (`roaring` crate) | Per-shard immutable segments + manifest |

VictoriaMetrics uses a global mutable index (mergeset) because it runs as a single process. CardinalSin's stateless query nodes cannot share mutable state, so the Quickwit model (immutable per-segment indexes merged during compaction) is the right fit.

### 2b. Bloom Filters / Xor Filters

**Purpose**: Probabilistic membership test — "does chunk X contain value Y?" — without reading the full column.

**Current state**: Parquet-native bloom filters on 7 columns (1% FPP, NDV=1000).

**Gap**: High-cardinality columns not in the hardcoded list get no filter. The adaptive index controller identifies these columns but has no mechanism to build standalone filters.

#### Proposal: Binary Fuse Filters via `xorf`

Binary Fuse filters (Fuse8, Fuse16) are a newer alternative to Bloom filters:

| Property | Bloom Filter | Xor Filter | BinaryFuse8 (`xorf`) |
|----------|-------------|------------|----------------------|
| Bits per element | ~10 (1% FPP) | 9.84 | **~9** (~0.4% FPP) |
| Lookup time | k hash probes | 3 lookups | 3 lookups |
| Construction | O(n) | O(n) expected | O(n), faster than Xor |
| Mutable | Yes (add-only) | No | No |
| Memory during build | Low | High | **Lower than Xor** |

Since CardinalSin's indexes are immutable after construction, the mutability limitation of xor/fuse filters is irrelevant, and they offer ~10% smaller size with faster lookups.

#### Usage

Standalone fuse filters are written as sections inside shard `.csi` segments. The adaptive index controller selects columns with cardinality >100K for fuse filter indexing (below that, the inverted index is more useful).

```
Segment .csi file:
  [header]
  [inverted index section: FST + roaring bitmaps]
  [fuse filter section: per-column Fuse8 filters]
  [footer with checksums]
```

Parquet-native bloom filters remain for the 7 hardcoded columns (they're free — DataFusion uses them automatically). Standalone fuse filters cover columns identified by the adaptive index controller.

### 2c. Zone Maps (Min/Max per Chunk)

**Purpose**: Prune entire chunks when a predicate's value falls outside the column's range.

**Current state**: Fully implemented via `ColumnStats { min, max, has_nulls }` in `ChunkMetadataExtended`, evaluated by `ColumnPredicate::evaluate_against_stats()` in `predicates.rs`.

**Evaluation**: Supports `Eq`, `Lt`, `LtEq`, `Gt`, `GtEq`, `In`, `Between`, `And`, `Or`. Negations (`Not`, `NotEq`, `NotIn`) conservatively return `true` (no pruning).

#### Gaps

1. **No per-row-group zone maps in metadata** — current `ColumnStats` are per-chunk aggregates. Parquet has per-row-group stats that DataFusion uses, but the metadata layer doesn't expose them for pre-fetch pruning.
2. **No zone maps for computed columns** — e.g., `hour(timestamp)` for time-of-day queries.

#### Recommendation

Zone maps are **baseline infrastructure** — already working, no changes needed for the indexing design. The per-row-group gap is handled by DataFusion at query time (it reads row-group metadata from Parquet footer). Adding per-row-group stats to the metadata catalog would only help if we want to prune at the metadata level before fetching any Parquet data — a future optimization.

### 2d. Skip Index (ClickHouse-Inspired)

**Purpose**: Block-level (granule) indexes for range predicates on non-primary-key columns.

ClickHouse's skip indexes (also called "data skipping indexes") work on granules (groups of rows, typically 8192). For each granule, the index stores a summary (bloom filter, min/max, or set of values). At query time, granules whose summary excludes the predicate value are skipped.

#### Relevance to CardinalSin

Parquet's **page-level statistics** (min/max per data page) already serve as skip indexes for range predicates. DataFusion uses these when `with_parquet_pruning(true)` is set (already configured).

ClickHouse needs custom skip indexes because its native MergeTree format doesn't have per-page statistics. Parquet does.

**Recommendation**: Do not implement custom skip indexes. Parquet page statistics + DataFusion predicate pushdown provide equivalent functionality. If specific workloads need finer granularity, configure smaller Parquet page sizes (currently 1MB) rather than building a parallel system.

### 2e. Pre-Aggregation Index (Pinot Star-Tree Inspired)

**Purpose**: Pre-compute aggregates for common `GROUP BY` patterns to avoid full scans.

Apache Pinot's Star-Tree index pre-materializes aggregations for dimension combinations. For a query like `SELECT host, region, AVG(value) FROM metrics GROUP BY host, region`, a star-tree with those dimensions returns results from pre-computed nodes instead of scanning raw data.

#### Relevance to CardinalSin

Pre-aggregation is valuable for dashboard queries that repeatedly aggregate the same dimensions. However:

| Factor | Assessment |
|--------|------------|
| Complexity | High — requires identifying hot GROUP BY patterns, building trees during compaction, query rewriting to use pre-agg |
| Storage overhead | 10-50% additional storage depending on dimension cardinality |
| Write overhead | Only during compaction (acceptable) |
| Query speedup | 10-100x for matching patterns |
| Existing alternative | DataFusion's columnar execution + Parquet predicate pushdown already handles medium-scale aggregation efficiently |

**Recommendation**: **Future phase only.** Document the approach but do not implement. The adaptive index controller already tracks `groupby_stats` — when these indicate repeated patterns on compacted data, star-tree construction can be added to the compaction pipeline. The `.csi` segment format (section 3) reserves a section type for pre-aggregation data.

---

## 3. Storage Model — Shard-Scoped Index Segments

### 3.1 Storage Layout

Index data is organized per-shard with an immutable segment model:

```
{tenant_id}/indexes/
  shard={shard_id}/
    manifest.json           # CAS-protected via ETag
    segments/
      seg_{uuid}_{level}.csi    # Immutable index segment (same .csi format as section 3.3)
```

This structure replaces both the per-chunk sidecar approach and tenant-global index from the earlier proposal. The shard-scoped design offers:

1. **Failure isolation**: A corrupt segment only affects one shard, not the entire tenant
2. **Compaction alignment**: Segments are built during leveled compaction (L0→L1→L2→L3), one per compaction output
3. **Split integration**: During shard splits, each new shard gets its own empty manifests and segment directories
4. **Scalability**: Segment count per shard is bounded by a tiered merge policy (max 10 segments before merging smaller ones)

### 3.2 Manifest Schema

```json
{
  "version": 1,
  "shard_id": "shard-abc123",
  "generation": 42,
  "indexed_through_ns": 1709654400000000000,
  "segments": [
    {
      "path": "seg_550e8400_L1.csi",
      "min_time_ns": 1709640000000000000,
      "max_time_ns": 1709654400000000000,
      "chunk_count": 15,
      "level": 1,
      "size_bytes": 524288,
      "created_at": "2026-03-05T12:00:00Z"
    }
  ],
  "frozen": false,
  "etag": ""
}
```

**Critical fields**:

- `generation`: Must match `ShardMetadata.generation` to prevent stale manifests during shard splits. Query nodes check this invariant.
- `indexed_through_ns`: Nanosecond timestamp watermark. Chunks with `max_timestamp > indexed_through_ns` are "frontier" chunks and must be scanned via fallback (zone maps). This watermark advances only when a segment is successfully published.
- `frozen`: Set to `true` during shard split phases (Preparation through Backfill). Compactor skips index builds for frozen manifests. Unfrozen during Cutover.
- `segments[].level`: Corresponds to compaction level, used for tiered segment merge decisions.

**Update semantics**: All updates to the manifest use compare-and-swap via the existing ETag-based CAS pattern from `src/metadata/s3.rs`.

### 3.3 Segment Binary Format (`.csi`)

All segments (whether built by compactor or, in Phase 3, by ingester) use the same `.csi` format:

```
┌──────────────────────────────────────────────┐
│ Magic: "CSIX" (4 bytes)                      │
│ Version: u16 (1)                             │
│ Flags: u16 (reserved)                        │
│ Section count: u32                           │
├──────────────────────────────────────────────┤
│ Chunk Ordinal Table:                         │
│   Count: u32                                 │
│   Entries (repeated):                        │
│     Ordinal: u32                             │
│     Chunk path length: u16                   │
│     Chunk path: UTF-8 bytes                  │
├──────────────────────────────────────────────┤
│ Section Directory (repeated per section):    │
│   Type: u8                                   │
│     0x01 = Inverted Index (FST + Roaring)    │
│     0x02 = Fuse Filter (Phase 2)             │
│     0x03 = Value Dictionary (Phase 2)        │
│     0x04 = Pre-Aggregation (reserved)        │
│   Column name length: u16                    │
│   Column name: UTF-8 bytes                   │
│   Offset: u64 (from file start)              │
│   Length: u64                                 │
│   CRC32: u32 (of section data)               │
├──────────────────────────────────────────────┤
│ Section Data (concatenated):                 │
│                                              │
│ [Inverted Index Section]                     │
│   FST bytes (fst::Map serialization)         │
│   Roaring bitmap count: u32                  │
│   Roaring bitmaps (serialized sequentially)  │
│                                              │
│ [Fuse Filter Section - Phase 2]              │
│   Filter type: u8 (0x01=Fuse8, 0x02=Fuse16) │
│   Filter bytes (xorf serialization)          │
│                                              │
│ [Value Dictionary Section - Phase 2]         │
│   FST of all unique values for column        │
│   (no postings, just terms)                  │
│                                              │
├──────────────────────────────────────────────┤
│ Footer:                                      │
│   File CRC32: u32                            │
│   Magic: "CSIX" (4 bytes)                    │
└──────────────────────────────────────────────┘
```

**Design details**:

The **Chunk Ordinal Table** maps segment-local integer ordinals to chunk paths. Roaring bitmaps in the Inverted Index section contain these ordinals as their elements. When a query looks up `host='web-01'` and gets a roaring bitmap back, the ordinals in that bitmap are resolved via the ordinal table to chunk paths for registration with DataFusion.

**Design rationale**:
- **Section-based**: New index types can be added without breaking existing readers (skip unknown section types)
- **Per-section CRC32**: Detect corruption at the section level; a corrupt fuse filter doesn't invalidate the inverted index
- **Footer magic**: Enables reverse scanning to verify file integrity (same pattern as Parquet)
- **No compression**: FST and roaring bitmaps are already compressed; additional compression adds latency with minimal benefit

### 3.4 Index Lifecycle: Garbage Collection and Orphan Reconciliation

Manifest is the source of truth. The compactor's GC phase (part of `garbage_collect()`) reconciles segments:

1. List all `.csi` files under `{tenant_id}/indexes/shard={shard_id}/segments/`
2. Load manifest for the shard
3. Compute `manifest_segment_paths = manifest.segments.map(|s| s.path)`
4. Delete any `.csi` file not in `manifest_segment_paths` (orphan from failed build)
5. For any segment in manifest whose `.csi` file does not exist on S3: remove from manifest and log a warning

This runs at low priority, gated by a configurable interval (default: every 10 compaction cycles).

---

## 4. Stateless Architecture Implications

### 4.1 Immutability and Failure Semantics

CardinalSin's ingester, query, and compactor nodes are stateless. Indexes must be immutable after construction. This enables:

| Property | Benefit |
|----------|---------|
| **Crash recovery** | If a segment write fails or a manifest CAS fails, the old state remains in S3. Re-read from S3 on recovery. No WAL needed. |
| **Query safety** | Queries can never see torn or partially-written indexes. Manifest CAS ensures atomicity of segment list updates. |
| **Multi-node caching** | Each query node independently caches manifests and segments via TieredCache. No inter-node coordination. |
| **Split-safety** | Shard manifests can be frozen independently during splits without affecting other shards. |

### 4.2 Index Lifecycle Across Nodes (Phase 1: Compactor-First)

**Phase 1** (this design): Compactor is the sole index builder.

```
Compactor                          Query Node
─────────                          ──────────
compact():                         query():
  read source chunk metadata         fetch manifest (cached)
  merge Parquet data                 check manifest.generation vs shard.generation
  build segment:                     partition chunks by indexed_through_ns watermark
    extract string columns             indexed_chunks: use segment FST+roaring
    build FST dict                     frontier_chunks: use zone map fallback
    build roaring postings             union both sets
    serialize .csi                     register unified set with DataFusion
  CAS-update manifest:               DataFusion executes with Parquet pushdown
    add segment entry
    advance indexed_through_ns
```

Fresh data ingested by the ingester remains unindexed until the next compaction cycle. The `indexed_through_ns` watermark makes this lag explicit and measurable. During this window, queries fall back to zone map pruning on frontier chunks (the existing behavior).

**Phase 3 enhancement** (future): Ingester builds optional single-chunk `.csi` segments to reduce freshness lag from "compaction interval" to "flush interval". Same `.csi` format, just one ordinal entry per segment.

### 4.3 Cache Integration

Index files flow through the existing `TieredCache` (RAM → NVMe → S3):

- **Manifest** (~1KB per shard, cached per-query-session): stays in L1 (RAM) after first access
- **Segments** (~100KB-1MB each, cached like Parquet data): cached in L2 (NVMe) alongside their data
- **No special cache eviction** needed — indexes are small relative to Parquet data

The `CachedObjectStore` already handles `.csi` files transparently (they're just S3 objects).

### 4.4 Compaction Integration: When Segments Are Built

Index segments are built as a post-step of `merge_chunks()` in the compactor:

```rust
async fn merge_chunks(&self, paths: &[String], level: Level) -> Result<String> {
    // ... existing: merge data, write Parquet ...
    let chunk_path = upload_parquet_to_s3(&parquet_bytes).await?;

    // [NEW] Build index segment for this compaction output
    if let Some(ref index_builder) = self.index_builder {
        let shard_id = self.determine_shard_for_chunks(paths)?;
        // Failure here is logged but does NOT fail the compaction
        let _ = index_builder.build_and_publish_segment(
            &sorted_batch,
            &chunk_path,
            &shard_id,
            level,
        ).await;
    }
    Ok(chunk_path)
}
```

**Critical design choice: Index build failure must not fail compaction.** If segment construction or manifest CAS fails, the compaction succeeds, data is safe, and the watermark simply does not advance. The next compaction cycle will re-attempt. This preserves "performance degradation only" failure semantics.

### 4.5 Segment Merge Policy

When segment count for a shard exceeds 10, the compactor runs a tiered merge:

1. Sort segments by size ascending
2. Merge the smallest segments until count is at or below 8
3. Merging means: union FST dictionaries, union roaring bitmaps (adjusted for ordinal table), write merged `.csi`
4. Update manifest atomically: remove old entries, add merged entry

This is triggered as a low-priority background task within `run_compaction_cycle()`, after main leveled compaction.

### 4.6 LSM-Style Merge-Tree Optimization Path (Future)

The baseline policy above is intentionally simple for Phase 1. Future iterations can keep the same correctness model (immutable segments + CAS manifests) while adopting more LSM-style merge mechanics for higher sustained throughput.

**Invariants to keep**:
1. Segment publication remains immutable and manifest-driven
2. Query correctness must not depend on successful index merge (fallback still valid)
3. Split protocol integration continues to use `generation` + `frozen` manifest fields

**Planned optimizations**:
1. **Leveled run sizing**: Move from fixed "count >10" trigger to size-tiered levels (for example 10x level ratio) to bound read amplification and merge churn.
2. **Streaming k-way term merge**: Keep terms lexicographically sorted in each segment, then merge multiple segments with iterator-style term walkers instead of loading full dictionaries into memory.
3. **Stable chunk identity for merge-time dedupe**: Add optional `chunk_uid` (stable hash/id) in segment metadata for dedupe and conflict resolution during merge; keep roaring postings on dense segment-local ordinals for query efficiency.
4. **Sparse term-offset memory index**: Load only term-offset metadata into RAM, mmap section payloads, and lazily hydrate postings to reduce heap pressure during large merges.

**Activation criteria**:
1. `index_segments_per_shard` regularly exceeds 100 for production shards
2. `index_segment_build_duration_seconds` merge buckets dominate compactor time
3. Query planner latency regresses due to high segment fan-out despite pruning effectiveness

---

## 5. Rust Crate Selection

| Crate | Purpose | Maturity | Used By | Size Impact |
|-------|---------|----------|---------|-------------|
| [`roaring`](https://crates.io/crates/roaring) | Postings list compression | Stable, >10M downloads | Tantivy, Quickwit, Meilisearch | ~100KB |
| [`fst`](https://crates.io/crates/fst) | Term dictionary (FST) | Stable, by BurntSushi | ripgrep, Tantivy | ~50KB |
| [`xorf`](https://crates.io/crates/xorf) | Binary Fuse filters | Stable | Various | ~30KB |
| [`crc32fast`](https://crates.io/crates/crc32fast) | Section checksums | Stable, >100M downloads | Widely used | ~10KB |

### Explicitly Avoided

| Crate | Reason |
|-------|--------|
| `tantivy` | Full-text search engine — too heavy for label indexing. Brings its own segment management, merge policies, and schema system. We only need FST + roaring, not the orchestration layer. |
| `bloomfilter` | Xor/Fuse filters are smaller and faster for immutable data. Bloom filters only win when mutability (add-only) is needed. |
| Custom implementations | FST and roaring bitmap implementations are non-trivial (papers: Lemire et al. 2018 for roaring, Mohri et al. for FST). Using battle-tested crates avoids subtle correctness bugs. |

---

## 6. Integration with Existing Systems

### 6.1 New Component: ManifestClient

A new `ManifestClient` struct handles shard-manifest operations (replaces the reverted inverted-index API calls from PR #152):

```rust
// New: src/index/manifest.rs
pub struct ManifestClient {
    object_store: Arc<dyn ObjectStore>,
}

impl ManifestClient {
    pub async fn load_manifest(&self, tenant_id: &str, shard_id: &str)
        -> Result<Option<(IndexManifest, String /* etag */)>>;

    pub async fn save_manifest(&self, tenant_id: &str, shard_id: &str,
        manifest: &IndexManifest, expected_etag: &str) -> Result<()>;

    pub async fn freeze_manifest(&self, tenant_id: &str, shard_id: &str) -> Result<()>;

    pub async fn unfreeze_manifest(&self, tenant_id: &str, shard_id: &str) -> Result<()>;
}
```

This avoids bloating the existing `MetadataClient` trait (which has 19 methods). Index manifest operations are a separate concern with their own CAS semantics.

**Note**: The methods `update_inverted_index`, `remove_from_inverted_index`, and `query_inverted_index` from PR #152 (reverted in #156) are not re-added. The segment+manifest model replaces them entirely.

### 6.2 Query Planner Integration: Index-Aware Chunk Pruning

The existing query path in `QueryNode` (line 158-241 of `src/query/mod.rs`) today flows:

```
SQL → extract_time_range → extract_column_predicates
    → metadata.get_chunks_with_predicates(time_range, predicates)
    → register_metrics_table_for_chunks(chunk_paths)
    → engine.execute(sql)
```

Enhanced flow adds index pruning between metadata retrieval and table registration:

```
SQL → extract_time_range → extract_column_predicates
    → metadata.get_chunks_with_predicates(time_range, predicates)
    → [NEW] index_prune(tenant_id, shard_assignments, equality_predicates)
    → register_metrics_table_for_chunks(pruned_chunk_paths)
    → engine.execute(sql)
```

#### index_prune() Algorithm (Step by Step)

1. **Determine shard assignments**: Group `chunk_paths` by their `shard_id` field from `ChunkMetadataExtended`

2. **For each shard**:
   a. Load manifest from cache (`TieredCache`). If missing or `frozen == true`: skip pruning for this shard, pass all chunks through unmodified
   b. Check `manifest.generation == shard_metadata.generation` (generation CAS invariant)
   c. Extract `indexed_through_ns` watermark
   d. Partition chunks into: **indexed** (`max_ts ≤ indexed_through_ns`) and **frontier** (`max_ts > indexed_through_ns`)
   e. For indexed chunks, extract equality/IN predicates
   f. For each relevant segment (time range overlap):
      - Load segment `.csi` file from cache
      - FST lookup: construct key `column=value`, get roaring bitmap of ordinals
      - If multiple predicates: AND the roaring bitmaps
      - Resolve ordinals to chunk paths via ordinal table
      - Result: `indexed_matching_chunks`
   g. Emit `indexed_matching_chunks ∪ frontier_chunks` for this shard

3. **Union across all shards** to produce final `pruned_chunk_paths`

4. **Fall through to existing path**: `register_metrics_table_for_chunks()` and DataFusion handles rest with Parquet-native pushdown

#### Safety Invariant

**The index can only prune, never add.** If the index is stale, missing, or corrupt, the fallback is to return all input chunks unchanged. This ensures zero false negatives.

#### Implementation

New `IndexPrefilter` struct injected into `QueryNode`:

```rust
// New field on QueryNode
index_prefilter: Option<Arc<IndexPrefilter>>,

// New builder method
pub fn with_index_prefilter(mut self, prefilter: Arc<IndexPrefilter>) -> Self {
    self.index_prefilter = Some(prefilter);
    self
}
```

### 6.3 Compactor Integration: IndexBuilder Component

A new `IndexBuilder` struct handles segment construction during compaction:

```rust
struct IndexBuilder {
    object_store: Arc<dyn ObjectStore>,
    segment_writer: SegmentWriter,  // Handles .csi binary format
    manifest_client: ManifestClient,
}

impl IndexBuilder {
    async fn build_and_publish_segment(
        &self,
        batch: &RecordBatch,
        chunk_path: &str,
        shard_id: &str,
        level: Level,
    ) -> Result<()> {
        // 1. Extract all string columns from the batch
        // 2. For each column: build FST term dict + roaring bitmaps
        // 3. Build chunk ordinal table: ordinal -> chunk_path
        // 4. Serialize to .csi format
        // 5. Upload segment to S3
        // 6. CAS-update manifest: add segment, advance watermark
        // Return Ok(()) even if CAS fails after upload (watermark just doesn't advance)
    }
}
```

**New field on Compactor**:

```rust
pub struct Compactor {
    // ... existing fields ...
    index_builder: Option<IndexBuilder>,  // Optional; enables index builds if present
}

// New builder method
pub fn with_index_builder(mut self, builder: IndexBuilder) -> Self {
    self.index_builder = Some(builder);
    self
}
```

### 6.4 Shard Split Interaction: Manifest Freeze/Unfreeze

The shard split protocol already uses generation-based CAS. Index manifests integrate as follows:

| Split Phase | Manifest Behavior |
|---|---|
| **Preparation** | Freeze old shard manifest. Create empty manifests for new shards (watermark=0, generation=1). |
| **DualWrite** | Old manifest frozen. New manifests writable but watermark=0. |
| **Backfill** | No index changes. Manifests stay frozen during backfill. |
| **Cutover** | Unfreeze new manifests. Archive old manifest. |
| **Cleanup** | Delete old manifest. New shards' watermarks advance naturally as compaction indexes their data. |

Recovery after crash: `SplitProgress.completed_phase` tells the manifest state to expect. Recovery re-applies manifest operations (freeze/unfreeze) as needed.

### 6.5 AdaptiveIndexController Integration (Phase 3)

Phase 3 persists the controller's lifecycle state to the shard manifests:

```
Adaptive Controller
  → "column 'host' cardinality=5000, promote from invisible→visible"
  → update manifest field per shard (not a separate catalog)

Compactor (on compaction):
  → builds segments as normal
  → segments are scoped to their shard, automatically incorporate visible-status columns

Query Node:
  → reads manifest.segments (no separate catalog fetch)
  → uses FST+roaring in visible segments for indexed columns
  → tracks "would have helped" for pre-visible columns
```

This is deferred to Phase 3 because Phase 1 builds segments with all indexed columns regardless of adaptive status.

---

## 7. Tradeoffs Matrix

| Index Type | Write Overhead | Storage Cost | Query Speedup | Complexity | When to Use |
|------------|---------------|--------------|---------------|------------|-------------|
| **Zone Maps** (existing) | None (Parquet-native) | None | 2-10x for range predicates | Already implemented | Always — baseline |
| **Parquet Bloom** (existing) | ~1% flush time | ~1% file size | 5-50x for equality on 7 cols | Already implemented | Always — baseline |
| **Inverted Index** (FST+roaring) | ~5% flush time | ~1-2% of data size | 100-1000x for label equality | Medium | Columns with cardinality <100K |
| **Fuse Filter** (xorf) | ~2% flush time | ~1 byte/element | 10-100x for high-card equality | Low | Columns with cardinality >100K (trace_id, request_id) |
| **Skip Index** | N/A | N/A | N/A | N/A | Not needed — Parquet page stats equivalent |
| **Pre-Aggregation** | 10-50% compaction time | 10-50% additional storage | 10-100x for matching GROUP BY | High | Future phase — repeated dashboard queries |

### Index Type Selection by Cardinality

| Cardinality | Recommended Index | Rationale |
|-------------|-------------------|-----------|
| <1,000 | Inverted (FST+roaring) | Few unique values → small FST, dense bitmaps compress well |
| 1,000–100,000 | Inverted (FST+roaring) | Still fits comfortably in FST; roaring bitmaps sparse but efficient |
| 100,000–10M | Fuse Filter (Fuse8) | Too many terms for practical inverted index; probabilistic filter sufficient |
| >10M | Fuse Filter (BinaryFuse16) | Higher precision (~0.0015% FPP via 16-bit fingerprints) for very high cardinality |

This aligns with the existing `IndexRecommendationEngine` thresholds, with the addition of the Fuse16 tier.

---

## 8. Phased Implementation Plan

### Phase 0: Correctness Harness (~1 week)

**Goal**: Build testing foundation before runtime code.

**Tasks**:
1. Implement `.csi` writer/reader with roundtrip tests
2. Property tests for FST+roaring serialization
3. Manifest CAS conflict simulation tests
4. Query correctness tests with forced stale/missing/corrupt indexes
5. Shard split integration tests with indexing enabled

**Deliverable**: `tests/index_*_tests.rs` test suite with 30+ test cases covering serialization, CAS, staleness, and split scenarios.

### Phase 1: Compactor-Built Segment MVP (~2-3 weeks)

**Goal**: End-to-end index build during compaction and index-aware query pruning.

**New module**: `src/index/`
- `mod.rs` — `IndexPrefilter`, `IndexBuilder` public API
- `manifest.rs` — `IndexManifest`, `ManifestClient` with CAS
- `segment.rs` — `SegmentWriter`, `SegmentReader` (.csi format)
- `fst_builder.rs` — FST construction from RecordBatch columns
- `postings.rs` — Roaring bitmap construction and query
- `config.rs` — `IndexConfig` (segment thresholds, merge policies)

**Compactor changes**:
- Add `index_builder: Option<IndexBuilder>` field
- Call `index_builder.build_and_publish()` after each `merge_chunks()`
- Implement GC reconciliation in `garbage_collect()`
- Implement tiered segment merge when segment count exceeds threshold

**Query changes**:
- Add `index_prefilter: Option<Arc<IndexPrefilter>>` field to `QueryNode`
- Call `prefilter.prune(chunks, predicates)` between `get_chunks_with_predicates()` and `register_metrics_table_for_chunks()`

**Metrics (Phase 1)**:
- `index_planner_pruned_chunks_total` (labels: tenant, shard, result={pruned,passed})
- `index_frontier_chunk_count` (labels: tenant, shard)
- `index_manifest_staleness_seconds` (labels: tenant, shard)
- `index_segment_build_duration_seconds` (labels: tenant, shard, level)

**Rollout gates (Phase 1)**:
1. Zero correctness regressions in existing test suite
2. Phase 0 test suite passes with >95% assertion coverage
3. On a test workload with 1000+ chunks and equality predicates, `pruned_chunks_total` shows ≥50% reduction
4. With index files deleted, queries return correct results (slower, verified)
5. Shard split completes with no data loss or false negatives
6. `index_manifest_staleness_seconds` stays <2× `compactor.check_interval` during normal operation

**Deliverable**: Compactor builds `.csi` segments during compaction, publishes manifests via CAS, query planner uses index for equality/IN predicates with explicit frontier union.

### Phase 2: Prom API + High-Cardinality Extensions (~2 weeks)

**Goal**: Accelerate `/api/v1/labels` and `/api/v1/series` endpoints. Add fuse filters for high-cardinality columns.

**New code**:
- `src/index/value_dict.rs` — Value dictionary section (FST of unique values, no postings)
- `src/index/fuse_filter.rs` — Binary fuse filter section

**Changes**:
- `src/index/segment.rs`: Add section types 0x02 (Fuse8/Fuse16) and 0x03 (value dictionary)
- `src/api/prometheus.rs` (or equivalent): Use value dictionary for `/api/v1/label/{label_name}/values` queries
- Add `xorf = "0.11"` to `Cargo.toml`
- Adaptive controller selects columns with cardinality >100K for fuse filter indexing

**Deliverable**: `/api/v1/labels` returns sub-second results even with millions of unique label values. High-cardinality columns benefit from Fuse8/Fuse16 membership tests during Parquet scanning.

### Phase 3: Adaptive Persistence + Ingester Segments (~2 weeks)

**Goal**: Persist adaptive index lifecycle state. Optionally build per-chunk segments at ingestion to reduce freshness lag.

**Changes**:
- `src/adaptive_index/lifecycle.rs`: Replace `DashMap<String, IndexMetadata>` with `ManifestClient`-backed persistence
- `src/ingester/mod.rs`: Optional single-chunk `.csi` segment build at flush (same `.csi` format)
- `src/index/config.rs`: Add `ingester_segments_enabled: bool` feature flag

**Benefit**: Freshness lag narrows from "compaction interval" (minutes) to "flush interval" (5 minutes default). Same `.csi` format ensures no duplication.

**Deliverable**: Index state persists across restarts. Ingester can optionally accelerate queries on fresh L0 data.

### Phase 4: LSM-Style Merge Optimization + Pre-Aggregation (Future)

**Goal**: Long-running shards with high segment fan-out. Add LSM-style merge improvements before introducing star-tree pre-aggregation.

**Changes**:
- `src/index/merge_planner.rs` (new): Level-aware merge selection by size ratio and compaction budget
- `src/index/segment.rs`: Streaming k-way term merge path for inverted sections
- `src/index/manifest.rs`: Optional merge budget/accounting fields (for observability only)
- `src/metadata/models.rs` (or index metadata equivalent): Optional stable `chunk_uid` field for merge-time dedupe
- `src/index/metrics.rs` (or equivalent): Add merge amplification/read amplification metrics

**Rollout gates**:
1. Merge throughput improves by at least 2x versus Phase 1 tiered merge on a shard with 500+ segments
2. Query p95 for equality predicates does not regress under sustained merge load
3. No correctness regressions in stale/missing/corrupt index fallback tests
4. Split and recovery behavior remains unchanged (manifest invariants intact)

**Deliverable**: Segment count growth is bounded under sustained ingest, with predictable merge cost and no change to query correctness semantics.

**Deferred**: Pre-aggregation/star-tree stays future work until LSM-style merge optimization is proven in production.

---

## 9. New Metrics and Operational SLOs

**Required metrics**:

```
index_planner_pruned_chunks_total{tenant, shard, result={pruned,passed}}
  Counter tracking chunks pruned vs passed through by index

index_frontier_chunk_count{tenant, shard}
  Gauge: unindexed chunks (newer than indexed_through_ns watermark)

index_manifest_staleness_seconds{tenant, shard}
  Gauge: seconds since last successful manifest watermark advance
  Alert if > 2 * compactor.check_interval

index_manifest_cas_conflicts_total{tenant, shard}
  Counter: manifest CAS conflicts requiring retry

index_lookup_latency_seconds{tenant, shard}
  Histogram: time to perform FST+roaring query during planner

index_segment_build_duration_seconds{tenant, shard, level}
  Histogram: time to build segment during compaction

index_segments_per_shard{tenant, shard}
  Gauge: current segment count per shard

index_segment_merges_total{tenant, shard, result={ok,error}}
  Counter: tiered segment merges performed
```

**Phase 4 (future) metrics**:

```
index_merge_write_amplification{tenant, shard}
  Gauge: bytes written during index merge / bytes of merged input segments

index_merge_read_amplification{tenant, shard}
  Gauge: number of segments consulted per predicate lookup (post-cache)

index_merge_backlog_segments{tenant, shard, level}
  Gauge: segments waiting for merge by level
```

---

## 10. Failure Modes and Recovery

| Failure | Impact | Recovery |
|---------|--------|----------|
| **Segment `.csi` corrupt on S3** | Query planner CRC32 check fails, segment skipped, full scan for time range | Compactor detects via header CRC32 check, rebuilds segment |
| **Manifest CAS conflict** | Concurrent manifest writers (rare), compactor retries via `cas_retry!` macro (5 attempts) | Automatic exponential backoff |
| **Compactor crashes mid-build** | Orphan `.csi` on S3, manifest not updated | Manifest is source of truth; orphan detected by GC reconciliation |
| **Manifest references deleted segment** | Segment load fails during query | Query planner treats as missing index, falls back to full scan |
| **Watermark stuck** | `index_manifest_staleness_seconds` alert, queries use full scan path | Restart compactor, watermark advances on next successful build |
| **Stale manifest during split** | `manifest.generation != shard.generation` mismatch | Query planner checks invariant, ignores stale manifest, uses fallback |
| **Query node crashes during fetch** | Partial manifest load, partial segment load | TieredCache expiration, next query refetches |

---

## 11. Design Decisions and Rationale

### Why Shard-Scoped Segments Instead of Tenant-Global Index?

The initial proposal (pre-convergence revision of this document) used a tenant-global `global.idx` rebuilt during compaction. This was changed to shard-scoped segments for three reasons:

1. **Rebuild cost scales with corpus, not increment**: A tenant-global rebuild processes the entire corpus on every compaction. A shard-scoped rebuild only processes that shard's data. For a 500K-chunk tenant, this is the difference between rebuilding a 10MB FST every 60 seconds vs only the affected shard.

2. **Failure isolation**: A corrupt or stale segment only blinds one shard. A corrupt tenant-global index blinds the entire tenant.

3. **Split alignment**: During shard splits, each new shard gets independent manifests and segment directories. No need to "split the index" as a separate coordination problem.

### Why Compactor-First (Phase 1), Not Ingester Segments?

The proposal evolves builds from ingester (per flush) to compactor (per compaction merge). Rationale:

1. **Ingest latency stable**: The ingester's flush path in `src/ingester/mod.rs` already handles schema heterogeneity, WAL truncation, and topic broadcast. Index construction here increases failure blast radius.

2. **Index quality**: Compactor merges and sorts data, producing high-quality input for FST/roaring construction. Unsorted per-flush data produces less-efficient indexes.

3. **"Degradation only" semantics**: If index build fails, data is safe and queries work (just slower). No new failure modes in the critical write path.

Ingester-built segments (Phase 3) become optional for reducing freshness lag, but are not required for correctness.

### Why Explicit Watermark Instead of Implicit Unindexed Union?

The `indexed_through_ns` watermark makes freshness explicit and measurable:

1. **Operational SLO**: `index_manifest_staleness_seconds` metric tracks lag directly. Teams can set alerts.

2. **Correctness verification**: Watermark proves "this shard's data is indexed up to time X; chunks after X must use fallback". Easy to verify in tests.

3. **Split safety**: During splits, watermarks can be frozen independently per shard without affecting query correctness.

### Why Segment-Local Ordinals in Phase 1 Instead of Global IDs?

Phase 1 keeps postings on segment-local dense ordinals because:

1. **Compression efficiency**: Roaring performs best with dense `u32` domains.
2. **Build simplicity**: No distributed ID allocator or cross-node coordination required.
3. **Failure isolation**: Segment rebuilds stay local and do not require global ID remapping transactions.

Future LSM-style optimization can add stable `chunk_uid` for merge-time dedupe while retaining dense local ordinals for query-time postings.

### Why Not Revive the Reverted Inverted-Index API?

PR #152 proposed `update_inverted_index`, `query_inverted_index`, and `remove_from_inverted_index` methods. These were reverted in #156. This design does not re-add them because:

1. **Wrong abstraction**: Those APIs operated on term-level mutations (add/remove label values). The segment model needs manifest operations (add/remove segments, CAS watermark).

2. **Mutable index antipattern**: Those APIs encouraged thinking about a mutable central index. The segment model is intentionally immutable.

3. **New APIs needed**: `ManifestClient` with CAS semantics is the right abstraction for this model.

---

## 12. Open Questions (Convergence Notes)

1. **Segment merge trigger**: Tiered merge is triggered at segment count >10. Is this the right threshold? Should it vary by shard cardinality?

2. **Index build on every compaction**: Should segments be built on every compaction output, or only at higher levels (L1+)? Building on every output maximizes freshness; skipping L0 saves compute.

3. **Watermark granularity**: Is nanosecond precision the right granularity? Microsecond or millisecond would reduce manifest size negligibly but simplify implementation.

4. **Multi-tenant index isolation**: Each tenant has independent manifests and segments. Cross-tenant queries (if ever supported) would union multiple shard results. Current design assumes single-tenant queries.

5. **Adaptive column selection**: Phase 3 will add columns to indexes based on adaptive controller recommendations. How should the compactor handle columns that are removed from recommendations (deprecated indexes)?

6. **Stable chunk identity scope**: Should `chunk_uid` be shard-scoped or tenant-scoped once LSM-style merge optimization is enabled?

7. **Merge policy target**: Should leveled merge use strict size ratio (for example 10x) or adaptive budgeting based on compactor backlog?
