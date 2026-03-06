# Design: Production-Grade Indexing Architecture

| Field       | Value                                    |
|-------------|------------------------------------------|
| **Status**  | Draft                                    |
| **Authors** | CardinalSin Team                         |
| **Issue**   | gh-157, cardinalsin-vf6                  |
| **Date**    | 2026-03-05                               |
| **Refs**    | Adaptive Indexing Epic (gh-134)          |

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

**Current state**: `BTreeMap<String, BTreeMap<String, Vec<String>>>` stored as JSON.

**Production approach**: FST term dictionary + roaring bitmap postings.

#### Architecture

```
┌─────────────────────────────────────┐
│         Per-Chunk Sidecar           │
│  chunks/{chunk_id}.idx              │
│                                     │
│  ┌─────────────┐                    │
│  │ FST (terms) │  metric_name=cpu   │
│  │             │  host=web-01       │
│  │             │  region=us-east-1  │
│  └──────┬──────┘                    │
│         │ offset into postings      │
│  ┌──────▼──────┐                    │
│  │   Roaring   │  row_group bitmap  │
│  │   Bitmaps   │  (which row groups │
│  │             │   contain value)   │
│  └─────────────┘                    │
└─────────────────────────────────────┘

┌─────────────────────────────────────┐
│      Tenant Global Index            │
│  indexes/{tenant}/global.idx        │
│                                     │
│  FST: col=val  →  roaring bitmap    │
│                    of chunk_ids     │
│                                     │
│  Rebuilt during compaction, not     │
│  during ingestion (immutable once   │
│  written)                           │
└─────────────────────────────────────┘
```

**Per-chunk sidecar** (built at flush time by ingester):
- FST maps `column_name=value` terms to offsets
- Roaring bitmaps at those offsets indicate which **row groups** within the chunk contain the value
- Small: a chunk with 10 columns × 1000 unique values ≈ FST ~50KB + bitmaps ~20KB

**Tenant global index** (built during compaction):
- FST maps `column_name=value` to roaring bitmaps of **chunk IDs** (integer-mapped)
- Enables chunk pruning before fetching any Parquet files
- Rebuilt by merging sidecar indexes during compaction — never mutated in place
- Chunk ID mapping stored in index header (chunk_path → integer ID)

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
| **CardinalSin (proposed)** | FST (`fst` crate) | Roaring bitmaps (`roaring` crate) | Per-chunk + tenant global |

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

Standalone fuse filters are written as part of the per-chunk sidecar `.idx` file. The adaptive index controller selects columns with cardinality >100K for fuse filter indexing (below that, the inverted index is more useful).

```
Sidecar .idx file:
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

**Recommendation**: **Future phase only.** Document the approach but do not implement. The adaptive index controller already tracks `groupby_stats` — when these indicate repeated patterns on compacted data, star-tree construction can be added to the compaction pipeline. The per-chunk sidecar format (section 3) reserves a section type for pre-aggregation data.

---

## 3. Storage Model

### 3.1 Per-Chunk Sidecar Files

Each chunk's index data is stored alongside the Parquet file:

```
chunks/
  {chunk_id}.parquet      # Data
  {chunk_id}.idx          # Index sidecar
```

Co-location simplifies lifecycle management — when a chunk is deleted (compaction cleanup), its index should be deleted too. However, since S3 objects are deleted independently, a partial failure during cleanup can leave orphan sidecars (`.idx` without a corresponding `.parquet`) or missing sidecars (`.parquet` without `.idx`).

**Garbage collection**: The compactor's cleanup phase must reconcile both objects:
1. List all `.idx` files in the chunk prefix
2. For each `.idx`, verify the corresponding `.parquet` exists in metadata; delete orphans
3. For chunks without sidecars, flag for sidecar rebuild during next compaction

This GC runs as part of the existing `cleanup_completed_jobs` flow. Orphan sidecars are harmless (they waste storage but don't affect correctness), so GC can run at low priority.

### 3.2 Index Sidecar Binary Format

```
┌──────────────────────────────────────────────┐
│ Magic: "CSIX" (4 bytes)                      │
│ Version: u16 (1)                             │
│ Flags: u16 (reserved)                        │
│ Section count: u32                           │
├──────────────────────────────────────────────┤
│ Section Directory (repeated per section):    │
│   Type: u8                                   │
│     0x01 = Inverted Index (FST + Roaring)    │
│     0x02 = Fuse Filter                       │
│     0x03 = Pre-Aggregation (reserved)        │
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
│ [Fuse Filter Section]                        │
│   Column name length: u16                    │
│   Column name: UTF-8                         │
│   Filter type: u8 (0x01=Fuse8, 0x02=Fuse16) │
│   Filter bytes (xorf serialization)          │
│                                              │
├──────────────────────────────────────────────┤
│ Footer:                                      │
│   File CRC32: u32                            │
│   Magic: "CSIX" (4 bytes)                    │
└──────────────────────────────────────────────┘
```

**Design rationale**:
- **Section-based**: New index types can be added without breaking existing readers (skip unknown section types)
- **Per-section CRC32**: Detect corruption at the section level; a corrupt fuse filter doesn't invalidate the inverted index
- **Footer magic**: Enables reverse scanning to verify file integrity (same pattern as Parquet)
- **No compression**: FST and roaring bitmaps are already compressed; additional compression adds latency with minimal benefit

### 3.3 Tenant-Level Index Catalog

```
metadata/indexes/{tenant_id}/catalog.json
```

```json
{
  "version": 1,
  "columns": {
    "host": {
      "index_type": "inverted",
      "status": "visible",
      "cardinality_estimate": 5000,
      "created_at": "2026-03-05T00:00:00Z",
      "last_used_at": "2026-03-05T12:00:00Z",
      "usage_count": 1500
    },
    "trace_id": {
      "index_type": "fuse_filter",
      "status": "visible",
      "cardinality_estimate": 50000000,
      "created_at": "2026-03-05T00:00:00Z",
      "last_used_at": "2026-03-05T11:30:00Z",
      "usage_count": 300
    }
  },
  "global_index": {
    "path": "indexes/{tenant_id}/global.idx",
    "built_at": "2026-03-05T10:00:00Z",
    "chunk_count": 15000,
    "etag": "abc123"
  }
}
```

This catalog:
- Replaces the in-memory-only `DashMap` in `IndexLifecycleManager` with persisted state
- Is fetched once per query session (lightweight, <1KB typically)
- Updated during compaction (not during ingestion — avoids CAS contention)
- Drives the adaptive index controller's decisions

### 3.4 Tenant Global Index

```
indexes/{tenant_id}/global.idx
```

Same binary format as sidecar `.idx` files, but the roaring bitmaps map to chunk IDs (integers) instead of row group IDs. The header includes a chunk ID mapping table:

```
[Global Index Header]
  Chunk count: u32
  Chunk ID table: (u32 → string path) repeated
[Inverted Index Section]
  FST: col=val → bitmap offset
  Bitmaps: roaring bitmaps of chunk IDs
```

**Lifecycle**:
- Built fresh during L1+ compaction (not incrementally updated)
- Compactor reads all sidecar indexes for the tenant, merges, writes global index
- Old global index is replaced atomically (write new file, update catalog, delete old)
- Query nodes cache global index in TieredCache like any other S3 object

---

## 4. Stateless Architecture Implications

### 4.1 Immutability Is the Key Constraint

CardinalSin's ingester, query, and compactor nodes are stateless. There is no shared mutable state beyond S3 + metadata CAS. This means:

| Constraint | Implication |
|------------|-------------|
| No mutable global index | Indexes must be immutable after construction |
| No inter-node coordination | Index builds are local operations (ingester builds sidecar at flush, compactor builds global during merge) |
| Crash recovery = re-read from S3 | No index write-ahead log needed — if a flush fails, the chunk and its sidecar are both absent |
| Multiple query nodes | Each independently caches index files via TieredCache |

### 4.2 Index Lifecycle Across Nodes

```
Ingester                    Compactor                   Query Node
────────                    ─────────                   ──────────
flush():                    compact():                  query():
  write chunk.parquet         read sidecar .idx files     fetch catalog.json (cached)
  build sidecar .idx          merge into global.idx       fetch global.idx (cached)
  upload both to S3           update catalog.json         prune chunks via global index
  register in metadata        (all via CAS)               fetch sidecar .idx (cached)
                                                          prune row groups via sidecar
                                                          read only matching Parquet data
```

### 4.3 Cache Integration

Index files flow through the existing `TieredCache` (RAM → NVMe → S3):

- **Global index** (~1-10MB per tenant): likely stays in L1 (RAM) after first access
- **Sidecar indexes** (~50-100KB each): cached in L2 (NVMe) alongside their Parquet files
- **No special cache eviction** needed — indexes are small relative to Parquet data

The `CachedObjectStore` already handles `.idx` files transparently (they're just S3 objects). Query nodes fetch them the same way they fetch `.parquet` files.

### 4.4 Compaction and Index Merging

During compaction (L0→L1→L2→L3):

1. Compactor reads source chunk sidecar `.idx` files
2. For inverted indexes: union the roaring bitmaps, rebuild FST for the merged key set
3. For fuse filters: rebuild from the merged column data (fuse filters can't be merged)
4. Write new sidecar `.idx` for the compacted chunk
5. Rebuild tenant global index from all current sidecar indexes
6. Update catalog atomically

**Cost**: Index merging adds ~10-20% to compaction time (dominated by roaring bitmap unions, which are fast — microseconds for million-entry bitmaps).

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

### 6.1 MetadataClient Trait

The existing trait already has inverted index methods:

```rust
// Existing (keep, but implementation changes)
async fn update_inverted_index(&self, tenant_id: &str, chunk_path: &str,
    columns: &[(String, String)]) -> Result<()>;
async fn remove_from_inverted_index(&self, tenant_id: &str,
    chunk_path: &str) -> Result<()>;
async fn query_inverted_index(&self, tenant_id: &str,
    predicates: &[(String, String)]) -> Result<Option<HashSet<String>>>;
```

**Changes needed**:

```rust
// New: Index catalog management
async fn get_index_catalog(&self, tenant_id: &str) -> Result<Option<IndexCatalog>>;
async fn update_index_catalog(&self, tenant_id: &str,
    catalog: &IndexCatalog) -> Result<()>;

// Existing methods evolve:
// - update_inverted_index: now writes sidecar .idx file instead of JSON
// - query_inverted_index: now reads global.idx (FST+roaring) instead of JSON
// - remove_from_inverted_index: deletes sidecar .idx, triggers global rebuild
```

The JSON-based inverted index methods can be migrated incrementally:
1. Phase 1: Write both JSON and sidecar `.idx` (dual-write)
2. Phase 2: Read from sidecar `.idx`, fall back to JSON
3. Phase 3: Remove JSON path

### 6.2 QueryEngine Integration

Current query flow in `execute_with_indexes` (engine.rs:259-300):

```
SQL → parse → extract_filter_columns → lifecycle_manager check → DataFusion execute
```

**Enhanced flow**:

```
SQL → parse → extract_filter_columns
  → fetch index catalog (cached)
  → for indexed columns:
      → fetch global.idx (cached)
      → FST lookup: col=val → roaring bitmap of chunk IDs
      → intersect bitmaps across predicates (AND)
      → result: indexed_chunk_set
  → union with non-indexed chunks (L0 / recently flushed):
      → metadata.get_chunks(time_range) returns all chunks
      → chunks NOT in global.idx chunk ID table are "unindexed"
      → final_chunk_set = indexed_chunk_set ∪ unindexed_chunks
  → for non-indexed columns on final_chunk_set:
      → fall back to zone map pruning (existing ColumnStats path)
  → register only matching chunks with DataFusion
  → DataFusion executes with Parquet-native pushdown on remaining data
```

**Important**: The global index is rebuilt during compaction, not ingestion. Newly flushed L0 chunks are absent from `global.idx` until the next compaction cycle. To avoid false negatives (missing fresh data), the query path must union in any chunks not present in the global index's chunk ID table. These unindexed chunks fall through to zone map pruning, which is the existing behavior. As chunks are compacted and the global index is rebuilt, they move from the "unindexed" path to the fast indexed path.

This extends the existing `get_chunks_with_predicates` method — the inverted index narrows the chunk set before zone map evaluation, while unindexed chunks are handled by the existing fallback.

### 6.3 AdaptiveIndexController Integration

The controller currently makes recommendations but cannot act on them. With the sidecar system:

```
Recommendation Engine
  → "index column 'host' with inverted index"
  → update index catalog: host → { index_type: "inverted", status: "invisible" }

Ingester (on next flush):
  → reads catalog, sees 'host' marked for indexing
  → builds inverted index entries for 'host' in sidecar .idx
  → (also builds for all columns already marked "visible")

Compactor (on next compaction):
  → reads all sidecar .idx files
  → merges into global.idx
  → updates catalog: host → status: "visible" (after threshold met)

Query Node:
  → reads catalog, sees 'host' is "visible"
  → uses global.idx for host= predicates
  → tracks "would have helped" for "invisible" columns (existing logic)
```

The key change: `IndexLifecycleManager` persists to `catalog.json` instead of only living in a `DashMap`. This is separately tracked in beads issue `cardinalsin-0ew`.

### 6.4 Ingester Flush Path

In `src/ingester/mod.rs`, after writing the Parquet file:

```rust
// Existing
let chunk_path = upload_to_s3(parquet_bytes).await?;
metadata.register_chunk(&chunk_path, &chunk_metadata).await?;

// New (added after Parquet upload)
let index_catalog = metadata.get_index_catalog(tenant_id).await?;
let sidecar = build_sidecar_index(&record_batches, &index_catalog)?;
upload_to_s3(sidecar_bytes, &format!("{}.idx", chunk_id)).await?;
metadata.update_inverted_index(tenant_id, &chunk_path, &label_pairs).await?;
```

Building the sidecar at flush time adds ~5-10ms (FST construction is fast for <10K unique terms per chunk). This is negligible compared to Parquet encoding (~100ms) and S3 upload (~200ms).

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

## 8. Migration Path

### Phase 1: Sidecar Infrastructure (This Design)
- Implement sidecar `.idx` binary format
- Build inverted index in sidecar at ingester flush
- Compactor reads sidecars, merges into global index
- Query nodes use global index for chunk pruning
- Dual-write JSON + sidecar for rollback safety

### Phase 2: Fuse Filters
- Add fuse filter sections to sidecar format
- Adaptive controller triggers fuse filter creation for high-cardinality columns
- Query nodes check fuse filters before fetching Parquet files

### Phase 3: Catalog Persistence
- Migrate `IndexLifecycleManager` state to `catalog.json`
- Ingester reads catalog to know which columns to index
- Compactor updates catalog with build status
- Remove in-memory-only lifecycle state

### Phase 4: Pre-Aggregation (Future)
- Add star-tree section to sidecar format
- Compactor builds star-trees for hot GROUP BY patterns
- Query engine rewrites matching queries to read pre-agg data

---

## 9. Open Questions

1. **Global index rebuild frequency** — Should the global index be rebuilt on every compaction, or on a separate schedule? Rebuilding on every compaction ensures freshness but adds work. A separate background job could rebuild periodically (e.g., every 10 minutes).

2. **Index versioning during compaction** — When the global index is being rebuilt, query nodes may read a stale version. This is acceptable (they'll scan a few extra chunks) but should be documented as expected behavior.

3. **Multi-tenant index isolation** — Each tenant has its own catalog and global index. Cross-tenant queries (if ever supported) would need to union multiple global indexes. Current design assumes single-tenant queries only.

4. **Sidecar upload atomicity** — The Parquet file and sidecar `.idx` are separate S3 objects. If the sidecar upload fails after the Parquet upload succeeds, the chunk exists without an index. This is safe (queries fall back to zone maps) but the compactor should detect and backfill missing sidecars.
