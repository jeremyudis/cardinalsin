# Indexing Design: Next Evolution (Follow-up to PR #158)

| Field | Value |
|---|---|
| Status | Draft |
| Date | 2026-03-06 |
| Parent Issue | #157 |
| Parent PR | #158 (`docs/design-indexing`) |
| This Doc | additive follow-up; does not replace `docs/design-indexing.md` |

## 1. Purpose

This document proposes the **next evolution** of the indexing design in PR #158 and explains why key decisions changed.

The goal is not to invalidate the original proposal. The goal is to harden it for:

1. correctness under failure,
2. compatibility with current code reality,
3. long-term scalability under shard growth and compaction concurrency.

## 2. Reality Check Against Current Mainline

Before changing design decisions, this follow-up aligns with what exists today in code:

1. No active inverted index implementation exists on `main` (PR #152 was reverted in #156).
2. `ParquetWriter` currently has page statistics enabled but bloom filters disabled globally.
3. `ChunkMetadataExtended.column_stats` exists, but ingester registration path currently writes empty stats by default.
4. Query path is `QueryNode -> metadata.get_chunks_with_predicates() -> DataFusion`, so metadata-level pruning is the primary prefilter point.
5. Adaptive index lifecycle is in-memory only and not persisted.

These facts change the safest rollout order.

## 3. Decision Changes From PR #158

## 3.1 Index Topology

**PR #158 direction**: tenant-wide `global.idx` rebuilt during compaction.

**New direction**: shard-scoped immutable index segments + shard manifest.

Why changed:

1. Tenant-global rebuild cost scales with total corpus and becomes a compaction hotspot.
2. Shard-local failure domains are safer and easier to debug.
3. Shard-scoped manifests align better with split/backfill/cutover semantics already present in the system.

## 3.2 Build Authority

**PR #158 direction**: heavy emphasis on ingester-side index production plus global rebuild.

**New direction**: compactor is phase-1 canonical segment builder; ingester sidecars become optional later.

Why changed:

1. Keeps ingest latency and ingest failure modes stable for initial rollout.
2. Compactor already owns immutable merge boundaries, so index segment boundaries naturally match storage lifecycle.
3. Allows “performance degradation only” failure semantics if indexing lags.

## 3.3 Freshness and Correctness Gate

**PR #158 direction**: union “unindexed chunks” conceptually, but tied to global index membership.

**New direction**: explicit `indexed_through_ns` watermark per shard manifest and mandatory frontier union.

Why changed:

1. Watermark makes stale-index behavior explicit and measurable.
2. Prevents false negatives during compaction/index publication lag.
3. Gives an operational SLO (`index_manifest_staleness_seconds`) instead of implicit behavior.

## 3.4 API Surface

**PR #158 direction**: evolves previously proposed `update_inverted_index/query_inverted_index` style APIs.

**New direction**: do not revive JSON-inverted-index APIs as the long-term abstraction.

Why changed:

1. Those APIs came from reverted architecture and encourage central mutable-index thinking.
2. Segment + manifest model needs manifest CAS and frontier queries, not term-level mutation APIs.

## 3.5 Rollout Sequence

**PR #158 direction**: broad design including multiple index types early.

**New direction**: strict phased rollout with correctness harness before planner wiring.

Why changed:

1. Current code has limited coverage for planner/index interaction and split/compaction races.
2. A smaller first cut reduces blast radius and simplifies verification.

## 4. Recommended Architecture (Delta)

## 4.1 Storage Model

```text
{tenant_id}/indexes/
  shard={shard_id}/
    manifest.json
    segments/
      seg_{uuid}.csi
```

Manifest fields (minimum):

1. `generation`
2. `indexed_through_ns`
3. segment list with `{path,min_time_ns,max_time_ns,chunk_count}`

Segment fields (minimum):

1. FST term dictionary (`column=value -> postings offset`)
2. Roaring postings bitmaps over segment-local chunk ordinals
3. Chunk ordinal table (`ordinal -> chunk_path`)
4. Checksum footer

## 4.2 Query Planner Contract

For each shard:

1. Lookup indexed candidates from overlapping segments.
2. Union with frontier chunks newer than `indexed_through_ns`.
3. Apply existing metadata predicate pruning on union result.
4. Execute through existing DataFusion table-registration flow.

Hard rule: index lookup can only prune chunks proven indexed for that shard/time window.

## 4.3 Failure Semantics

If index build/publication fails:

1. Data chunk remains queryable.
2. Manifest watermark does not advance.
3. Planner frontier union preserves correctness.
4. Background catch-up reattempts index publication.

This preserves “performance loss only” behavior.

## 5. Index Type Strategy (Updated)

## 5.1 Phase 1 (required)

1. Inverted postings index for equality/IN predicates.
2. Existing Parquet and metadata stats path as fallback.

## 5.2 Phase 2 (optional, after planner stability)

1. Value dictionary section to accelerate `/api/v1/label/*/values` and `/api/v1/series`.
2. Binary fuse filters (`xorf`) for high-cardinality membership negative checks.

## 5.3 Deferred

1. Pre-aggregation/star-tree style indexes.
2. Ingest-side hot-path sidecars.

## 6. Rust Library Selection (What Changed)

No change to primary recommendation of using battle-tested primitives, but stronger stance on composition:

1. `fst` for term dictionaries.
2. `roaring` (or `croaring` if profiling later warrants FFI path).
3. `xorf` for immutable high-card filters.
4. `crc32fast` for segment integrity.

Changed decision: avoid taking a dependency on full `tantivy` engine as the core index runtime for this phase.

Reason:

1. Great reference architecture, but more subsystem ownership than needed for this storage model.
2. Segment lifecycle and manifest semantics are simpler to own directly here.

## 7. Proposed Implementation Phases

## 7.1 Phase 0: Correctness Harness

1. Serialization/property tests for postings and dictionaries.
2. Failure-injection tests for manifest CAS conflicts and object-store errors.
3. Query correctness tests with forced stale/missing index objects.

## 7.2 Phase 1: Compactor-Built Segment MVP

1. Implement `.csi` writer/reader.
2. Build segment on compaction outputs.
3. Publish shard manifest via CAS.
4. Add index-aware prefilter in query planner with mandatory frontier union.

## 7.3 Phase 2: Prom API + High-Card Extensions

1. Value dictionary acceleration for label/series endpoints.
2. Optional fuse filters for selected columns.

## 7.4 Phase 3: Adaptive Persistence

1. Persist lifecycle/index catalog state.
2. Connect adaptive recommendations to persisted index capabilities.

## 8. New Metrics and Rollout Gates

Required metrics:

1. `index_planner_pruned_chunks_total`
2. `index_frontier_chunk_count`
3. `index_manifest_staleness_seconds`
4. `index_manifest_cas_conflicts_total`
5. `index_lookup_latency_seconds`

Phase-1 rollout gates:

1. Zero correctness regressions in e2e tests.
2. Verified no false negatives under stale/missing index artifacts.
3. Measurable chunk-pruning win on equality label workloads.

## 9. Tradeoffs of the New Direction

| Dimension | Tenant Global Rebuild | Shard Segment + Manifest (new) |
|---|---|---|
| Build cost | potentially global | incremental per shard |
| Failure scope | tenant-wide | shard-local |
| Query complexity | simpler single file | moderate (multi-segment) |
| Scale with compaction parallelism | weak | strong |
| Split/cutover alignment | weaker | strong |

## 10. Recommended Next PR Shape

This follow-up should land as a PR against `docs/design-indexing` branch with:

1. this additive document,
2. explicit “decision changes and reasons” section (above),
3. no destructive edits to `docs/design-indexing.md`.

This keeps original proposal intact while documenting a stricter, production-hardening evolution path.
