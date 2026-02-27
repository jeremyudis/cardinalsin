# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Critical Rules

**If an implementation is incorrect, FIX IT! Do NOT modify tests to match broken code.**

**Always create a PR after committing and pushing.** Beads issues cannot be closed until the full PR lifecycle is complete: opened → comments addressed → merged. After opening a PR, always check for merge conflicts (`gh pr view <number> --json mergeable`) and resolve them before moving on.

## Epic Delivery Workflow

When executing an Epic from GitHub issues:

1. Identify and pick up the first five sub-items unless the request specifies a different slice.
2. Run work in parallel agents/worktrees where possible.
3. Map each sub-item to its Beads issue and mark/update the associated Beads entries as work progresses.
4. Prefer one PR per sub-item when file changes do not conflict; if there are conflicts, condense into fewer PRs.
5. Ensure every PR is attached to its issue with an explicit closing keyword in the PR body (for example, `Closes #123`) so merge auto-closes the issue.
6. Include epic linkage in each PR (for example, `Refs: #<epic>`).

## Project Overview

CardinalSin is a high-cardinality time-series database built on object storage (S3/GCS/Azure) that solves the cardinality explosion problem through columnar storage instead of per-tag-combination indexing.

**Key Innovation**: Labels are stored as columns (not tag sets), eliminating series-set explosion. Built on DataFusion, Arrow, and Parquet.

## Build & Test Commands

### Building
```bash
# Development build
cargo build

# Release build
cargo build --release

# Build specific binary
cargo build --bin cardinalsin-ingester
cargo build --bin cardinalsin-query
cargo build --bin cardinalsin-compactor
cargo build --bin backfill-levels
```

### Testing
```bash
# Run all tests
cargo test

# Run specific test file
cargo test --test atomic_metadata_tests
cargo test --test level_tracking_tests
cargo test --test shard_split_tests
cargo test --test generation_cas_tests
cargo test --test adaptive_indexing_tests

# Run single test
cargo test --test atomic_metadata_tests test_concurrent_chunk_registration

# Run with output
cargo test -- --nocapture

# Check compilation without building
cargo check
```

### Running Services
```bash
# Ingester (port 8081 HTTP, 4317 OTLP gRPC)
./target/release/cardinalsin-ingester

# Query node (port 8080)
./target/release/cardinalsin-query

# Compactor (background service)
./target/release/cardinalsin-compactor

# Migration tool
cargo run --bin backfill-levels -- --bucket cardinalsin-metadata --prefix metadata/
```

### Docker
```bash
cd deploy
docker-compose up -d  # Starts MinIO, ingester, query node, compactor, Grafana
```

### Worktree Maintenance
When using git worktrees for parallel work, each worktree creates its own `target/` directory. Clean up when you have more than 10 worktrees:

```bash
# Remove all target directories (safe - cargo will rebuild as needed)
find ~/code/cardinalsin -name "target" -type d -exec rm -rf {} +

# Check worktree count
git worktree list | wc -l
```

**Note**: This is safe because Cargo will rebuild dependencies as needed. Cleaning up saves disk space (each target dir can be 1-5GB).

## Architecture Overview

### Three-Tier System

1. **Ingester Pool**: Buffers writes → batches to Parquet → flushes to S3
2. **Query Node Pool**: Executes SQL via DataFusion with 3-tier caching (RAM → NVMe → S3)
3. **Compactor Pool**: Merges files, downsamples, enforces retention, manages leveled compaction

All nodes are **stateless** - data lives in S3, metadata coordination via FoundationDB (or S3 for MVP).

### Critical Components

#### Metadata Layer (`src/metadata/`)
- **MetadataClient trait**: Abstract interface for metadata operations
- **S3MetadataClient**: Production implementation with **atomic operations using S3 ETags**
  - `atomic_register_chunk()`: CAS with exponential backoff (max 5 retries)
  - `complete_compaction()`: Atomic with level tracking
  - Each operation uses ETag-based optimistic locking to prevent lost updates
- **LocalMetadataClient**: In-memory implementation for testing
- **Atomicity**: All metadata mutations use compare-and-swap (CAS) semantics

**Key Files**:
- `src/metadata/client.rs`: Trait definition
- `src/metadata/s3.rs`: S3 implementation with atomic operations (~800 LOC)
- `src/metadata/local.rs`: Local testing implementation

#### Compaction (`src/compactor/`)
**Leveled Compaction (LSM-Tree)**:
- L0: Initial ingested chunks (uncompacted)
- L1-L3: Progressively compacted and merged levels
- Level tracking: Each chunk has a `level` field that increments during compaction
- Level progression: `max(source_levels) + 1`

**Compaction Flow**:
1. `get_l0_candidates()` finds L0 chunks (level==0)
2. `get_level_candidates()` finds chunks at specific level
3. Merge operation combines chunks
4. `complete_compaction()` atomically updates metadata with new level

**Key Files**:
- `src/compactor/mod.rs`: Main compaction orchestration
- `src/compactor/merge.rs`: Chunk merging logic

#### Sharding (`src/sharding/`)
**5-Phase Zero-Downtime Split**:
1. **Preparation**: Create new shard metadata
2. **Dual-Write**: Ingester writes to old + new shards simultaneously
3. **Backfill**: Copy historical data to new shards
4. **Cutover**: Atomic switch using generation-based CAS
5. **Cleanup**: Remove old shard data after grace period

**Generation-Based CAS**:
- Each shard has a `generation` number
- `update_shard_metadata()` requires expected generation
- Uses S3 ETags + generation for double verification
- Prevents stale routing during splits

**Dual-Write in Ingester**:
- `write_with_split_awareness()` checks split state before each write
- During DualWrite/Backfill phases: writes to old shard + both new shards
- `split_batch_by_key()` splits batches by timestamp boundary

**Key Files**:
- `src/sharding/splitter.rs`: Split orchestration, all 5 phases
- `src/sharding/router.rs`: Shard routing with generation awareness
- `src/sharding/monitor.rs`: Hot shard detection
- `src/ingester/mod.rs`: Dual-write integration (see `write_with_split_awareness()`)

#### Adaptive Indexing (`src/adaptive_index/`)
**Lifecycle**: Invisible → Visible → Deprecated
- Creates indexes based on query patterns
- Tracks filter column usage via SQL plan analysis
- Promotes to visible after 100 "would have helped" hits
- Deprecates after 30 days unused

**Integration**:
- `QueryEngine.execute_with_indexes()` extracts filter columns from logical plans
- Tracks usage for visible indexes, "would have helped" for invisible
- Optional feature via `QueryNode.with_adaptive_indexing()`

**Key Files**:
- `src/adaptive_index/lifecycle.rs`: Index lifecycle management
- `src/adaptive_index/stats_collector.rs`: Query pattern tracking
- `src/query/engine.rs`: `extract_filter_columns()` for SQL plan analysis

#### Query Engine (`src/query/`)
Built on **DataFusion** for vectorized SQL execution:
- `QueryEngine`: Wraps DataFusion SessionContext
- `TieredCache`: RAM → NVMe → S3 caching (moka + foyer)
- `CachedObjectStore`: Transparent caching layer for DataFusion
- `StreamingQueryExecutor`: Historical + live query support

**Query Flow**:
1. `extract_time_range()` analyzes SQL for time predicates
2. `metadata.get_chunks()` finds relevant Parquet files
3. `register_chunk()` adds files to DataFusion
4. `execute()` runs query with predicate pushdown
5. Results flow through cache hierarchy

**Key Files**:
- `src/query/engine.rs`: DataFusion integration
- `src/query/cache.rs`: 3-tier caching
- `src/query/streaming.rs`: Live query support

#### Ingester (`src/ingester/`)
**Write Path**:
1. Receive metrics via OTLP/Prometheus/Arrow Flight
2. Buffer in memory (WriteBuffer)
3. Flush triggers: time (5min) OR row count (1M) OR size (100MB)
4. Convert to Parquet with Zstd compression
5. Upload to S3
6. Register chunk metadata atomically
7. Broadcast to streaming query subscribers

**Hot Shard Monitoring**:
- `compute_shard_id()` calculates shard from tenant + metric + time
- `shard_monitor.record_write()` tracks metrics per shard
- `evaluate_shards()` detects hot shards for splitting

**Key Files**:
- `src/ingester/mod.rs`: Main ingestion logic, dual-write support
- `src/ingester/buffer.rs`: In-memory batching
- `src/ingester/parquet_writer.rs`: Parquet conversion

## Important Implementation Details

### Atomicity & Concurrency
- **All metadata operations are atomic** via S3 ETags + exponential backoff
- **Shard operations use generation-based CAS** to prevent split-brain
- **Compaction uses atomic completion** to avoid race conditions
- Retry pattern: 5 attempts with exponential backoff (100ms, 200ms, 400ms, 800ms, 1600ms)

### Error Handling
Critical errors in `src/error.rs`:
- `Error::Conflict`: ETag mismatch, triggers retry
- `Error::StaleGeneration`: Generation mismatch during CAS
- `Error::TooManyRetries`: Exhausted retry attempts (>5)
- `Error::ShardNotFound`: Shard doesn't exist

### S3 Integration
- Uses `object_store` crate for S3/GCS/Azure abstraction
- Atomic operations via `PutMode::Update` with ETag
- Metadata stored in JSON files: `metadata/chunks/metadata.json`, `metadata/shards/{shard_id}.json`
- Time index: `metadata/time-index.json` (BTreeMap for range queries)

### Testing Structure
Located in `tests/`:
- `atomic_metadata_tests.rs`: Concurrent metadata operations (6 tests)
- `level_tracking_tests.rs`: L0→L1→L2→L3 progression (5 tests)
- `shard_split_tests.rs`: 5-phase split testing (8 tests)
- `generation_cas_tests.rs`: CAS and retry logic (9 tests)
- `adaptive_indexing_tests.rs`: Index lifecycle (10 tests)

**Total: 38 integration tests**

## Configuration

### Environment Variables
- `S3_BUCKET`: S3 bucket name (default: `cardinalsin-data`)
- `S3_REGION`: S3 region (default: `us-east-1`)
- `S3_ENDPOINT`: S3 endpoint for MinIO or other S3-compatible storage
- `TENANT_ID`: Tenant identifier (default: `default`)
- `RUST_LOG`: Log level (`trace`, `debug`, `info`, `warn`, `error`)

### Ingester Config
- `flush_interval`: 300 seconds
- `flush_row_count`: 1,000,000 rows
- `flush_size_bytes`: 100MB
- `max_buffer_size_bytes`: 512MB

### Query Config
- `l1_cache_size`: 1GB RAM cache
- `l2_cache_size`: 10GB NVMe cache
- `l2_cache_dir`: Optional NVMe directory

## Data Flow

### Write Path
```
OTLP/Prometheus → Ingester → WriteBuffer → Parquet → S3
                              ↓
                         Shard Monitor → Split Detection
                              ↓
                         Metadata (atomic CAS)
                              ↓
                         Broadcast → Streaming Queries
```

### Query Path
```
SQL → QueryEngine → DataFusion
                    ↓
          extract_time_range()
                    ↓
          metadata.get_chunks()
                    ↓
          register_chunk() (DataFusion)
                    ↓
          execute() with predicate pushdown
                    ↓
          TieredCache (RAM → NVMe → S3)
```

### Compaction Path
```
Compactor → get_l0_candidates() (level==0)
         → merge chunks
         → complete_compaction() (atomic, level++)
         → update metadata with new level
```

### Shard Split Path
```
Monitor → Hot Shard Detection
       → ShardSplitter.execute_split_with_monitoring()
       → Phase 1: Preparation (create metadata)
       → Phase 2: Dual-Write (ingester detects, writes to 3 shards)
       → Phase 3: Backfill (copy historical data)
       → Phase 4: Cutover (atomic CAS with generation)
       → Phase 5: Cleanup (remove old shard)
```

## Recent Implementation Notes

**All gaps from gap analysis are now closed**:
- ✅ P0-A: S3 metadata atomicity using ETags
- ✅ P0-B: Compaction level tracking (L0→L1→L2→L3)
- ✅ P1-A: Complete 5-phase shard split orchestration
- ✅ P1-B: Generation number CAS enforcement
- ✅ P2: Adaptive indexing integration with query planner

See `IMPLEMENTATION_SUMMARY.md` and `FINAL_SUMMARY.md` for detailed implementation documentation.

## Migration Tool

`bin/backfill_levels.rs` - Migrate existing chunks to have proper level assignments:
```bash
cargo run --bin backfill-levels -- \
  --bucket cardinalsin-metadata \
  --prefix metadata/ \
  --region us-east-1 \
  --dry-run  # Optional: preview without changes
```

## API Endpoints

### Ingestion
- OTLP gRPC: `:4317` (OpenTelemetry metrics)
- OTLP HTTP: `:4318`
- Prometheus Remote Write: `:9090`
- Arrow Flight: `:8815`

### Query
- SQL HTTP: `:8080/api/v1/sql` (JSON/Arrow/CSV)
- Prometheus API: `:9090/api/v1/query` (PromQL compatibility)
- Arrow Flight SQL: `:8815`
- WebSocket: `:8080/api/v1/stream` (live streaming)

## Key Design Principles

1. **Stateless Compute**: All nodes are stateless; state lives in S3
2. **Atomic Metadata**: All mutations use CAS semantics (ETags + retries)
3. **Columnar Everything**: Labels as columns, not tag sets
4. **Zero Downtime**: Dual-write during splits, graceful degradation
5. **Observable**: Comprehensive tracing with `tracing` crate

## Performance Targets

- Write throughput: >1M samples/sec/ingester
- Query latency (warm): <100ms p99
- Query latency (cold): <500ms p99
- Cardinality: >1B unique series
- Compression: 10-20x
- Storage cost: ~$20/TB/month
