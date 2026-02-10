# TSDB Industry Benchmark & Testing Standards: Research Findings

## Executive Summary

This document surveys industry-standard benchmarks, academic research, production TSDB system designs, and testing standards, then compares them against CardinalSin's current implementation. The goal is to identify gaps and provide actionable recommendations for making CardinalSin production-ready.

**Key finding**: CardinalSin has solid architectural bones (DataFusion + Arrow + Parquet on S3, columnar label storage, leveled compaction, CAS-based metadata). However, critical production-readiness gaps exist around **durability guarantees (no WAL)**, **benchmark methodology (custom-only, no industry-standard comparisons)**, **crash recovery testing**, and **long-duration stress testing**. The benchmark suite measures the right things but lacks rigor in methodology and reproducibility.

---

## 1. Industry Benchmark Suites

### 1.1 TSBS (Time Series Benchmark Suite)

**Source**: [TimescaleDB TSBS](https://github.com/timescale/tsbs)

TSBS is the de facto industry standard, used by TimescaleDB, InfluxDB, QuestDB, VictoriaMetrics, ClickHouse, TDengine, and others.

**Methodology** (three-phase):
1. **Data generation**: Pre-generate datasets offline so benchmarking is not affected by on-the-fly data creation
2. **Data loading**: Measure insert/write performance with deterministic data
3. **Query execution**: Measure query performance against loaded data

**Use cases**: DevOps (host metrics) and IoT (trucks, diagnostics)

**Key queries tested**:
- Single groupby (1, 5, all hosts)
- Double groupby
- High CPU queries (1, 5, all hosts)
- Last point queries
- Groupby-orderby-limit

**Gap in CardinalSin**: Our benchmarks (`tests/benchmark_*.rs`) use custom data generators and ad-hoc queries. They are not comparable to any industry standard. We cannot publish competitive benchmark numbers because our methodology is incomparable.

**Recommendation (P1)**: Implement a TSBS-compatible data loader and query runner. This allows head-to-head comparison with every major TSDB. Given our DataFusion + SQL architecture, the query layer adapter is straightforward. Priority: implement DevOps use case first (10 hosts to 100K hosts scale).

### 1.2 ClickBench

**Source**: [ClickBench](https://benchmark.clickhouse.com/)

ClickBench tests analytical query performance using a 14.8 GB Parquet dataset of ~100M rows with 43 SQL queries. DataFusion recently became the fastest single-node engine on ClickBench.

**Gap in CardinalSin**: We use DataFusion but have not validated our configuration against ClickBench queries. DataFusion's ParquetExec supports projection pushdown, predicate pushdown (row group metadata, page index, bloom filters), limit pushdown, parallel reading, and late materialization. We should verify we enable all of these.

**Recommendation (P2)**: Run ClickBench adapted queries against our query engine to validate that our DataFusion configuration is not leaving performance on the table. Specifically check:
- `src/query/engine.rs:55-60` - Our session config enables `with_parquet_pruning(true)` and `with_collect_statistics(true)`, which is good
- Missing: Bloom filter support in Parquet writing (`src/ingester/parquet_writer.rs`)
- Missing: Page index support in Parquet writing

### 1.3 VictoriaMetrics Benchmark Methodology

**Source**: [VictoriaMetrics Benchmark](https://victoriametrics.com/blog/benchmark-100m/)

VictoriaMetrics emphasizes:
- **Simultaneous read+write**: Benchmarks must run reads and writes concurrently, measuring how they affect each other
- **Duration**: Tests must run for 72+ hours to catch compaction-related anomalies (they found query error spikes at 2-hour intervals during scheduled compactions)
- **Scale**: Testing up to 100M samples/sec with configurable cardinality
- **Production alerting rules**: Using real alerting rules as read load, not synthetic queries

**Gap in CardinalSin**: Our benchmark suite (`tests/benchmarks/`) runs reads and writes separately. Our longest test duration appears to be 60 seconds (`ingestion_benchmark.rs:99`). We do not test compaction impact on query latency.

**Recommendation (P0)**: Add mixed read+write benchmarks and extend test duration to at least 1 hour for stability testing, 72 hours for soak testing. Specifically:
- Create a "soak test" that runs ingestion + queries + compaction simultaneously
- Monitor query latency during compaction cycles
- Test at `tests/benchmarks/concurrent_load.rs` only measures write concurrency, not read+write interaction

---

## 2. Academic Research

### 2.1 Compaction Strategies

**Relevant papers**:

1. **"Time-Tiered Compaction"** (ScienceDirect, 2024) - Proposes compaction customized for time-series workloads. Leverages time-series characteristics to estimate query loads and select optimal SSTables for merging. Results: 30% reduction in range query latency with only 5% increase in point queries.

2. **"Rethinking The Compaction Policies in LSM-trees" (EcoTune, SIGMOD 2025)** - Dynamic programming algorithm to determine optimal compaction policy for a given workload. Key insight: compaction policy should be workload-adaptive, not static.

3. **"CaaS-LSM: Compaction-as-a-Service" (SIGMOD 2024)** - Proposes stateless compaction independent of the main database process. Directly relevant to CardinalSin's architecture where the compactor is a separate service.

**Gap in CardinalSin**: Our compaction strategy (`src/compactor/mod.rs`) uses static thresholds:
- L0 merge threshold: 15 files (line 58)
- Fixed target sizes per level (lines 59-61)
- No workload-adaptive tuning

Our compaction does not consider time-series-specific characteristics (e.g., queries predominantly hit recent data, old data is rarely queried).

**Recommendation (P2)**: Investigate time-tiered compaction for L0 (where most churn occurs). Consider making compaction thresholds adaptive based on query patterns from the adaptive indexing stats collector (`src/adaptive_index/stats_collector.rs`).

### 2.2 Gorilla Compression (Facebook, 2015)

**Source**: [Gorilla Paper Summary](https://blog.acolyer.org/2016/05/03/gorilla-a-fast-scalable-in-memory-time-series-database/)

Gorilla achieves 12x compression for time-series data using delta-of-delta timestamps and XOR-encoded values. 96% of timestamps compress to a single bit. ~51% of values compress to a single bit.

**Gap in CardinalSin**: We use Parquet with Zstd compression (`src/ingester/parquet_writer.rs`), which is good for columnar analytics but not time-series-optimized. Parquet's general-purpose compression ratios for time-series data are typically 10-20x (our claimed target), whereas Gorilla-style encoding can achieve higher ratios for in-memory buffers.

**Recommendation (P3)**: Not urgent. Parquet + Zstd is the right choice for our object-storage-first architecture. However, consider Gorilla-style encoding for the in-memory write buffer (`src/ingester/buffer.rs`) to reduce memory pressure before flush. This is what InfluxDB IOx does.

### 2.3 Storage System Correctness Testing

**Source**: [Testing Storage-System Correctness](https://arxiv.org/html/2602.02614)

Key findings:
- Many storage bugs manifest as **semantic violations** rather than immediate crashes
- Effects are **delayed, state-dependent, or distributed** across execution phases
- Critical areas: metadata inconsistencies, incorrect recovery outcomes, replica divergence
- Schedule control testing is essential for components where correctness depends on visibility and ordering semantics (log/apply paths, metadata updates, checkpoint/compaction interactions)

**Gap in CardinalSin**: We test metadata atomicity under concurrent access (`tests/atomic_metadata_tests.rs`), but we do not test:
- Recovery after crash during flush
- Recovery after crash during compaction
- Metadata consistency after partial S3 upload failure
- Correctness of compaction level tracking after process restart

**Recommendation (P0)**: Add fault-injection tests that simulate crashes at critical points. See Section 4 for details.

---

## 3. Production System Comparisons

### 3.1 InfluxDB IOx / InfluxDB 3.0

**Architecture**: Rust + Apache Arrow + DataFusion + Parquet on object storage. **Directly comparable to CardinalSin**.

**Key differences from CardinalSin**:

| Feature | InfluxDB IOx | CardinalSin |
|---------|-------------|-------------|
| WAL | Yes - persists writes before ack | **No** - writes buffered in memory only |
| Catalog | Centralized (Postgres/SQLite) | S3-based JSON with CAS |
| Query optimizer | Custom DataFusion extensions | Vanilla DataFusion |
| Compaction | Background with priority scheduling | Static threshold-based |
| Ingestion (independent benchmark) | ~320K rows/sec (single node) | Claims >1M samples/sec (untested independently) |

**Critical gap**: InfluxDB IOx uses a WAL to ensure writes are durable before acknowledging to the client. CardinalSin buffers writes in memory (`src/ingester/buffer.rs`) and only persists on flush. A crash between write acknowledgment and flush loses data.

**Recommendation (P0)**: Implement a write-ahead log. This is a non-negotiable requirement for any production database. The current architecture at `src/ingester/mod.rs:159-201` acknowledges writes after appending to the in-memory buffer, but before any durable persistence.

### 3.2 Prometheus / Thanos

**Architecture**: Per-node TSDB with sidecar upload to object storage (Thanos). Block-based storage with label-indexed series.

**Key learnings for CardinalSin**:

- **Compactor sharding**: Thanos shards compaction by label to parallelize. CardinalSin's compactor (`src/compactor/mod.rs`) is single-threaded with `max_concurrent_compactions: 4` (line 96). Label-based sharding could improve throughput.
- **Downsampling**: Thanos creates 5m and 1h downsampled blocks. CardinalSin has `downsample_after_days` config (line 44) but no implementation visible.
- **Block format**: Prometheus uses fixed 2-hour blocks. CardinalSin's flush is time/size/row-count based, producing variable-sized chunks. This makes compaction planning harder.

**Recommendation (P2)**: Implement time-aligned block boundaries. Instead of flushing at arbitrary times, align flush boundaries to fixed time windows (e.g., 1-hour blocks). This simplifies compaction planning and makes time-range queries more efficient.

### 3.3 VictoriaMetrics

**Architecture**: Monolithic, vertically-scalable, custom storage engine with merge-tree-like structure.

**Key learnings for CardinalSin**:

- **Ingestion rate**: VictoriaMetrics handles 100M+ samples/sec on a single node through careful memory management and batch processing
- **Deduplication**: Built-in deduplication for HA setups where multiple Prometheus instances scrape the same targets
- **72-hour soak tests**: Standard practice for detecting compaction-related anomalies

**Gap in CardinalSin**: No deduplication support. If multiple ingesters write the same data (e.g., during shard split dual-write at `src/ingester/mod.rs:205-241`), duplicates will exist in storage.

**Recommendation (P2)**: Add deduplication during compaction. When merging chunks in `src/compactor/merge.rs`, detect and eliminate duplicate (timestamp, metric_name, labels) tuples.

### 3.4 ClickHouse

**Architecture**: Column-oriented OLAP with MergeTree engine. Not specifically a TSDB but widely used for time-series analytics.

**Key learnings for CardinalSin**:

- **Vectorized execution**: ClickHouse processes data in column batches. CardinalSin gets this for free via DataFusion/Arrow.
- **Materialized views**: Pre-aggregated rollups for common queries. Reduces query latency for dashboards.
- **Projection pushdown**: ClickHouse aggressively prunes columns. DataFusion supports this but we need to verify our configuration enables it.

**Recommendation (P3)**: Consider materialized view support for common aggregation patterns (e.g., 1m/5m/1h rollups). This is a significant feature but lower priority than durability and correctness.

---

## 4. Industry-Standard Test Patterns

### 4.1 Tests That Mature TSDBs Run

Based on analysis of Prometheus, InfluxDB IOx, VictoriaMetrics, and Thanos test suites:

| Test Category | Description | CardinalSin Status |
|--------------|-------------|-------------------|
| **Unit tests** | Component-level correctness | Partial - 38 integration tests |
| **Crash recovery** | Restart after kill -9 during write/compaction | **Missing** |
| **WAL replay** | Correct state after WAL replay | **Missing (no WAL)** |
| **Concurrent access** | Multi-writer correctness | Present (`atomic_metadata_tests.rs`) |
| **Compaction correctness** | Data integrity after compaction | Present (`level_tracking_tests.rs`) |
| **Query correctness** | Correct results for various SQL patterns | **Minimal** |
| **Long-duration soak** | 24-72 hour stability tests | **Missing** |
| **Chaos/fault injection** | Network partitions, S3 failures | **Missing** |
| **Data integrity verification** | Checksums, row counts after operations | **Missing** |
| **Backpressure testing** | Behavior under overload | Partial (`concurrent_load.rs`) |
| **Schema evolution** | Adding/removing columns | **Missing** |
| **Retention enforcement** | Data deleted after retention period | **Missing** |
| **Multi-tenant isolation** | Tenant A cannot see tenant B's data | Present (`end_to_end_tests.rs`) |
| **Upgrade/migration** | Backward compatibility of on-disk format | **Missing** |

### 4.2 Jepsen-Style Testing

**Source**: [Jepsen](https://jepsen.io/)

Jepsen tests distributed systems under failure modes including faulty networks, unsynchronized clocks, and partial failures. Used by CockroachDB, ScyllaDB, YugabyteDB, and others.

**Gap in CardinalSin**: No fault injection testing. Our metadata layer uses S3 ETags for atomicity (`src/metadata/s3.rs`), but we never test what happens when:
- S3 returns intermittent 500 errors during CAS operations
- S3 PUT succeeds but GET returns stale data (despite strong consistency claims)
- Network partition between ingester and S3 during flush
- Clock skew between nodes affecting timestamp-based shard routing

**Recommendation (P1)**: Implement a chaos testing framework:
1. Create an `ObjectStore` wrapper that injects configurable failures (latency, errors, stale reads)
2. Test all critical paths (`atomic_register_chunk`, `complete_compaction`, `execute_split_with_monitoring`) under failure injection
3. Verify system recovers to a consistent state after failures

### 4.3 Deterministic Simulation Testing

**Source**: [WarpStream DST](https://www.warpstream.com/blog/deterministic-simulation-testing-for-our-entire-saas)

WarpStream (another S3-based streaming system) uses deterministic simulation testing to find bugs that only manifest under specific timing conditions. This is particularly relevant for systems with CAS-based coordination.

**Gap in CardinalSin**: Our CAS retry logic (`src/metadata/s3.rs`, MAX_CAS_RETRIES=5) is tested with in-memory ObjectStore which has zero latency and perfect consistency. Production S3 has variable latency and (historically) eventual consistency edge cases.

**Recommendation (P2)**: Consider implementing a simulation-based test harness for the metadata layer that can replay arbitrary orderings of concurrent operations.

---

## 5. Benchmark Gaps in Our Codebase

### 5.1 Current Benchmark Coverage

| Benchmark File | What It Tests | Methodology Quality |
|---------------|---------------|-------------------|
| `benchmark_ingestion.rs` | Write throughput (>1M/s target) | Good structure, but needs deterministic data |
| `benchmark_query.rs` | Query latency (cold/warm/hot) | Good variety of query patterns |
| `benchmark_cardinality.rs` | High cardinality (>1B series) | Good stress test, but cardinality claim untested at scale |
| `benchmark_compaction.rs` | Compaction level tracking | Basic, needs duration and data integrity verification |
| `benchmark_concurrent.rs` | Concurrent write load | Only tests writes, not read+write interaction |
| `benchmark_resource.rs` | Memory/CPU monitoring | Framework only, no actual assertions |
| `benchmark_metadata.rs` | S3 atomic operations under load | Basic, needs fault injection |
| `benches/write_throughput.rs` | Parquet write microbenchmark | Good, uses criterion properly |
| `benches/query_latency.rs` | Cache hit/miss microbenchmark | Good, uses criterion properly |

### 5.2 Specific Gaps

**1. No deterministic data generation**

Our `DataGenerator` (`tests/benchmarks/data_generators.rs:110`) uses `rand::random()` for values and `uuid::Uuid::new_v4()` for high-cardinality labels (line 93). This means benchmark runs are not reproducible. TSBS pre-generates deterministic data.

**Action**: Add a seeded RNG to `DataGenerator` for reproducible benchmarks.

**2. No baseline comparison / regression detection**

The benchmark harness (`tests/benchmarks/mod.rs`) outputs results to stdout but does not persist them. There is no mechanism to detect performance regressions across commits.

**Action**: Add JSON result persistence and a comparison tool. The `BenchmarkReporter::to_json()` method exists (line 257) but is never used in the test runners.

**3. No end-to-end data integrity verification**

None of the benchmarks verify that data written can be correctly read back. They measure throughput and latency but not correctness.

**Action**: Add a "write-then-read-verify" benchmark that:
1. Writes N samples with known values
2. Waits for flush
3. Queries and verifies exact match

**4. No mixed workload benchmarks**

`benchmark_concurrent.rs` tests concurrent writes only. No benchmark combines writes + queries + compaction.

**Action**: Create a mixed workload benchmark that runs all three simultaneously, measuring the impact of each on the others.

**5. Criterion benchmarks are too narrow**

`benches/write_throughput.rs` only benchmarks `ParquetWriter::write_batch()` and `WriteBuffer::append()`. Missing:
- Full write path (buffer -> Parquet -> S3 upload -> metadata registration)
- Query execution with DataFusion (not just cache hit/miss)
- Compaction merge performance at different data sizes

**Action**: Add criterion benchmarks for the complete write path and query execution.

**6. Resource monitoring is Linux-only and incomplete**

`BenchmarkHarness::sample_resources()` at `tests/benchmarks/harness.rs:119-143` returns zeros on macOS and only reads VmRSS on Linux. CPU monitoring returns 0.0.

**Action**: Use the `sysinfo` crate for cross-platform resource monitoring.

**7. No test for the 1B cardinality claim**

`benchmark_cardinality.rs` tests cardinality but the smoke test only creates 10,000 unique series (line 80). The full stress test has a 1B target but is informational-only (does not fail on miss, lines 43-44).

**Action**: Create a targeted cardinality scalability test that progressively increases series count and measures memory/query performance at each scale point.

---

## 6. Prioritized Recommendations

### P0 - Critical (must fix for production)

| # | Recommendation | Effort | Impact | Relevant Files |
|---|---------------|--------|--------|----------------|
| 1 | **Implement Write-Ahead Log (WAL)** | High | Critical - data loss prevention | `src/ingester/mod.rs`, `src/ingester/buffer.rs` |
| 2 | **Add crash recovery tests** | Medium | Verifies durability guarantees | New test file |
| 3 | **Add data integrity verification to benchmarks** | Low | Validates correctness under load | `tests/benchmarks/` |
| 4 | **Mixed read+write+compaction benchmarks** | Medium | Tests interaction effects | `tests/benchmarks/concurrent_load.rs` |

### P1 - High Priority (production hardening)

| # | Recommendation | Effort | Impact | Relevant Files |
|---|---------------|--------|--------|----------------|
| 5 | **TSBS-compatible benchmark adapter** | Medium | Industry-comparable numbers | New module in `tests/benchmarks/` |
| 6 | **Fault injection testing framework** | Medium | Finds edge-case bugs | New ObjectStore wrapper |
| 7 | **Long-duration soak tests (72h)** | Low | Catches compaction anomalies | `tests/benchmarks/` |
| 8 | **Benchmark result persistence + regression detection** | Low | Prevents performance regressions | `tests/benchmarks/mod.rs` |

### P2 - Important (performance and reliability)

| # | Recommendation | Effort | Impact | Relevant Files |
|---|---------------|--------|--------|----------------|
| 9 | **Deterministic data generation (seeded RNG)** | Low | Reproducible benchmarks | `tests/benchmarks/data_generators.rs` |
| 10 | **Time-aligned block boundaries** | Medium | Better compaction planning | `src/ingester/mod.rs:504-517` |
| 11 | **Deduplication during compaction** | Medium | Correctness for HA setups | `src/compactor/merge.rs` |
| 12 | **Bloom filter support in Parquet writes** | Low | Faster point queries | `src/ingester/parquet_writer.rs` |
| 13 | **Implement downsampling** | High | Storage cost reduction | `src/compactor/mod.rs` |
| 14 | **Cross-platform resource monitoring** | Low | macOS benchmark support | `tests/benchmarks/harness.rs` |

### P3 - Nice to Have (advanced features)

| # | Recommendation | Effort | Impact | Relevant Files |
|---|---------------|--------|--------|----------------|
| 15 | **Gorilla encoding for in-memory buffer** | High | Reduced memory pressure | `src/ingester/buffer.rs` |
| 16 | **Materialized views / pre-aggregation** | High | Dashboard query speedup | `src/query/` |
| 17 | **Workload-adaptive compaction** | High | Better resource utilization | `src/compactor/mod.rs` |
| 18 | **Deterministic simulation testing** | Very High | Deep correctness guarantees | New framework |

---

## 7. Detailed Implementation Notes

### 7.1 WAL Implementation (P0-1)

The WAL should be implemented between write acknowledgment and buffer append in `src/ingester/mod.rs`:

```
Current flow:  write() -> buffer.append() -> ack -> ... -> flush_batches() -> S3
Proposed flow: write() -> WAL.append() -> buffer.append() -> ack -> ... -> flush_batches() -> S3 -> WAL.truncate()
```

On startup:
1. Read WAL entries not yet flushed
2. Replay into buffer
3. Resume normal operation

WAL format options:
- Simple: append-only file with length-prefixed Arrow IPC batches
- Advanced: segmented WAL with checkpoints (like Prometheus)

Recommended: Start with simple append-only WAL using Arrow IPC format, since we already use Arrow RecordBatch everywhere.

### 7.2 Fault Injection ObjectStore (P1-6)

```rust
// Conceptual design
pub struct FaultInjectionObjectStore {
    inner: Arc<dyn ObjectStore>,
    config: FaultConfig,
}

pub struct FaultConfig {
    /// Probability of a GET returning an error (0.0-1.0)
    pub get_error_rate: f64,
    /// Probability of a PUT returning an error
    pub put_error_rate: f64,
    /// Additional latency to add to operations
    pub added_latency: Duration,
    /// Probability of returning stale data on GET
    pub stale_read_rate: f64,
}
```

This would wrap the InMemory ObjectStore and be used in integration tests to verify retry logic and error handling.

### 7.3 TSBS Adapter (P1-5)

Minimum viable TSBS integration:
1. Implement TSBS DevOps data generator in Rust (or use the Go tooling to generate data files)
2. Create a loader that converts TSBS format to our Prometheus Remote Write or Arrow Flight ingest
3. Implement the 8 standard TSBS queries as SQL against our query engine
4. Compare results against published TSBS numbers for other databases

---

## 8. Sources

- [TSBS - Time Series Benchmark Suite](https://github.com/timescale/tsbs)
- [TSBS Methodology](https://medium.com/timescale/time-series-database-benchmarks-timescaledb-influxdb-cassandra-mongodb-bc702b72927e)
- [ClickBench](https://benchmark.clickhouse.com/)
- [VictoriaMetrics Benchmark (100M samples/sec)](https://victoriametrics.com/blog/benchmark-100m/)
- [VictoriaMetrics High Cardinality Benchmarks](https://valyala.medium.com/high-cardinality-tsdb-benchmarks-victoriametrics-vs-timescaledb-vs-influxdb-13e6ee64dd6b)
- [InfluxDB IOx Architecture (Rust + Arrow + DataFusion)](https://www.infoq.com/articles/timeseries-db-rust/)
- [QuestDB InfluxDB 3 Benchmarks](https://questdb.com/blog/influxdb3-core-benchmarks/)
- [DataFusion: Fastest Parquet Engine (ClickBench)](https://datafusion.apache.org/blog/2024/11/18/datafusion-fastest-single-node-parquet-clickbench/)
- [DataFusion SIGMOD 2024 Paper](https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf)
- [Thanos Compaction](https://thanos.io/tip/components/compact.md/)
- [Gorilla Paper Summary](https://blog.acolyer.org/2016/05/03/gorilla-a-fast-scalable-in-memory-time-series-database/)
- [Time-Tiered Compaction (ScienceDirect)](https://www.sciencedirect.com/science/article/abs/pii/S147403462300352X)
- [EcoTune: Rethinking Compaction Policies (SIGMOD 2025)](https://dl.acm.org/doi/10.1145/3725344)
- [CaaS-LSM: Compaction-as-a-Service (SIGMOD 2024)](https://www.cs.purdue.edu/homes/csjgwang/pubs/SIGMOD24_CaaSLSM.pdf)
- [Jepsen: Distributed Systems Verification](https://jepsen.io/)
- [WarpStream: Deterministic Simulation Testing](https://www.warpstream.com/blog/deterministic-simulation-testing-for-our-entire-saas)
- [Testing Storage-System Correctness (2025)](https://arxiv.org/html/2602.02614)
- [S3 Strong Consistency](https://www.allthingsdistributed.com/2021/04/s3-strong-consistency.html)
- [Prometheus TSDB WAL and Checkpoint](https://ganeshvernekar.com/blog/prometheus-tsdb-wal-and-checkpoint/)
