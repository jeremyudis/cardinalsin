# Benchmark Improvement Plan

## Executive Summary

This document audits CardinalSin's benchmark infrastructure against industry standards
(TSBS, ClickBench, VictoriaMetrics methodology) and provides a concrete plan for
building a minimal-viable-production benchmark suite. It also catalogs code quality
issues in the existing benchmark scaffolding.

---

## 1. Benchmark Infrastructure Code Quality Audit

### Bug 1: `percentile()` truncation error

**File**: `tests/benchmarks/mod.rs:127-137`

```rust
fn percentile(values: &[u64], p: f64) -> f64 {
    let index = (p * (sorted.len() - 1) as f64) as usize;
    sorted[index] as f64
}
```

**Test at line 338**: `assert_eq!(percentile(&[1,2,3,4,5], 0.99), 5.0)`

**Bug**: For p=0.99 with 5 elements, `index = floor(0.99 * 4) = floor(3.96) = 3`.
The function returns `sorted[3] = 4.0`, not `5.0`. The `as usize` cast truncates
instead of rounding. The same issue affects `percentile_f64()` at line 216-226.

**Fix**: Use linear interpolation (the standard "nearest-rank" or NIST method):

```rust
fn percentile(values: &[u64], p: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = p * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil().min((sorted.len() - 1) as f64) as usize;
    let frac = rank - lower as f64;
    sorted[lower] as f64 * (1.0 - frac) + sorted[upper] as f64 * frac
}
```

**Impact**: All percentile calculations across the entire benchmark suite use this
function (or its `f64` twin). Every p99 value reported is systematically biased low.

---

### Bug 2: `test_format_results_report` expects comma-formatted numbers

**File**: `tests/benchmarks/ingestion_benchmark.rs:535`

```rust
assert!(report.contains("1,000,000"));
```

**Bug**: Rust's `{}` format specifier produces `"1000000"`, not `"1,000,000"`.
The `format_results_report` function at line 438 uses `result.total_samples` with `{}`
formatting, which does not insert thousands separators.

**Fix**: Either remove the comma from the assertion, or add a number formatting
helper that inserts thousands separators.

---

### Bug 3: Wildcard match in metadata benchmark filter

**File**: `tests/benchmarks/metadata_benchmark.rs:347`

```rust
.filter(|r| matches!(r.operation, _op_type))
```

**Bug**: `_op_type` is a wildcard pattern (leading underscore = "ignore this binding"),
not a reference to the outer `op_type` variable. This filter matches ALL operations,
so the "Detailed Results by Operation Type" section duplicates every result under
every operation heading.

**Fix**: Use a guard clause or direct comparison instead of `matches!`:

```rust
.filter(|r| std::mem::discriminant(&r.operation) == std::mem::discriminant(&op_type))
```

---

### Bug 4: Duplicate percentile calculation logic

Percentile is calculated in at least 4 different ways across the benchmark suite:

| Location | Method |
|----------|--------|
| `mod.rs:127` | `percentile()` - truncating floor index |
| `mod.rs:216` | `percentile_f64()` - truncating floor index |
| `cardinality_stress.rs:370` | Inline `((len * p) as usize).min(len-1)` |
| `concurrent_load.rs:319` | Inline `((len * p) as usize).min(len-1)` |
| `metadata_benchmark.rs:269` | Inline `((len * p) as usize).min(len-1)` |

The inline versions also use `(sorted.len() as f64 * p) as usize` which is subtly
different from `(p * (len - 1))` -- for 100 elements at p99, the first gives index 99
(out of bounds without `.min()`), the second gives index 98. Neither uses interpolation.

**Fix**: Consolidate to a single correct `percentile()` function in `mod.rs` and use
it everywhere. Use linear interpolation per NIST recommendation.

---

### Issue 5: Non-deterministic data generators

**File**: `tests/benchmarks/data_generators.rs`

All value generation functions use `rand::random::<f64>()` without seeding. This means:
- Benchmark results are not reproducible between runs
- Cannot bisect performance regressions to specific commits
- Statistical comparisons between runs are confounded by data variation

**TSBS approach**: Deterministic generation using sequential seed progression. Each
run produces identical data, making results directly comparable.

**Fix**: Accept an optional `seed: u64` parameter in `DataGeneratorConfig` and use
`StdRng::seed_from_u64(seed)` for reproducible generation. Default to a fixed seed
for benchmark runs.

---

### Issue 6: Resource monitoring returns zeros on macOS

**File**: `tests/benchmarks/harness.rs` (resource monitoring) and
`tests/benchmarks/resource_usage.rs:313-343`

The `sample_rss_memory()` function only works on Linux (`/proc/self/status`).
On macOS (the likely dev platform), it returns 0 for all resource metrics.
The `sample_system_resources()` function always returns `cpu=0.0` and `heap=0`.

**Fix**: Use the `sysinfo` crate for cross-platform resource monitoring, or at minimum
document that resource benchmarks require Linux.

---

### Issue 7: `BaselineStorage` in `metrics.rs` is unused

**File**: `tests/benchmarks/metrics.rs`

`BaselineStorage` provides save/load/compare functionality for benchmark baselines
but is never called from any benchmark runner. The `MetricsCollector` is similarly
defined but unused in actual test code. This infrastructure was designed but never
integrated.

---

### Issue 8: Error rate denominator bug in concurrent benchmark

**File**: `tests/benchmarks/concurrent_load.rs:368-371`

```rust
let total_operations =
    write_stats.total_samples + write_stats.total_batches + read_stats.total_queries;
let total_errors = write_stats.total_errors + read_stats.total_errors;
```

The denominator double-counts write operations by adding both `total_samples` and
`total_batches`. A write of 1000 samples = 1 batch = 1 operation, but this counts it
as 1001. Error rate is artificially deflated.

---

## 2. TSBS Query Prioritization

TSBS defines standard query types for time-series databases. Here is the priority
order for CardinalSin, based on our columnar architecture advantages:

### Phase 1: Must-Have (validates core value proposition)

| Priority | TSBS Query | CardinalSin SQL Equivalent | Why First |
|----------|-----------|---------------------------|-----------|
| P0 | `single-groupby-1-1-1` | `SELECT host, AVG(value) FROM metrics WHERE timestamp BETWEEN ? AND ? AND host = ? GROUP BY host` | Validates predicate pushdown to Parquet |
| P0 | `single-groupby-1-1-12` | Same but 12-hour window | Validates multi-chunk scan + merge |
| P0 | `single-groupby-5-1-1` | 5 metrics, 1 host, 1 hour | Validates columnar advantage over tag-indexed |
| P0 | `high-cpu-all` | `SELECT * FROM metrics WHERE value > 90.0 AND metric_name = 'cpu' ORDER BY timestamp DESC LIMIT 10` | Value-based predicate on columnar storage |
| P0 | `lastpoint` | `SELECT DISTINCT ON (host) * FROM metrics ORDER BY host, timestamp DESC` | Critical for dashboards |

### Phase 2: Important (validates scalability)

| Priority | TSBS Query | Why |
|----------|-----------|-----|
| P1 | `double-groupby-1` | Multi-level aggregation |
| P1 | `double-groupby-5` | Multi-level with more metrics |
| P1 | `groupby-orderby-limit` | ORDER BY + LIMIT optimization |
| P1 | `cpu-max-all-1` through `cpu-max-all-8` | Scaling with host count |

### Phase 3: Nice-to-Have (validates edge cases)

| Priority | TSBS Query | Why |
|----------|-----------|-----|
| P2 | `high-cpu-and-target` | Multi-predicate optimization |
| P2 | `long-daily-sessions` | Very long time range scan |
| P2 | `last-loc` | Spatial data (if supported) |

### Implementation Notes

TSBS queries are parameterized by `num_hosts` (scale), `time_range`, and `num_metrics`.
CardinalSin should implement these as parameterized SQL templates, not hardcoded strings.
The current query benchmark uses static strings like
`"SELECT * FROM metrics WHERE host = 'host-0001' LIMIT 1000"` -- these need to be
replaced with parameterized queries that vary across runs.

---

## 3. Data Generator Adaptation for TSBS Compatibility

### Current State

The `DataGenerator` generates synthetic metrics with:
- `metric_0` through `metric_N` naming (not TSBS-compatible)
- Random values without realistic distribution
- Non-deterministic output (no seed)
- No schema compatibility with TSBS `cpu`, `diskio`, `net` schemas

### Required Changes

#### 3.1 TSBS-Compatible Schema

TSBS uses a fixed schema per "use case". The `cpu` use case (most common) has:

```
hostname, region, datacenter, rack, os, arch, team, service, service_version,
service_environment, usage_user, usage_system, usage_idle, usage_nice,
usage_iowait, usage_irq, usage_softirq, usage_steal, usage_guest,
usage_guest_nice
```

**Action**: Add a `TsbsCpuGenerator` that produces this schema. This is the
minimum needed for TSBS query compatibility.

#### 3.2 Deterministic Generation

```rust
pub struct TsbsDataGenerator {
    rng: StdRng,
    config: TsbsConfig,
    // Pre-computed host/region/datacenter assignments
    host_assignments: Vec<HostAssignment>,
}

impl TsbsDataGenerator {
    pub fn new(seed: u64, scale: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let host_assignments = Self::generate_assignments(&mut rng, scale);
        Self { rng, config: TsbsConfig::default(), host_assignments }
    }
}
```

#### 3.3 Realistic Value Distributions

Current generators use `rand::random::<f64>()` which produces uniform [0,1).
Real CPU metrics follow different distributions:

- `usage_user`: Normal(30, 15), clipped to [0, 100]
- `usage_system`: Normal(10, 5), clipped to [0, 100]
- `usage_idle`: 100 - sum(other usages)
- `usage_iowait`: Exponential(0.05), occasional spikes

**Action**: Implement per-column distribution functions. This is important for
testing predicate selectivity -- uniform distributions produce unrealistically
high cardinality for value-based predicates.

#### 3.4 Scale Factor

TSBS uses a `--scale` parameter (number of simulated hosts) to control dataset size:

| Scale | Hosts | Series | Description |
|-------|-------|--------|-------------|
| 100 | 100 | 1,000 | Smoke test |
| 1,000 | 1,000 | 10,000 | Small |
| 10,000 | 10,000 | 100,000 | Medium |
| 100,000 | 100,000 | 1,000,000 | Large |
| 1,000,000 | 1,000,000 | 10,000,000 | Stress |

**Action**: Support `--scale` in CardinalSin's generator. Map directly to the
existing `num_hosts` config, but fix the metric naming to match TSBS schema.

---

## 4. Minimum Test Duration for Meaningful Results

### Industry Standards

| System | Recommended Duration | Rationale |
|--------|---------------------|-----------|
| TSBS | "Until steady state" (~5 min load phase) | Captures buffer flushes, compaction |
| VictoriaMetrics | 72 hours soak, minimum 30 min for quick | Captures memory leaks, GC pauses |
| ClickBench | 3 runs minimum, report median | Reduces variance |
| InfluxDB | "At least 10 minutes" | Captures compaction effects |

### Current State

CardinalSin benchmarks use:
- Ingestion: `Duration::from_secs(60)` measurement period
- Query: `Duration::from_secs(30)` per iteration
- Resource: `Duration::from_secs(120)` total
- Concurrent: `Duration::from_secs(60)` per scenario

### Recommendations

#### 4.1 Minimum Durations by Test Type

| Test Type | Current | Recommended Minimum | Recommended Target | Rationale |
|-----------|---------|--------------------|--------------------|-----------|
| Ingestion throughput | 60s | 5 minutes | 15 minutes | Must capture at least 1 full buffer flush cycle (5 min default) |
| Query latency (cold) | 30s | 3 minutes | 5 minutes | Need enough iterations for stable p99 |
| Query latency (warm) | 30s | 3 minutes | 5 minutes | Cache must fully warm before measurement |
| Concurrent load | 60s | 10 minutes | 30 minutes | Need to observe contention patterns |
| Compaction | 300s | 15 minutes | 30 minutes | Must see at least L0->L1 complete |
| Resource/soak | 120s | 30 minutes | 4 hours | Must detect memory leaks |
| Cardinality stress | 60s | 10 minutes | 1 hour | Must see query degradation curve |

#### 4.2 Statistical Rigor

Current benchmarks run 1 iteration. Minimum for statistical validity:
- **3 runs minimum**, report median (ClickBench standard)
- **5 runs recommended**, report mean + stddev + min + max
- **Warmup**: At least 30 seconds before measurement begins (current code
  sometimes uses 0s warmup, e.g. `compaction_benchmark.rs:146`)
- **Cooldown**: 10 second gap between test scenarios to avoid carryover effects

#### 4.3 CI vs Full Benchmark

Not every PR needs a 4-hour soak test. Recommended tiers:

| Tier | Duration | When | What |
|------|----------|------|------|
| Smoke | 30s total | Every PR | Basic ingestion + query roundtrip |
| Quick | 5 min total | Nightly CI | Throughput targets with 3 iterations |
| Full | 30 min | Weekly CI | All scenarios, concurrency, resource |
| Soak | 4+ hours | Release candidate | Memory leaks, long-term stability |

---

## 5. Read + Write + Compaction Interaction Testing

### The Gap

This is the single largest methodology gap in the current benchmark suite.

Current benchmarks test dimensions in isolation:
- `benchmark_ingestion.rs`: Write-only
- `benchmark_query.rs`: Read-only (data pre-loaded)
- `benchmark_compaction.rs`: Compaction-only (data pre-loaded)
- `benchmark_concurrent.rs`: Mixed read/write, but **no compaction running**

No benchmark tests the critical interaction where all three run simultaneously,
which is the normal production operating mode.

### Why This Matters

VictoriaMetrics discovered that many TSDB benchmarks are misleading because they
don't test the interaction between reads, writes, and background compaction:

1. **Compaction I/O contention**: Compaction reads and writes large files,
   competing with query I/O and write flushes for disk and network bandwidth.
2. **Lock contention**: Metadata updates during compaction can delay write
   acknowledgements.
3. **Cache pollution**: Compaction reads large files that evict hot query data
   from cache.
4. **Memory pressure**: Compaction holds multiple chunks in memory during merge,
   reducing memory available for write buffers and query execution.

### Proposed Test: Mixed Workload Benchmark

```
Phase 1 (0-5 min): Write-only warmup
  - Ingest at target rate (1M samples/sec)
  - Accumulate L0 files

Phase 2 (5-15 min): Write + Read
  - Continue ingestion at target rate
  - Start query workload (TSBS queries, 10 concurrent)
  - Measure: write throughput degradation, query latency

Phase 3 (15-30 min): Write + Read + Compaction
  - Continue ingestion and queries
  - Trigger compaction (enough L0 files should have accumulated)
  - Measure: write throughput, query latency, compaction throughput
  - Key metric: query p99 during active compaction vs without

Phase 4 (30-35 min): Read-only cooldown
  - Stop writes
  - Continue queries while compaction finishes
  - Measure: query latency recovery time
```

**Key Metrics to Report**:

| Metric | Without Compaction | During Compaction | Acceptable Degradation |
|--------|-------------------|-------------------|----------------------|
| Write throughput | Baseline | Measured | <20% drop |
| Query p99 (warm) | <100ms | Measured | <200ms (2x) |
| Query p99 (cold) | <500ms | Measured | <1000ms (2x) |
| Compaction throughput | N/A | Measured | N/A (informational) |

### Proposed Test: Metadata Contention Under Load

```
Scenario: 10 concurrent ingesters registering chunks
          + 5 concurrent compaction completions
          + 20 concurrent query metadata lookups

Duration: 5 minutes

Measure:
- CAS retry rate (should be <5%)
- Metadata operation p99 latency
- Failed operations (should be 0 after retries)
```

This specifically tests the S3 ETag-based CAS mechanism under realistic
multi-component contention.

---

## 6. Implementation Roadmap

### Phase 1: Fix Infrastructure (Week 1)

1. Fix `percentile()` and `percentile_f64()` with linear interpolation
2. Fix `test_format_results_report` assertion
3. Fix `matches!(r.operation, _op_type)` wildcard bug
4. Consolidate all inline percentile calculations
5. Add `seed` parameter to `DataGeneratorConfig`
6. Fix error rate denominator in concurrent benchmark

### Phase 2: TSBS Compatibility (Week 2-3)

1. Implement `TsbsCpuGenerator` with standard schema
2. Implement P0 TSBS queries as parameterized SQL templates
3. Add scale factor support
4. Add deterministic value distributions

### Phase 3: Mixed Workload Testing (Week 3-4)

1. Implement mixed workload benchmark (write + read + compaction)
2. Implement metadata contention benchmark
3. Add compaction-aware query latency tracking
4. Integrate `BaselineStorage` for regression detection

### Phase 4: Duration & Statistical Rigor (Week 4)

1. Increase minimum durations per recommendations
2. Add multi-iteration support with statistical reporting
3. Implement CI tier system (smoke/quick/full/soak)
4. Add warmup periods where missing

---

## 7. Summary of Findings

### Confirmed Test Bugs

| # | File | Line | Description | Severity |
|---|------|------|-------------|----------|
| 1 | `mod.rs` | 338 | `percentile` test expects 5.0 but function returns 4.0 | High - all p99 values wrong |
| 2 | `ingestion_benchmark.rs` | 535 | Test expects "1,000,000" but `{}` produces "1000000" | Low - test-only |
| 3 | `metadata_benchmark.rs` | 347 | `_op_type` is wildcard, matches everything | Medium - report duplication |

### Additional Code Quality Issues

| # | Description | Severity |
|---|-------------|----------|
| 4 | Percentile logic duplicated 5 times with inconsistent implementations | Medium |
| 5 | Data generators are non-deterministic (no seed) | High - non-reproducible |
| 6 | Resource monitoring returns zeros on macOS | Medium |
| 7 | `BaselineStorage` and `MetricsCollector` defined but never used | Low |
| 8 | Error rate denominator double-counts in concurrent benchmark | Medium |

### Methodology Gaps vs Industry Standards

| Gap | TSBS | VictoriaMetrics | CardinalSin Current |
|-----|------|----------------|-------------------|
| Mixed workload | N/A | Core methodology | Missing entirely |
| Deterministic data | Yes | Yes | No |
| Multi-run statistics | 3+ runs, median | 72h soak | 1 run |
| Standard query set | 15+ parameterized | Custom | 5 static SQL strings |
| Compaction interaction | N/A | Tested | Isolated only |
| Baseline regression | Built-in | Built-in | Defined but not used |
