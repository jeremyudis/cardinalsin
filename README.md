# CardinalSin

> *"High cardinality is the cardinal sin of time-series databases. We fixed it."*

A Rust-based, serverless time-series database built on object storage (S3/GCS/Azure), designed to solve the high-cardinality problem that plagues modern observability systems. Inspired by [turbopuffer](https://turbopuffer.com), [WarpStream](https://warpstream.com), [ClickHouse](https://clickhouse.com), [Datadog Husky](https://www.datadoghq.com/blog/engineering/introducing-husky/), and [InfluxDB IOx](https://www.influxdata.com/blog/announcing-influxdb-iox/).

## Key Innovations

- **Columnar storage** eliminates series-set explosion (no per-tag-combination indexing)
- **Zero-disk architecture** with stateless compute nodes
- **3-tier caching** (RAM → NVMe → S3) for cost-efficient performance
- **Adaptive indexing** automatically promotes hot dimensions based on query patterns
- **Dynamic sharding** with zero-downtime shard splitting and hot shard rebalancing
- **Hybrid compaction** (size-tiered + leveled LSM) for optimal read/write tradeoffs
- Built on battle-tested primitives: **DataFusion**, **Arrow**, **Parquet**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Control Plane                                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  Metadata Store │  │  Schema Registry│  │  Coordinator    │              │
│  │  (FoundationDB) │  │                 │  │  (Assignment)   │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│    Ingester Pool    │  │   Query Node Pool   │  │   Compactor Pool    │
│  (Stateless Rust)   │  │  (Stateless Rust)   │  │  (Stateless Rust)   │
│                     │  │                     │  │                     │
│  - Batch writes     │  │  - DataFusion       │  │  - Merge small files│
│  - Buffer in memory │  │  - Arrow Flight     │  │  - Downsample old   │
│  - Flush to S3      │  │  - NVMe cache       │  │  - Garbage collect  │
└─────────────────────┘  └─────────────────────┘  └─────────────────────┘
          │                       │                        │
          ▼                       ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Object Storage (S3/GCS/Azure)                        │
│                                                                              │
│   /tenant-123/                                                               │
│     /data/                                                                   │
│       /year=2024/month=01/day=15/                                           │
│         chunk_00001.parquet   (time-sorted, columnar)                       │
│     /indexes/                                                                │
│       bloom_filters.bin       (per-chunk bloom filters)                     │
│     /wal/                                                                   │
│       pending_00001.arrow     (unflushed batches)                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Component | Responsibility | Scaling |
|-----------|---------------|---------|
| **Ingester** | Buffer writes, batch to Parquet, flush to S3 | Horizontal by write volume |
| **Query Node** | Execute queries via DataFusion, manage cache | Horizontal by query load |
| **Compactor** | Merge files, downsample, enforce retention | 1-3 per tenant typically |
| **Metadata Store** | Track files, schemas, assignments | FoundationDB / S3 (MVP) |

---

## The Problem & Our Solution

### Traditional TSDBs (Prometheus, InfluxDB v1)

```
series_id -> {metric="cpu", host="server1", pod="abc123"}
             Creates inverted index entry for EVERY unique combination
             1M hosts × 100 pods = 100M index entries (EXPLOSION)
```

### CardinalSin Approach (ClickHouse/IOx-style)

```
| timestamp | metric | host    | pod     | value |
|-----------|--------|---------|---------|-------|
| 12345     | cpu    | server1 | abc123  | 0.85  |

Each label is a column. High-cardinality columns (pod) are NOT indexed.
Queries use columnar scans with predicate pushdown. No cardinality explosion.
```

| Cardinality | Strategy | Example |
|-------------|----------|---------|
| **Low** (<1K unique) | Dictionary encoding + Bloom filter | `region`, `env`, `service` |
| **Medium** (1K-100K) | Dictionary encoding, scan with SIMD | `host`, `instance` |
| **High** (>100K) | No indexing, columnar scan only | `trace_id`, `request_id`, `pod_id` |

---

## Quick Start

### Using Docker Compose

```bash
cd deploy
docker-compose up -d
```

This starts:
- **MinIO** (S3-compatible storage) on ports 9000/9001
- **Ingester** on port 8081 (HTTP) and 4317 (OTLP gRPC + Arrow Flight ingest)
- **Query node** on port 8080 (HTTP/SQL/Prometheus API) and 8815 (Arrow Flight SQL)
- **Compactor** (background service)
- **Grafana** on port 3000

Grafana dashboards are provisioned automatically from:

- `deploy/grafana/provisioning/datasources/cardinalsin.yaml`
- `deploy/grafana/provisioning/dashboards/ingest-health.json`
- `deploy/grafana/provisioning/dashboards/query-performance.json`
- `deploy/grafana/provisioning/dashboards/compactor-metadata-health.json`
- `deploy/grafana/provisioning/dashboards/run-overview.json`

### Building from Source

```bash
cargo build --release

# Run each service
./target/release/cardinalsin-ingester
./target/release/cardinalsin-query
./target/release/cardinalsin-compactor
```

### Running Tests

```bash
cargo test
```

### Telemetry Query Pack

```bash
scripts/telemetry/run_query_pack.sh --mode all --run-id "run-20260222T190000Z"
```

Guide: `docs/telemetry/query-pack.md`

### Real-Time Observability Dogfooding

```bash
RUN_ID="run-$(date -u +%Y%m%dT%H%M%SZ)"
scripts/telemetry/run_mixed_workload.sh --duration 30m --run-id "$RUN_ID"
scripts/telemetry/run_query_pack.sh --mode all --run-id "$RUN_ID" --out-dir "benchmarks/results/$RUN_ID/query-pack"
```

Operator runbook: `docs/observability-dogfooding-runbook.md`
Script reference: `docs/telemetry/mixed-workload-runner.md`

## Telemetry Contract

Telemetry metric names, labels, semantic mappings, and cardinality rules are defined in:

- `docs/telemetry/metric-catalog.md`

When adding or changing instrumentation, keep dashboards and query-pack assets aligned with this contract.

---

## API Interfaces

### Ingestion Protocols

| Protocol | Port | Transport | Format | Use Case |
|----------|------|-----------|--------|----------|
| **OTLP gRPC** | 4317 | HTTP/2 gRPC | Protobuf | OpenTelemetry agents, SDKs |
| **Prometheus Remote Write** | 8080 (8081 in `docker-compose`) | HTTP POST | Protobuf+Snappy | Prometheus, Grafana Agent |
| **Arrow Flight DoPut (ingest)** | 4317 | gRPC | Arrow IPC | Bulk data loading |

### Query Protocols

| Protocol | Port | Transport | Format | Use Case |
|----------|------|-----------|--------|----------|
| **SQL HTTP** | 8080 | HTTP REST | JSON/Arrow/CSV | Web apps, ad-hoc queries |
| **Prometheus API** | 8080 | HTTP REST | JSON | Grafana, existing dashboards |
| **Arrow Flight SQL** | 8815 | gRPC | Arrow IPC | Analytics tools, DuckDB |
| **WebSocket/SSE** | 8080 | WS/HTTP | JSON | Live dashboards, alerting |

Flight protocol notes:
- Ingest-side Flight (`:4317`) is write-focused: `DoPut` is fully supported for bulk ingestion.
- Non-ingest Flight calls on the ingest service (for example `DoGet`/`DoExchange`) return explicit capability errors (`FAILED_PRECONDITION`) instead of generic unimplemented stubs.
- Query-side Flight SQL (`:8815`) supports statement queries, prepared statement lifecycle actions, SQL info metadata, catalogs/schemas/tables/table types, and XDBC type info.

### Usage Examples

**SQL Query:**
```bash
curl -X POST http://localhost:8080/api/v1/sql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT date_trunc('\''minute'\'', timestamp) as minute, service, avg(value) as avg_latency, percentile_cont(0.99) WITHIN GROUP (ORDER BY value) as p99 FROM metrics WHERE metric_name = '\''http_request_duration_seconds'\'' AND timestamp > now() - interval '\''1 hour'\'' GROUP BY 1, 2 ORDER BY minute DESC",
    "format": "json"
  }'
```

**Prometheus Query:**
```bash
curl "http://localhost:8080/api/v1/query?query=rate(http_requests_total[5m])"
```

**Streaming Query (WebSocket):**
```javascript
const ws = new WebSocket('ws://localhost:8080/api/v1/stream');
ws.send(JSON.stringify({
  query: "SELECT * FROM metrics WHERE service = 'api'",
  live: true
}));
ws.onmessage = (event) => console.log(JSON.parse(event.data));
```

---

## Write Path

All ingestion protocols converge to a unified path: receive metrics → buffer in memory → batch to Parquet → flush to S3 → register metadata atomically.

```
Client (OTLP/Prometheus/Flight)
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│                       Ingester                            │
│  Write Buffer (in-memory) → Arrow Batch → Parquet Writer │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│                          S3                               │
│  1. PUT pending/batch_{id}.parquet                        │
│  2. Update metadata (atomic via ETags / FoundationDB)     │
│  3. ACK to client                                         │
│  4. Broadcast to streaming query subscribers              │
└──────────────────────────────────────────────────────────┘
```

**Flush triggers** (whichever comes first):
- **Time**: 5 minutes (background timer)
- **Row count**: 1,000,000 rows
- **Size**: 100MB

**Parquet encoding optimizations:**
- ZSTD level 3 compression (good ratio, fast)
- Dictionary encoding for low-cardinality columns
- `DELTA_BINARY_PACKED` for timestamps (delta-of-delta)
- Bloom filters for equality predicates (1% FPP)
- Row group statistics for predicate pushdown

---

## Read Path

```
Client (SQL/PromQL)
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│                      Query Node                           │
│  Parser → Planner (pruning) → DataFusion (vectorized)    │
│                                                           │
│  ┌────────────── Cache Hierarchy ──────────────────────┐ │
│  │  L1: RAM (moka, ~10ms)                              │ │
│  │  L2: NVMe (foyer, ~50ms)                            │ │
│  │  L3: S3 (source of truth, ~200ms)                   │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

**Query optimization flow:**
1. **Time index pruning** — metadata query returns only chunks in time range (eliminates 99%+ of data)
2. **Row group statistics** — skip row groups via min/max on filtered columns (~90% reduction)
3. **Bloom filters** — confirm values might exist in row group (fast negative lookup)
4. **Columnar scan** — SIMD-accelerated comparison on remaining data

---

## Adaptive Indexing

The adaptive indexing system automatically detects per-tenant query patterns and promotes frequently-filtered dimensions to dedicated indexed columns.

**Lifecycle:** `Invisible → Visible → Deprecated`

1. **Stats Collection** — track column usage in WHERE clauses across a sliding 24-48 hour window
2. **Recommendation Scoring** — cost-benefit analysis balancing query benefit vs. storage cost + write overhead
3. **Index Creation** — create indexes as **invisible** (not used by query planner yet)
4. **Validation** — after 48 hours, promote to **visible** if the index would have helped enough queries
5. **Retirement** — deprecate indexes unused for 30+ days; remove after 7-day grace period

Each tenant has isolated statistics and configurable quotas (max indexes, max storage, minimum query threshold).

---

## Dynamic Sharding

**Shard key:** `tenant_id + metric_hash + time_bucket`

### Hot Shard Detection

Monitors write QPS, bytes/sec, CPU utilization, and p99 latency per shard. When any threshold is exceeded for a full detection window (60s), a split is triggered.

### Zero-Downtime 5-Phase Split

1. **Preparation** — create new shard metadata (not yet active)
2. **Dual-Write** — route new writes to both old and new shards simultaneously
3. **Backfill** — copy historical data to new shards in parallel
4. **Atomic Cutover** — single metadata update using generation-based CAS (compare-and-swap)
5. **Cleanup** — remove old shard data after 60-second grace period for in-flight queries

**Generation-based CAS** ensures no stale routing during splits: each shard has a generation number, and all metadata updates require the expected generation to match.

---

## Compaction

Hybrid compaction strategy inspired by Datadog Husky — lazy compaction reduces S3 API costs by 10x.

| Level | Strategy | Target Size | Trigger |
|-------|----------|-------------|---------|
| **L0** | Size-tiered | 250-500 MB | 15+ files accumulated (~75 min of data) |
| **L1** | Leveled | 2 GB | After L0 compaction |
| **L2** | Leveled | 10 GB | After L1 compaction |
| **L3** | Leveled | 50 GB | After L2 compaction |

**Why 5-minute write granularity:**

| Granularity | Files/Day | Query Slowdown | Latency to Query |
|-------------|-----------|----------------|------------------|
| 1-minute | 1,440 | 34x slower | ~1 min |
| **5-minute** | **288** | **Baseline** | **~5 min** |
| Hourly | 24 | Best | ~60 min |

---

## Streaming Queries

CardinalSin supports real-time streaming for live dashboards and alerting by merging historical query results with live ingester broadcasts.

```
┌──────────────────────────────────────────────────────────┐
│              Streaming Query Executor                     │
│  ┌──────────────┐    ┌──────────────────────┐            │
│  │ Historical   │ +  │ Live Tail            │            │
│  │ (S3/cache)   │    │ (ingester broadcast) │            │
│  └──────────────┘    └──────────────────────┘            │
└──────────────────────────────────────────────────────────┘
```

- `tokio::sync::broadcast` for ingester → query node fan-out
- Server-Sent Events (SSE) or WebSocket for client delivery
- Topic-based filtering reduces bandwidth by ~90%
- Query deduplication groups identical subscriptions

---

## Technology Stack

| Primitive | Rationale |
|-----------|-----------|
| **DataFusion** | Production-proven in InfluxDB IOx, Coralogix. Vectorized, SIMD-optimized, extensible. |
| **Arrow/Parquet** | Industry standard. Excellent compression, predicate pushdown, ecosystem tooling. |
| **object_store** | From Arrow ecosystem. Handles retries, multipart uploads, all major clouds. |
| **moka** | High-performance Rust concurrent cache. |
| **foyer** | Hybrid RAM/disk cache for NVMe tier. |
| **tokio/axum/tonic** | Async runtime, HTTP framework, gRPC framework. |

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | `cardinalsin-data` | S3 bucket name |
| `S3_REGION` | `us-east-1` | S3 region |
| `S3_ENDPOINT` | — | S3 endpoint (for MinIO or other S3-compatible storage) |
| `TENANT_ID` | `default` | Tenant identifier |
| `RUST_LOG` | `info` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |

### Ingester Options

```bash
cardinalsin-ingester \
  --http-port 8080 \
  --grpc-port 4317 \
  --flush-interval-secs 300 \
  --bucket my-metrics
```

### Query Node Options

```bash
cardinalsin-query \
  --http-port 8080 \
  --grpc-port 8815 \
  --l1-cache-mb 4096 \
  --l2-cache-mb 102400 \
  --cache-dir /mnt/nvme
```

---

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Write throughput | >1M samples/sec/ingester | Batching amortizes S3 costs |
| Query latency (warm) | <100ms p99 | Data in NVMe cache |
| Query latency (cold) | <500ms p99 | First access from S3 |
| Cardinality | >1B unique series | Columnar = no index explosion |
| Compression ratio | 10-20x | Delta + dictionary + ZSTD |
| Storage cost | ~$20/TB/month | S3 standard pricing |

---

## Project Structure

```
cardinalsin/
├── src/
│   ├── lib.rs                     # Library root
│   ├── config.rs                  # Configuration management
│   ├── error.rs                   # Error types
│   ├── api/
│   │   ├── ingest/
│   │   │   ├── otlp.rs            # OTLP gRPC receiver + OTLP conversion helpers
│   │   │   ├── prometheus.rs      # Prometheus Remote Write
│   │   │   └── flight_ingest.rs   # Arrow Flight bulk ingest
│   │   └── query/
│   │       ├── sql_http.rs        # SQL REST API
│   │       ├── prometheus_api.rs  # Prometheus HTTP API (PromQL)
│   │       ├── flight_sql.rs      # Arrow Flight SQL
│   │       └── streaming.rs       # WebSocket/SSE live queries
│   ├── ingester/                  # Write path (buffer → Parquet → S3)
│   ├── query/                     # Read path (DataFusion + caching)
│   ├── compactor/                 # Leveled compaction (L0-L3)
│   ├── adaptive_index/            # Automatic index management
│   ├── sharding/                  # Dynamic shard splitting
│   ├── cluster/                   # Cluster coordination
│   ├── metadata/                  # S3/FoundationDB metadata layer
│   └── schema/                    # Arrow schema definitions
├── tests/                         # 38 integration tests
├── benches/                       # Write throughput & query latency benchmarks
└── deploy/
    ├── docker-compose.yml         # Full stack (MinIO + services + Grafana)
    ├── Dockerfile                 # Multi-stage build
    ├── kubernetes/                # K8s manifests
    └── terraform/                 # Infrastructure as code
```

---

## License

Apache 2.0

## Acknowledgments

Inspired by:
- [turbopuffer](https://turbopuffer.com) — zero-disk vector database architecture
- [WarpStream](https://warpstream.com) — Kafka-compatible streaming on object storage
- [InfluxDB IOx](https://www.influxdata.com/blog/announcing-influxdb-iox/) — DataFusion-based TSDB
- [Datadog Husky](https://www.datadoghq.com/blog/engineering/introducing-husky/) — lazy compaction strategy
- [ClickHouse](https://clickhouse.com) — columnar analytics and sparse indexing

## References

- [Apache DataFusion](https://datafusion.apache.org/)
- [Apache Parquet](https://parquet.apache.org/)
- [Apache Arrow](https://arrow.apache.org/)
- [ClickHouse Sparse Primary Indexes](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes)
