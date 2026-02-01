# CardinalSin

> *"High cardinality is the cardinal sin of time-series databases. We fixed it."*

A high-cardinality time-series database built on object storage (S3), designed to solve the cardinality explosion problem that plagues modern observability systems.

## Key Features

- **Columnar Storage**: Labels are stored as columns, not tag sets, eliminating the series-set explosion problem
- **Zero-Disk Architecture**: Stateless compute nodes with data stored in S3/GCS/Azure
- **3-Tier Caching**: RAM → NVMe → S3 for cost-efficient performance
- **Adaptive Indexing**: Automatically promotes hot dimensions based on query patterns
- **Battle-Tested Primitives**: Built on DataFusion, Arrow, and Parquet

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
└─────────────────────┘  └─────────────────────┘  └─────────────────────┘
          │                       │                        │
          ▼                       ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Object Storage (S3/GCS/Azure)                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Using Docker Compose

```bash
cd deploy
docker-compose up -d
```

This starts:
- MinIO (S3-compatible storage) on ports 9000/9001
- Ingester on port 8081 (HTTP) and 4317 (OTLP gRPC)
- Query node on port 8080 (HTTP/SQL/Prometheus API)
- Compactor (background service)
- Grafana on port 3000

### Building from Source

```bash
cargo build --release

# Run ingester
./target/release/cardinalsin-ingester

# Run query node
./target/release/cardinalsin-query

# Run compactor
./target/release/cardinalsin-compactor
```

## API Interfaces

### Ingestion

| Protocol | Port | Description |
|----------|------|-------------|
| OTLP gRPC | 4317 | OpenTelemetry metrics |
| OTLP HTTP | 4318 | OpenTelemetry HTTP |
| Prometheus Remote Write | 9090 | Prometheus/Grafana Agent |
| Arrow Flight | 8815 | Bulk data loading |

### Query

| Protocol | Port | Description |
|----------|------|-------------|
| SQL HTTP | 8080 | REST API (JSON/Arrow/CSV) |
| Prometheus API | 9090 | PromQL compatibility |
| Arrow Flight SQL | 8815 | High-performance analytics |
| WebSocket/SSE | 8080 | Live streaming queries |

## Usage Examples

### SQL Query

```bash
curl -X POST http://localhost:8080/api/v1/sql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM metrics WHERE timestamp > now() - interval '\''1 hour'\''",
    "format": "json"
  }'
```

### Prometheus Query

```bash
curl "http://localhost:8080/api/v1/query?query=cpu_usage"
```

### Streaming Query (WebSocket)

```javascript
const ws = new WebSocket('ws://localhost:8080/api/v1/stream');
ws.send(JSON.stringify({ query: "SELECT * FROM metrics", live: true }));
ws.onmessage = (event) => console.log(JSON.parse(event.data));
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | `cardinalsin-data` | S3 bucket name |
| `S3_REGION` | `us-east-1` | S3 region |
| `S3_ENDPOINT` | - | S3 endpoint (for MinIO) |
| `TENANT_ID` | `default` | Tenant identifier |
| `RUST_LOG` | `info` | Log level |

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

## Why CardinalSin?

### The Problem

Traditional time-series databases (Prometheus, InfluxDB v1) create an inverted index entry for every unique label combination:

```
series_id -> {metric="cpu", host="server1", pod="abc123"}
             Creates inverted index entry for EVERY unique combination
             1M hosts × 100 pods = 100M index entries (EXPLOSION)
```

### Our Solution

CardinalSin stores labels as columns, not tag sets:

```
| timestamp | metric | host    | pod     | value |
|-----------|--------|---------|---------|-------|
| 12345     | cpu    | server1 | abc123  | 0.85  |

Each label is a column. High-cardinality columns (pod) are NOT indexed.
Queries use columnar scans with predicate pushdown.
No cardinality explosion.
```

## Performance

| Metric | Target |
|--------|--------|
| Write throughput | >1M samples/sec/ingester |
| Query latency (warm) | <100ms p99 |
| Query latency (cold) | <500ms p99 |
| Cardinality | >1B unique series |
| Compression ratio | 10-20x |
| Storage cost | ~$20/TB/month |

## Technology Stack

- **DataFusion**: Vectorized SQL query engine
- **Arrow/Parquet**: Columnar storage format
- **object_store**: S3/GCS/Azure interface
- **moka/foyer**: High-performance caching
- **tokio/axum/tonic**: Async runtime and networking

## License

Apache 2.0

## Acknowledgments

Inspired by:
- [turbopuffer](https://turbopuffer.com)
- [WarpStream](https://warpstream.com)
- [InfluxDB IOx](https://www.influxdata.com/blog/announcing-influxdb-iox/)
- [Datadog Husky](https://www.datadoghq.com/blog/engineering/introducing-husky/)
- [ClickHouse](https://clickhouse.com)
