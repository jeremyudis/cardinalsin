# Operator Runbook: Real-Time Observability Dogfooding

This runbook covers the full operator loop for CardinalSin self-observability:

1. start the local stack
2. run a tagged workload
3. monitor live dashboards and query packs
4. run post-run diagnostics
5. troubleshoot common failures

## Prerequisites

- Docker with Compose plugin (`docker compose`)
- Rust toolchain (`cargo`)
- `curl` and `jq`
- open ports `3000`, `8080`, `8081`, `4317`, `4318`, `8815`, `9000`, `9001`, `9090`, `13133`

## Signal Map (Step -> Dashboard -> Queries)

| Step | Primary dashboard(s) | Query pack signals |
|---|---|---|
| Startup validation | `cardinalsin-run-overview` | `ingest_throughput_rps`, `query_error_rps` |
| Active workload monitoring | `cardinalsin-run-overview`, `cardinalsin-ingest-health`, `cardinalsin-query-performance`, `cardinalsin-compactor-metadata` | all `live.*` pack queries |
| Post-run diagnosis | `cardinalsin-query-performance`, `cardinalsin-compactor-metadata` | all `postrun.*` pack queries |

Dashboard URLs (local):

- http://localhost:3000/d/cardinalsin-run-overview
- http://localhost:3000/d/cardinalsin-ingest-health
- http://localhost:3000/d/cardinalsin-query-performance
- http://localhost:3000/d/cardinalsin-compactor-metadata

Set dashboard variables for focused diagnosis:

- `run_id=<your run id>` (required)
- `service=All` unless narrowing to one component
- `tenant=default` for local compose defaults

## 1) Startup the Stack

```bash
cd deploy
docker compose up -d --build
docker compose ps
```

Sanity checks:

```bash
curl -fsS http://localhost:8080/health
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8080/ready
curl -fsS http://localhost:9090/-/healthy
curl -fsS http://localhost:13133/
```

Use run overview dashboard to confirm baseline health before load:

- http://localhost:3000/d/cardinalsin-run-overview

## 2) Execute a Tagged Benchmark Run

Create a run id and execute the mixed workload runner:

```bash
RUN_ID="run-$(date -u +%Y%m%dT%H%M%SZ)"
scripts/telemetry/run_mixed_workload.sh --duration 30m --run-id "$RUN_ID"
```

The runner:

- exports `CARDINALSIN_TELEMETRY_RUN_ID`
- drives ingest load via `test-data-generator`
- runs concurrent SQL pressure
- writes results to `benchmarks/results/$RUN_ID/`

Primary artifacts:

- `benchmarks/results/$RUN_ID/ingest.log`
- `benchmarks/results/$RUN_ID/query.log`
- `benchmarks/results/$RUN_ID/summary.json`

## 3) Live Dashboard Usage During the Run

Open dashboards and set `run_id=$RUN_ID`:

- run overview: ingest/query/compaction/error pressure
- ingest health: throughput, write latency p95, buffer fullness, WAL errors
- query performance: query latency p99, error rate, cache hit ratio, scan throughput
- compactor and metadata health: cycle p95, L0 backlog, CAS conflicts, lease errors

Quick CLI probes that match live dashboard panels:

```bash
scripts/telemetry/run_query_pack.sh --mode live --run-id "$RUN_ID" \
  --out-dir "benchmarks/results/$RUN_ID/query-pack-live"
```

Live query aliases come from:

- `scripts/telemetry/query-pack/live.promql`
- `scripts/telemetry/query-pack/live.sql`

## 4) Post-Run Forensics and Budget Checks

Run the post-run query pack:

```bash
scripts/telemetry/run_query_pack.sh --mode postrun --run-id "$RUN_ID" \
  --out-dir "benchmarks/results/$RUN_ID/query-pack-postrun"
```

Compare candidate overhead against baseline:

```bash
scripts/telemetry/check_overhead_budget.sh \
  --baseline docs/telemetry/overhead-baseline.json \
  --candidate "benchmarks/results/$RUN_ID/summary.json"
```

Inspect summary fields used for pass/fail:

```bash
jq '.' "benchmarks/results/$RUN_ID/summary.json"
```

Run dual-publish parity checks (CardinalSin vs Prometheus baseline):

```bash
scripts/telemetry/compare_dual_publish.sh --mode all --run-id "$RUN_ID" \
  --out-dir "benchmarks/results/$RUN_ID/parity"
```

## 5) Query Pack Usage Reference

Run both packs in one command:

```bash
scripts/telemetry/run_query_pack.sh --mode all --run-id "$RUN_ID" \
  --out-dir "benchmarks/results/$RUN_ID/query-pack-all"
```

Definitions:

- `scripts/telemetry/query-pack/live.promql`
- `scripts/telemetry/query-pack/live.sql`
- `scripts/telemetry/query-pack/postrun.promql`
- `scripts/telemetry/query-pack/postrun.sql`

## Troubleshooting

### A) Collector Ingestion Failures (OTLP/Remote Write Path)

Symptoms:

- low or zero ingest throughput in run overview
- increasing query `ingest_error_rps` from live query pack
- errors in `benchmarks/results/$RUN_ID/ingest.log`

Checks:

```bash
docker compose -f deploy/docker-compose.yml ps ingester
docker compose -f deploy/docker-compose.yml logs --tail=200 ingester
curl -fsS http://localhost:8081/health
```

If the ingester is down or unhealthy, restart just that service:

```bash
docker compose -f deploy/docker-compose.yml up -d --build ingester
```

### B) Exporter Failures (Service -> OTLP Export Endpoint)

Applies when running services with telemetry export enabled (`CARDINALSIN_TELEMETRY_ENABLED=true`).

Common configuration failures:

- `CARDINALSIN_TELEMETRY_ENABLED=true` without `OTEL_EXPORTER_OTLP_ENDPOINT`
- unsupported `OTEL_EXPORTER_OTLP_PROTOCOL` (must be `grpc` or `http/protobuf`)

Checks:

```bash
env | rg '^(CARDINALSIN_TELEMETRY|OTEL_)'
docker compose -f deploy/docker-compose.yml logs --tail=200 query ingester compactor | rg -i 'telemetry|otlp|export'
```

If using a local collector, verify endpoint reachability and protocol match.

### C) Dashboard Failures (Grafana or Datasource)

Symptoms:

- dashboards missing from Grafana
- empty panels despite active load
- datasource errors in Grafana UI

Checks:

```bash
docker compose -f deploy/docker-compose.yml ps grafana query
docker compose -f deploy/docker-compose.yml logs --tail=200 grafana
curl -fsS http://localhost:3000/api/health
curl -fsS --get http://localhost:8080/api/v1/query \
  --data-urlencode 'query=sum(rate(cardinalsin_query_requests_total[5m]))'
curl -fsS --get http://localhost:9090/api/v1/query \
  --data-urlencode 'query=sum(rate(cardinalsin_query_requests_total[5m]))'
```

If Grafana is running but dashboards are missing, verify provisioning mounts and files:

- `deploy/grafana/provisioning/datasources/cardinalsin.yaml`
- `deploy/grafana/provisioning/datasources/prometheus-baseline.yaml`
- `deploy/grafana/provisioning/dashboards/dashboards.yaml`
- `deploy/grafana/provisioning/dashboards/*.json`

### D) Fanout Pipeline Failures (Collector -> CardinalSin + Prometheus)

Symptoms:

- CardinalSin panels have data but Prometheus overlay is empty (or vice-versa)
- parity script fails with `missing_series`

Checks:

```bash
docker compose -f deploy/docker-compose.yml ps otel-collector prometheus ingester
docker compose -f deploy/docker-compose.yml logs --tail=200 otel-collector | rg -i 'error|retry|export'
curl -fsS http://localhost:13133/
```

## Related Docs

- `docs/telemetry/mixed-workload-runner.md`
- `docs/telemetry/query-pack.md`
- `docs/telemetry/metric-catalog.md`
