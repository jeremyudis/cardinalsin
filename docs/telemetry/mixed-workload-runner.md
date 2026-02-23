# Mixed Workload Runner

Run a reproducible benchmark-style workload with a telemetry `run_id`:

```bash
scripts/telemetry/run_mixed_workload.sh --duration 30m
```

What the runner does:

- emits/exports `CARDINALSIN_TELEMETRY_RUN_ID`
- starts local compose stack (unless `--no-compose`)
- runs ingest load via `test-data-generator`
- runs concurrent SQL query pressure against query node
- records KPI pass/fail summary to `benchmarks/results/<run_id>/summary.json`

Outputs:

- `benchmarks/results/<run_id>/ingest.log`
- `benchmarks/results/<run_id>/query.log`
- `benchmarks/results/<run_id>/summary.json`

Tunable KPI thresholds:

- `KPI_MIN_INGEST_RPS` (default: `1000`)
- `KPI_MAX_QUERY_ERROR_RPS` (default: `5`)
- `KPI_MAX_L0_PENDING` (default: `500`)
