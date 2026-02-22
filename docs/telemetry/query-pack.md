# Telemetry Query Pack

Run live diagnostics:

```bash
scripts/telemetry/run_query_pack.sh --mode live --run-id "run-20260222T190000Z"
```

Run post-run forensics:

```bash
scripts/telemetry/run_query_pack.sh --mode postrun --run-id "run-20260222T190000Z"
```

Run both packs:

```bash
scripts/telemetry/run_query_pack.sh --mode all --run-id "run-20260222T190000Z"
```

Query definitions live in:

- `scripts/telemetry/query-pack/live.promql`
- `scripts/telemetry/query-pack/postrun.promql`
- `scripts/telemetry/query-pack/live.sql`
- `scripts/telemetry/query-pack/postrun.sql`

Output artifacts are written to:

- `benchmarks/results/query-pack-<timestamp>/promql/*.json`
- `benchmarks/results/query-pack-<timestamp>/sql/*.json`
