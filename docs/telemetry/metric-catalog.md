# CardinalSin Telemetry Metric Catalog

Status: Draft contract for Epic #65 (Self-Observability Hardening)\
Owner: `cardinalsin-c5e.7`\
Last Updated: 2026-02-22

## Contract Scope

This file defines the telemetry contract used by:

- runtime instrumentation in `src/`
- dashboard panels in `deploy/grafana/provisioning/dashboards/`
- telemetry regression checks in CI
- query pack docs/scripts for dogfooding runs

Contract rules:

- Prefer OpenTelemetry semantic conventions when a standard metric exists.
- Use custom metrics only for CardinalSin-specific internals.
- Custom metric names use the `cardinalsin_` prefix and base units.
- Every metric listed here must remain backward-compatible across patch releases.

## Resource Attributes

The following resource attributes are required on telemetry emitted by binaries:

| Attribute | Required | Notes |
| --- | --- | --- |
| `service.name` | yes | `cardinalsin-ingester`, `cardinalsin-query`, `cardinalsin-compactor`, etc. |
| `service.instance.id` | yes | per-process unique id |
| `deployment.environment` | yes | `dev`, `staging`, `prod`, `benchmark` |
| `cardinalsin.run_id` | yes (benchmark runs) | workload correlation key |

## Label Governance

Allowed labels (bounded cardinality):

- `service`
- `component`
- `operation`
- `result`
- `level`
- `phase`
- `reason`
- `protocol`

Conditionally allowed labels:

- `tenant` only for bounded-tenant deployments (cardinality budget <= 100)

Prohibited labels (unbounded/high churn):

- `chunk_path`
- `job_id`
- `lease_id`
- `shard_id`
- `trace_id`
- raw SQL query text
- full error messages

## Metric Catalog

### A. OTel Semantic Metrics (Preferred)

| Metric | Type / Unit | Required Labels | Source | Semantic Mapping | Notes |
| --- | --- | --- | --- | --- | --- |
| `http.server.request.duration` | histogram / s | `http.request.method`, `http.route`, `http.response.status_code`, `service` | `src/api/mod.rs` | OTel HTTP server metric | Top-level HTTP latency |
| `rpc.server.duration` | histogram / s | `rpc.system`, `rpc.service`, `rpc.method`, `rpc.grpc.status_code`, `service` | `src/api/grpc.rs` | OTel RPC metric | gRPC request duration |
| `rpc.server.requests_per_rpc` | histogram / 1 | `rpc.system`, `rpc.service`, `rpc.method`, `service` | `src/api/grpc.rs` | OTel RPC metric | request size/load proxy |
| `process.cpu.time` | counter / s | `service` | runtime exporter | OTel process metric | used in overhead checks |
| `process.memory.usage` | gauge / By | `service` | runtime exporter | OTel process metric | used in overhead checks |

### B. Compactor + Metadata + Sharding (Custom)

| Metric | Type / Unit | Required Labels | Source | Semantic Mapping | Custom Justification |
| --- | --- | --- | --- | --- | --- |
| `cardinalsin_compaction_cycle_duration_seconds` | histogram / s | `service`, `result` | `src/compactor/mod.rs` | none | no standard metric for compaction cycle runtime |
| `cardinalsin_compaction_active` | gauge / 1 | `service` | `src/compactor/mod.rs` | up/down style gauge | active compactions are CardinalSin-specific |
| `cardinalsin_compaction_l0_pending_files` | gauge / 1 | `service` | `src/compactor/mod.rs` | queue depth style gauge | backlog visibility for ingest pressure |
| `cardinalsin_compaction_backpressure` | gauge / 1 | `service` | `src/compactor/mod.rs` | state gauge | backpressure state is domain-specific |
| `cardinalsin_compaction_jobs_total` | counter / 1 | `service`, `level`, `result` | `src/compactor/mod.rs` | operation counter | job success/failure by level |
| `cardinalsin_compaction_gc_deletions_total` | counter / 1 | `service`, `result` | `src/compactor/mod.rs` | operation counter | GC delete outcomes |
| `cardinalsin_compaction_gc_duration_seconds` | histogram / s | `service`, `result` | `src/compactor/mod.rs` | none | no standard "compaction GC pass" metric |
| `cardinalsin_metadata_cas_attempts_total` | counter / 1 | `service`, `operation`, `result` | `src/metadata/s3.rs` | operation counter | CAS is core correctness path |
| `cardinalsin_metadata_cas_retries_total` | counter / 1 | `service`, `operation` | `src/metadata/s3.rs` | retry counter | contention signal |
| `cardinalsin_metadata_cas_backoff_seconds` | histogram / s | `service`, `operation` | `src/metadata/s3.rs` | backoff duration | conflict severity signal |
| `cardinalsin_metadata_cas_duration_seconds` | histogram / s | `service`, `operation`, `result` | `src/metadata/s3.rs` | operation duration | tail latency in metadata path |
| `cardinalsin_metadata_lease_operations_total` | counter / 1 | `service`, `operation`, `result` | `src/metadata/s3.rs` | operation counter | lease lifecycle outcomes |
| `cardinalsin_metadata_lease_active` | gauge / 1 | `service` | `src/metadata/s3.rs` | queue/state gauge | active lease pressure |
| `cardinalsin_split_actions_total` | counter / 1 | `service`, `action`, `result` | `src/compactor/mod.rs` | operation counter | split orchestration outcomes |
| `cardinalsin_split_phase_transitions_total` | counter / 1 | `service`, `phase` | `src/metadata/s3.rs` | state transition counter | split progress signal |
| `cardinalsin_shard_hot_evaluations_total` | counter / 1 | `service`, `result` | `src/sharding/monitor.rs` | decision counter | hot-shard decision visibility |
| `cardinalsin_shard_write_rate` | gauge / 1/s | `service` | `src/sharding/monitor.rs` | rate gauge | bounded aggregate throughput signal |
| `cardinalsin_shard_latency_p99_seconds` | gauge / s | `service` | `src/sharding/monitor.rs` | latency gauge | bounded aggregate hot-shard signal |

## Dashboard Coverage Requirements

Every dashboard panel must reference only metrics in this file.

Required panel groups:

- ingest health
- query performance
- compactor and metadata CAS health
- benchmark run overview with `cardinalsin.run_id` filtering

## Change Management

Any change to this catalog requires:

1. update to this file
2. dashboard/query-pack update in the same PR
3. CI contract fixture update (`cardinalsin-c5e.11`)

Allowed compatibility changes:

- add a new metric
- add a new label with a default value and bounded cardinality

Breaking changes (require major revision):

- remove metric
- rename metric
- change metric type or unit
- remove required label
