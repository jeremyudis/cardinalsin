#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DURATION="${DURATION:-30m}"
INGEST_ENDPOINT="${INGEST_ENDPOINT:-http://localhost:8081}"
QUERY_ENDPOINT="${QUERY_ENDPOINT:-http://localhost:8080}"
RUN_ID="${CARDINALSIN_TELEMETRY_RUN_ID:-run-$(date -u +%Y%m%dT%H%M%SZ)}"
START_COMPOSE="${START_COMPOSE:-1}"

MIN_INGEST_RPS="${KPI_MIN_INGEST_RPS:-1000}"
MAX_QUERY_ERROR_RPS="${KPI_MAX_QUERY_ERROR_RPS:-5}"
MAX_L0_PENDING="${KPI_MAX_L0_PENDING:-500}"

usage() {
  cat <<EOF
Usage: scripts/telemetry/run_mixed_workload.sh [--duration 30m] [--run-id id] [--no-compose]

Environment overrides:
  DURATION=30m
  CARDINALSIN_TELEMETRY_RUN_ID=run-...
  INGEST_ENDPOINT=http://localhost:8081
  QUERY_ENDPOINT=http://localhost:8080
  START_COMPOSE=1
  KPI_MIN_INGEST_RPS=1000
  KPI_MAX_QUERY_ERROR_RPS=5
  KPI_MAX_L0_PENDING=500
EOF
}

duration_to_seconds() {
  local input="$1"
  if [[ "$input" =~ ^([0-9]+)s$ ]]; then
    echo "${BASH_REMATCH[1]}"
  elif [[ "$input" =~ ^([0-9]+)m$ ]]; then
    echo "$(( BASH_REMATCH[1] * 60 ))"
  elif [[ "$input" =~ ^([0-9]+)h$ ]]; then
    echo "$(( BASH_REMATCH[1] * 3600 ))"
  else
    echo "Unsupported duration format: $input (use Ns/Nm/Nh)" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --no-compose)
      START_COMPOSE=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

DURATION_SECS="$(duration_to_seconds "$DURATION")"
RESULT_DIR="$ROOT_DIR/benchmarks/results/$RUN_ID"
mkdir -p "$RESULT_DIR"

export CARDINALSIN_TELEMETRY_RUN_ID="$RUN_ID"

echo "run_id=$RUN_ID"
echo "duration=$DURATION"
echo "results=$RESULT_DIR"

if [[ "$START_COMPOSE" == "1" ]]; then
  (
    cd "$ROOT_DIR/deploy"
    docker compose up -d
  )
fi

query_worker() {
  local end_ts
  end_ts=$(( $(date +%s) + DURATION_SECS ))
  while [[ $(date +%s) -lt $end_ts ]]; do
    curl -sS -X POST "$QUERY_ENDPOINT/api/v1/sql" \
      -H "Content-Type: application/json" \
      -d '{"query":"SELECT metric_name, avg(value) FROM metrics WHERE timestamp > now() - interval '\''5 minutes'\'' GROUP BY metric_name LIMIT 25","format":"json"}' \
      >/dev/null || true
    sleep 1
  done
}

cargo run --release --bin test-data-generator -- \
  --endpoint "$INGEST_ENDPOINT" \
  --metrics 24 \
  --hosts 24 \
  --regions 3 \
  --samples-per-second 2000 \
  --duration "$DURATION" \
  --batch-size 400 \
  >"$RESULT_DIR/ingest.log" 2>&1 &
INGEST_PID=$!

query_worker >"$RESULT_DIR/query.log" 2>&1 &
QUERY_PID=$!

INGEST_RC=0
QUERY_RC=0
wait "$INGEST_PID" || INGEST_RC=$?
wait "$QUERY_PID" || QUERY_RC=$?

prom_query() {
  local expr="$1"
  local raw
  raw="$(curl -fsS --get "$QUERY_ENDPOINT/api/v1/query" --data-urlencode "query=$expr" || true)"
  if [[ -z "$raw" ]]; then
    echo "0"
    return
  fi
  jq -r '.data.result[0].value[1] // "0"' <<<"$raw" 2>/dev/null || echo "0"
}

INGEST_RPS="$(prom_query "sum(rate(cardinalsin_ingester_write_rows_total{run_id=\"$RUN_ID\"}[5m]))")"
QUERY_ERROR_RPS="$(prom_query "sum(rate(cardinalsin_query_requests_total{result=\"error\",run_id=\"$RUN_ID\"}[5m]))")"
L0_PENDING="$(prom_query "max(cardinalsin_compaction_l0_pending_files{run_id=\"$RUN_ID\"})")"

ingest_pass=$(awk -v x="$INGEST_RPS" -v min="$MIN_INGEST_RPS" 'BEGIN{if ((x+0) >= (min+0)) print "true"; else print "false"}')
query_err_pass=$(awk -v x="$QUERY_ERROR_RPS" -v max="$MAX_QUERY_ERROR_RPS" 'BEGIN{if ((x+0) <= (max+0)) print "true"; else print "false"}')
l0_pass=$(awk -v x="$L0_PENDING" -v max="$MAX_L0_PENDING" 'BEGIN{if ((x+0) <= (max+0)) print "true"; else print "false"}')

if [[ "$ingest_pass" == "true" && "$query_err_pass" == "true" && "$l0_pass" == "true" && "$INGEST_RC" == "0" && "$QUERY_RC" == "0" ]]; then
  OVERALL_PASS=true
else
  OVERALL_PASS=false
fi

jq -n \
  --arg run_id "$RUN_ID" \
  --arg duration "$DURATION" \
  --arg ingest_rps "$INGEST_RPS" \
  --arg query_error_rps "$QUERY_ERROR_RPS" \
  --arg l0_pending "$L0_PENDING" \
  --argjson ingest_process_ok "$( [[ "$INGEST_RC" == "0" ]] && echo true || echo false )" \
  --argjson query_process_ok "$( [[ "$QUERY_RC" == "0" ]] && echo true || echo false )" \
  --argjson ingest_pass "$ingest_pass" \
  --argjson query_error_pass "$query_err_pass" \
  --argjson l0_pending_pass "$l0_pass" \
  --argjson overall_pass "$OVERALL_PASS" \
  '{
    run_id: $run_id,
    duration: $duration,
    ingest_rps_last_5m: ($ingest_rps | tonumber),
    query_error_rps_last_5m: ($query_error_rps | tonumber),
    l0_pending_max: ($l0_pending | tonumber),
    kpi: {
      ingest_process_ok: $ingest_process_ok,
      query_process_ok: $query_process_ok,
      ingest_rps_pass: $ingest_pass,
      query_error_rps_pass: $query_error_pass,
      l0_pending_pass: $l0_pending_pass,
      overall_pass: $overall_pass
    }
  }' > "$RESULT_DIR/summary.json"

cat "$RESULT_DIR/summary.json"
echo "Wrote run artifacts to $RESULT_DIR"
