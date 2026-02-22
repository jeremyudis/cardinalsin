#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PACK_DIR="$ROOT_DIR/scripts/telemetry/query-pack"
QUERY_ENDPOINT="${QUERY_ENDPOINT:-http://localhost:8080}"
RUN_ID="${CARDINALSIN_TELEMETRY_RUN_ID:-.*}"
MODE="${MODE:-live}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/benchmarks/results/query-pack-$(date -u +%Y%m%dT%H%M%SZ)}"

usage() {
  cat <<EOF
Usage: scripts/telemetry/run_query_pack.sh [--mode live|postrun|all] [--run-id run-...] [--out-dir path]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"
      shift 2
      ;;
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
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

mkdir -p "$OUT_DIR/promql" "$OUT_DIR/sql"

echo "query-pack mode=$MODE run_id=$RUN_ID"
echo "out_dir=$OUT_DIR"

run_prom_file() {
  local file="$1"
  while IFS='|' read -r name expr; do
    [[ -z "${name:-}" ]] && continue
    local rendered
    rendered="${expr//__RUN_ID__/$RUN_ID}"
    curl -fsS --get "$QUERY_ENDPOINT/api/v1/query" \
      --data-urlencode "query=$rendered" \
      > "$OUT_DIR/promql/$name.json" || true
  done < "$file"
}

run_sql_file() {
  local file="$1"
  while IFS='|' read -r name query; do
    [[ -z "${name:-}" ]] && continue
    jq -n --arg q "$query" '{query: $q, format: "json"}' \
      | curl -fsS -X POST "$QUERY_ENDPOINT/api/v1/sql" \
          -H 'Content-Type: application/json' \
          -d @- \
      > "$OUT_DIR/sql/$name.json" || true
  done < "$file"
}

case "$MODE" in
  live)
    run_prom_file "$PACK_DIR/live.promql"
    run_sql_file "$PACK_DIR/live.sql"
    ;;
  postrun)
    run_prom_file "$PACK_DIR/postrun.promql"
    run_sql_file "$PACK_DIR/postrun.sql"
    ;;
  all)
    run_prom_file "$PACK_DIR/live.promql"
    run_prom_file "$PACK_DIR/postrun.promql"
    run_sql_file "$PACK_DIR/live.sql"
    run_sql_file "$PACK_DIR/postrun.sql"
    ;;
  *)
    echo "Invalid mode: $MODE" >&2
    exit 1
    ;;
esac

echo "Wrote query pack outputs to $OUT_DIR"
