#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PACK_DIR="$ROOT_DIR/scripts/telemetry/query-pack"

MODE="all"
RUN_ID="${CARDINALSIN_TELEMETRY_RUN_ID:-.*}"
CARDINALSIN_ENDPOINT="${CARDINALSIN_ENDPOINT:-http://localhost:8080}"
PROMETHEUS_ENDPOINT="${PROMETHEUS_ENDPOINT:-http://localhost:9090}"
DEFAULT_REL_THRESHOLD="${DEFAULT_REL_THRESHOLD:-0.02}"
LATENCY_REL_THRESHOLD="${LATENCY_REL_THRESHOLD:-0.05}"
OUT_DIR=""

usage() {
  cat <<EOF
Usage: scripts/telemetry/compare_dual_publish.sh [--mode live|postrun|all] [--run-id run-...] [--out-dir path]

Environment overrides:
  CARDINALSIN_ENDPOINT=http://localhost:8080
  PROMETHEUS_ENDPOINT=http://localhost:9090
  DEFAULT_REL_THRESHOLD=0.02
  LATENCY_REL_THRESHOLD=0.05
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

case "$MODE" in
  live|postrun|all) ;;
  *)
    echo "Invalid mode '$MODE'. Use live, postrun, or all." >&2
    exit 1
    ;;
esac

safe_run_id="$(echo "$RUN_ID" | tr -c 'A-Za-z0-9._-' '_')"
if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="$ROOT_DIR/benchmarks/results/$safe_run_id/parity"
fi
mkdir -p "$OUT_DIR"

query_api() {
  local endpoint="$1"
  local expr="$2"
  curl -fsS --get "$endpoint/api/v1/query" --data-urlencode "query=$expr"
}

normalize_results() {
  jq -c '
    if .status != "success" then
      []
    else
      (.data.result // [])
      | map({
          key: ((.metric // {}) | tojson),
          value: ((.value[1] // "0") | tonumber)
        })
      | sort_by(.key)
    end
  '
}

threshold_for_alias() {
  local alias="$1"
  if [[ "$alias" =~ (latency|duration|p95|p99) ]]; then
    echo "$LATENCY_REL_THRESHOLD"
  else
    echo "$DEFAULT_REL_THRESHOLD"
  fi
}

tmp_results="$(mktemp)"
trap 'rm -f "$tmp_results"' EXIT

pack_files=()
if [[ "$MODE" == "live" || "$MODE" == "all" ]]; then
  pack_files+=("$PACK_DIR/live.promql")
fi
if [[ "$MODE" == "postrun" || "$MODE" == "all" ]]; then
  pack_files+=("$PACK_DIR/postrun.promql")
fi

for pack in "${pack_files[@]}"; do
  while IFS='|' read -r alias expr; do
    [[ -z "${alias// }" ]] && continue
    [[ "$alias" =~ ^# ]] && continue
    [[ -z "${expr// }" ]] && continue

    query="${expr//__RUN_ID__/$RUN_ID}"
    threshold="$(threshold_for_alias "$alias")"

    cardinalsin_raw="$(
      query_api "$CARDINALSIN_ENDPOINT" "$query" \
        || echo '{"status":"error","data":{"result":[]}}'
    )"
    prometheus_raw="$(
      query_api "$PROMETHEUS_ENDPOINT" "$query" \
        || echo '{"status":"error","data":{"result":[]}}'
    )"

    cardinalsin_norm="$(echo "$cardinalsin_raw" | normalize_results)"
    prometheus_norm="$(echo "$prometheus_raw" | normalize_results)"

    comparison="$(
      jq -n \
        --arg alias "$alias" \
        --arg query "$query" \
        --arg source_pack "$(basename "$pack")" \
        --argjson threshold "$threshold" \
        --argjson cardinalsin "$cardinalsin_norm" \
        --argjson prometheus "$prometheus_norm" '
          def to_map(xs):
            reduce xs[] as $item ({}; .[$item.key] = $item.value);
          (to_map($cardinalsin)) as $cm
          | (to_map($prometheus)) as $pm
          | (($cm + $pm) | keys_unsorted | unique) as $keys
          | {
              alias: $alias,
              source_pack: $source_pack,
              query: $query,
              threshold: $threshold,
              series: (
                $keys | map(
                  . as $k
                  | ($cm[$k] // null) as $cv
                  | ($pm[$k] // null) as $pv
                  | (if ($cv != null and $pv != null) then (($cv - $pv) | abs) else null end) as $abs
                  | (if ($cv == null or $pv == null) then null
                     elif ($pv | abs) == 0 then (if ($cv | abs) == 0 then 0 else null end)
                     else ($abs / ($pv | abs))
                     end) as $rel
                  | {
                      series_key: $k,
                      cardinalsin: $cv,
                      prometheus: $pv,
                      abs_delta: $abs,
                      rel_delta: $rel,
                      status: (
                        if ($cv == null or $pv == null) then "missing_series"
                        elif ($rel == null) then "delta_exceeds_threshold"
                        elif $rel > $threshold then "delta_exceeds_threshold"
                        else "pass"
                        end
                      )
                    }
                )
              )
            }
          | .pass = ((.series | length) > 0 and (.series | all(.status == "pass")))
        '
    )"
    echo "$comparison" >> "$tmp_results"
  done < "$pack"
done

summary_json="$OUT_DIR/summary.json"
summary_md="$OUT_DIR/summary.md"

jq -s \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg run_id "$RUN_ID" \
  --arg mode "$MODE" \
  '{
    generated_at: $generated_at,
    run_id: $run_id,
    mode: $mode,
    checks: .,
    check_count: length,
    failing_check_count: (map(select(.pass == false)) | length),
    pass: (all(.[]; .pass))
  }' \
  "$tmp_results" > "$summary_json"

{
  echo "# Dual Publish Parity Summary"
  echo
  echo "- run_id: \`$RUN_ID\`"
  echo "- mode: \`$MODE\`"
  echo "- generated_at: \`$(date -u +%Y-%m-%dT%H:%M:%SZ)\`"
  echo
  jq -r '
    "## Results",
    "",
    ("- checks: " + (.check_count | tostring)),
    ("- failing checks: " + (.failing_check_count | tostring)),
    ("- overall pass: " + (if .pass then "true" else "false" end)),
    "",
    ( .checks[]
      | "- " + .alias + " [" + .source_pack + "]: " + (if .pass then "pass" else "FAIL" end)
    )
  ' "$summary_json"
} > "$summary_md"

cat "$summary_json"
if [[ "$(jq -r '.pass' "$summary_json")" != "true" ]]; then
  echo "Parity check failed. See $summary_md" >&2
  exit 1
fi

echo "Parity check passed. Artifacts written to $OUT_DIR"
