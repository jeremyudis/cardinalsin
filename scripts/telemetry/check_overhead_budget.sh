#!/usr/bin/env bash
set -euo pipefail

BASELINE=""
CANDIDATE=""
CPU_BUDGET_PCT="${CPU_BUDGET_PCT:-5}"
P99_BUDGET_PCT="${P99_BUDGET_PCT:-10}"

usage() {
  cat <<'EOF'
Usage: scripts/telemetry/check_overhead_budget.sh --baseline <summary.json> --candidate <summary.json> [--cpu-budget-pct 5] [--p99-budget-pct 10]

Expected JSON fields:
  cpu_seconds_per_second
  query_latency_p99_seconds
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline)
      BASELINE="$2"
      shift 2
      ;;
    --candidate)
      CANDIDATE="$2"
      shift 2
      ;;
    --cpu-budget-pct)
      CPU_BUDGET_PCT="$2"
      shift 2
      ;;
    --p99-budget-pct)
      P99_BUDGET_PCT="$2"
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

if [[ -z "$BASELINE" || -z "$CANDIDATE" ]]; then
  usage
  exit 1
fi

if [[ ! -f "$BASELINE" ]]; then
  echo "Baseline summary not found: $BASELINE" >&2
  exit 1
fi

if [[ ! -f "$CANDIDATE" ]]; then
  echo "Candidate summary not found: $CANDIDATE" >&2
  exit 1
fi

read_number() {
  local file="$1"
  local expr="$2"
  jq -er "$expr | numbers" "$file"
}

baseline_cpu="$(read_number "$BASELINE" '.cpu_seconds_per_second')"
candidate_cpu="$(read_number "$CANDIDATE" '.cpu_seconds_per_second')"
baseline_p99="$(read_number "$BASELINE" '.query_latency_p99_seconds')"
candidate_p99="$(read_number "$CANDIDATE" '.query_latency_p99_seconds')"

percent_regression() {
  local baseline="$1"
  local candidate="$2"
  awk -v b="$baseline" -v c="$candidate" 'BEGIN { if ((b + 0) == 0) { print "0.00"; } else { printf "%.2f", ((c - b) / b) * 100.0; } }'
}

cpu_reg_pct="$(percent_regression "$baseline_cpu" "$candidate_cpu")"
p99_reg_pct="$(percent_regression "$baseline_p99" "$candidate_p99")"

echo "Telemetry overhead regression report"
echo "baseline: $BASELINE"
echo "candidate: $CANDIDATE"
echo "cpu_seconds_per_second baseline=$baseline_cpu candidate=$candidate_cpu regression_pct=$cpu_reg_pct budget_pct=$CPU_BUDGET_PCT"
echo "query_latency_p99_seconds baseline=$baseline_p99 candidate=$candidate_p99 regression_pct=$p99_reg_pct budget_pct=$P99_BUDGET_PCT"

cpu_ok="$(awk -v r="$cpu_reg_pct" -v b="$CPU_BUDGET_PCT" 'BEGIN { if ((r + 0) <= (b + 0)) print "true"; else print "false"; }')"
p99_ok="$(awk -v r="$p99_reg_pct" -v b="$P99_BUDGET_PCT" 'BEGIN { if ((r + 0) <= (b + 0)) print "true"; else print "false"; }')"

if [[ "$cpu_ok" != "true" || "$p99_ok" != "true" ]]; then
  echo "Overhead budget check failed" >&2
  exit 1
fi

echo "Overhead budget check passed"
