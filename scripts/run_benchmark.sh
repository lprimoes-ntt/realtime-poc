#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

load_env_file() {
  local file="$1"
  while IFS='=' read -r key value; do
    [[ -z "$key" ]] && continue
    [[ "${key:0:1}" == "#" ]] && continue
    value="${value%$'\r'}"
    export "${key}=${value}"
  done < "$file"
}

PROFILE_FILE="${1:-scripts/benchmark_profiles/heavy_10k.env}"
if [[ ! -f "$PROFILE_FILE" ]]; then
  echo "Profile not found: $PROFILE_FILE"
  exit 1
fi

if [[ -f .env ]]; then
  load_env_file .env
fi

set -a
# shellcheck disable=SC1090
source "$PROFILE_FILE"
set +a

: "${SCENARIO_NAME:=heavy-10k}"
: "${BENCH_DURATION_SECONDS:=180}"
: "${BENCH_BASE_RPS:=25}"
: "${BENCH_BURST_RPS:=120}"
: "${BENCH_BURST_INTERVAL_SECONDS:=120}"
: "${BENCH_BURST_DURATION_SECONDS:=30}"
: "${BENCH_UPDATE_RATIO:=0.35}"
: "${BENCH_PAYMENT_RATIO:=0.65}"
: "${BENCH_LOADGEN_WORKERS:=64}"
: "${BENCH_LOADGEN_GOMAXPROCS:=0}"
: "${BENCH_LOADGEN_DISPATCH_EVERY_MS:=100}"
: "${BENCH_LOADGEN_MAX_DISPATCH_CHUNK:=250}"
: "${BENCH_LOADGEN_MAX_TRACKED_ORDER_IDS:=250000}"
: "${BENCH_LOADGEN_REPORT_EVERY_SECONDS:=5}"
: "${BENCH_POLL_SECONDS:=5}"
: "${BENCH_QUERY_TIMEOUT_SECONDS:=30}"
: "${BENCH_TARGET_PROJECTION_BACKLOG_P95_SECONDS:=${BENCH_TARGET_P95_SECONDS:-30}}"
: "${BENCH_ENFORCE_TARGET:=true}"
: "${BENCH_RESET_STACK:=true}"
: "${BENCH_CUSTOMER_SEED_COUNT:=200}"
: "${SOURCE_NAME:=source1}"

if [[ -z "${SOURCE_DSN:-}" || -z "${SERVING_DSN:-}" ]]; then
  echo "SOURCE_DSN and SERVING_DSN must be set in .env"
  exit 1
fi

LOADGEN_SOURCE_DSN="${LOADGEN_SOURCE_DSN:-${SOURCE_DSN/source-db:1433/localhost:14331}}"
BENCHSTAT_SOURCE_DSN="${BENCHSTAT_SOURCE_DSN:-${SOURCE_DSN/source-db:1433/localhost:14331}}"
BENCHSTAT_SERVING_DSN="${BENCHSTAT_SERVING_DSN:-${SERVING_DSN/serving-db:1433/localhost:14332}}"
OUT_DIR="${BENCH_OUTPUT_DIR:-benchmarks/$(date -u +%Y%m%d_%H%M%S)_${SCENARIO_NAME}}"
mkdir -p "$OUT_DIR"

if [[ "$BENCH_RESET_STACK" == "true" ]]; then
  echo "[1/6] Resetting docker stack and volumes"
  docker compose down -v
else
  echo "[1/6] Keeping existing volumes"
fi

echo "[2/6] Starting stack"
docker compose up -d --build

echo "[3/6] Starting load generator"
go run ./cmd/loadgen \
  -source-dsn "$LOADGEN_SOURCE_DSN" \
  -duration-seconds "$BENCH_DURATION_SECONDS" \
  -base-rps "$BENCH_BASE_RPS" \
  -burst-rps "$BENCH_BURST_RPS" \
  -burst-interval-seconds "$BENCH_BURST_INTERVAL_SECONDS" \
  -burst-duration-seconds "$BENCH_BURST_DURATION_SECONDS" \
  -update-ratio "$BENCH_UPDATE_RATIO" \
  -payment-ratio "$BENCH_PAYMENT_RATIO" \
  -customer-seed-count "$BENCH_CUSTOMER_SEED_COUNT" \
  -workers "$BENCH_LOADGEN_WORKERS" \
  -gomaxprocs "$BENCH_LOADGEN_GOMAXPROCS" \
  -dispatch-every-ms "$BENCH_LOADGEN_DISPATCH_EVERY_MS" \
  -max-dispatch-chunk "$BENCH_LOADGEN_MAX_DISPATCH_CHUNK" \
  -max-tracked-order-ids "$BENCH_LOADGEN_MAX_TRACKED_ORDER_IDS" \
  -report-every-seconds "$BENCH_LOADGEN_REPORT_EVERY_SECONDS" \
  >"$OUT_DIR/loadgen.log" 2>&1 &
LOADGEN_PID=$!

BENCHSTAT_DURATION_SECONDS="${BENCHSTAT_DURATION_SECONDS:-$((BENCH_DURATION_SECONDS + 30))}"

echo "[4/6] Collecting benchmark samples"
set +e
go run ./cmd/benchstat \
  -source-dsn "$BENCHSTAT_SOURCE_DSN" \
  -serving-dsn "$BENCHSTAT_SERVING_DSN" \
  -source-name "$SOURCE_NAME" \
  -duration-seconds "$BENCHSTAT_DURATION_SECONDS" \
  -poll-seconds "$BENCH_POLL_SECONDS" \
  -request-timeout-seconds "$BENCH_QUERY_TIMEOUT_SECONDS" \
  -out-dir "$OUT_DIR" \
  -scenario "$SCENARIO_NAME" \
  -target-projection-backlog-p95-seconds "$BENCH_TARGET_PROJECTION_BACKLOG_P95_SECONDS" \
  -enforce-target "$BENCH_ENFORCE_TARGET"
BENCH_EXIT=$?
set -e

echo "[5/6] Waiting for load generator"
set +e
wait "$LOADGEN_PID"
LOADGEN_EXIT=$?
set -e

echo "[6/6] Benchmark artifacts"
echo "Output directory: $OUT_DIR"
echo "- samples: $OUT_DIR/samples.csv"
echo "- summary: $OUT_DIR/summary.json"
echo "- loadgen log: $OUT_DIR/loadgen.log"

if [[ "$LOADGEN_EXIT" -ne 0 ]]; then
  echo "Load generator failed with exit code $LOADGEN_EXIT"
  exit "$LOADGEN_EXIT"
fi

if [[ "$BENCH_EXIT" -ne 0 ]]; then
  echo "Benchmark target failed or benchstat errored (exit $BENCH_EXIT)"
  exit "$BENCH_EXIT"
fi

echo "Benchmark completed successfully"
