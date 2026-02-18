#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SA_PASSWORD:-}" ]]; then
  echo "SA_PASSWORD is required"
  exit 1
fi

SQLCMD_BIN="/opt/mssql-tools18/bin/sqlcmd"

wait_for_db() {
  local host="$1"
  local name="$2"
  echo "Waiting for ${name} (${host})..."
  for attempt in $(seq 1 120); do
    if "${SQLCMD_BIN}" -C -S "${host}" -U sa -P "${SA_PASSWORD}" -Q "SELECT 1" >/dev/null 2>&1; then
      echo "${name} is ready"
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for ${name}"
  exit 1
}

run_script() {
  local host="$1"
  local file="$2"
  echo "Applying ${file} on ${host}"
  "${SQLCMD_BIN}" -C -b -S "${host}" -U sa -P "${SA_PASSWORD}" -i "${file}"
}

wait_for_db source-db "source-db"
wait_for_db serving-db "serving-db"

run_script source-db /app/sql/source/00_init.sql
run_script source-db /app/sql/source/10_schema.sql
run_script source-db /app/sql/source/20_seed.sql
run_script source-db /app/sql/source/30_enable_cdc.sql

run_script serving-db /app/sql/serving/00_init.sql
run_script serving-db /app/sql/serving/10_schema.sql

echo "Starting pipeline service"
exec /app/pipeline
