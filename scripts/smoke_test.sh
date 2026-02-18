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

if [[ -f .env ]]; then
  load_env_file .env
fi

echo "[1/7] Resetting stack and volumes"
docker compose down -v

echo "[2/7] Starting stack"
docker compose up -d --build

echo "[3/7] Waiting for pipeline to initialize"
sleep 20

echo "[4/7] Applying change generator"
docker compose exec -T source-db /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$SA_PASSWORD" -i /dev/stdin < scripts/generate_changes.sql

echo "[5/7] Waiting for ingestion/projection"
sleep 35

echo "[6/7] Inspecting serving DB"
docker compose exec -T serving-db /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$SA_PASSWORD" -d ServingDB -Q "SELECT TOP 10 source_name, capture_instance, CONVERT(VARCHAR(64), last_ingested_lsn, 1) AS last_ingested_lsn, updated_at FROM dbo.ctl_ingestion_watermarks ORDER BY capture_instance;"
docker compose exec -T serving-db /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$SA_PASSWORD" -d ServingDB -Q "SELECT TOP 50 * FROM dbo.proj_orders_kpi_by_minute_segment ORDER BY minute_bucket DESC, segment;"
docker compose exec -T serving-db /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$SA_PASSWORD" -d ServingDB -Q "SELECT projection_name, status, as_of_time, built_at, CONVERT(VARCHAR(64), as_of_lsn, 1) AS as_of_lsn, last_error FROM dbo.ctl_projection_metadata ORDER BY projection_name;"

echo "[7/7] Restarting pipeline and validating resume"
docker compose restart pipeline
sleep 25
docker compose exec -T serving-db /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$SA_PASSWORD" -d ServingDB -Q "SELECT projection_name, status, as_of_time, built_at FROM dbo.ctl_projection_metadata ORDER BY projection_name;"

echo "Smoke test complete"
