#!/usr/bin/env bash
set -euo pipefail

# Ensure we run from the project root regardless of where script is called
cd "$(dirname "$0")/.."

SQLSERVER_SA_PASSWORD="${SQLSERVER_SA_PASSWORD:-YourStr0ngP@ssw0rd!}"

echo "=== Resetting shori-realtime to clean slate ==="

# 1. Stop and remove all containers + anonymous volumes
echo "[1/4] Stopping and removing containers..."
docker compose down -v 2>/dev/null || true

# 2. Remove the named debezium-data volumes (may use project prefix)
echo "[2/4] Removing debezium-data volumes..."
docker volume rm shori-realtime_debezium1-data 2>/dev/null || true
docker volume rm debezium1-data 2>/dev/null || true
docker volume rm shori-realtime_debezium2-data 2>/dev/null || true
docker volume rm debezium2-data 2>/dev/null || true

# 3. Clean Python and data artifacts
echo "[3/4] Cleaning local artifacts..."
rm -rf .ruff_cache/ .venv/ 2>/dev/null || true
rm -f results.csv

# 4. Bring everything back up, wait for health, then init the DB
echo "[4/4] Starting fresh infrastructure (Database and Kafka)..."
docker compose up -d shorisql_1 shorisql_2 redpanda

echo ""
echo "Waiting for ShoriSQL_1 to become healthy..."
until docker inspect --format='{{.State.Health.Status}}' shorisql_1 2>/dev/null | grep -q "healthy"; do
    sleep 2
done
echo "ShoriSQL_1 is healthy."

echo "Waiting for ShoriSQL_2 to become healthy..."
until docker inspect --format='{{.State.Health.Status}}' shorisql_2 2>/dev/null | grep -q "healthy"; do
    sleep 2
done
echo "ShoriSQL_2 is healthy."

echo "Waiting for Redpanda to become healthy..."
until docker inspect --format='{{.State.Health.Status}}' redpanda 2>/dev/null | grep -q "healthy"; do
    sleep 2
done
echo "Redpanda is healthy."

echo ""
echo "Initializing ShoriDB_1 on ShoriSQL_1 (CDC + tables)..."
docker exec -i shorisql_1 /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_SA_PASSWORD}" -C \
    -i /dev/stdin < db/init_sql1.sql

echo "Initializing ShoriDB_2 on ShoriSQL_2 (CDC + tables)..."
docker exec -i shorisql_2 /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_SA_PASSWORD}" -C \
    -i /dev/stdin < db/init_sql2.sql

echo ""
echo "Starting Debezium servers..."
docker compose up -d debezium_1 debezium_2

echo ""
echo "Waiting for Debezium to start streaming..."
sleep 10

echo ""
echo "=== Reset complete. Infrastructure is ready. ==="
