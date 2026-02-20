#!/usr/bin/env bash
set -euo pipefail

echo "=== Resetting shori-realtime to clean slate ==="

# 1. Stop and remove all containers + anonymous volumes
echo "[1/4] Stopping and removing containers..."
docker compose down -v 2>/dev/null || true

# 2. Remove the named debezium-data volume (may use project prefix)
echo "[2/4] Removing debezium-data volume..."
docker volume rm shori-realtime_debezium-data 2>/dev/null || true
docker volume rm debezium-data 2>/dev/null || true

# 3. Clean Rust build artifacts
echo "[3/4] Cleaning Rust build artifacts..."
cargo clean --quiet 2>/dev/null || true
rm -f results.csv

# 4. Bring everything back up, wait for health, then init the DB
echo "[4/4] Starting fresh infrastructure..."
docker compose up -d

echo ""
echo "Waiting for SQL Server to become healthy..."
until docker inspect --format='{{.State.Health.Status}}' sqlserver 2>/dev/null | grep -q "healthy"; do
    sleep 2
done
echo "SQL Server is healthy."

echo "Waiting for Redpanda to become healthy..."
until docker inspect --format='{{.State.Health.Status}}' redpanda 2>/dev/null | grep -q "healthy"; do
    sleep 2
done
echo "Redpanda is healthy."

echo ""
echo "Initializing ShoriDB (CDC + tables)..."
docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P 'YourStr0ngP@ssw0rd!' -C \
    -i /dev/stdin < init.sql

echo ""
echo "Waiting for Debezium to start streaming..."
sleep 10

echo ""
echo "=== Reset complete. Infrastructure is ready. ==="
