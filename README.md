# shori-realtime — CDC -> Kafka -> Medallion -> React PoC

A local PoC that captures SQL Server CDC events with Debezium, streams through Redpanda, builds Bronze/Silver/Gold Delta tables with Polars, and serves a real-time dashboard through FastAPI SSE + React.

## What This PoC Guarantees

- Kafka offsets are committed **only after** successful micro-batch processing.
- Fatal Lakehouse errors are emitted as `pipeline_error` SSE events and the Lakehouse worker stops.
- SSE delivery is broadcast to multiple clients with per-client queues and overflow handling.
- Runtime metrics are exposed via `/api/health`.
- Debezium scope is restricted to `Tickets` table for this PoC.

## Architecture

```text
ShoriSQL_1 (CDC) -> Debezium_1 --\
                                ---> Redpanda -> FastAPI backend -> React dashboard
ShoriSQL_2 (CDC) -> Debezium_2 --/                   |            
                                                      -> Delta Bronze/Silver/Gold
```

## Prerequisites

- Docker + Docker Compose
- `uv`
- Node.js + npm

## Configuration

Environment defaults are documented in `.env.example`.

Key variables:
- `SQLSERVER_SA_PASSWORD`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_UNASSIGNED_RESTART_SEC`
- `KAFKA_CONSUMER_RESTART_BACKOFF_SEC`
- `LAKEHOUSE_FLUSH_INTERVAL_SEC`
- `LAKEHOUSE_MAX_BATCH_SIZE`
- `GOLD_REFRESH_INTERVAL_SEC`
- `SSE_CLIENT_QUEUE_SIZE`
- `SSE_CLIENT_OVERFLOW_LIMIT`
- `SSE_EMIT_CDC_RAW`
- `SSE_CDC_STATS_INTERVAL_SEC`
- `CORS_ALLOW_ORIGINS`
- `VITE_API_BASE_URL`

## Quick Start

### 1) Infrastructure + DB bootstrap

```bash
./scripts/setup.sh
```

### 2) Backend

```bash
uv run main.py
```

### 3) Frontend

```bash
cd frontend
npm install --legacy-peer-deps
npm run dev
```

Open `http://localhost:5173`.

### 4) Workload generator

```bash
uv run generator.py
```

## API

### `GET /api/stream`
SSE stream with a stable envelope:

```json
{
  "type": "cdc_stats | cdc_raw | lakehouse_update | pipeline_error | heartbeat",
  "ts": "2026-02-21T20:00:00.000000+00:00",
  "data": {}
}
```

Event payloads:
- `cdc_stats`: `{ events_in_interval, interval_sec, events_per_sec, total_received, total_dropped }`
- `cdc_raw`: `{ topic, partition, offset, payload }`
- `lakehouse_update`: `{ summary, processed, failed, gold_recomputed }`
- `pipeline_error`: `{ message, fatal }`
- `heartbeat`: `{}`

### `GET /api/health`
Returns:
- thread status (`ui_consumer_alive`, `lakehouse_alive`, `lakehouse_running`)
- client count
- counters (`events_received`, `events_dropped`, `batches_ok`, `batches_failed`, `last_batch_at`, `last_error`)
- active runtime knobs

## Backpressure Behavior

- Backend fan-out uses per-client bounded queues.
- On queue overflow, oldest messages are dropped for that client.
- Clients that overflow repeatedly are disconnected.
- Dropped counts are reflected in `/api/health` (`events_dropped`).
- For high-throughput runs, keep `SSE_EMIT_CDC_RAW=false` and rely on `cdc_stats` for UI counters.

## Repository Hygiene

`data/datalake/` is intentionally ignored in git. Delta artifacts are runtime data, not source code.

## Troubleshooting

**No events in UI:**
- Check backend health: `curl http://localhost:8000/api/health`
- Verify Redpanda topics: `docker exec -it redpanda rpk topic list`

**Debezium not streaming:**
- `docker compose logs debezium_1`
- `docker compose logs debezium_2`
- Verify SQL Server services are healthy and CDC is enabled.

**Pipeline halted:**
- Inspect `last_error` in `/api/health`
- Look for `pipeline_error` in browser console or UI banner

**SQL Server Agent status check:**

```bash
docker exec -it shorisql_1 /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$SQLSERVER_SA_PASSWORD" -C \
  -Q "SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE '%Agent%'"
```
