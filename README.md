# Shori Realtime PoC

## What this PoC is
This project is a proof of concept for a near-real-time analytical data pipeline:

1. A SQL Server source database captures OLTP changes using CDC.
2. A Go service polls CDC changes by LSN.
3. Changes are persisted to staging tables in a serving SQL Server.
4. DuckDB runs projection transforms inside the Go service.
5. Projection tables are updated for BI/serving use cases.
6. Benchmark tooling generates repeatable load and reports backlog and throughput results.

The goal is to validate the architecture and operational behavior, not to be production-ready.

Deep dive: [`docs/overview-and-goals.md`](docs/overview-and-goals.md)

## Why this PoC exists
The PoC is designed to answer these engineering questions:

- Can we propagate source-of-truth changes to analytical projections in seconds?
- Can we restart the pipeline safely without losing progress?
- Can multiple projections share the same CDC ingestion stream?
- Can we use embedded DuckDB to keep transformation logic simple and fast?

Deep dive: [`docs/overview-and-goals.md`](docs/overview-and-goals.md)

## Architecture

### Components
- `source-db` (`SQL Server 2022`): OLTP-like dataset and CDC source.
- `serving-db` (`SQL Server 2022`): staging logs, control/checkpoint tables, and projection tables.
- `pipeline` (Go): CDC ingestors + projection workers.

### Data flow
`Source tables` -> `CDC functions` -> `Go ingestors` -> `stg_cdc_*` -> `DuckDB transforms` -> `proj_*`

Deep dive: [`docs/architecture.md`](docs/architecture.md)

## Technical decisions and rationale

### 1) Two SQL Server instances (source and serving)
- Why: isolates write-heavy OLTP workload from analytical serving workload.
- Result: ingestion and projection writes do not interfere with source transactional tables.

### 2) One ingestor per capture instance
- Why: poll each CDC capture stream once and fan out from staging.
- Result: avoids duplicate CDC polling when multiple projections need the same source table.

### 3) Append-only staging tables with unique constraints
- Why: gives us a durable, replayable change log in the serving DB.
- Result: ingestion is idempotent (`INSERT ... WHERE NOT EXISTS` + unique keys on `(lsn, seqval, business_key)`).

### 4) Explicit LSN state tables
- Why: ingestion and projection progress are different concerns.
- Result:
  - `ctl_ingestion_watermarks` tracks per-capture ingestion progress.
  - `ctl_projection_checkpoints` tracks per-projection consumption progress.
  - `ctl_projection_metadata` exposes status and freshness (`OK/ERROR`, `as_of_lsn`, `as_of_time`).

### 5) Cutoff LSN = minimum ingestion watermark across required captures
- Why: projections that join multiple source streams must only consume data that is available from all required captures.
- Result: consistent cross-table projection boundaries.

### 6) Embedded DuckDB for transformations
- Why: DuckDB gives fast SQL-based analytical transforms without operating a separate transform service.
- Result: projection logic is pure SQL with window functions and aggregations, but executed inside the Go process.

### 7) Recompute window for KPI projection
- Why: incremental aggregates are complex when updates and deletes arrive out of order.
- Result: `proj_orders_kpi_by_minute_segment` deletes and rebuilds a bounded recent window (`N` minutes), which is simpler and robust for PoC.

### 8) Transactional projection writes + checkpoint updates
- Why: avoid partial projection state.
- Result: projection table writes, checkpoint advancement, and metadata updates are committed atomically.

Deep dive: [`docs/technical-decisions.md`](docs/technical-decisions.md)

## Data model

### Source tables
- `dbo.customers`
- `dbo.orders`
- `dbo.payments`

CDC is enabled for all three in `sql/source/30_enable_cdc.sql`.
Primary keys in source tables are `IDENTITY` columns to simplify load generation.

### Serving control tables
- `dbo.ctl_ingestion_watermarks`
- `dbo.ctl_projection_checkpoints`
- `dbo.ctl_projection_metadata`

### Serving staging tables
- `dbo.stg_cdc_customers`
- `dbo.stg_cdc_orders`
- `dbo.stg_cdc_payments`

### Projection tables
- `dbo.proj_orders_kpi_by_minute_segment` (enabled by default)
- `dbo.proj_orders_latest` (optional, disabled by default)

Deep dive: [`docs/data-model.md`](docs/data-model.md)

## Runtime behavior

### Ingestion loop
For each capture instance (`customers`, `orders`, `payments`):

1. Read last ingested LSN from `ctl_ingestion_watermarks`.
2. Read CDC rows in `(from_lsn, max_lsn]`, bounded by `TOP (CDC_BATCH_MAX_ROWS)`.
3. Insert into staging idempotently.
4. Update ingestion watermark to the last row LSN in the same transaction.

### Projection loop (KPI)
1. Compute `cutoff_lsn` as the minimum ingestion watermark across required captures.
2. Check if there are staged deltas since projection checkpoints.
3. Load staged rows into DuckDB and collapse to latest state by `(lsn, seqval)`.
4. Recompute the configured recent window and write projection rows.
5. Update projection checkpoints and metadata in one transaction.

Deep dive: [`docs/runtime-behavior.md`](docs/runtime-behavior.md)

## Configuration
Environment variables are loaded from `.env`:

- `SOURCE_DSN`
- `SERVING_DSN`
- `POLL_INTERVAL_SECONDS` (default `5`)
- `CDC_BATCH_MAX_ROWS` (default `5000`)
- `PROJECTION_INTERVAL_SECONDS` (default `15`)
- `PROJECTION_RECOMPUTE_WINDOW_MINUTES` (default `15`)
- `ENABLE_PROJ_ORDERS_KPI` (default `true`)
- `ENABLE_PROJ_ORDERS_LATEST` (default `false`)
- `LOG_LEVEL` (default `info`)
- `SOURCE_NAME` (default `source1`)

Deep dive: [`docs/configuration.md`](docs/configuration.md)

## How to run

### Prerequisites
- Docker Engine + Docker Compose plugin.

### Start the stack
```bash
docker compose up -d --build
```

The pipeline container waits for both DBs, runs SQL bootstrap scripts, then starts the Go workers.

### Run smoke test
```bash
./scripts/smoke_test.sh
```

The smoke test does the following:

1. Starts the stack.
2. Applies source changes from `scripts/generate_changes.sql`.
3. Verifies ingestion watermarks and projection rows.
4. Restarts pipeline to validate restart behavior.

Deep dive: [`docs/operations.md`](docs/operations.md)

## Useful scripts
- `scripts/generate_changes.sql`: sample source mutations (insert/update/delete).
- `scripts/smoke_test.sh`: end-to-end validation.
- `scripts/rebuild_all.sql`: clears projections and resets control tables in serving DB.
- `scripts/run_benchmark.sh`: orchestrates load generation + benchmark sampling/reporting.
- `scripts/benchmark_profiles/heavy_10k.env`: default benchmark scenario profile.

Deep dive: [`docs/operations.md`](docs/operations.md)

## Repository layout
- `cmd/pipeline/main.go`: worker orchestration and startup.
- `internal/cdc`: CDC ingestion workers.
- `internal/projections`: projection workers and DuckDB SQL logic.
- `internal/staging`: staging read/write repository.
- `internal/metadata`: watermarks/checkpoints/metadata repository.
- `sql/source`: source DB init/schema/seed/CDC enablement scripts.
- `sql/serving`: serving DB schema scripts.
- `docker-compose.yml`, `Dockerfile`, `entrypoint.sh`: local runtime.

Deep dive: [`docs/architecture.md`](docs/architecture.md)

## Current PoC limits
- Not production-hardened (auth, TLS, secret management, observability).
- No retention cleanup for staging tables yet.
- No long-term metrics backend yet.
- KPI worker currently loads all staged orders/customers each run and only windows payments for recompute simplicity.
- No explicit schema-evolution strategy beyond rebuild/reset scripts.

Deep dive: [`docs/limitations-and-roadmap.md`](docs/limitations-and-roadmap.md)

## Next steps toward production
- Make change generator and test harness idempotent.
- Add staging retention and compaction strategy.
- Optimize incremental projection logic for larger volumes.
- Add structured metrics/tracing and alerting hooks.
- Add secure authentication and secret handling for cloud environments.

Deep dive: [`docs/limitations-and-roadmap.md`](docs/limitations-and-roadmap.md)

## Documentation index
- Overview and goals: [`docs/overview-and-goals.md`](docs/overview-and-goals.md)
- Architecture: [`docs/architecture.md`](docs/architecture.md)
- Technical decisions: [`docs/technical-decisions.md`](docs/technical-decisions.md)
- Data model: [`docs/data-model.md`](docs/data-model.md)
- Runtime behavior: [`docs/runtime-behavior.md`](docs/runtime-behavior.md)
- Configuration: [`docs/configuration.md`](docs/configuration.md)
- Operations and local development: [`docs/operations.md`](docs/operations.md)
- Benchmarks: [`docs/benchmarks.md`](docs/benchmarks.md)
- Limitations and roadmap: [`docs/limitations-and-roadmap.md`](docs/limitations-and-roadmap.md)
