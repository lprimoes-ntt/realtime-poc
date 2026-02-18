# Architecture

## System components

### 1) Source SQL Server (`source-db`)
Responsibilities:

- Stores OLTP-like source tables (`customers`, `orders`, `payments`).
- Has CDC enabled at database and table level.
- Exposes CDC table functions used by ingestors:
  - `cdc.fn_cdc_get_all_changes_dbo_customers`
  - `cdc.fn_cdc_get_all_changes_dbo_orders`
  - `cdc.fn_cdc_get_all_changes_dbo_payments`

### 2) Serving SQL Server (`serving-db`)
Responsibilities:

- Stores staging CDC logs (`stg_cdc_*`).
- Stores control and checkpoint state (`ctl_*`).
- Stores query-facing projection tables (`proj_*`).

### 3) Pipeline service (`pipeline`)
Responsibilities:

- Runs one ingestor worker per capture instance.
- Runs projection workers on independent intervals.
- Uses DuckDB for transformation logic.
- Updates metadata and checkpoints atomically with projection writes.

## End-to-end data flow
`source tables` -> `CDC function rows` -> `ingestors` -> `stg_cdc_*` -> `projection workers + DuckDB` -> `proj_*`

## Worker topology
Current implementation in `cmd/pipeline/main.go`:

- Ingestors always enabled:
  - `dbo_customers`
  - `dbo_orders`
  - `dbo_payments`
- Projections conditionally enabled by env flags:
  - `orders_kpi_by_minute_segment`
  - `orders_latest`

## Consistency boundary
For any projection that depends on multiple capture streams, the worker computes:

- `cutoff_lsn = MIN(last_ingested_lsn)` across required captures

This enforces a shared visibility boundary so the projection does not use one stream ahead of another.

## Deployment architecture
Local environment uses Docker Compose:

- `source-db` and `serving-db` start first with healthchecks.
- `pipeline` starts after both DB services are healthy.
- Pipeline entrypoint waits for DB readiness and applies SQL bootstrap scripts before running workers.

## Why this architecture is practical for a PoC
- Minimal moving parts: one app container + two DB containers.
- Clear state ownership: source emits changes, serving owns pipeline state.
- Deterministic recovery with explicit LSN checkpoints.
- Simple to port to managed SQL services later by replacing DSNs and auth.
