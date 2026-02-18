# Operations and Local Development

## Prerequisites
- Docker Engine
- Docker Compose plugin

## Start the stack
```bash
docker compose up -d --build
```

What happens:

1. DB containers start and pass healthchecks.
2. Pipeline container starts.
3. `entrypoint.sh` applies SQL scripts (idempotent).
4. Pipeline workers begin polling and projection cycles.

## Run smoke test
```bash
./scripts/smoke_test.sh
```

Smoke test flow:

1. Resets stack and volumes (`docker compose down -v`).
2. Starts stack from blank databases.
3. Waits for initialization.
4. Applies `scripts/generate_changes.sql` to source.
5. Waits for ingestion/projection cycles.
6. Queries serving state tables and projections.
7. Restarts pipeline and verifies projection metadata.

`scripts/generate_changes.sql` now uses generated identity keys and can be re-run safely.

## Run benchmark
Default heavy benchmark (180 seconds):
```bash
./scripts/run_benchmark.sh
```

Explicit profile run:
```bash
./scripts/run_benchmark.sh scripts/benchmark_profiles/heavy_10k.env
```

Benchmark artifacts are written under `benchmarks/`:
- `samples.csv`
- `summary.json`
- `loadgen.log`

The reset behavior is profile-driven via `BENCH_RESET_STACK` (default `true` in bundled profiles).

## Reset strategy

### Reset serving state only
```bash
docker compose exec -T serving-db /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P \"$SA_PASSWORD\" -i /dev/stdin < scripts/rebuild_all.sql
```

This clears projection tables and resets serving control tables, but does not reset source tables or CDC history.

### Full environment reset
```bash
docker compose down -v
docker compose up -d --build
```

This recreates source and serving volumes and re-runs bootstrap scripts from scratch.

## Useful SQL checks

### Ingestion progress
```sql
SELECT source_name, capture_instance, CONVERT(VARCHAR(64), last_ingested_lsn, 1) AS last_ingested_lsn, updated_at
FROM dbo.ctl_ingestion_watermarks
ORDER BY capture_instance;
```

### Projection checkpoints
```sql
SELECT projection_name, capture_instance, CONVERT(VARCHAR(64), last_consumed_lsn, 1) AS last_consumed_lsn, updated_at
FROM dbo.ctl_projection_checkpoints
ORDER BY projection_name, capture_instance;
```

### Projection freshness/status
```sql
SELECT projection_name, status, as_of_time, built_at, CONVERT(VARCHAR(64), as_of_lsn, 1) AS as_of_lsn, last_error
FROM dbo.ctl_projection_metadata
ORDER BY projection_name;
```

## Common troubleshooting

### Projection not updating
- Check ingestion watermarks are moving.
- Check projection metadata for `ERROR` and inspect `last_error`.
- Verify `ENABLE_PROJ_ORDERS_KPI`/`ENABLE_PROJ_ORDERS_LATEST` flags.

### No CDC rows consumed
- Confirm CDC enabled on source DB/tables.
- Verify source mutations are actually executed.
- Check if watermark is behind CDC min LSN due to retention truncation.

### DB startup race conditions
- Re-run `docker compose up -d`.
- Pipeline waits for DB readiness, but initial container startup can still require extra time under constrained environments.
