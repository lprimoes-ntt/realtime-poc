# Runtime Behavior

## Startup sequence
Pipeline startup is handled by `entrypoint.sh` and `cmd/pipeline/main.go`.

Order of operations:

1. Wait for `source-db` and `serving-db` connectivity.
2. Run SQL bootstrap scripts:
   - Source init/schema/seed/CDC enablement
   - Serving init/schema
3. Start Go pipeline process.
4. Load configuration from environment and open DB pools.
5. Bootstrap control rows in serving DB if missing.
6. Start worker goroutines.

## Ingestion worker lifecycle
Implemented in `internal/cdc/ingestor.go`.

Per cycle:

1. Read current watermark from `ctl_ingestion_watermarks`.
2. Read CDC min/max LSN bounds from source DB.
3. Compute next `from_lsn`:
   - start at min LSN when watermark is zero/older than min
   - otherwise increment previous LSN
4. Query CDC function with bounded LSN window and row cap.
5. In one transaction on serving DB:
   - insert staged rows idempotently
   - update ingestion watermark to batch end LSN
6. Sleep until next poll interval.

Failure behavior:

- Errors are logged and retried next cycle.
- Idempotent staging constraints make retries safe.

## KPI projection worker lifecycle
Implemented in `internal/projections/kpi_worker.go`.

Per cycle:

1. Compute `cutoff_lsn` as minimum ingestion watermark across required captures.
2. Read projection checkpoints for required captures.
3. Check whether staged deltas exist in `(checkpoint, cutoff]`.
4. Build recompute window start (`now_floor_minute - N minutes`).
5. Load staged rows into DuckDB.
6. Build latest-state views by key using `ROW_NUMBER` over `(lsn DESC, seqval DESC)`.
7. Produce KPI aggregates by minute and segment.
8. In one serving DB transaction:
   - delete projection rows for recompute window
   - insert new rows
   - advance projection checkpoints to cutoff
   - update projection metadata to `OK`

Error behavior:

- Worker logs error and writes `ERROR` status in projection metadata.

## Orders-latest projection worker lifecycle
Implemented in `internal/projections/latest_worker.go`.

Per cycle:

1. Compute projection cutoff LSN from required captures.
2. Verify deltas since projection checkpoints.
3. Load relevant staging tables into DuckDB.
4. Collapse latest order and customer state by `(lsn, seqval)`.
5. In one transaction:
   - rebuild `proj_orders_latest`
   - advance projection checkpoints
   - update metadata

## Consistency and idempotency properties

### Ingestion idempotency
- Enforced by staging uniqueness + existence-check insert pattern.

### Projection atomicity
- Projection table writes and checkpoint/metadata updates share one transaction.

### Restart safety
- Workers recover state from control tables.
- No in-memory-only offsets are required for correctness.

## Observability surfaces
Even without metrics stack, state can be inspected via SQL:

- `ctl_ingestion_watermarks`: ingestion progress.
- `ctl_projection_checkpoints`: per-projection consumption.
- `ctl_projection_metadata`: health and freshness.
- `stg_cdc_*`: raw captured events.
- `proj_*`: serving outputs.
