# Technical Decisions

## Overview
This document captures the main implementation choices and their tradeoffs for the PoC.

## Decision 1: Use SQL Server CDC as change source
Choice:

- Pull from CDC functions using LSN windows.

Why:

- Native SQL Server mechanism for ordered row-level changes.
- Avoids triggers and custom change tables in source OLTP schema.

Tradeoff:

- CDC retention can truncate old LSN windows if consumers lag too long.
- Requires handling start/min/max LSN carefully.

## Decision 2: One ingestor per capture stream
Choice:

- Poll each capture instance once and write to shared staging.

Why:

- Prevents duplicate reads when multiple projections need the same source table.
- Separates ingestion concerns from projection concerns.

Tradeoff:

- Requires explicit per-projection checkpoints downstream.

## Decision 3: Append-only staging with idempotent inserts
Choice:

- `stg_cdc_*` tables have unique constraints over `(lsn, seqval, business_key)`.
- Writes use `INSERT ... WHERE NOT EXISTS`.

Why:

- Safe retries after transient failures.
- Durable replay substrate for projection logic.

Tradeoff:

- Staging grows over time and needs cleanup strategy (not yet implemented).

## Decision 4: Separate ingestion and projection control tables
Choice:

- Ingestion uses `ctl_ingestion_watermarks`.
- Projections use `ctl_projection_checkpoints`.

Why:

- Allows independent progress for each projection.
- Enables one-to-many fanout from shared staging.

Tradeoff:

- More control state to manage and inspect.

## Decision 5: Embedded DuckDB for transforms
Choice:

- Projection workers open in-memory DuckDB and run SQL transforms.

Why:

- Fast analytical SQL execution in-process.
- Keeps projection logic declarative and readable.

Tradeoff:

- Data is loaded into DuckDB per run.
- For larger volumes, batching and memory tuning become important.

## Decision 6: Recompute-window strategy for KPI projection
Choice:

- Delete and rebuild recent `N` minutes every projection cycle.

Why:

- Simplifies logic for updates/deletes and late-arriving changes.
- Stronger correctness for PoC than naive incremental aggregates.

Tradeoff:

- Extra compute cost each run.
- Window size tuning becomes a latency vs cost tradeoff.

## Decision 7: Atomic projection commit
Choice:

- Projection table writes, checkpoint updates, and metadata updates are one transaction.

Why:

- Prevents partial projection state.
- Keeps reported metadata aligned with actual table contents.

Tradeoff:

- Larger transactions if projection windows are large.

## Decision 8: Keep runtime simple (single pipeline instance)
Choice:

- No distributed coordination in PoC.

Why:

- Faster iteration and easier debugging.

Tradeoff:

- Not horizontally scalable as-is.
- Future multi-instance deployment needs partitioning/leader election.
