# Overview and Goals

## What this PoC is
This repository demonstrates a near-real-time data pipeline from transactional SQL Server data into serving projections suitable for BI-style querying.

At a high level:

1. Source database emits row-level changes through SQL Server CDC.
2. A Go service pulls CDC changes in LSN order.
3. Changes are persisted into append-only staging tables in a serving database.
4. Projection workers transform staged data using embedded DuckDB.
5. Projection tables are updated transactionally with checkpoint and metadata updates.

## Why this PoC exists
The project validates architectural viability before production hardening.

Primary questions:

- Can source changes appear in serving projections with low latency?
- Is restart behavior safe and deterministic?
- Can many projections share one ingestion stream per source table?
- Can DuckDB simplify transform complexity while keeping performance acceptable?

## Explicit scope
In scope:

- End-to-end CDC ingestion and projection flow.
- Idempotent ingestion writes.
- Projection checkpoints and freshness metadata.
- Local reproducibility via Docker Compose.

Out of scope:

- Security hardening (managed identities, vault integration, TLS policy).
- Horizontal scaling and leader election.
- Full schema-evolution strategy.
- Production-grade observability and SLO instrumentation.

## Success criteria for this PoC
- Pipeline starts from empty environment using included scripts.
- CDC changes are captured and materialized in serving tables.
- Projection metadata shows successful builds.
- Restarting the pipeline does not corrupt control state.

## Non-obvious implementation details
- The pipeline container runs database init scripts on startup. Scripts are written to be idempotent.
- Projection freshness is tracked by LSN and timestamp, not only by row counts.
- KPI projection favors recompute-window correctness over fully incremental complexity.
