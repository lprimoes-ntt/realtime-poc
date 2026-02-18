# Limitations and Roadmap

## Current limitations

### Security and environment hardening
- Local SA credentials in `.env`.
- No managed identity or secret manager integration.
- No formal TLS/auth hardening model.

### Scale and performance
- Single pipeline instance without distributed coordination.
- KPI projection currently reprocesses all staged orders/customers and a windowed payments set for simplicity.
- No adaptive backpressure strategy.

### Data lifecycle
- Staging tables are append-only with no retention cleanup job.
- Unbounded growth is expected if left running long-term.

### Testing and reproducibility
- Benchmark outcomes still depend on local machine/container resource contention.
- There is no CI benchmark baseline comparison yet.

### Schema evolution
- No migration/versioning strategy for source or projection schema changes.
- Rebuild/reset scripts are the current fallback.

## Recommended next steps

### Near-term hardening
1. Add CI jobs for smoke test and short benchmark sanity runs.
2. Add a stage cleanup policy based on minimum projection checkpoint and retention.
3. Persist backlog and throughput metrics to a time-series backend for long-term trend analysis.
4. Add structured logs with correlation fields for worker cycle IDs.

### Mid-term improvements
1. Optimize projection incrementality for larger datasets.
2. Introduce schema migration management.
3. Add alertable health probes for metadata freshness.
4. Add integration tests that assert checkpoint monotonicity and restart behavior.

### Production path
1. Replace SQL auth with managed identity or equivalent.
2. Move secrets out of `.env` into a secret store.
3. Add horizontal scaling design (partition ownership or leader election).
4. Define SLOs and retention guarantees for CDC and staging.
