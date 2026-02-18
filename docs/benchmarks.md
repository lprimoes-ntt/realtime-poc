# Benchmarks

## Goal
Provide reproducible evidence of pipeline feasibility using measurable queue/backlog and throughput outputs.

Primary benchmark KPI:

- Projection backlog percentiles (especially P95)
- Ingestion backlog percentiles
- Recent ingestion throughput (`rows_1m`)

Default target:

- `projection_backlog_p95_seconds < 30`

## Benchmark architecture
Benchmark flow uses three pieces:

1. `cmd/loadgen`: generates source writes against SQL Server using identity keys.
2. `cmd/benchstat`: polls source/serving SQL Server state, records `samples.csv`, computes `summary.json`.

The wrapper script `scripts/run_benchmark.sh` orchestrates all steps.
Default run length is 180 seconds for load generation (`BENCH_DURATION_SECONDS=180`).

## Profile
Benchmark config is defined in:

- `heavy_10k.env` (default, higher write pressure)
- `heavy_5k.env`

Each profile controls:

- load duration
- base and burst RPS
- burst cadence
- update/payment ratios
- poll cadence
- target threshold
- loadgen concurrency/dispatch tuning
- optional stack reset behavior

## Run benchmark
```bash
./scripts/run_benchmark.sh
```

High-pressure profile (production-like write target):
```bash
./scripts/run_benchmark.sh scripts/benchmark_profiles/heavy_10k.env
```

## Outputs
Each run creates a folder under `benchmarks/` (unless overridden):

- `samples.csv`: raw timeseries samples from polling source/serving DB state
- `summary.json`: aggregate stats and pass/fail
- `loadgen.log`: load generator progress and summary

## Interpreting summary
Important fields in `summary.json`:

- `projection_backlog_p95_seconds`
- `projection_backlog_p99_seconds`
- `ingestion_backlog_p95_seconds`
- `projection_build_delay_p95_seconds`
- `rows_1m_total_avg`
- `rows_1m_total_max`
- `pass` (threshold check result)

Important field in `loadgen.log` summary:

- `approx_changes_per_sec` (orders + updates + payments per second observed by loadgen)

A practical read:

- If backlog percentiles stay under target and error counts are low, pipeline behavior is feasible at that profile.
- If burst windows push backlog too high, tune poll intervals, batch size, or projection window strategy.

## Notes and constraints
- `run_benchmark.sh` can reset the stack and volumes (`BENCH_RESET_STACK=true`) for clean comparisons.
- Projection backlog target can be set with `BENCH_TARGET_PROJECTION_BACKLOG_P95_SECONDS` (legacy `BENCH_TARGET_P95_SECONDS` is still accepted).
- High-rate scenarios can tune loadgen with:
  - `BENCH_LOADGEN_GOMAXPROCS` (`0` means auto: half of host CPU cores)
  - `BENCH_LOADGEN_WORKERS`
  - `BENCH_LOADGEN_DISPATCH_EVERY_MS`
  - `BENCH_LOADGEN_MAX_DISPATCH_CHUNK`
  - `BENCH_LOADGEN_MAX_TRACKED_ORDER_IDS`
  - `BENCH_QUERY_TIMEOUT_SECONDS` (query timeout per poll; default `30`)
  - `BENCH_ENFORCE_TARGET` (set `false` to always collect artifacts even if target fails)
- `loadgen` uses source identity columns, so explicit PK management is not required.
- Benchmark reflects local container resources; compare runs using similar host conditions.
