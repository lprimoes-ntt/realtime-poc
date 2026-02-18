# Configuration

## Environment variables
Configuration is loaded from environment by `internal/config/config.go`.

### Connectivity
- `SOURCE_DSN`: SQL Server DSN for source DB.
- `SERVING_DSN`: SQL Server DSN for serving DB.
- `SOURCE_NAME`: logical source label used in control tables (`default: source1`).

### Ingestion tuning
- `POLL_INTERVAL_SECONDS` (`default: 5`)
- `CDC_BATCH_MAX_ROWS` (`default: 5000`)

### Projection tuning
- `PROJECTION_INTERVAL_SECONDS` (`default: 15`)
- `PROJECTION_RECOMPUTE_WINDOW_MINUTES` (`default: 15`)

### Feature flags
- `ENABLE_PROJ_ORDERS_KPI` (`default: true`)
- `ENABLE_PROJ_ORDERS_LATEST` (`default: false`)

### Logging
- `LOG_LEVEL` (`debug`, `info`, `warn`, `error`; default `info`)

## Default local values
The repository `.env` provides defaults for local Compose runs, including:

- SA password
- Source/serving DSNs using Docker service names
- Worker intervals and flags

## Validation rules in code
Pipeline startup fails fast when:

- DSNs are empty.
- Batch size is not positive.
- Poll/projection/recompute intervals are not positive.

## Practical tuning guidance

### Latency tuning
To reduce end-to-end latency:

- Lower `POLL_INTERVAL_SECONDS`.
- Lower `PROJECTION_INTERVAL_SECONDS`.
- Keep recompute window only as large as needed for late updates.

### Throughput tuning
For higher change volume:

- Increase `CDC_BATCH_MAX_ROWS`.
- Consider reducing projection frequency while increasing batch size.

### Correctness vs cost
`PROJECTION_RECOMPUTE_WINDOW_MINUTES` is the main correctness/cost lever:

- Larger window handles later updates better.
- Smaller window reduces projection compute and write costs.
