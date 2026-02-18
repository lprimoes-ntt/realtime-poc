# Data Model

## Source database model
Defined under `sql/source/10_schema.sql`.

### `dbo.customers`
- `customer_id` (PK, identity)
- `segment`
- `is_active`
- `updated_at`

### `dbo.orders`
- `order_id` (PK, identity)
- `customer_id`
- `amount`
- `status`
- `created_at`
- `updated_at`

### `dbo.payments`
- `payment_id` (PK, identity)
- `order_id`
- `paid_amount`
- `paid_at`

CDC is enabled in `sql/source/30_enable_cdc.sql`.

## Serving control model
Defined under `sql/serving/10_schema.sql`.

### `dbo.ctl_ingestion_watermarks`
Purpose:

- Tracks ingestor progress per source + capture instance.

Key fields:

- `source_name`
- `capture_instance`
- `last_ingested_lsn`
- `updated_at`

### `dbo.ctl_projection_checkpoints`
Purpose:

- Tracks projection consumption progress per projection + capture instance.

Key fields:

- `projection_name`
- `capture_instance`
- `last_consumed_lsn`
- `updated_at`

### `dbo.ctl_projection_metadata`
Purpose:

- Human-readable projection health and freshness.

Key fields:

- `projection_name`
- `status` (`INIT`, `OK`, `ERROR`)
- `as_of_lsn`
- `as_of_time`
- `built_at`
- `last_error`

## Serving staging model

### `dbo.stg_cdc_customers`
Stores raw customer CDC rows with:

- `lsn`, `seqval`, `op`
- customer payload fields
- `ingested_at`

Uniqueness:

- `(lsn, seqval, customer_id)`

### `dbo.stg_cdc_orders`
Stores raw order CDC rows with:

- `lsn`, `seqval`, `op`
- order payload fields
- `ingested_at`

Uniqueness:

- `(lsn, seqval, order_id)`

### `dbo.stg_cdc_payments`
Stores raw payment CDC rows with:

- `lsn`, `seqval`, `op`
- payment payload fields
- `ingested_at`

Uniqueness:

- `(lsn, seqval, payment_id)`

## Projection model

### `dbo.proj_orders_kpi_by_minute_segment`
Grain:

- One row per `(minute_bucket, segment)`.

Metrics:

- `orders_count`
- `orders_amount_sum`
- `paid_amount_sum`

Usage:

- Fast serving table for DirectQuery-like access patterns.

### `dbo.proj_orders_latest`
Grain:

- One row per `order_id`.

Payload:

- Current order attributes + customer segment.
- `__source_lsn` for source ordering.

Usage:

- Serving-friendly latest-state lookup.

## CDC operation handling
SQL Server CDC operation codes used by workers:

- `1`: delete
- `2`: insert
- `3`: update-before image (ignored in projections)
- `4`: update-after image

Projection collapse logic uses latest `(lsn, seqval)` by business key and excludes latest delete rows.
