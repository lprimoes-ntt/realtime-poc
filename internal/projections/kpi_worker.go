package projections

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"shori-realtime/pipeline/internal/metadata"
	"shori-realtime/pipeline/internal/staging"
	"shori-realtime/pipeline/internal/util"
)

type KPIWorker struct {
	log             *slog.Logger
	servingDB       *sql.DB
	metadata        *metadata.Repository
	staging         *staging.Repository
	sourceName      string
	interval        time.Duration
	recomputeWindow time.Duration
	captures        []string
}

func NewKPIWorker(
	log *slog.Logger,
	servingDB *sql.DB,
	metadataRepo *metadata.Repository,
	stagingRepo *staging.Repository,
	sourceName string,
	interval time.Duration,
	recomputeWindow time.Duration,
) *KPIWorker {
	return &KPIWorker{
		log:             log.With("worker", "projection", "projection", ProjectionOrdersKPI),
		servingDB:       servingDB,
		metadata:        metadataRepo,
		staging:         stagingRepo,
		sourceName:      sourceName,
		interval:        interval,
		recomputeWindow: recomputeWindow,
		captures:        []string{"dbo_customers", "dbo_orders", "dbo_payments"},
	}
}

func (w *KPIWorker) Run(ctx context.Context) {
	w.log.Info("projection worker started")
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		if err := w.runOnce(ctx); err != nil {
			w.log.Error("projection cycle failed", "error", err)
			_ = w.metadata.MarkProjectionError(ctx, ProjectionOrdersKPI, err.Error())
		}

		select {
		case <-ctx.Done():
			w.log.Info("projection worker stopped")
			return
		case <-ticker.C:
		}
	}
}

func (w *KPIWorker) runOnce(ctx context.Context) error {
	cutoffLSN, err := w.metadata.GetMinIngestionWatermark(ctx, w.sourceName, w.captures)
	if err != nil {
		return fmt.Errorf("get cutoff lsn: %w", err)
	}
	if util.IsZeroLSN(cutoffLSN) {
		return nil
	}

	checkpoints, err := w.metadata.GetProjectionCheckpoints(ctx, ProjectionOrdersKPI, w.captures)
	if err != nil {
		return fmt.Errorf("get projection checkpoints: %w", err)
	}

	hasDelta, err := w.hasDeltasSinceCheckpoint(ctx, checkpoints, cutoffLSN)
	if err != nil {
		return fmt.Errorf("delta check: %w", err)
	}
	if !hasDelta {
		return nil
	}

	windowStart := floorToMinute(time.Now().UTC()).Add(-w.recomputeWindow)
	orders, err := w.staging.LoadOrdersAll(ctx)
	if err != nil {
		return fmt.Errorf("load orders staging: %w", err)
	}
	customers, err := w.staging.LoadCustomersAll(ctx)
	if err != nil {
		return fmt.Errorf("load customers staging: %w", err)
	}
	payments, err := w.staging.LoadPaymentsWindow(ctx, windowStart)
	if err != nil {
		return fmt.Errorf("load payments staging: %w", err)
	}

	kpiRows, err := computeKPIWithDuckDB(windowStart, orders, customers, payments)
	if err != nil {
		return fmt.Errorf("duckdb kpi transform: %w", err)
	}

	tx, err := w.servingDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
DELETE FROM dbo.proj_orders_kpi_by_minute_segment
WHERE minute_bucket >= @p1;`, windowStart.UTC()); err != nil {
		return fmt.Errorf("delete kpi window: %w", err)
	}

	for _, row := range kpiRows {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO dbo.proj_orders_kpi_by_minute_segment
  (minute_bucket, segment, orders_count, orders_amount_sum, paid_amount_sum)
VALUES (@p1, @p2, @p3, @p4, @p5);`, row.MinuteBucket.UTC(), row.Segment, row.OrdersCount, row.OrdersAmountSum, row.PaidAmountSum); err != nil {
			return fmt.Errorf("insert kpi row: %w", err)
		}
	}

	for _, capture := range metadata.SortedKeys(checkpoints) {
		if err := w.metadata.UpdateProjectionCheckpointTx(ctx, tx, ProjectionOrdersKPI, capture, cutoffLSN); err != nil {
			return fmt.Errorf("update projection checkpoint %s: %w", capture, err)
		}
	}
	if err := w.metadata.UpsertProjectionMetadataTx(ctx, tx, ProjectionOrdersKPI, cutoffLSN, "OK", nil); err != nil {
		return fmt.Errorf("update projection metadata: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	w.log.Info("projection built", "rows", len(kpiRows), "window_start", windowStart.Format(time.RFC3339), "as_of_lsn", util.LSNHex(cutoffLSN))
	return nil
}

func (w *KPIWorker) hasDeltasSinceCheckpoint(ctx context.Context, checkpoints map[string][]byte, cutoffLSN []byte) (bool, error) {
	tables := map[string]string{
		"dbo_customers": "dbo.stg_cdc_customers",
		"dbo_orders":    "dbo.stg_cdc_orders",
		"dbo_payments":  "dbo.stg_cdc_payments",
	}

	for capture, from := range checkpoints {
		table, ok := tables[capture]
		if !ok {
			continue
		}
		hasDelta, err := w.staging.HasDeltas(ctx, table, from, cutoffLSN)
		if err != nil {
			return false, err
		}
		if hasDelta {
			return true, nil
		}
	}
	return false, nil
}

func computeKPIWithDuckDB(
	windowStart time.Time,
	orders []staging.OrderChange,
	customers []staging.CustomerChange,
	payments []staging.PaymentChange,
) ([]KPIOutputRow, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if _, err := db.Exec(`
CREATE TEMP TABLE orders_delta (
  lsn BLOB,
  seqval BLOB,
  op UTINYINT,
  order_id BIGINT,
  customer_id INTEGER,
  amount DOUBLE,
  status VARCHAR,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
CREATE TEMP TABLE customers_delta (
  lsn BLOB,
  seqval BLOB,
  op UTINYINT,
  customer_id INTEGER,
  segment VARCHAR,
  is_active BOOLEAN,
  updated_at TIMESTAMP
);
CREATE TEMP TABLE payments_delta (
  lsn BLOB,
  seqval BLOB,
  op UTINYINT,
  payment_id BIGINT,
  order_id BIGINT,
  paid_amount DOUBLE,
  paid_at TIMESTAMP
);
CREATE TEMP TABLE projection_params (
  window_start TIMESTAMP
);`); err != nil {
		return nil, err
	}

	if _, err := db.Exec(`INSERT INTO projection_params(window_start) VALUES (?);`, windowStart.UTC()); err != nil {
		return nil, err
	}

	if err := insertOrdersIntoDuckDB(db, orders); err != nil {
		return nil, err
	}
	if err := insertCustomersIntoDuckDB(db, customers); err != nil {
		return nil, err
	}
	if err := insertPaymentsIntoDuckDB(db, payments); err != nil {
		return nil, err
	}

	if _, err := db.Exec(`
CREATE OR REPLACE TEMP VIEW orders_ranked AS
SELECT
  lsn,
  seqval,
  op,
  order_id,
  customer_id,
  amount,
  status,
  created_at,
  updated_at,
  ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY lsn DESC, seqval DESC) AS rn
FROM orders_delta
WHERE op IN (1, 2, 4);

CREATE OR REPLACE TEMP VIEW orders_latest_all AS
SELECT *
FROM orders_ranked
WHERE rn = 1
  AND op <> 1;

CREATE OR REPLACE TEMP VIEW customers_ranked AS
SELECT
  lsn,
  seqval,
  op,
  customer_id,
  segment,
  is_active,
  updated_at,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY lsn DESC, seqval DESC) AS rn
FROM customers_delta
WHERE op IN (1, 2, 4);

CREATE OR REPLACE TEMP VIEW customers_latest_all AS
SELECT *
FROM customers_ranked
WHERE rn = 1
  AND op <> 1;

CREATE OR REPLACE TEMP VIEW payments_ranked AS
SELECT
  lsn,
  seqval,
  op,
  payment_id,
  order_id,
  paid_amount,
  paid_at,
  ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY lsn DESC, seqval DESC) AS rn
FROM payments_delta
WHERE op IN (1, 2, 4);

CREATE OR REPLACE TEMP VIEW payments_latest_all AS
SELECT *
FROM payments_ranked
WHERE rn = 1
  AND op <> 1;

CREATE OR REPLACE TEMP VIEW orders_enriched AS
SELECT
  date_trunc('minute', o.created_at) AS minute_bucket,
  COALESCE(c.segment, 'UNKNOWN') AS segment,
  o.order_id,
  COALESCE(o.amount, 0) AS amount
FROM orders_latest_all o
LEFT JOIN customers_latest_all c
  ON c.customer_id = o.customer_id
WHERE o.created_at >= (SELECT window_start FROM projection_params LIMIT 1);

CREATE OR REPLACE TEMP VIEW payments_enriched AS
SELECT
  date_trunc('minute', p.paid_at) AS minute_bucket,
  COALESCE(c.segment, 'UNKNOWN') AS segment,
  COALESCE(p.paid_amount, 0) AS paid_amount
FROM payments_latest_all p
JOIN orders_latest_all o
  ON o.order_id = p.order_id
LEFT JOIN customers_latest_all c
  ON c.customer_id = o.customer_id
WHERE p.paid_at >= (SELECT window_start FROM projection_params LIMIT 1);

CREATE OR REPLACE TEMP VIEW kpi_orders AS
SELECT
  minute_bucket,
  segment,
  COUNT(DISTINCT order_id) AS orders_count,
  COALESCE(SUM(amount), 0) AS orders_amount_sum
FROM orders_enriched
GROUP BY minute_bucket, segment;

CREATE OR REPLACE TEMP VIEW kpi_payments AS
SELECT
  minute_bucket,
  segment,
  COALESCE(SUM(paid_amount), 0) AS paid_amount_sum
FROM payments_enriched
GROUP BY minute_bucket, segment;

CREATE OR REPLACE TEMP VIEW kpi_final AS
SELECT
  COALESCE(o.minute_bucket, p.minute_bucket) AS minute_bucket,
  COALESCE(o.segment, p.segment) AS segment,
  COALESCE(o.orders_count, 0) AS orders_count,
  COALESCE(o.orders_amount_sum, 0) AS orders_amount_sum,
  COALESCE(p.paid_amount_sum, 0) AS paid_amount_sum
FROM kpi_orders o
FULL OUTER JOIN kpi_payments p
  ON o.minute_bucket = p.minute_bucket
 AND o.segment = p.segment;
`); err != nil {
		return nil, err
	}

	rows, err := db.Query(`
SELECT minute_bucket, segment, orders_count, orders_amount_sum, paid_amount_sum
FROM kpi_final
ORDER BY minute_bucket, segment;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]KPIOutputRow, 0, 256)
	for rows.Next() {
		var row KPIOutputRow
		if err := rows.Scan(&row.MinuteBucket, &row.Segment, &row.OrdersCount, &row.OrdersAmountSum, &row.PaidAmountSum); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func insertOrdersIntoDuckDB(db *sql.DB, orders []staging.OrderChange) error {
	stmt, err := db.Prepare(`
INSERT INTO orders_delta (lsn, seqval, op, order_id, customer_id, amount, status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range orders {
		if _, err := stmt.Exec(
			util.PadLSN(row.LSN),
			util.PadLSN(row.SeqVal),
			row.Op,
			row.OrderID,
			nullableInt32(row.CustomerID),
			nullableFloat64(row.Amount),
			nullableString(row.Status),
			nullableTime(row.CreatedAt),
			nullableTime(row.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return nil
}

func insertCustomersIntoDuckDB(db *sql.DB, customers []staging.CustomerChange) error {
	stmt, err := db.Prepare(`
INSERT INTO customers_delta (lsn, seqval, op, customer_id, segment, is_active, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range customers {
		if _, err := stmt.Exec(
			util.PadLSN(row.LSN),
			util.PadLSN(row.SeqVal),
			row.Op,
			row.CustomerID,
			nullableString(row.Segment),
			nullableBool(row.IsActive),
			nullableTime(row.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return nil
}

func insertPaymentsIntoDuckDB(db *sql.DB, payments []staging.PaymentChange) error {
	stmt, err := db.Prepare(`
INSERT INTO payments_delta (lsn, seqval, op, payment_id, order_id, paid_amount, paid_at)
VALUES (?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range payments {
		if _, err := stmt.Exec(
			util.PadLSN(row.LSN),
			util.PadLSN(row.SeqVal),
			row.Op,
			row.PaymentID,
			nullableInt64(row.OrderID),
			nullableFloat64(row.PaidAmount),
			nullableTime(row.PaidAt),
		); err != nil {
			return err
		}
	}
	return nil
}

func nullableString(v sql.NullString) any {
	if !v.Valid {
		return nil
	}
	return v.String
}

func nullableBool(v sql.NullBool) any {
	if !v.Valid {
		return nil
	}
	return v.Bool
}

func nullableTime(v sql.NullTime) any {
	if !v.Valid {
		return nil
	}
	return v.Time.UTC()
}

func nullableInt32(v sql.NullInt32) any {
	if !v.Valid {
		return nil
	}
	return v.Int32
}

func nullableInt64(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

func nullableFloat64(v sql.NullFloat64) any {
	if !v.Valid {
		return nil
	}
	return v.Float64
}
