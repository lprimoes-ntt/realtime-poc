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

type OrdersLatestWorker struct {
	log        *slog.Logger
	servingDB  *sql.DB
	metadata   *metadata.Repository
	staging    *staging.Repository
	sourceName string
	interval   time.Duration
	captures   []string
}

func NewOrdersLatestWorker(
	log *slog.Logger,
	servingDB *sql.DB,
	metadataRepo *metadata.Repository,
	stagingRepo *staging.Repository,
	sourceName string,
	interval time.Duration,
) *OrdersLatestWorker {
	return &OrdersLatestWorker{
		log:        log.With("worker", "projection", "projection", ProjectionOrdersLatest),
		servingDB:  servingDB,
		metadata:   metadataRepo,
		staging:    stagingRepo,
		sourceName: sourceName,
		interval:   interval,
		captures:   []string{"dbo_customers", "dbo_orders"},
	}
}

func (w *OrdersLatestWorker) Run(ctx context.Context) {
	w.log.Info("projection worker started")
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		if err := w.runOnce(ctx); err != nil {
			w.log.Error("projection cycle failed", "error", err)
			_ = w.metadata.MarkProjectionError(ctx, ProjectionOrdersLatest, err.Error())
		}

		select {
		case <-ctx.Done():
			w.log.Info("projection worker stopped")
			return
		case <-ticker.C:
		}
	}
}

func (w *OrdersLatestWorker) runOnce(ctx context.Context) error {
	cutoffLSN, err := w.metadata.GetMinIngestionWatermark(ctx, w.sourceName, w.captures)
	if err != nil {
		return fmt.Errorf("get cutoff lsn: %w", err)
	}
	if util.IsZeroLSN(cutoffLSN) {
		return nil
	}

	checkpoints, err := w.metadata.GetProjectionCheckpoints(ctx, ProjectionOrdersLatest, w.captures)
	if err != nil {
		return fmt.Errorf("get projection checkpoints: %w", err)
	}

	ordersDelta, err := w.staging.HasDeltas(ctx, "dbo.stg_cdc_orders", checkpoints["dbo_orders"], cutoffLSN)
	if err != nil {
		return err
	}
	customersDelta, err := w.staging.HasDeltas(ctx, "dbo.stg_cdc_customers", checkpoints["dbo_customers"], cutoffLSN)
	if err != nil {
		return err
	}
	if !ordersDelta && !customersDelta {
		return nil
	}

	orders, err := w.staging.LoadOrdersAll(ctx)
	if err != nil {
		return fmt.Errorf("load orders staging: %w", err)
	}
	customers, err := w.staging.LoadCustomersAll(ctx)
	if err != nil {
		return fmt.Errorf("load customers staging: %w", err)
	}

	latestRows, err := computeOrdersLatestWithDuckDB(orders, customers)
	if err != nil {
		return fmt.Errorf("duckdb latest transform: %w", err)
	}

	tx, err := w.servingDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM dbo.proj_orders_latest;`); err != nil {
		return fmt.Errorf("clear proj_orders_latest: %w", err)
	}

	for _, row := range latestRows {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO dbo.proj_orders_latest
  (order_id, customer_id, segment, amount, status, created_at, updated_at, __source_lsn)
VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8);`,
			row.OrderID,
			row.CustomerID,
			row.Segment,
			row.Amount,
			row.Status,
			nullableTimePtr(row.CreatedAt),
			nullableTimePtr(row.UpdatedAt),
			util.PadLSN(row.SourceLSN),
		); err != nil {
			return fmt.Errorf("insert proj_orders_latest row: %w", err)
		}
	}

	for _, capture := range metadata.SortedKeys(checkpoints) {
		if err := w.metadata.UpdateProjectionCheckpointTx(ctx, tx, ProjectionOrdersLatest, capture, cutoffLSN); err != nil {
			return fmt.Errorf("update projection checkpoint %s: %w", capture, err)
		}
	}
	if err := w.metadata.UpsertProjectionMetadataTx(ctx, tx, ProjectionOrdersLatest, cutoffLSN, "OK", nil); err != nil {
		return fmt.Errorf("update projection metadata: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	w.log.Info("projection built", "rows", len(latestRows), "as_of_lsn", util.LSNHex(cutoffLSN))
	return nil
}

func computeOrdersLatestWithDuckDB(
	orders []staging.OrderChange,
	customers []staging.CustomerChange,
) ([]LatestOrderOutputRow, error) {
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
);`); err != nil {
		return nil, err
	}

	if err := insertOrdersIntoDuckDB(db, orders); err != nil {
		return nil, err
	}
	if err := insertCustomersIntoDuckDB(db, customers); err != nil {
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

CREATE OR REPLACE TEMP VIEW orders_latest AS
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

CREATE OR REPLACE TEMP VIEW customers_latest AS
SELECT *
FROM customers_ranked
WHERE rn = 1
  AND op <> 1;

CREATE OR REPLACE TEMP VIEW orders_latest_enriched AS
SELECT
  o.order_id,
  o.customer_id,
  c.segment,
  o.amount,
  o.status,
  o.created_at,
  o.updated_at,
  o.lsn
FROM orders_latest o
LEFT JOIN customers_latest c
  ON c.customer_id = o.customer_id;
`); err != nil {
		return nil, err
	}

	rows, err := db.Query(`
SELECT
  order_id,
  customer_id,
  segment,
  amount,
  status,
  created_at,
  updated_at,
  lsn
FROM orders_latest_enriched
ORDER BY order_id;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]LatestOrderOutputRow, 0, 1024)
	for rows.Next() {
		var row LatestOrderOutputRow
		var customerID sql.NullInt32
		var segment sql.NullString
		var amount sql.NullFloat64
		var status sql.NullString
		var createdAt sql.NullTime
		var updatedAt sql.NullTime
		if err := rows.Scan(&row.OrderID, &customerID, &segment, &amount, &status, &createdAt, &updatedAt, &row.SourceLSN); err != nil {
			return nil, err
		}
		if customerID.Valid {
			v := customerID.Int32
			row.CustomerID = &v
		}
		if segment.Valid {
			v := segment.String
			row.Segment = &v
		}
		if amount.Valid {
			v := amount.Float64
			row.Amount = &v
		}
		if status.Valid {
			v := status.String
			row.Status = &v
		}
		if createdAt.Valid {
			v := createdAt.Time.UTC()
			row.CreatedAt = &v
		}
		if updatedAt.Valid {
			v := updatedAt.Time.UTC()
			row.UpdatedAt = &v
		}
		row.SourceLSN = util.PadLSN(row.SourceLSN)
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func nullableTimePtr(v *time.Time) any {
	if v == nil {
		return nil
	}
	return v.UTC()
}
