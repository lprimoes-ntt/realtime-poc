package staging

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"shori-realtime/pipeline/internal/util"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

type CustomerChange struct {
	LSN        []byte
	SeqVal     []byte
	Op         uint8
	CustomerID int
	Segment    sql.NullString
	IsActive   sql.NullBool
	UpdatedAt  sql.NullTime
}

type OrderChange struct {
	LSN        []byte
	SeqVal     []byte
	Op         uint8
	OrderID    int64
	CustomerID sql.NullInt32
	Amount     sql.NullFloat64
	Status     sql.NullString
	CreatedAt  sql.NullTime
	UpdatedAt  sql.NullTime
}

type PaymentChange struct {
	LSN        []byte
	SeqVal     []byte
	Op         uint8
	PaymentID  int64
	OrderID    sql.NullInt64
	PaidAmount sql.NullFloat64
	PaidAt     sql.NullTime
}

func (r *Repository) InsertCustomersTx(ctx context.Context, tx *sql.Tx, rows []CustomerChange) error {
	for _, row := range rows {
		_, err := tx.ExecContext(ctx, `
INSERT INTO dbo.stg_cdc_customers (lsn, seqval, op, customer_id, segment, is_active, updated_at)
SELECT @p1, @p2, @p3, @p4, @p5, @p6, @p7
WHERE NOT EXISTS (
  SELECT 1
  FROM dbo.stg_cdc_customers
  WHERE lsn = @p1
    AND seqval = @p2
    AND customer_id = @p4
);`, util.PadLSN(row.LSN), util.PadLSN(row.SeqVal), row.Op, row.CustomerID, nullableString(row.Segment), nullableBool(row.IsActive), nullableTime(row.UpdatedAt))
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) InsertOrdersTx(ctx context.Context, tx *sql.Tx, rows []OrderChange) error {
	for _, row := range rows {
		_, err := tx.ExecContext(ctx, `
INSERT INTO dbo.stg_cdc_orders (lsn, seqval, op, order_id, customer_id, amount, status, created_at, updated_at)
SELECT @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9
WHERE NOT EXISTS (
  SELECT 1
  FROM dbo.stg_cdc_orders
  WHERE lsn = @p1
    AND seqval = @p2
    AND order_id = @p4
);`, util.PadLSN(row.LSN), util.PadLSN(row.SeqVal), row.Op, row.OrderID, nullableInt32(row.CustomerID), nullableFloat(row.Amount), nullableString(row.Status), nullableTime(row.CreatedAt), nullableTime(row.UpdatedAt))
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) InsertPaymentsTx(ctx context.Context, tx *sql.Tx, rows []PaymentChange) error {
	for _, row := range rows {
		_, err := tx.ExecContext(ctx, `
INSERT INTO dbo.stg_cdc_payments (lsn, seqval, op, payment_id, order_id, paid_amount, paid_at)
SELECT @p1, @p2, @p3, @p4, @p5, @p6, @p7
WHERE NOT EXISTS (
  SELECT 1
  FROM dbo.stg_cdc_payments
  WHERE lsn = @p1
    AND seqval = @p2
    AND payment_id = @p4
);`, util.PadLSN(row.LSN), util.PadLSN(row.SeqVal), row.Op, row.PaymentID, nullableInt64(row.OrderID), nullableFloat(row.PaidAmount), nullableTime(row.PaidAt))
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) HasDeltas(ctx context.Context, table string, fromLSN, toLSN []byte) (bool, error) {
	if strings.TrimSpace(table) == "" {
		return false, fmt.Errorf("table name is required")
	}
	query := fmt.Sprintf(`
SELECT TOP (1) 1
FROM %s
WHERE lsn > @p1
  AND lsn <= @p2;`, table)

	var marker int
	err := r.db.QueryRowContext(ctx, query, util.PadLSN(fromLSN), util.PadLSN(toLSN)).Scan(&marker)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return marker == 1, nil
}

func (r *Repository) LoadCustomersAll(ctx context.Context) ([]CustomerChange, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT lsn, seqval, op, customer_id, segment, is_active, updated_at
FROM dbo.stg_cdc_customers
ORDER BY lsn, seqval;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]CustomerChange, 0, 1024)
	for rows.Next() {
		var c CustomerChange
		var op int
		if err := rows.Scan(&c.LSN, &c.SeqVal, &op, &c.CustomerID, &c.Segment, &c.IsActive, &c.UpdatedAt); err != nil {
			return nil, err
		}
		c.Op = uint8(op)
		result = append(result, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Repository) LoadOrdersWindow(ctx context.Context, windowStart time.Time) ([]OrderChange, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT lsn, seqval, op, order_id, customer_id, amount, status, created_at, updated_at
FROM dbo.stg_cdc_orders
WHERE created_at >= @p1
ORDER BY lsn, seqval;`, windowStart.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]OrderChange, 0, 1024)
	for rows.Next() {
		var o OrderChange
		var op int
		if err := rows.Scan(&o.LSN, &o.SeqVal, &op, &o.OrderID, &o.CustomerID, &o.Amount, &o.Status, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		o.Op = uint8(op)
		result = append(result, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Repository) LoadPaymentsWindow(ctx context.Context, windowStart time.Time) ([]PaymentChange, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT lsn, seqval, op, payment_id, order_id, paid_amount, paid_at
FROM dbo.stg_cdc_payments
WHERE paid_at >= @p1
ORDER BY lsn, seqval;`, windowStart.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]PaymentChange, 0, 1024)
	for rows.Next() {
		var p PaymentChange
		var op int
		if err := rows.Scan(&p.LSN, &p.SeqVal, &op, &p.PaymentID, &p.OrderID, &p.PaidAmount, &p.PaidAt); err != nil {
			return nil, err
		}
		p.Op = uint8(op)
		result = append(result, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Repository) LoadOrdersAll(ctx context.Context) ([]OrderChange, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT lsn, seqval, op, order_id, customer_id, amount, status, created_at, updated_at
FROM dbo.stg_cdc_orders
ORDER BY lsn, seqval;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]OrderChange, 0, 2048)
	for rows.Next() {
		var o OrderChange
		var op int
		if err := rows.Scan(&o.LSN, &o.SeqVal, &op, &o.OrderID, &o.CustomerID, &o.Amount, &o.Status, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		o.Op = uint8(op)
		result = append(result, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
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

func nullableFloat(v sql.NullFloat64) any {
	if !v.Valid {
		return nil
	}
	return v.Float64
}
