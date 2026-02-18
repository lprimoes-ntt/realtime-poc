package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"shori-realtime/pipeline/internal/metadata"
	"shori-realtime/pipeline/internal/staging"
	"shori-realtime/pipeline/internal/util"
)

type CaptureSpec struct {
	CaptureInstance string
	StageTable      string
}

type Ingestor struct {
	log        *slog.Logger
	sourceDB   *sql.DB
	servingDB  *sql.DB
	metadata   *metadata.Repository
	staging    *staging.Repository
	sourceName string
	spec       CaptureSpec
	batchSize  int
	interval   time.Duration
}

func NewIngestor(
	log *slog.Logger,
	sourceDB *sql.DB,
	servingDB *sql.DB,
	metadataRepo *metadata.Repository,
	stagingRepo *staging.Repository,
	sourceName string,
	spec CaptureSpec,
	batchSize int,
	interval time.Duration,
) *Ingestor {
	return &Ingestor{
		log:        log.With("worker", "ingestor", "capture_instance", spec.CaptureInstance),
		sourceDB:   sourceDB,
		servingDB:  servingDB,
		metadata:   metadataRepo,
		staging:    stagingRepo,
		sourceName: sourceName,
		spec:       spec,
		batchSize:  batchSize,
		interval:   interval,
	}
}

func (i *Ingestor) Run(ctx context.Context) {
	i.log.Info("ingestor started")
	ticker := time.NewTicker(i.interval)
	defer ticker.Stop()

	for {
		if err := i.runOnce(ctx); err != nil {
			i.log.Error("ingestion cycle failed", "error", err)
		}

		select {
		case <-ctx.Done():
			i.log.Info("ingestor stopped")
			return
		case <-ticker.C:
		}
	}
}

func (i *Ingestor) runOnce(ctx context.Context) error {
	lastLSN, err := i.metadata.GetIngestionWatermark(ctx, i.sourceName, i.spec.CaptureInstance)
	if err != nil {
		return fmt.Errorf("get ingestion watermark: %w", err)
	}

	minLSN, maxLSN, err := i.getLSNBounds(ctx)
	if err != nil {
		return fmt.Errorf("get cdc lsn bounds: %w", err)
	}
	if len(minLSN) == 0 || len(maxLSN) == 0 {
		return nil
	}

	fromLSN, err := i.computeFromLSN(ctx, lastLSN, minLSN)
	if err != nil {
		return fmt.Errorf("compute from_lsn: %w", err)
	}
	if util.CompareLSN(fromLSN, maxLSN) > 0 {
		return nil
	}

	switch i.spec.CaptureInstance {
	case "dbo_customers":
		rows, err := i.fetchCustomers(ctx, fromLSN, maxLSN)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		endLSN := rows[len(rows)-1].LSN
		return i.persistCustomers(ctx, rows, endLSN)
	case "dbo_orders":
		rows, err := i.fetchOrders(ctx, fromLSN, maxLSN)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		endLSN := rows[len(rows)-1].LSN
		return i.persistOrders(ctx, rows, endLSN)
	case "dbo_payments":
		rows, err := i.fetchPayments(ctx, fromLSN, maxLSN)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		endLSN := rows[len(rows)-1].LSN
		return i.persistPayments(ctx, rows, endLSN)
	default:
		return fmt.Errorf("unsupported capture instance: %s", i.spec.CaptureInstance)
	}
}

func (i *Ingestor) getLSNBounds(ctx context.Context) ([]byte, []byte, error) {
	var minLSN, maxLSN []byte
	if err := i.sourceDB.QueryRowContext(ctx, `
SELECT sys.fn_cdc_get_min_lsn(@p1), sys.fn_cdc_get_max_lsn();`, i.spec.CaptureInstance).Scan(&minLSN, &maxLSN); err != nil {
		return nil, nil, err
	}
	return util.PadLSN(minLSN), util.PadLSN(maxLSN), nil
}

func (i *Ingestor) computeFromLSN(ctx context.Context, lastLSN, minLSN []byte) ([]byte, error) {
	if util.IsZeroLSN(lastLSN) || util.CompareLSN(lastLSN, minLSN) < 0 {
		return util.PadLSN(minLSN), nil
	}

	var incremented []byte
	if err := i.sourceDB.QueryRowContext(ctx, `SELECT sys.fn_cdc_increment_lsn(@p1);`, util.PadLSN(lastLSN)).Scan(&incremented); err != nil {
		return nil, err
	}
	return util.PadLSN(incremented), nil
}

func (i *Ingestor) fetchCustomers(ctx context.Context, fromLSN, toLSN []byte) ([]staging.CustomerChange, error) {
	rows, err := i.sourceDB.QueryContext(ctx, `
SELECT TOP (@p1)
  __$start_lsn,
  __$seqval,
  __$operation,
  customer_id,
  segment,
  is_active,
  updated_at
FROM cdc.fn_cdc_get_all_changes_dbo_customers(@p2, @p3, 'all')
ORDER BY __$start_lsn, __$seqval;`, i.batchSize, util.PadLSN(fromLSN), util.PadLSN(toLSN))
	if err != nil {
		if isCDCWindowError(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	result := make([]staging.CustomerChange, 0, i.batchSize)
	for rows.Next() {
		var c staging.CustomerChange
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

func (i *Ingestor) fetchOrders(ctx context.Context, fromLSN, toLSN []byte) ([]staging.OrderChange, error) {
	rows, err := i.sourceDB.QueryContext(ctx, `
SELECT TOP (@p1)
  __$start_lsn,
  __$seqval,
  __$operation,
  order_id,
  customer_id,
  amount,
  status,
  created_at,
  updated_at
FROM cdc.fn_cdc_get_all_changes_dbo_orders(@p2, @p3, 'all')
ORDER BY __$start_lsn, __$seqval;`, i.batchSize, util.PadLSN(fromLSN), util.PadLSN(toLSN))
	if err != nil {
		if isCDCWindowError(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	result := make([]staging.OrderChange, 0, i.batchSize)
	for rows.Next() {
		var o staging.OrderChange
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

func (i *Ingestor) fetchPayments(ctx context.Context, fromLSN, toLSN []byte) ([]staging.PaymentChange, error) {
	rows, err := i.sourceDB.QueryContext(ctx, `
SELECT TOP (@p1)
  __$start_lsn,
  __$seqval,
  __$operation,
  payment_id,
  order_id,
  paid_amount,
  paid_at
FROM cdc.fn_cdc_get_all_changes_dbo_payments(@p2, @p3, 'all')
ORDER BY __$start_lsn, __$seqval;`, i.batchSize, util.PadLSN(fromLSN), util.PadLSN(toLSN))
	if err != nil {
		if isCDCWindowError(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	result := make([]staging.PaymentChange, 0, i.batchSize)
	for rows.Next() {
		var p staging.PaymentChange
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

func (i *Ingestor) persistCustomers(ctx context.Context, rows []staging.CustomerChange, endLSN []byte) error {
	tx, err := i.servingDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := i.staging.InsertCustomersTx(ctx, tx, rows); err != nil {
		return err
	}
	if err := i.metadata.UpdateIngestionWatermarkTx(ctx, tx, i.sourceName, i.spec.CaptureInstance, endLSN); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	i.log.Debug("ingested cdc batch", "rows", len(rows), "end_lsn", util.LSNHex(endLSN))
	return nil
}

func (i *Ingestor) persistOrders(ctx context.Context, rows []staging.OrderChange, endLSN []byte) error {
	tx, err := i.servingDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := i.staging.InsertOrdersTx(ctx, tx, rows); err != nil {
		return err
	}
	if err := i.metadata.UpdateIngestionWatermarkTx(ctx, tx, i.sourceName, i.spec.CaptureInstance, endLSN); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	i.log.Debug("ingested cdc batch", "rows", len(rows), "end_lsn", util.LSNHex(endLSN))
	return nil
}

func (i *Ingestor) persistPayments(ctx context.Context, rows []staging.PaymentChange, endLSN []byte) error {
	tx, err := i.servingDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := i.staging.InsertPaymentsTx(ctx, tx, rows); err != nil {
		return err
	}
	if err := i.metadata.UpdateIngestionWatermarkTx(ctx, tx, i.sourceName, i.spec.CaptureInstance, endLSN); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	i.log.Debug("ingested cdc batch", "rows", len(rows), "end_lsn", util.LSNHex(endLSN))
	return nil
}

func isCDCWindowError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	errText := err.Error()
	return containsAny(errText, []string{
		"insufficient number of arguments",
		"start LSN",
		"cannot be greater than",
	})
}

func containsAny(text string, patterns []string) bool {
	for _, p := range patterns {
		if p != "" && (len(text) >= len(p) && containsFold(text, p)) {
			return true
		}
	}
	return false
}

func containsFold(s, substr string) bool {
	return len(substr) == 0 || indexFold(s, substr) >= 0
}

func indexFold(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	for i := 0; i+len(substr) <= len(s); i++ {
		if equalFoldASCII(s[i:i+len(substr)], substr) {
			return i
		}
	}
	return -1
}

func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca := a[i]
		cb := b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca = ca + 32
		}
		if 'A' <= cb && cb <= 'Z' {
			cb = cb + 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}
