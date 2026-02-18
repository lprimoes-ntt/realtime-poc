package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"shori-realtime/pipeline/internal/util"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) EnsureBootstrap(ctx context.Context, sourceName string, captures []string, projections map[string][]string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, capture := range captures {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO dbo.ctl_ingestion_watermarks (source_name, capture_instance, last_ingested_lsn, updated_at)
SELECT @p1, @p2, @p3, SYSUTCDATETIME()
WHERE NOT EXISTS (
  SELECT 1
  FROM dbo.ctl_ingestion_watermarks
  WHERE source_name = @p1
    AND capture_instance = @p2
);`, sourceName, capture, util.ZeroLSN); err != nil {
			return fmt.Errorf("bootstrap ingestion watermark %s: %w", capture, err)
		}
	}

	for projection, projectionCaptures := range projections {
		for _, capture := range projectionCaptures {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO dbo.ctl_projection_checkpoints (projection_name, capture_instance, last_consumed_lsn, updated_at)
SELECT @p1, @p2, @p3, SYSUTCDATETIME()
WHERE NOT EXISTS (
  SELECT 1
  FROM dbo.ctl_projection_checkpoints
  WHERE projection_name = @p1
    AND capture_instance = @p2
);`, projection, capture, util.ZeroLSN); err != nil {
				return fmt.Errorf("bootstrap projection checkpoint %s/%s: %w", projection, capture, err)
			}
		}

		if _, err := tx.ExecContext(ctx, `
INSERT INTO dbo.ctl_projection_metadata (projection_name, status)
SELECT @p1, 'INIT'
WHERE NOT EXISTS (
  SELECT 1
  FROM dbo.ctl_projection_metadata
  WHERE projection_name = @p1
);`, projection); err != nil {
			return fmt.Errorf("bootstrap projection metadata %s: %w", projection, err)
		}
	}

	return tx.Commit()
}

func (r *Repository) GetIngestionWatermark(ctx context.Context, sourceName, capture string) ([]byte, error) {
	var lsn []byte
	err := r.db.QueryRowContext(ctx, `
SELECT last_ingested_lsn
FROM dbo.ctl_ingestion_watermarks
WHERE source_name = @p1
  AND capture_instance = @p2;`, sourceName, capture).Scan(&lsn)
	if err != nil {
		if err == sql.ErrNoRows {
			return util.ZeroLSN, nil
		}
		return nil, err
	}
	return util.PadLSN(lsn), nil
}

func (r *Repository) GetMinIngestionWatermark(ctx context.Context, sourceName string, captures []string) ([]byte, error) {
	if len(captures) == 0 {
		return nil, fmt.Errorf("captures cannot be empty")
	}

	placeholders := make([]string, 0, len(captures))
	args := make([]any, 0, len(captures)+1)
	args = append(args, sourceName)
	for i, c := range captures {
		placeholders = append(placeholders, fmt.Sprintf("@p%d", i+2))
		args = append(args, c)
	}

	query := fmt.Sprintf(`
SELECT MIN(last_ingested_lsn)
FROM dbo.ctl_ingestion_watermarks
WHERE source_name = @p1
  AND capture_instance IN (%s);`, strings.Join(placeholders, ", "))

	var lsn []byte
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&lsn); err != nil {
		return nil, err
	}
	if len(lsn) == 0 {
		return util.ZeroLSN, nil
	}
	return util.PadLSN(lsn), nil
}

func (r *Repository) UpdateIngestionWatermarkTx(ctx context.Context, tx *sql.Tx, sourceName, capture string, lsn []byte) error {
	_, err := tx.ExecContext(ctx, `
UPDATE dbo.ctl_ingestion_watermarks
SET last_ingested_lsn = @p3,
    updated_at = SYSUTCDATETIME()
WHERE source_name = @p1
  AND capture_instance = @p2;`, sourceName, capture, util.PadLSN(lsn))
	return err
}

func (r *Repository) GetProjectionCheckpoints(ctx context.Context, projection string, captures []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(captures))
	for _, capture := range captures {
		result[capture] = util.ZeroLSN
	}

	if len(captures) == 0 {
		return result, nil
	}

	placeholders := make([]string, 0, len(captures))
	args := make([]any, 0, len(captures)+1)
	args = append(args, projection)
	for i, c := range captures {
		placeholders = append(placeholders, fmt.Sprintf("@p%d", i+2))
		args = append(args, c)
	}

	query := fmt.Sprintf(`
SELECT capture_instance, last_consumed_lsn
FROM dbo.ctl_projection_checkpoints
WHERE projection_name = @p1
  AND capture_instance IN (%s);`, strings.Join(placeholders, ", "))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var capture string
		var lsn []byte
		if err := rows.Scan(&capture, &lsn); err != nil {
			return nil, err
		}
		result[capture] = util.PadLSN(lsn)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Repository) UpdateProjectionCheckpointTx(ctx context.Context, tx *sql.Tx, projection, capture string, lsn []byte) error {
	_, err := tx.ExecContext(ctx, `
UPDATE dbo.ctl_projection_checkpoints
SET last_consumed_lsn = @p3,
    updated_at = SYSUTCDATETIME()
WHERE projection_name = @p1
  AND capture_instance = @p2;`, projection, capture, util.PadLSN(lsn))
	return err
}

func (r *Repository) UpsertProjectionMetadataTx(ctx context.Context, tx *sql.Tx, projection string, cutoffLSN []byte, status string, lastError *string) error {
	res, err := tx.ExecContext(ctx, `
UPDATE dbo.ctl_projection_metadata
SET as_of_lsn = @p2,
    as_of_time = SYSUTCDATETIME(),
    built_at = SYSUTCDATETIME(),
    status = @p3,
    last_error = @p4
WHERE projection_name = @p1;`, projection, util.PadLSN(cutoffLSN), status, nullableString(lastError))
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}

	_, err = tx.ExecContext(ctx, `
INSERT INTO dbo.ctl_projection_metadata (projection_name, as_of_lsn, as_of_time, built_at, status, last_error)
VALUES (@p1, @p2, SYSUTCDATETIME(), SYSUTCDATETIME(), @p3, @p4);`, projection, util.PadLSN(cutoffLSN), status, nullableString(lastError))
	return err
}

func (r *Repository) MarkProjectionError(ctx context.Context, projection string, errText string) error {
	if len(errText) > 3900 {
		errText = errText[:3900]
	}
	_, err := r.db.ExecContext(ctx, `
UPDATE dbo.ctl_projection_metadata
SET status = 'ERROR',
    last_error = @p2,
    built_at = SYSUTCDATETIME()
WHERE projection_name = @p1;`, projection, errText)
	return err
}

func SortedKeys(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func nullableString(v *string) any {
	if v == nil {
		return nil
	}
	if strings.TrimSpace(*v) == "" {
		return nil
	}
	return *v
}
