package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

func OpenWithRetry(ctx context.Context, dsn string, attempts int, delay time.Duration) (*sql.DB, error) {
	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for i := 1; i <= attempts; i++ {
		db, err := sql.Open("sqlserver", dsn)
		if err != nil {
			lastErr = err
		} else {
			db.SetMaxIdleConns(10)
			db.SetMaxOpenConns(25)
			db.SetConnMaxIdleTime(5 * time.Minute)
			db.SetConnMaxLifetime(30 * time.Minute)

			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = db.PingContext(pingCtx)
			cancel()
			if err == nil {
				return db, nil
			}
			lastErr = err
			_ = db.Close()
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("connection failed with unknown error")
	}
	return nil, fmt.Errorf("open with retry exhausted: %w", lastErr)
}
