package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"shori-realtime/pipeline/internal/cdc"
	"shori-realtime/pipeline/internal/config"
	"shori-realtime/pipeline/internal/db"
	"shori-realtime/pipeline/internal/metadata"
	"shori-realtime/pipeline/internal/projections"
	"shori-realtime/pipeline/internal/staging"
)

type runnable interface {
	Run(ctx context.Context)
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	logger := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceDB, err := db.OpenWithRetry(ctx, cfg.SourceDSN, 90, 2*time.Second)
	if err != nil {
		logger.Error("unable to connect source DB", "error", err)
		os.Exit(1)
	}
	defer sourceDB.Close()

	servingDB, err := db.OpenWithRetry(ctx, cfg.ServingDSN, 90, 2*time.Second)
	if err != nil {
		logger.Error("unable to connect serving DB", "error", err)
		os.Exit(1)
	}
	defer servingDB.Close()

	metadataRepo := metadata.NewRepository(servingDB)
	stagingRepo := staging.NewRepository(servingDB)

	captures := []string{"dbo_customers", "dbo_orders", "dbo_payments"}
	projectionMap := map[string][]string{}
	if cfg.EnableProjOrdersKPI {
		projectionMap[projections.ProjectionOrdersKPI] = []string{"dbo_customers", "dbo_orders", "dbo_payments"}
	}
	if cfg.EnableProjOrdersLatest {
		projectionMap[projections.ProjectionOrdersLatest] = []string{"dbo_customers", "dbo_orders"}
	}

	if err := metadataRepo.EnsureBootstrap(ctx, cfg.SourceName, captures, projectionMap); err != nil {
		logger.Error("unable to bootstrap metadata rows", "error", err)
		os.Exit(1)
	}

	workers := make([]runnable, 0, 8)
	workers = append(workers,
		cdc.NewIngestor(logger, sourceDB, servingDB, metadataRepo, stagingRepo, cfg.SourceName, cdc.CaptureSpec{CaptureInstance: "dbo_customers", StageTable: "dbo.stg_cdc_customers"}, cfg.CDCBatchMaxRows, cfg.PollInterval),
		cdc.NewIngestor(logger, sourceDB, servingDB, metadataRepo, stagingRepo, cfg.SourceName, cdc.CaptureSpec{CaptureInstance: "dbo_orders", StageTable: "dbo.stg_cdc_orders"}, cfg.CDCBatchMaxRows, cfg.PollInterval),
		cdc.NewIngestor(logger, sourceDB, servingDB, metadataRepo, stagingRepo, cfg.SourceName, cdc.CaptureSpec{CaptureInstance: "dbo_payments", StageTable: "dbo.stg_cdc_payments"}, cfg.CDCBatchMaxRows, cfg.PollInterval),
	)

	if cfg.EnableProjOrdersKPI {
		workers = append(workers, projections.NewKPIWorker(logger, servingDB, metadataRepo, stagingRepo, cfg.SourceName, cfg.ProjectionInterval, cfg.ProjectionRecomputeWindow))
	}
	if cfg.EnableProjOrdersLatest {
		workers = append(workers, projections.NewOrdersLatestWorker(logger, servingDB, metadataRepo, stagingRepo, cfg.SourceName, cfg.ProjectionInterval))
	}

	logger.Info("pipeline started", "workers", len(workers))
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(w runnable) {
			defer wg.Done()
			w.Run(ctx)
		}(worker)
	}

	<-ctx.Done()
	logger.Info("shutdown signal received")
	wg.Wait()
	logger.Info("pipeline stopped")
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
