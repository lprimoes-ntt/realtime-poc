package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	SourceDSN                 string
	ServingDSN                string
	PollInterval              time.Duration
	CDCBatchMaxRows           int
	ProjectionInterval        time.Duration
	ProjectionRecomputeWindow time.Duration
	EnableProjOrdersKPI       bool
	EnableProjOrdersLatest    bool
	LogLevel                  string
	SourceName                string
}

func Load() (Config, error) {
	cfg := Config{
		SourceDSN:                 strings.TrimSpace(os.Getenv("SOURCE_DSN")),
		ServingDSN:                strings.TrimSpace(os.Getenv("SERVING_DSN")),
		PollInterval:              time.Duration(getInt("POLL_INTERVAL_SECONDS", 5)) * time.Second,
		CDCBatchMaxRows:           getInt("CDC_BATCH_MAX_ROWS", 5000),
		ProjectionInterval:        time.Duration(getInt("PROJECTION_INTERVAL_SECONDS", 15)) * time.Second,
		ProjectionRecomputeWindow: time.Duration(getInt("PROJECTION_RECOMPUTE_WINDOW_MINUTES", 15)) * time.Minute,
		EnableProjOrdersKPI:       getBool("ENABLE_PROJ_ORDERS_KPI", true),
		EnableProjOrdersLatest:    getBool("ENABLE_PROJ_ORDERS_LATEST", false),
		LogLevel:                  strings.ToLower(strings.TrimSpace(getString("LOG_LEVEL", "info"))),
		SourceName:                getString("SOURCE_NAME", "source1"),
	}

	if cfg.SourceDSN == "" {
		return Config{}, fmt.Errorf("SOURCE_DSN is required")
	}
	if cfg.ServingDSN == "" {
		return Config{}, fmt.Errorf("SERVING_DSN is required")
	}
	if cfg.CDCBatchMaxRows <= 0 {
		return Config{}, fmt.Errorf("CDC_BATCH_MAX_ROWS must be > 0")
	}
	if cfg.PollInterval <= 0 {
		return Config{}, fmt.Errorf("POLL_INTERVAL_SECONDS must be > 0")
	}
	if cfg.ProjectionInterval <= 0 {
		return Config{}, fmt.Errorf("PROJECTION_INTERVAL_SECONDS must be > 0")
	}
	if cfg.ProjectionRecomputeWindow <= 0 {
		return Config{}, fmt.Errorf("PROJECTION_RECOMPUTE_WINDOW_MINUTES must be > 0")
	}
	return cfg, nil
}

func getInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getBool(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func getString(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
