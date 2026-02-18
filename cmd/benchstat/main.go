package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"shori-realtime/pipeline/internal/util"
)

var captureToStaging = map[string]string{
	"dbo_customers": "dbo.stg_cdc_customers",
	"dbo_orders":    "dbo.stg_cdc_orders",
	"dbo_payments":  "dbo.stg_cdc_payments",
}

type ingestionItem struct {
	CaptureInstance string
	BacklogSeconds  *float64
	LagSeconds      *float64 // backward compatibility
	Rows1m          int64
}

type projectionItem struct {
	ProjectionName    string
	Status            string
	BacklogSeconds    *float64
	BuildDelaySeconds *float64
	LagSeconds        *float64 // backward compatibility
}

type sample struct {
	Timestamp                  time.Time
	IngestionMaxBacklogSec     float64
	ProjectionMaxBacklogSec    float64
	ProjectionMaxBuildDelaySec float64
	Rows1mTotal                int64
	ProjectionErrors           int
}

type summary struct {
	Scenario                   string    `json:"scenario"`
	StartedAt                  time.Time `json:"started_at"`
	EndedAt                    time.Time `json:"ended_at"`
	DurationSeconds            float64   `json:"duration_seconds"`
	SampleCount                int       `json:"sample_count"`
	PollErrors                 int       `json:"poll_errors"`
	IngestionBacklogP50        float64   `json:"ingestion_backlog_p50_seconds"`
	IngestionBacklogP95        float64   `json:"ingestion_backlog_p95_seconds"`
	IngestionBacklogP99        float64   `json:"ingestion_backlog_p99_seconds"`
	IngestionBacklogMax        float64   `json:"ingestion_backlog_max_seconds"`
	ProjectionBacklogP50       float64   `json:"projection_backlog_p50_seconds"`
	ProjectionBacklogP95       float64   `json:"projection_backlog_p95_seconds"`
	ProjectionBacklogP99       float64   `json:"projection_backlog_p99_seconds"`
	ProjectionBacklogMax       float64   `json:"projection_backlog_max_seconds"`
	ProjectionBuildDelayP50    float64   `json:"projection_build_delay_p50_seconds"`
	ProjectionBuildDelayP95    float64   `json:"projection_build_delay_p95_seconds"`
	ProjectionBuildDelayP99    float64   `json:"projection_build_delay_p99_seconds"`
	ProjectionBuildDelayMax    float64   `json:"projection_build_delay_max_seconds"`
	Rows1mTotalAvg             float64   `json:"rows_1m_total_avg"`
	Rows1mTotalMax             int64     `json:"rows_1m_total_max"`
	TargetProjectionBacklogP95 float64   `json:"target_projection_backlog_p95_seconds"`
	Pass                       bool      `json:"pass"`
}

func main() {
	var (
		sourceDSN                     string
		servingDSN                    string
		sourceName                    string
		durationSeconds               int
		pollSeconds                   int
		queryTimeoutSeconds           int
		outDir                        string
		scenario                      string
		targetProjectionBacklogP95    float64
		deprecatedTargetProjectionP95 float64
		enforceTarget                 bool
	)

	flag.StringVar(&sourceDSN, "source-dsn", env("SOURCE_DSN", ""), "source sql server dsn")
	flag.StringVar(&servingDSN, "serving-dsn", env("SERVING_DSN", ""), "serving sql server dsn")
	flag.StringVar(&sourceName, "source-name", env("SOURCE_NAME", "source1"), "source name used in ingestion watermarks")
	flag.IntVar(&durationSeconds, "duration-seconds", 600, "benchmark duration seconds")
	flag.IntVar(&pollSeconds, "poll-seconds", 5, "poll interval seconds")
	flag.IntVar(&queryTimeoutSeconds, "request-timeout-seconds", envInt("BENCHSTAT_QUERY_TIMEOUT_SECONDS", 30), "query timeout per poll cycle in seconds")
	flag.StringVar(&outDir, "out-dir", "", "output directory for CSV and summary")
	flag.StringVar(&scenario, "scenario", "steady", "benchmark scenario label")
	flag.Float64Var(&targetProjectionBacklogP95, "target-projection-backlog-p95-seconds", 30, "pass/fail threshold for projection backlog p95")
	flag.Float64Var(&deprecatedTargetProjectionP95, "target-projection-p95-seconds", math.NaN(), "deprecated alias for target-projection-backlog-p95-seconds")
	flag.BoolVar(&enforceTarget, "enforce-target", true, "exit with non-zero status when target fails")
	flag.Parse()

	if !math.IsNaN(deprecatedTargetProjectionP95) {
		targetProjectionBacklogP95 = deprecatedTargetProjectionP95
	}

	if strings.TrimSpace(sourceDSN) == "" || strings.TrimSpace(servingDSN) == "" {
		log.Fatal("source-dsn and serving-dsn are required")
	}
	if durationSeconds <= 0 || pollSeconds <= 0 {
		log.Fatal("duration-seconds and poll-seconds must be > 0")
	}
	if queryTimeoutSeconds <= 0 {
		log.Fatal("request-timeout-seconds must be > 0")
	}
	if strings.TrimSpace(outDir) == "" {
		outDir = filepath.Join("benchmarks", time.Now().UTC().Format("20060102_150405"))
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("create out dir: %v", err)
	}

	sourceDB, err := sql.Open("sqlserver", sourceDSN)
	if err != nil {
		log.Fatalf("open source db: %v", err)
	}
	defer sourceDB.Close()
	sourceDB.SetMaxOpenConns(16)
	sourceDB.SetMaxIdleConns(16)

	servingDB, err := sql.Open("sqlserver", servingDSN)
	if err != nil {
		log.Fatalf("open serving db: %v", err)
	}
	defer servingDB.Close()
	servingDB.SetMaxOpenConns(16)
	servingDB.SetMaxIdleConns(16)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer pingCancel()
	if err := sourceDB.PingContext(pingCtx); err != nil {
		log.Fatalf("ping source db: %v", err)
	}
	if err := servingDB.PingContext(pingCtx); err != nil {
		log.Fatalf("ping serving db: %v", err)
	}

	csvPath := filepath.Join(outDir, "samples.csv")
	csvFile, err := os.Create(csvPath)
	if err != nil {
		log.Fatalf("create csv: %v", err)
	}
	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	if err := csvWriter.Write([]string{"timestamp", "ingestion_max_backlog_seconds", "projection_max_backlog_seconds", "projection_max_build_delay_seconds", "rows_1m_total", "projection_errors"}); err != nil {
		log.Fatalf("write csv header: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durationSeconds)*time.Second)
	defer cancel()

	started := time.Now().UTC()
	ticker := time.NewTicker(time.Duration(pollSeconds) * time.Second)
	defer ticker.Stop()

	samples := make([]sample, 0, durationSeconds/maxInt(1, pollSeconds))
	pollErrors := 0

	runPoll := func() {
		s, err := fetchSample(ctx, sourceDB, servingDB, sourceName, time.Duration(queryTimeoutSeconds)*time.Second)
		if err != nil {
			pollErrors++
			log.Printf("poll error: %v", err)
			return
		}
		samples = append(samples, s)
		rec := []string{
			s.Timestamp.Format(time.RFC3339),
			fmt.Sprintf("%.4f", s.IngestionMaxBacklogSec),
			fmt.Sprintf("%.4f", s.ProjectionMaxBacklogSec),
			fmt.Sprintf("%.4f", s.ProjectionMaxBuildDelaySec),
			fmt.Sprintf("%d", s.Rows1mTotal),
			fmt.Sprintf("%d", s.ProjectionErrors),
		}
		if err := csvWriter.Write(rec); err != nil {
			pollErrors++
			log.Printf("csv write error: %v", err)
		}
		csvWriter.Flush()
	}

	runPoll()
	for {
		select {
		case <-ctx.Done():
			goto DONE
		case <-ticker.C:
			runPoll()
		}
	}

DONE:
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		log.Fatalf("csv error: %v", err)
	}

	sum := buildSummary(samples, scenario, started, time.Now().UTC(), pollErrors, targetProjectionBacklogP95)
	jsonPath := filepath.Join(outDir, "summary.json")
	if err := writeJSON(jsonPath, sum); err != nil {
		log.Fatalf("write summary json: %v", err)
	}

	log.Printf("benchmark summary: samples=%d projection_backlog_p95=%.2fs target=%.2fs pass=%t output=%s",
		sum.SampleCount, sum.ProjectionBacklogP95, sum.TargetProjectionBacklogP95, sum.Pass, outDir)

	if enforceTarget && !sum.Pass {
		os.Exit(2)
	}
}

func fetchSample(parent context.Context, sourceDB, servingDB *sql.DB, sourceName string, timeout time.Duration) (sample, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	ingestion, err := getIngestionStatus(ctx, sourceDB, servingDB, sourceName)
	if err != nil {
		return sample{}, err
	}
	projections, err := getProjectionStatus(ctx, sourceDB, servingDB, sourceName)
	if err != nil {
		return sample{}, err
	}

	ingestionMaxBacklog := 0.0
	rows1mTotal := int64(0)
	for _, item := range ingestion {
		if v := firstDefined(item.BacklogSeconds, item.LagSeconds); v > ingestionMaxBacklog {
			ingestionMaxBacklog = v
		}
		rows1mTotal += item.Rows1m
	}

	projectionMaxBacklog := 0.0
	projectionMaxBuildDelay := 0.0
	projectionErrors := 0
	for _, item := range projections {
		if v := firstDefined(item.BacklogSeconds, item.LagSeconds); v > projectionMaxBacklog {
			projectionMaxBacklog = v
		}
		if v := valueOrZero(item.BuildDelaySeconds); v > projectionMaxBuildDelay {
			projectionMaxBuildDelay = v
		}
		if !strings.EqualFold(item.Status, "OK") {
			projectionErrors++
		}
	}

	return sample{
		Timestamp:                  time.Now().UTC(),
		IngestionMaxBacklogSec:     ingestionMaxBacklog,
		ProjectionMaxBacklogSec:    projectionMaxBacklog,
		ProjectionMaxBuildDelaySec: projectionMaxBuildDelay,
		Rows1mTotal:                rows1mTotal,
		ProjectionErrors:           projectionErrors,
	}, nil
}

func getIngestionStatus(ctx context.Context, sourceDB, servingDB *sql.DB, sourceName string) ([]ingestionItem, error) {
	rows, err := servingDB.QueryContext(ctx, `
SELECT capture_instance, last_ingested_lsn
FROM dbo.ctl_ingestion_watermarks WITH (READUNCOMMITTED)
WHERE source_name = @p1
ORDER BY capture_instance;`, sourceName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]ingestionItem, 0, 8)
	for rows.Next() {
		var capture string
		var ingestedLSN []byte
		if err := rows.Scan(&capture, &ingestedLSN); err != nil {
			return nil, err
		}

		lastCommit, err := mapLSNToTime(ctx, sourceDB, ingestedLSN)
		if err != nil {
			return nil, err
		}

		sourceMaxLSN, err := getCaptureMaxLSN(ctx, sourceDB, capture)
		if err != nil {
			return nil, err
		}
		sourceMaxCommit, err := mapLSNToTime(ctx, sourceDB, sourceMaxLSN)
		if err != nil {
			return nil, err
		}

		rows1m := int64(0)
		if table := captureToStaging[capture]; table != "" {
			rows1m, err = countRowsSince(ctx, servingDB, table, 1)
			if err != nil {
				return nil, err
			}
		}

		items = append(items, ingestionItem{
			CaptureInstance: capture,
			BacklogSeconds:  positiveDiffSeconds(sourceMaxCommit, lastCommit),
			Rows1m:          rows1m,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func getProjectionStatus(ctx context.Context, sourceDB, servingDB *sql.DB, sourceName string) ([]projectionItem, error) {
	ingestionLSNByCapture, err := getIngestionLSNByCapture(ctx, servingDB, sourceName)
	if err != nil {
		return nil, err
	}
	captureCommitByCapture := make(map[string]*time.Time, len(ingestionLSNByCapture))
	for capture, lsn := range ingestionLSNByCapture {
		commit, err := mapLSNToTime(ctx, sourceDB, lsn)
		if err != nil {
			return nil, err
		}
		captureCommitByCapture[capture] = commit
	}

	projectionCaptures, err := getProjectionCaptures(ctx, servingDB)
	if err != nil {
		return nil, err
	}

	rows, err := servingDB.QueryContext(ctx, `
SELECT projection_name, as_of_lsn, built_at, status
FROM dbo.ctl_projection_metadata WITH (READUNCOMMITTED)
ORDER BY projection_name;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]projectionItem, 0, 8)
	for rows.Next() {
		var projection string
		var asOfLSN []byte
		var builtAt sql.NullTime
		var status string
		if err := rows.Scan(&projection, &asOfLSN, &builtAt, &status); err != nil {
			return nil, err
		}

		asOfCommit, err := mapLSNToTime(ctx, sourceDB, asOfLSN)
		if err != nil {
			return nil, err
		}
		frontier := minCommitTime(projectionCaptures[projection], captureCommitByCapture)

		item := projectionItem{
			ProjectionName: projection,
			Status:         status,
			BacklogSeconds: positiveDiffSeconds(frontier, asOfCommit),
		}
		if builtAt.Valid {
			built := builtAt.Time.UTC()
			item.BuildDelaySeconds = positiveDiffSeconds(&built, asOfCommit)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func getIngestionLSNByCapture(ctx context.Context, servingDB *sql.DB, sourceName string) (map[string][]byte, error) {
	rows, err := servingDB.QueryContext(ctx, `
SELECT capture_instance, last_ingested_lsn
FROM dbo.ctl_ingestion_watermarks WITH (READUNCOMMITTED)
WHERE source_name = @p1;`, sourceName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[string][]byte{}
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

func getProjectionCaptures(ctx context.Context, servingDB *sql.DB) (map[string][]string, error) {
	rows, err := servingDB.QueryContext(ctx, `
SELECT projection_name, capture_instance
FROM dbo.ctl_projection_checkpoints WITH (READUNCOMMITTED)
ORDER BY projection_name, capture_instance;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[string][]string{}
	for rows.Next() {
		var projection string
		var capture string
		if err := rows.Scan(&projection, &capture); err != nil {
			return nil, err
		}
		result[projection] = append(result[projection], capture)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func countRowsSince(ctx context.Context, servingDB *sql.DB, table string, minutes int) (int64, error) {
	query := fmt.Sprintf(`
SELECT COUNT_BIG(*)
FROM %s WITH (READUNCOMMITTED)
WHERE ingested_at >= DATEADD(MINUTE, -%d, SYSUTCDATETIME());`, table, minutes)
	var count int64
	if err := servingDB.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func getCaptureMaxLSN(ctx context.Context, sourceDB *sql.DB, capture string) ([]byte, error) {
	if !safeIdentifier(capture) {
		return nil, fmt.Errorf("unsafe capture identifier: %s", capture)
	}

	query := fmt.Sprintf(`SELECT TOP (1) __$start_lsn FROM cdc.%s_CT WITH (READUNCOMMITTED) ORDER BY __$start_lsn DESC;`, capture)
	var lsn []byte
	if err := sourceDB.QueryRowContext(ctx, query).Scan(&lsn); err != nil {
		if isMissingObjectError(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(lsn) == 0 {
		return nil, nil
	}
	return util.PadLSN(lsn), nil
}

func mapLSNToTime(ctx context.Context, sourceDB *sql.DB, lsn []byte) (*time.Time, error) {
	if len(lsn) == 0 || util.IsZeroLSN(lsn) {
		return nil, nil
	}

	var commitTime sql.NullTime
	if err := sourceDB.QueryRowContext(ctx, `SELECT sys.fn_cdc_map_lsn_to_time(@p1);`, util.PadLSN(lsn)).Scan(&commitTime); err != nil {
		return nil, err
	}
	if !commitTime.Valid {
		return nil, nil
	}
	v := commitTime.Time.UTC()
	return &v, nil
}

func safeIdentifier(s string) bool {
	if strings.TrimSpace(s) == "" {
		return false
	}
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			continue
		}
		return false
	}
	return true
}

func isMissingObjectError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "invalid object name")
}

func minCommitTime(captures []string, commits map[string]*time.Time) *time.Time {
	var out *time.Time
	for _, capture := range captures {
		c := commits[capture]
		if c == nil {
			continue
		}
		if out == nil || c.Before(*out) {
			v := *c
			out = &v
		}
	}
	return out
}

func positiveDiffSeconds(later, earlier *time.Time) *float64 {
	if later == nil || earlier == nil {
		return nil
	}
	d := later.Sub(*earlier).Seconds()
	if d < 0 {
		d = 0
	}
	return floatPtr(d)
}

func floatPtr(v float64) *float64 {
	rounded := math.Round(v*100) / 100
	return &rounded
}

func buildSummary(samples []sample, scenario string, startedAt, endedAt time.Time, pollErrors int, targetProjectionBacklogP95 float64) summary {
	ingestionBacklog := make([]float64, 0, len(samples))
	projectionBacklog := make([]float64, 0, len(samples))
	projectionBuildDelay := make([]float64, 0, len(samples))
	rows := make([]float64, 0, len(samples))
	rowsMax := int64(0)

	for _, s := range samples {
		ingestionBacklog = append(ingestionBacklog, s.IngestionMaxBacklogSec)
		projectionBacklog = append(projectionBacklog, s.ProjectionMaxBacklogSec)
		projectionBuildDelay = append(projectionBuildDelay, s.ProjectionMaxBuildDelaySec)
		rows = append(rows, float64(s.Rows1mTotal))
		if s.Rows1mTotal > rowsMax {
			rowsMax = s.Rows1mTotal
		}
	}

	p95ProjectionBacklog := percentile(projectionBacklog, 95)
	return summary{
		Scenario:                   scenario,
		StartedAt:                  startedAt,
		EndedAt:                    endedAt,
		DurationSeconds:            endedAt.Sub(startedAt).Seconds(),
		SampleCount:                len(samples),
		PollErrors:                 pollErrors,
		IngestionBacklogP50:        percentile(ingestionBacklog, 50),
		IngestionBacklogP95:        percentile(ingestionBacklog, 95),
		IngestionBacklogP99:        percentile(ingestionBacklog, 99),
		IngestionBacklogMax:        maxFloat(ingestionBacklog),
		ProjectionBacklogP50:       percentile(projectionBacklog, 50),
		ProjectionBacklogP95:       p95ProjectionBacklog,
		ProjectionBacklogP99:       percentile(projectionBacklog, 99),
		ProjectionBacklogMax:       maxFloat(projectionBacklog),
		ProjectionBuildDelayP50:    percentile(projectionBuildDelay, 50),
		ProjectionBuildDelayP95:    percentile(projectionBuildDelay, 95),
		ProjectionBuildDelayP99:    percentile(projectionBuildDelay, 99),
		ProjectionBuildDelayMax:    maxFloat(projectionBuildDelay),
		Rows1mTotalAvg:             average(rows),
		Rows1mTotalMax:             rowsMax,
		TargetProjectionBacklogP95: targetProjectionBacklogP95,
		Pass:                       p95ProjectionBacklog <= targetProjectionBacklogP95,
	}
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		return minFloat(values)
	}
	if p >= 100 {
		return maxFloat(values)
	}

	copyVals := append([]float64(nil), values...)
	sort.Float64s(copyVals)
	idx := (p / 100) * float64(len(copyVals)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper {
		return round2(copyVals[lower])
	}
	weight := idx - float64(lower)
	v := copyVals[lower]*(1-weight) + copyVals[upper]*weight
	return round2(v)
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0.0
	for _, v := range values {
		total += v
	}
	return round2(total / float64(len(values)))
}

func minFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, v := range values[1:] {
		if v < m {
			m = v
		}
	}
	return round2(m)
}

func maxFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, v := range values[1:] {
		if v > m {
			m = v
		}
	}
	return round2(m)
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func firstDefined(primary, fallback *float64) float64 {
	if primary != nil {
		return *primary
	}
	return valueOrZero(fallback)
}

func valueOrZero(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	return nil
}

func env(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func envInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
