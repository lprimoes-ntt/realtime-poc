package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

type config struct {
	SourceDSN          string
	Duration           time.Duration
	BaseRPS            int
	BurstRPS           int
	BurstInterval      time.Duration
	BurstDuration      time.Duration
	CustomerSeedCount  int
	UpdateRatio        float64
	PaymentRatio       float64
	ReportEvery        time.Duration
	MaxTrackedOrderIDs int
	Workers            int
	GoMaxProcs         int
	DispatchEvery      time.Duration
	MaxDispatchChunk   int
}

type stats struct {
	ordersInserted   int64
	ordersUpdated    int64
	paymentsInserted int64
	errors           int64
}

type generator struct {
	db          *sql.DB
	customerIDs []int
	orderIDs    []int64
	maxOrderIDs int
	mu          sync.RWMutex
}

func main() {
	cfg := loadConfig()
	runtime.GOMAXPROCS(cfg.GoMaxProcs)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration+2*time.Minute)
	defer cancel()

	db, err := sql.Open("sqlserver", cfg.SourceDSN)
	if err != nil {
		log.Fatalf("open source db: %v", err)
	}
	defer db.Close()

	maxConns := clampInt(cfg.Workers*2, 24, 512)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping source db: %v", err)
	}

	g := &generator{
		db:          db,
		maxOrderIDs: cfg.MaxTrackedOrderIDs,
	}

	if err := g.ensureCustomers(ctx, cfg.CustomerSeedCount); err != nil {
		log.Fatalf("seed customers: %v", err)
	}
	if err := g.loadRecentOrderIDs(ctx, cfg.MaxTrackedOrderIDs); err != nil {
		log.Fatalf("load order ids: %v", err)
	}

	log.Printf("loadgen start: duration=%s base_rps=%d burst_rps=%d burst_interval=%s burst_duration=%s workers=%d gomaxprocs=%d dispatch_every=%s customers=%d tracked_orders=%d",
		cfg.Duration, cfg.BaseRPS, cfg.BurstRPS, cfg.BurstInterval, cfg.BurstDuration, cfg.Workers, cfg.GoMaxProcs, cfg.DispatchEvery, len(g.customerIDs), len(g.orderIDs))

	st := &stats{}
	start := time.Now()
	reportTicker := time.NewTicker(cfg.ReportEvery)
	defer reportTicker.Stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := g.runLoad(ctx, cfg, start, st); err != nil {
			log.Printf("loadgen run error: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Fatalf("loadgen context done: %v", ctx.Err())
		case <-done:
			printSummary(start, st)
			return
		case <-reportTicker.C:
			printProgress(start, st)
		}
	}
}

func loadConfig() config {
	var cfg config

	flag.StringVar(&cfg.SourceDSN, "source-dsn", env("SOURCE_DSN", ""), "source sql server dsn")
	durationSeconds := flag.Int("duration-seconds", 600, "total run duration seconds")
	flag.IntVar(&cfg.BaseRPS, "base-rps", 20, "baseline orders inserted per second")
	flag.IntVar(&cfg.BurstRPS, "burst-rps", 100, "burst orders inserted per second")
	burstIntervalSeconds := flag.Int("burst-interval-seconds", 120, "burst interval seconds")
	burstDurationSeconds := flag.Int("burst-duration-seconds", 30, "burst duration seconds")
	flag.IntVar(&cfg.CustomerSeedCount, "customer-seed-count", 100, "minimum customers to keep for random order assignment")
	flag.Float64Var(&cfg.UpdateRatio, "update-ratio", 0.35, "ratio of orders that trigger an update")
	flag.Float64Var(&cfg.PaymentRatio, "payment-ratio", 0.65, "ratio of orders that trigger payment insert")
	reportEverySeconds := flag.Int("report-every-seconds", 5, "progress report interval seconds")
	flag.IntVar(&cfg.MaxTrackedOrderIDs, "max-tracked-order-ids", 250000, "max in-memory order ids used for updates/payments")
	flag.IntVar(&cfg.Workers, "workers", 64, "parallel loadgen workers")
	flag.IntVar(&cfg.GoMaxProcs, "gomaxprocs", 0, "max Go scheduler threads (0 uses half available CPU cores)")
	dispatchEveryMs := flag.Int("dispatch-every-ms", 100, "dispatch cadence in milliseconds")
	flag.IntVar(&cfg.MaxDispatchChunk, "max-dispatch-chunk", 250, "max operations per queue dispatch chunk")
	flag.Parse()

	cfg.Duration = time.Duration(*durationSeconds) * time.Second
	cfg.BurstInterval = time.Duration(*burstIntervalSeconds) * time.Second
	cfg.BurstDuration = time.Duration(*burstDurationSeconds) * time.Second
	cfg.ReportEvery = time.Duration(*reportEverySeconds) * time.Second
	cfg.DispatchEvery = time.Duration(*dispatchEveryMs) * time.Millisecond

	if strings.TrimSpace(cfg.SourceDSN) == "" {
		log.Fatal("source dsn is required (set -source-dsn or SOURCE_DSN)")
	}
	if cfg.BaseRPS <= 0 || cfg.BurstRPS <= 0 {
		log.Fatal("base-rps and burst-rps must be > 0")
	}
	if cfg.BurstInterval <= 0 || cfg.BurstDuration <= 0 {
		log.Fatal("burst interval and duration must be > 0")
	}
	if cfg.CustomerSeedCount <= 0 || cfg.MaxTrackedOrderIDs <= 0 {
		log.Fatal("customer seed count and max tracked order ids must be > 0")
	}
	if cfg.UpdateRatio < 0 || cfg.UpdateRatio > 1 || cfg.PaymentRatio < 0 || cfg.PaymentRatio > 1 {
		log.Fatal("update-ratio and payment-ratio must be in [0,1]")
	}
	if cfg.ReportEvery <= 0 {
		cfg.ReportEvery = 5 * time.Second
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 64
	}
	if cfg.GoMaxProcs <= 0 {
		cfg.GoMaxProcs = halfCPUCount()
	}
	if cfg.DispatchEvery <= 0 {
		cfg.DispatchEvery = 100 * time.Millisecond
	}
	if cfg.MaxDispatchChunk <= 0 {
		cfg.MaxDispatchChunk = 250
	}

	return cfg
}

func (g *generator) runLoad(ctx context.Context, cfg config, start time.Time, st *stats) error {
	jobs := make(chan int, cfg.Workers*8)
	var wg sync.WaitGroup

	for workerID := 0; workerID < cfg.Workers; workerID++ {
		wg.Add(1)
		go g.runWorker(ctx, cfg, st, workerID, jobs, &wg)
	}

	ticker := time.NewTicker(cfg.DispatchEvery)
	defer ticker.Stop()

	carry := 0.0
	for {
		elapsed := time.Since(start)
		if elapsed >= cfg.Duration {
			break
		}

		target := cfg.BaseRPS
		if inBurst(elapsed, cfg.BurstInterval, cfg.BurstDuration) {
			target = cfg.BurstRPS
		}

		carry += float64(target) * cfg.DispatchEvery.Seconds()
		toDispatch := int(math.Floor(carry))
		carry -= float64(toDispatch)

		if toDispatch > 0 {
			if err := enqueueOps(ctx, jobs, toDispatch, cfg.MaxDispatchChunk); err != nil {
				close(jobs)
				wg.Wait()
				return err
			}
		}

		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case <-ticker.C:
		}
	}

	close(jobs)
	wg.Wait()
	return nil
}

func enqueueOps(ctx context.Context, jobs chan<- int, total int, maxChunk int) error {
	remaining := total
	for remaining > 0 {
		chunk := minInt(maxChunk, remaining)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobs <- chunk:
			remaining -= chunk
		}
	}
	return nil
}

func (g *generator) runWorker(ctx context.Context, cfg config, st *stats, workerID int, jobs <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID*104729)))
	for {
		select {
		case <-ctx.Done():
			return
		case count, ok := <-jobs:
			if !ok {
				return
			}
			for i := 0; i < count; i++ {
				if err := g.insertOrder(ctx, st, r); err != nil {
					atomic.AddInt64(&st.errors, 1)
					continue
				}
				if r.Float64() < cfg.UpdateRatio {
					if err := g.updateRandomOrder(ctx, st, r); err != nil {
						atomic.AddInt64(&st.errors, 1)
					}
				}
				if r.Float64() < cfg.PaymentRatio {
					if err := g.insertRandomPayment(ctx, st, r); err != nil {
						atomic.AddInt64(&st.errors, 1)
					}
				}
			}
		}
	}
}

func (g *generator) ensureCustomers(ctx context.Context, minimum int) error {
	var current int
	if err := g.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dbo.customers;`).Scan(&current); err != nil {
		return err
	}

	toInsert := minimum - current
	if toInsert > 0 {
		seedRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		segments := []string{"SMB", "Enterprise", "MidMarket"}
		for i := 0; i < toInsert; i++ {
			segment := segments[seedRand.Intn(len(segments))]
			var id int
			if err := g.db.QueryRowContext(ctx, `
INSERT INTO dbo.customers (segment, is_active, updated_at)
OUTPUT INSERTED.customer_id
VALUES (@p1, 1, SYSUTCDATETIME());`, segment).Scan(&id); err != nil {
				return err
			}
		}
	}

	rows, err := g.db.QueryContext(ctx, `
SELECT customer_id
FROM dbo.customers
ORDER BY customer_id;`)
	if err != nil {
		return err
	}
	defer rows.Close()

	ids := make([]int, 0, minimum)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	g.mu.Lock()
	g.customerIDs = ids
	g.mu.Unlock()
	return nil
}

func (g *generator) loadRecentOrderIDs(ctx context.Context, limit int) error {
	readLimit := clampInt(limit, 10000, 200000)
	rows, err := g.db.QueryContext(ctx, `
SELECT TOP (@p1) order_id
FROM dbo.orders
ORDER BY order_id DESC;`, readLimit)
	if err != nil {
		return err
	}
	defer rows.Close()

	ids := make([]int64, 0, readLimit)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	g.mu.Lock()
	g.orderIDs = ids
	g.mu.Unlock()
	return nil
}

func (g *generator) insertOrder(ctx context.Context, st *stats, r *rand.Rand) error {
	customerID := g.randomCustomerID(r)
	if customerID == 0 {
		return fmt.Errorf("no customers available")
	}

	amount := float64(50+r.Intn(5000)) / 10
	status := randomOrderStatus(r)

	var orderID int64
	if err := g.db.QueryRowContext(ctx, `
INSERT INTO dbo.orders (customer_id, amount, status, created_at, updated_at)
OUTPUT INSERTED.order_id
VALUES (@p1, @p2, @p3, SYSUTCDATETIME(), SYSUTCDATETIME());`, customerID, amount, status).Scan(&orderID); err != nil {
		return err
	}

	g.pushOrderID(orderID)
	atomic.AddInt64(&st.ordersInserted, 1)

	if status == "paid" {
		if _, err := g.db.ExecContext(ctx, `
INSERT INTO dbo.payments (order_id, paid_amount, paid_at)
VALUES (@p1, @p2, SYSUTCDATETIME());`, orderID, amount); err != nil {
			return err
		}
		atomic.AddInt64(&st.paymentsInserted, 1)
	}

	return nil
}

func (g *generator) updateRandomOrder(ctx context.Context, st *stats, r *rand.Rand) error {
	orderID := g.randomOrderID(r)
	if orderID == 0 {
		return nil
	}

	status := randomOrderStatus(r)
	bump := float64(1+r.Intn(200)) / 10

	if _, err := g.db.ExecContext(ctx, `
UPDATE dbo.orders
SET amount = amount + @p1,
    status = @p2,
    updated_at = SYSUTCDATETIME()
WHERE order_id = @p3;`, bump, status, orderID); err != nil {
		return err
	}
	atomic.AddInt64(&st.ordersUpdated, 1)
	return nil
}

func (g *generator) insertRandomPayment(ctx context.Context, st *stats, r *rand.Rand) error {
	orderID := g.randomOrderID(r)
	if orderID == 0 {
		return nil
	}

	amount := float64(10+r.Intn(2000)) / 10
	if _, err := g.db.ExecContext(ctx, `
INSERT INTO dbo.payments (order_id, paid_amount, paid_at)
VALUES (@p1, @p2, SYSUTCDATETIME());`, orderID, amount); err != nil {
		return err
	}
	atomic.AddInt64(&st.paymentsInserted, 1)
	return nil
}

func (g *generator) randomCustomerID(r *rand.Rand) int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.customerIDs) == 0 {
		return 0
	}
	return g.customerIDs[r.Intn(len(g.customerIDs))]
}

func (g *generator) randomOrderID(r *rand.Rand) int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.orderIDs) == 0 {
		return 0
	}
	return g.orderIDs[r.Intn(len(g.orderIDs))]
}

func (g *generator) pushOrderID(id int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.orderIDs = append(g.orderIDs, id)
	if len(g.orderIDs) > g.maxOrderIDs {
		drop := len(g.orderIDs) - g.maxOrderIDs
		copy(g.orderIDs, g.orderIDs[drop:])
		g.orderIDs = g.orderIDs[:g.maxOrderIDs]
	}
}

func printProgress(start time.Time, st *stats) {
	elapsed := time.Since(start)
	orders := atomic.LoadInt64(&st.ordersInserted)
	updates := atomic.LoadInt64(&st.ordersUpdated)
	payments := atomic.LoadInt64(&st.paymentsInserted)
	errors := atomic.LoadInt64(&st.errors)

	log.Printf("progress elapsed=%s orders_inserted=%d orders_updated=%d payments_inserted=%d errors=%d",
		elapsed.Truncate(time.Second), orders, updates, payments, errors)
}

func printSummary(start time.Time, st *stats) {
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}

	orders := atomic.LoadInt64(&st.ordersInserted)
	updates := atomic.LoadInt64(&st.ordersUpdated)
	payments := atomic.LoadInt64(&st.paymentsInserted)
	errors := atomic.LoadInt64(&st.errors)
	totalChanges := orders + updates + payments

	log.Printf("summary elapsed_seconds=%.2f orders_inserted=%d orders_rps=%.2f orders_updated=%d payments_inserted=%d approx_changes_per_sec=%.2f errors=%d",
		elapsed, orders, float64(orders)/elapsed, updates, payments, float64(totalChanges)/elapsed, errors)
}

func inBurst(elapsed time.Duration, interval time.Duration, duration time.Duration) bool {
	if interval <= 0 || duration <= 0 {
		return false
	}
	mod := elapsed % interval
	return mod < duration
}

func randomOrderStatus(r *rand.Rand) string {
	p := r.Float64()
	switch {
	case p < 0.7:
		return "open"
	case p < 0.9:
		return "paid"
	default:
		return "cancelled"
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func halfCPUCount() int {
	n := runtime.NumCPU()
	if n < 2 {
		return 1
	}
	return n / 2
}

func env(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
