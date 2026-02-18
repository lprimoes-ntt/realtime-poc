package projections

import (
	"time"
)

const (
	ProjectionOrdersKPI    = "orders_kpi_by_minute_segment"
	ProjectionOrdersLatest = "orders_latest"
)

func floorToMinute(t time.Time) time.Time {
	t = t.UTC()
	return t.Truncate(time.Minute)
}

type KPIOutputRow struct {
	MinuteBucket    time.Time
	Segment         string
	OrdersCount     int64
	OrdersAmountSum float64
	PaidAmountSum   float64
}

type LatestOrderOutputRow struct {
	OrderID    int64
	CustomerID *int32
	Segment    *string
	Amount     *float64
	Status     *string
	CreatedAt  *time.Time
	UpdatedAt  *time.Time
	SourceLSN  []byte
}
