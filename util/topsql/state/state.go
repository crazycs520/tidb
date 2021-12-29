package state

import "go.uber.org/atomic"

const (
	DefTiDBTopSQLEnable                = false
	DefTiDBTopSQLPrecisionSeconds      = 1
	DefTiDBTopSQLMaxStatementCount     = 200
	DefTiDBTopSQLMaxCollect            = 5000
	DefTiDBTopSQLReportIntervalSeconds = 60
)

var GlobalState = State{
	Enable:                atomic.NewBool(DefTiDBTopSQLEnable),
	PrecisionSeconds:      atomic.NewInt64(DefTiDBTopSQLPrecisionSeconds),
	MaxStatementCount:     atomic.NewInt64(DefTiDBTopSQLMaxStatementCount),
	MaxCollect:            atomic.NewInt64(DefTiDBTopSQLMaxCollect),
	ReportIntervalSeconds: atomic.NewInt64(DefTiDBTopSQLReportIntervalSeconds),
}

// State is the state for control top sql feature.
type State struct {
	// Enable top-sql or not.
	Enable *atomic.Bool
	// The refresh interval of top-sql.
	PrecisionSeconds *atomic.Int64
	// The maximum number of statements kept in memory.
	MaxStatementCount *atomic.Int64
	// The maximum capacity of the collect map.
	MaxCollect *atomic.Int64
	// The report data interval of top-sql.
	ReportIntervalSeconds *atomic.Int64
}

// TopSQLEnabled uses to check whether enabled the top SQL feature.
func TopSQLEnabled() bool {
	return GlobalState.Enable.Load()
}
