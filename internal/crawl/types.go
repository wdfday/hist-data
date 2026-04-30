package crawl

import (
	"log/slog"
	"time"
)

// AssetClass identifies the type of financial instrument.
type AssetClass string

const (
	AssetStocks  AssetClass = "stocks"
	AssetCrypto  AssetClass = "crypto"
	AssetForex   AssetClass = "forex"
	AssetIndices AssetClass = "indices"

	DefaultSource     = "massive"
	DefaultAssetClass = AssetStocks
)

// Job is a fully-resolved crawl unit. From/To are computed by the producer
// (via resolveJobRange) so the worker is a pure "fetch + save" unit with no
// date or progress logic.
type Job struct {
	Source  string
	Class   AssetClass
	Frame   string // e.g. M1, D1 — required to scope progress per timeframe
	Ticker  string
	From    time.Time
	To      time.Time
	SaveDir string // e.g. data/Polygon/stocks | data/Polygon/crypto
}

// JobResult is the outcome of one Job, fanned-in to the result collector.
type JobResult struct {
	Ok        bool
	Ticker    string
	DateRange string
	Reason    string
	Bars      int
	KeyPrefix string
}

// Done signals that a Runner cycle has finished.
type Done struct{}

// LogEntry is a structured log event sent from workers through the log channel.
// A single log-writer goroutine drains the channel and calls slog — workers
// never call slog directly, keeping them pure compute units.
type LogEntry struct {
	Level slog.Level
	Msg   string
	Args  []any
}
