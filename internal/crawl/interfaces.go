package crawl

import (
	"time"

	"hist-data/internal/model"
)

// BarFetcher is the only interface the crawl engine depends on from the provider layer.
//
// DIP: crawl (high-level) defines this interface; providers (low-level) implement it.
// Runner never imports a concrete provider — it depends only on this abstraction.
//
// When `from` is the zero time, the crawler is expected to self-resolve the
// start of the fetch range (via provider listTime / onboardDate / backward
// pagination until empty). This is the contract for "full history" mode
// (backfillYears = 0 in config).
type BarFetcher interface {
	// FetchBars retrieves minute OHLCV bars for one instrument over [from, to].
	FetchBars(ticker, apiKey string, from, to time.Time) ([]model.Bar, error)

	// SaveBars persists bars to dir/ticker/ using the configured storage format.
	// dir is the frame-specific directory (e.g. data/Polygon/M1).
	SaveBars(dir, ticker string, from, to time.Time, bars []model.Bar)
}
