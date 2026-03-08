package crawl

import (
	"time"

	"hist-data/internal/model"
)

// BarFetcher is the only interface the crawl engine depends on from the provider layer.
//
// DIP: crawl (high-level) defines this interface; providers (low-level) implement it.
// Runner never imports a concrete provider — it depends only on this abstraction.
type BarFetcher interface {
	// FetchBars retrieves minute OHLCV bars for one instrument over [from, to].
	FetchBars(ticker, apiKey string, from, to time.Time) ([]model.Bar, error)

	// SaveBars persists bars to dir/ticker/ using the configured storage format.
	// dir is the asset-class-specific directory (e.g. data/Polygon/stocks).
	SaveBars(dir, ticker string, from, to time.Time, bars []model.Bar)
}
