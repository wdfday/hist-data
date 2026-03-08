package polygon

import (
	"time"

	"hist-data/internal/model"
)

// BarFetcherAdapter wraps Crawler to satisfy crawl.BarFetcher.
// Needed because crawler's fetch method is CrawlBarsWithKey (legacy naming).
// Binance and HistData crawlers implement FetchBars/SaveBars directly.
type BarFetcherAdapter struct {
	*Crawler
}

func (a *BarFetcherAdapter) FetchBars(ticker, apiKey string, from, to time.Time) ([]model.Bar, error) {
	return a.CrawlBarsWithKey(ticker, apiKey, from, to)
}

func (a *BarFetcherAdapter) SaveBars(dir, ticker string, from, to time.Time, bars []model.Bar) {
	a.Crawler.SaveBars(dir, ticker, from, to, bars)
}
