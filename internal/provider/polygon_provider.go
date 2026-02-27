package provider

import (
	"time"

	"us-data/internal/model"
	"us-data/internal/provider/polygon"
	"us-data/internal/saver"
)

// PolygonProvider implements crawl.BarFetcher backed by the Massive/Polygon API.
type PolygonProvider struct {
	*polygon.Crawler
}

// NewPolygonProvider creates a fully configured PolygonProvider.
// saveDir  is the root output directory (e.g. data/Polygon).
// ps       is the persistence backend used by SaveBars.
// timespan is the bar timeframe: minute | hour | day | week | month.
// mult     is the timeframe multiplier (e.g. 1, 5, 15).
// API keys are not stored here — they are passed per-request by the Runner's key pool.
func NewPolygonProvider(saveDir string, ps saver.PacketSaver, timespan string, mult int) (*PolygonProvider, error) {
	crawler, err := polygon.NewCrawler()
	if err != nil {
		return nil, err
	}
	crawler.SavePacketDir = saveDir
	crawler.PacketSaver = ps
	crawler.Timespan = timespan
	crawler.Multiplier = mult
	return &PolygonProvider{Crawler: crawler}, nil
}

func (p *PolygonProvider) GetName() string { return "Massive/Polygon" }
func (p *PolygonProvider) Close() error    { return p.Crawler.Close() }

// FetchBars retrieves OHLCV bars for ticker over [from, to] using apiKey.
// The timeframe (timespan × multiplier) is determined at construction time.
func (p *PolygonProvider) FetchBars(ticker, apiKey string, from, to time.Time) ([]model.Bar, error) {
	return p.Crawler.CrawlBarsWithKey(ticker, apiKey, from, to)
}

// SaveBars persists bars to dir/ticker/ using the configured storage format.
func (p *PolygonProvider) SaveBars(dir, ticker string, from, to time.Time, bars []model.Bar) {
	p.Crawler.SaveBars(dir, ticker, from, to, bars)
}
