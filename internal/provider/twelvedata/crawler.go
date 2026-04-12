package twelvedata

import (
	"fmt"
	"strings"
	"time"

	"hist-data/internal/model"
	"hist-data/internal/saver"
)

// Crawler fetches OHLCV bars from TwelveData and persists them.
// Supports forex, stocks, ETFs, indices, and crypto.
// Free tier: 8 req/min, 800 credits/day — set workers: 1 in config.
type Crawler struct {
	client        *Client
	Interval      string // TwelveData interval: "1min", "1day", etc.
	SavePacketDir string
	PacketSaver   saver.PacketSaver
	FrameLabel    string
	SinkFrames    []string
}

// NewCrawler creates a TwelveData Crawler.
// interval must be a valid TwelveData interval string (e.g. "1min", "1day").
func NewCrawler(baseURL, apiKey, saveDir, interval string, ps saver.PacketSaver) (*Crawler, error) {
	if _, ok := intervalDuration[interval]; !ok {
		return nil, fmt.Errorf("twelvedata: unsupported interval %q (valid: 1min, 5min, 1h, 1day, ...)", interval)
	}
	return &Crawler{
		client:        NewClient(baseURL, apiKey),
		Interval:      interval,
		SavePacketDir: saveDir,
		PacketSaver:   ps,
	}, nil
}

// FetchBars retrieves bars for symbol over [from, to]. apiKey is ignored — set on the client.
// Paginates automatically: each request fetches up to 5000 bars.
// For "1min": 5000 bars ≈ 3.5 days per request.
func (c *Crawler) FetchBars(symbol, _ string, from, to time.Time) ([]model.Bar, error) {
	barDur := intervalDuration[c.Interval]
	chunkDur := barDur * maxOutputSize

	var all []model.Bar
	cur := from

	for cur.Before(to) {
		end := cur.Add(chunkDur)
		if end.After(to) {
			end = to
		}

		bars, err := c.client.GetBars(symbol, c.Interval, cur, end)
		if err != nil {
			return nil, fmt.Errorf("fetch %s [%s, %s]: %w",
				symbol, cur.Format(time.DateOnly), end.Format(time.DateOnly), err)
		}
		all = append(all, bars...)

		if len(bars) < maxOutputSize {
			break // got less than a full page — no more data in range
		}
		// Advance past the last bar's timestamp to avoid re-fetching it.
		lastTs := time.UnixMilli(bars[len(bars)-1].Timestamp)
		cur = lastTs.Add(barDur)
	}

	return all, nil
}

// SaveBars persists bars to {dir}/{symbol}/ using the configured saver.
func (c *Crawler) SaveBars(dir, symbol string, from, to time.Time, bars []model.Bar) {
	frameLabel := c.FrameLabel
	if frameLabel == "" {
		frameLabel = strings.ToUpper(c.Interval)
	}
	saver.SaveFrameSet("twelvedata", frameLabel, c.SinkFrames, dir, symbol, from, to, bars, c.PacketSaver)
}
