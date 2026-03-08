package binance

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"hist-data/internal/model"
	"hist-data/internal/saver"
)

// intervalToMinutes converts Binance interval string to duration for chunking.
var intervalDuration = map[string]time.Duration{
	"1m":  time.Minute,
	"3m":  3 * time.Minute,
	"5m":  5 * time.Minute,
	"15m": 15 * time.Minute,
	"30m": 30 * time.Minute,
	"1h":  time.Hour,
	"2h":  2 * time.Hour,
	"4h":  4 * time.Hour,
	"6h":  6 * time.Hour,
	"8h":  8 * time.Hour,
	"12h": 12 * time.Hour,
	"1d":  24 * time.Hour,
	"3d":  72 * time.Hour,
	"1w":  7 * 24 * time.Hour,
}

// Crawler fetches OHLCV bars from Binance public API and persists them.
// No tick data — minimum granularity is 1-minute bars.
type Crawler struct {
	client        *Client
	SavePacketDir string
	PacketSaver   saver.PacketSaver
	Interval      string // e.g. "5m", "1h", "1d"
}

// NewCrawler creates a Binance Crawler.
// interval is a Binance interval string (e.g. "5m").
// saveDir is the root output directory (e.g. "data/Binance").
// No API key required.
func NewCrawler(baseURL, saveDir, interval string, ps saver.PacketSaver) (*Crawler, error) {
	if _, ok := intervalDuration[interval]; !ok {
		return nil, fmt.Errorf("unsupported Binance interval %q (valid: 1m, 5m, 15m, 1h, 4h, 1d, ...)", interval)
	}
	return &Crawler{
		client:        NewClient(baseURL),
		SavePacketDir: saveDir,
		PacketSaver:   ps,
		Interval:      interval,
	}, nil
}

// FetchBars retrieves complete OHLCV bars for symbol over [from, to].
// apiKey is ignored — Binance klines are public.
// Automatically chunks requests at 1000 bars (Binance API limit).
func (c *Crawler) FetchBars(symbol, _ string, from, to time.Time) ([]model.Bar, error) {
	barDur, ok := intervalDuration[c.Interval]
	if !ok {
		return nil, fmt.Errorf("unknown interval %q", c.Interval)
	}

	chunkDur := barDur * 1000 // 1000 bars per request
	var all []model.Bar

	cur := from
	for cur.Before(to) {
		end := cur.Add(chunkDur)
		if end.After(to) {
			end = to
		}

		bars, err := c.client.GetKlines(symbol, c.Interval, cur.UnixMilli(), end.UnixMilli())
		if err != nil {
			return nil, fmt.Errorf("fetch %s [%s, %s]: %w", symbol, cur.Format(time.RFC3339), end.Format(time.RFC3339), err)
		}
		all = append(all, bars...)

		if len(bars) == 0 {
			break // no more data in range
		}
		// Advance past the last bar's timestamp
		lastTs := time.UnixMilli(bars[len(bars)-1].Timestamp)
		cur = lastTs.Add(barDur)
	}

	return all, nil
}

// SaveBars persists bars to {dir}/{symbol}/ using the configured saver.
// Follows the same file naming convention as the Polygon crawler.
func (c *Crawler) SaveBars(dir, symbol string, from, to time.Time, bars []model.Bar) {
	if dir == "" || c.PacketSaver == nil || len(bars) == 0 {
		return
	}
	tickerDir := filepath.Join(dir, symbol)
	if err := os.MkdirAll(tickerDir, 0o755); err != nil {
		slog.Error("binance save: mkdir failed", "symbol", symbol, "dir", tickerDir, "err", err)
		return
	}
	ext := c.PacketSaver.Extension()
	name := fmt.Sprintf("%s_%s_%s_to_%s.%s",
		symbol, c.Interval,
		from.Format("2006-01-02"), to.Format("2006-01-02"), ext)
	path := filepath.Join(tickerDir, name)
	if err := c.PacketSaver.Save(bars, path); err != nil {
		slog.Error("binance save: write failed", "symbol", symbol, "path", path, "err", err)
	} else {
		slog.Info("binance save ok", "symbol", symbol, "bars", len(bars), "path", path)
	}
}
