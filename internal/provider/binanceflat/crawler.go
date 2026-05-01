package binanceflat

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"hist-data/internal/model"
	"hist-data/internal/saver"
)

// Crawler implements crawl.BarFetcher by pulling kline ZIPs from Binance Vision.
//
// Strategy: walk the requested date range one calendar month at a time.
// Closed months use the monthly aggregate ZIP (1 request per month per ticker).
// The current/in-progress month falls back to daily ZIPs (1 request per day).
// Missing files (HTTP 404) are skipped — they typically mean "before the
// instrument was listed" or "T+1 publication lag for today".
type Crawler struct {
	client        *Client
	Interval      string // 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1mo
	SavePacketDir string
	PacketSaver   saver.PacketSaver
	FrameLabel    string
}

// NewCrawler creates a Vision Crawler. saveDir is the root for SaveBars.
func NewCrawler(baseURL, saveDir, interval string, ps saver.PacketSaver) (*Crawler, error) {
	if interval == "" {
		return nil, fmt.Errorf("binanceflat: interval required")
	}
	return &Crawler{
		client:        NewClient(baseURL),
		Interval:      interval,
		SavePacketDir: saveDir,
		PacketSaver:   ps,
	}, nil
}

// FetchBars retrieves complete OHLCV bars for symbol over [from, to].
// apiKey is ignored — Vision is public, no auth.
//
// `from` may be the zero time for full-history mode; we walk from 2017-01-01
// (a few months before BTCUSDT genesis) and rely on 404 handling to skip
// pre-listing months.
//
// The range is always rewound to the start of the previous calendar month so
// that data previously accumulated from daily ZIPs is replaced by the
// official monthly ZIP on the first run after a month closes.
func (c *Crawler) FetchBars(symbol, _ string, from, to time.Time) ([]model.Bar, error) {
	if from.IsZero() {
		from = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	now := time.Now().UTC()

	// Rewind to start of the previous month only when `from` is behind the
	// current month — meaning this is the first run of a new month.
	// This replaces daily-accumulated data for the just-closed month with the
	// canonical monthly ZIP that Binance publishes after month-end.
	if from.Year() != now.Year() || from.Month() != now.Month() {
		prevMonthStart := time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, time.UTC)
		if from.After(prevMonthStart) {
			from = prevMonthStart
		}
	}

	if !from.Before(to) {
		return nil, nil
	}

	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()

	currentMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	var all []model.Bar
	cur := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	for !cur.After(to) {
		nextMonth := cur.AddDate(0, 1, 0)

		if cur.Before(currentMonthStart) {
			bars, err := c.client.FetchMonthly(symbol, c.Interval, cur.Year(), cur.Month())
			switch {
			case errors.Is(err, ErrNotFound):
				slog.Debug("binanceflat monthly not found", "symbol", symbol, "month", cur.Format("2006-01"))
			case err != nil:
				return nil, fmt.Errorf("binanceflat %s %s: %w", symbol, cur.Format("2006-01"), err)
			default:
				all = appendInRange(all, bars, fromMs, toMs)
			}
			cur = nextMonth
			continue
		}

		// Current month: walk daily.
		day := cur
		if day.Before(from) {
			day = time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)
		}
		for !day.After(to) && day.Before(nextMonth) {
			bars, err := c.client.FetchDaily(symbol, c.Interval, day)
			switch {
			case errors.Is(err, ErrNotFound):
				// today/tomorrow not yet published; skip silently.
			case err != nil:
				return nil, fmt.Errorf("binanceflat %s %s: %w", symbol, day.Format("2006-01-02"), err)
			default:
				all = appendInRange(all, bars, fromMs, toMs)
			}
			day = day.AddDate(0, 0, 1)
		}
		cur = nextMonth
	}

	return all, nil
}

// SaveBars persists bars to {dir}/{symbol}/ using the configured saver.
func (c *Crawler) SaveBars(dir, symbol string, from, to time.Time, bars []model.Bar) {
	frameLabel := c.FrameLabel
	if frameLabel == "" {
		frameLabel = strings.ToUpper(c.Interval)
	}
	saver.SaveBars("binance", frameLabel, dir, symbol, bars, c.PacketSaver)
}

func appendInRange(dst, src []model.Bar, fromMs, toMs int64) []model.Bar {
	for _, b := range src {
		if b.Timestamp >= fromMs && b.Timestamp <= toMs {
			dst = append(dst, b)
		}
	}
	return dst
}
