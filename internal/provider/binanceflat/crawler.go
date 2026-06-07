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
// Strategy:
//   - Full-history backfill (from = zero): walk month-by-month. Closed months
//     use monthly ZIPs first (404 → fallback to daily). Current month: daily.
//   - Incremental crawl (from ≠ zero): always daily ZIPs regardless of whether
//     the month is closed. saver.CompactMonthly merges daily → monthly at the
//     next startup.
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
// Full-history (from=zero): closed months use monthly ZIPs (404 → daily fallback).
// Incremental (from≠zero): always daily ZIPs; CompactMonthly merges at startup.
func (c *Crawler) FetchBars(symbol, _ string, from, to time.Time) ([]model.Bar, error) {
	// fullHistory = true when the caller provides a zero `from`, meaning
	// "start from the beginning". Only in this mode do we use monthly ZIPs
	// for closed months (bulk backfill). Incremental crawl always uses daily
	// ZIPs; CompactMonthly merges them at next startup.
	fullHistory := from.IsZero()

	if from.IsZero() {
		from = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	if !from.Before(to) {
		return nil, nil
	}

	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()

	now := time.Now().UTC()
	currentMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	var all []model.Bar
	cur := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	for !cur.After(to) {
		nextMonth := cur.AddDate(0, 1, 0)

		// Use monthly ZIPs only during full-history backfill for closed months.
		if fullHistory && cur.Before(currentMonthStart) {
			bars, err := c.client.FetchMonthly(symbol, c.Interval, cur.Year(), cur.Month())
			switch {
			case errors.Is(err, ErrNotFound):
				// Monthly ZIP not yet published — fall back to daily ZIPs.
				slog.Debug("binanceflat monthly not found, falling back to daily", "symbol", symbol, "month", cur.Format("2006-01"))
				dailyBars, err2 := c.fetchDailyRange(symbol, from, to, cur, nextMonth, fromMs, toMs)
				if err2 != nil {
					return nil, err2
				}
				all = append(all, dailyBars...)
			case err != nil:
				return nil, fmt.Errorf("binanceflat %s %s: %w", symbol, cur.Format("2006-01"), err)
			default:
				all = appendInRange(all, bars, fromMs, toMs)
			}
			cur = nextMonth
			continue
		}

		// Incremental crawl or current month: always daily ZIPs.
		dailyBars, err := c.fetchDailyRange(symbol, from, to, cur, nextMonth, fromMs, toMs)
		if err != nil {
			return nil, err
		}
		all = append(all, dailyBars...)
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

// fetchDailyRange fetches day-by-day from max(dayStart, from) up to min(to, monthEnd-1).
func (c *Crawler) fetchDailyRange(symbol string, from, to, monthStart, monthEnd time.Time, fromMs, toMs int64) ([]model.Bar, error) {
	day := monthStart
	if day.Before(from) {
		day = time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)
	}
	var out []model.Bar
	for !day.After(to) && day.Before(monthEnd) {
		bars, err := c.client.FetchDaily(symbol, c.Interval, day)
		switch {
		case errors.Is(err, ErrNotFound):
			// not yet published (T+1 lag or future date); skip silently.
		case err != nil:
			return nil, fmt.Errorf("binanceflat %s %s: %w", symbol, day.Format("2006-01-02"), err)
		default:
			out = appendInRange(out, bars, fromMs, toMs)
		}
		day = day.AddDate(0, 0, 1)
	}
	return out, nil
}

func appendInRange(dst, src []model.Bar, fromMs, toMs int64) []model.Bar {
	for _, b := range src {
		if b.Timestamp >= fromMs && b.Timestamp <= toMs {
			dst = append(dst, b)
		}
	}
	return dst
}
