package vci

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"hist-data/internal/model"
	"hist-data/internal/saver"
)

// MaxBarsPerRequest limits one gap-chart request to avoid 502 (doc §7).
const MaxBarsPerRequest = 30000

// Crawler fetches OHLCV bars from VCI (Vietcap) and persists them.
// Supports ONE_DAY and ONE_MINUTE; ONE_HOUR can be added if needed.
type Crawler struct {
	client         *Client
	TimeFrame      string // ONE_DAY | ONE_MINUTE | ONE_HOUR
	SavePacketDir  string
	PacketSaver    saver.PacketSaver
}

// NewCrawler creates a VCI crawler. timeFrame must be ONE_DAY, ONE_MINUTE, or ONE_HOUR.
func NewCrawler(baseURL, saveDir, timeFrame string, ps saver.PacketSaver) (*Crawler, error) {
	switch timeFrame {
	case TimeFrameDay, TimeFrameMinute, TimeFrameHour:
	default:
		return nil, fmt.Errorf("vci: unsupported timeFrame %q (use ONE_DAY, ONE_MINUTE, ONE_HOUR)", timeFrame)
	}
	return &Crawler{
		client:        NewClient(baseURL),
		TimeFrame:     timeFrame,
		SavePacketDir: saveDir,
		PacketSaver:   ps,
	}, nil
}

// FetchBars retrieves bars for symbol over [from, to]. apiKey is ignored (no key required).
// Chunks requests to avoid 502; for ONE_MINUTE uses at most MaxBarsPerRequest per call.
func (c *Crawler) FetchBars(symbol, _ string, from, to time.Time) ([]model.Bar, error) {
	var all []model.Bar
	toSec := to.Unix()
	if to.Before(from) {
		return nil, fmt.Errorf("vci: from must be before to")
	}

	switch c.TimeFrame {
	case TimeFrameDay:
		days := int(to.Sub(from).Hours()/24) + 1
		if days > 5000 {
			days = 5000 // doc: ~5k bars max for daily
		}
		if days <= 0 {
			return nil, nil
		}
		bars, err := c.client.GetOHLC(TimeFrameDay, []string{symbol}, toSec, days)
		if err != nil {
			return nil, err
		}
		// Filter to [from, to] (VCI returns backward from to; bars may be newest-first or oldest-first — doc says "lùi từ to")
		all = filterBarsInRange(bars, from, to)
	case TimeFrameMinute, TimeFrameHour:
		chunk := MaxBarsPerRequest
		curTo := toSec
		for {
			bars, err := c.client.GetOHLC(c.TimeFrame, []string{symbol}, curTo, chunk)
			if err != nil {
				return nil, err
			}
			if len(bars) == 0 {
				break
			}
			filtered := filterBarsInRange(bars, from, to)
			all = append(all, filtered...)
			// Next chunk: move curTo to before the oldest bar we got
			oldestTs := bars[len(bars)-1].Timestamp
			if oldestTs/1000 >= curTo {
				break
			}
			curTo = oldestTs/1000 - 1
			if curTo < from.Unix() {
				break
			}
		}
		// Reverse so chronological order (oldest first)
		for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
			all[i], all[j] = all[j], all[i]
		}
	default:
		return nil, fmt.Errorf("vci: unsupported timeFrame %s", c.TimeFrame)
	}

	return all, nil
}

func filterBarsInRange(bars []model.Bar, from, to time.Time) []model.Bar {
	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()
	var out []model.Bar
	for _, b := range bars {
		if b.Timestamp >= fromMs && b.Timestamp <= toMs {
			out = append(out, b)
		}
	}
	return out
}

// SaveBars persists bars to dir/symbol/ using the configured saver.
func (c *Crawler) SaveBars(dir, symbol string, from, to time.Time, bars []model.Bar) {
	if dir == "" || c.PacketSaver == nil || len(bars) == 0 {
		return
	}
	tickerDir := filepath.Join(dir, symbol)
	if err := os.MkdirAll(tickerDir, 0o755); err != nil {
		slog.Error("vci save: mkdir failed", "symbol", symbol, "dir", tickerDir, "err", err)
		return
	}
	ext := c.PacketSaver.Extension()
	interval := "1d"
	if c.TimeFrame == TimeFrameMinute {
		interval = "1m"
	} else if c.TimeFrame == TimeFrameHour {
		interval = "1h"
	}
	name := fmt.Sprintf("%s_%s_%s_to_%s.%s",
		symbol, interval,
		from.Format("2006-01-02"), to.Format("2006-01-02"), ext)
	path := filepath.Join(tickerDir, name)
	if err := c.PacketSaver.Save(bars, path); err != nil {
		slog.Error("vci save: write failed", "symbol", symbol, "path", path, "err", err)
	} else {
		slog.Info("vci save ok", "symbol", symbol, "bars", len(bars), "path", path)
	}
}
