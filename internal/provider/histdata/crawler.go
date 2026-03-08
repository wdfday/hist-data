package histdata

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"hist-data/internal/model"
	"hist-data/internal/saver"
)

// Crawler fetches 1-minute OHLCV bars from HistData free CSV downloads.
// Only 1-minute bars are available — no tick data.
type Crawler struct {
	client        *Client
	SavePacketDir string
	PacketSaver   saver.PacketSaver
}

// NewCrawler creates a HistData Crawler.
// saveDir is the root output directory (e.g. "data/HistData").
// No API key required.
func NewCrawler(baseURL, saveDir string, ps saver.PacketSaver) *Crawler {
	return &Crawler{
		client:        NewClient(baseURL),
		SavePacketDir: saveDir,
		PacketSaver:   ps,
	}
}

// FetchBars downloads and parses 1-minute OHLCV bars for pair over [from, to].
// pair must be an uppercase joined pair code, e.g. "EURUSD", "GBPUSD".
// apiKey is ignored — HistData is a free public download, no key needed.
// Data is sourced at 1-minute granularity — the finest available.
func (c *Crawler) FetchBars(pair, _ string, from, to time.Time) ([]model.Bar, error) {
	var all []model.Bar

	// Iterate month by month
	cur := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(to.Year(), to.Month(), 1, 0, 0, 0, 0, time.UTC)

	for !cur.After(end) {
		bars, err := c.client.GetBars(pair, cur.Year(), int(cur.Month()))
		if err != nil {
			return nil, fmt.Errorf("histdata %s %04d-%02d: %w", pair, cur.Year(), cur.Month(), err)
		}
		// Filter to [from, to] range
		for _, b := range bars {
			ts := time.UnixMilli(b.Timestamp)
			if (ts.Equal(from) || ts.After(from)) && ts.Before(to) {
				all = append(all, b)
			}
		}
		cur = cur.AddDate(0, 1, 0)
	}

	return all, nil
}

// SaveBars persists bars to {dir}/{pair}/ using the configured saver.
func (c *Crawler) SaveBars(dir, pair string, from, to time.Time, bars []model.Bar) {
	if dir == "" || c.PacketSaver == nil || len(bars) == 0 {
		return
	}
	pairDir := filepath.Join(dir, pair)
	if err := os.MkdirAll(pairDir, 0o755); err != nil {
		slog.Error("histdata save: mkdir failed", "pair", pair, "dir", pairDir, "err", err)
		return
	}
	ext := c.PacketSaver.Extension()
	name := fmt.Sprintf("%s_1m_%s_to_%s.%s",
		pair, from.Format("2006-01-02"), to.Format("2006-01-02"), ext)
	path := filepath.Join(pairDir, name)
	if err := c.PacketSaver.Save(bars, path); err != nil {
		slog.Error("histdata save: write failed", "pair", pair, "path", path, "err", err)
	} else {
		slog.Info("histdata save ok", "pair", pair, "bars", len(bars), "path", path)
	}
}
