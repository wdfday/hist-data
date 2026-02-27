package crawl

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ProgressUpdate is sent when a crawl unit succeeds.
// We include Source/Class so that the same symbol (e.g. "BTCUSD") in different
// markets/providers can be tracked independently in the progress file.
type ProgressUpdate struct {
	Source string
	Class  AssetClass
	Ticker string
	Date   string
}

func progressKey(source string, class AssetClass, symbol string) string {
	s := strings.TrimSpace(strings.ToLower(source))
	if s == "" {
		s = DefaultSource
	}
	c := strings.TrimSpace(string(class))
	if c == "" {
		c = string(DefaultAssetClass)
	}
	return fmt.Sprintf("%s:%s:%s", s, c, strings.ToUpper(strings.TrimSpace(symbol)))
}

func loadProgress(path string) map[string]string {
	data, err := os.ReadFile(path)
	if err != nil {
		return make(map[string]string)
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return make(map[string]string)
	}
	return m
}

func saveProgress(path string, m map[string]string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// BootstrapProgress ensures that progress file has an entry for every Job
// identity (Source/Class/Ticker). For missing ones it seeds last-day so that
// the next crawl starts from 2 years ago.
func BootstrapProgress(path string, targets []Job, now time.Time) {
	m := loadProgress(path)
	if m == nil {
		m = make(map[string]string)
	}

	// Seed so that next crawl starts from (now - 2y)
	start := time.Date(now.Year()-2, now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	seedLast := start.AddDate(0, 0, -1).Format("2006-01-02") // last + 1 = start

	added := 0
	for _, target := range targets {
		key := progressKey(target.Source, target.Class, target.Ticker)
		if _, ok := m[key]; ok {
			continue
		}
		// Backwards-compat: if legacy plain ticker entry exists, keep it and
		// don't overwrite; future writes will use the new composite key.
		if _, okLegacy := m[target.Ticker]; okLegacy {
			continue
		}
		m[key] = seedLast
		added++
	}
	if added == 0 {
		return
	}

	if err := saveProgress(path, m); err != nil {
		slog.Warn("progress bootstrap write failed", "err", err)
		return
	}
	slog.Info("progress bootstrapped", "path", path, "new_entries", added)
}

// RunProgressWriter receives updates and persists to file (run as goroutine)
func RunProgressWriter(path string, updates <-chan ProgressUpdate) {
	m := loadProgress(path)
	for u := range updates {
		key := progressKey(u.Source, u.Class, u.Ticker)
		m[key] = u.Date
		if err := saveProgress(path, m); err != nil {
			slog.Warn("progress write failed", "ticker", u.Ticker, "err", err)
		}
	}
}
