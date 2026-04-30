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
// We include Source/Class/Frame so that the same symbol (e.g. "BTCUSD") in
// different markets/providers/timeframes can be tracked independently in the
// progress file.
type ProgressUpdate struct {
	Source string
	Class  AssetClass
	Frame  string
	Ticker string
	Date   string
}

func progressKey(source string, class AssetClass, frame, symbol string) string {
	s := strings.TrimSpace(strings.ToLower(source))
	if s == "" {
		s = DefaultSource
	}
	c := strings.TrimSpace(string(class))
	if c == "" {
		c = string(DefaultAssetClass)
	}
	f := strings.ToUpper(strings.TrimSpace(frame))
	return fmt.Sprintf("%s:%s:%s:%s", s, c, f, strings.ToUpper(strings.TrimSpace(symbol)))
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

func loadProgressFrom(path string) map[string]string {
	if strings.TrimSpace(path) == "" {
		return map[string]string{}
	}
	return loadProgress(path)
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

// MigrateLegacyProgress copies entries from the old .lastday.json file (or any
// other legacyPath) into the new .lastrun.success.json when the latter is
// missing entries for those targets. No synthetic seeding — targets without a
// progress entry are left absent so resolveJobRange falls back to the configured
// backfillYears (or hands the crawler a zero `from` for full-history mode).
func MigrateLegacyProgress(path, legacyPath string, targets []Job) {
	if strings.TrimSpace(legacyPath) == "" {
		return
	}
	legacy := loadProgressFrom(legacyPath)
	if len(legacy) == 0 {
		return
	}
	m := loadProgress(path)
	if m == nil {
		m = make(map[string]string)
	}
	updated := 0
	for _, target := range targets {
		key := progressKey(target.Source, target.Class, target.Frame, target.Ticker)
		if _, ok := m[key]; ok {
			continue
		}
		if old, ok := legacy[key]; ok && old != "" {
			m[key] = old
			updated++
		}
	}
	if updated == 0 {
		return
	}
	if err := saveProgress(path, m); err != nil {
		slog.Warn("progress migrate write failed", "err", err)
		return
	}
	slog.Info("progress migrated from legacy", "path", path, "entries", updated)
}

// RunProgressWriter receives updates and persists to file (run as goroutine)
func RunProgressWriter(path string, updates <-chan ProgressUpdate) {
	m := loadProgress(path)
	for u := range updates {
		key := progressKey(u.Source, u.Class, u.Frame, u.Ticker)
		m[key] = u.Date
		if err := saveProgress(path, m); err != nil {
			slog.Warn("progress write failed", "ticker", u.Ticker, "err", err)
		}
	}
}

func WriteProviderLastDay(path, provider string, day time.Time) error {
	if strings.TrimSpace(path) == "" || day.IsZero() {
		return nil
	}
	payload := map[string]string{
		"provider": provider,
		"last_day": day.UTC().Format("2006-01-02"),
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
