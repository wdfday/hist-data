package saver

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/parquet-go/parquet-go"

	"hist-data/internal/model"
)

// dailyPartRe matches daily-partition files: SYMBOL_TF_YYYY-MM-DD.parquet
var dailyPartRe = regexp.MustCompile(
	`^(.+)_([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})\.parquet$`,
)

type monthKey struct {
	symbol string
	tf     string
	year   int
	month  time.Month
}

// CompactMonthly walks providerDir and compacts daily-partition Parquet files
// from completed months into one monthly file per (symbol, tf, month).
//
// Expected structure: {providerDir}/{tf}/{symbol}/*.parquet
// Daily files:        {symbol}_{TF}_{YYYY-MM-DD}.parquet   (written by saveBarsByMonth)
// Monthly output:     {symbol}_{TF}_{YYYY-MM}.parquet
//
// Skips the current month. Skips groups where a monthly file already exists.
func CompactMonthly(providerDir string, saver PacketSaver) error {
	if _, err := os.Stat(providerDir); os.IsNotExist(err) {
		return nil
	}

	slog.Info("compact: scanning", "dir", providerDir)

	now := time.Now().UTC()
	currentYM := now.Format("2006-01")

	// Collect daily-partition files grouped by (symbol, tf, year-month) → dir
	type entry struct {
		dir   string
		files []string // full paths
	}
	groups := map[monthKey]*entry{}

	err := filepath.WalkDir(providerDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		m := dailyPartRe.FindStringSubmatch(d.Name())
		if m == nil {
			return nil
		}
		symbol, tf, dateStr := m[1], m[2], m[3]

		t, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			return nil
		}

		// Skip current month
		if t.Format("2006-01") == currentYM {
			return nil
		}

		k := monthKey{symbol: symbol, tf: tf, year: t.Year(), month: t.Month()}
		if _, ok := groups[k]; !ok {
			groups[k] = &entry{dir: filepath.Dir(path)}
		}
		groups[k].files = append(groups[k].files, path)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk %s: %w", providerDir, err)
	}

	if len(groups) == 0 {
		slog.Info("compact: nothing to compact", "dir", providerDir)
		return nil
	}
	slog.Info("compact: found groups", "dir", providerDir, "months", len(groups))

	for k, e := range groups {
		ym := fmt.Sprintf("%04d-%02d", k.year, int(k.month))
		monthlyName := fmt.Sprintf("%s_%s_%s.parquet", k.symbol, k.tf, ym)
		monthlyPath := filepath.Join(e.dir, monthlyName)

		if _, err := os.Stat(monthlyPath); err == nil {
			slog.Info("compact: already exists, skipping",
				"symbol", k.symbol, "tf", k.tf, "month", ym)
			continue
		}

		slog.Info("compact: merging", "symbol", k.symbol, "tf", k.tf, "month", ym, "daily_files", len(e.files))
		sort.Strings(e.files)
		var all []model.Bar
		for _, f := range e.files {
			bars, err := parquet.ReadFile[model.Bar](f)
			if err != nil {
				slog.Warn("compact: read failed, skipping file", "path", f, "err", err)
				continue
			}
			all = append(all, bars...)
		}
		if len(all) == 0 {
			slog.Warn("compact: no bars read, skipping", "symbol", k.symbol, "tf", k.tf, "month", ym)
			continue
		}

		sort.Slice(all, func(i, j int) bool { return all[i].Timestamp < all[j].Timestamp })

		if err := saver.Save(all, monthlyPath); err != nil {
			slog.Error("compact: write failed", "path", monthlyPath, "err", err)
			continue
		}

		removed := 0
		for _, f := range e.files {
			if err := os.Remove(f); err != nil {
				slog.Warn("compact: remove daily file failed", "path", f, "err", err)
			} else {
				removed++
			}
		}

		slog.Info("compact: done",
			"symbol", k.symbol, "tf", k.tf, "month", ym,
			"daily_files", len(e.files), "removed", removed, "bars", len(all), "out", monthlyPath)
	}

	return nil
}
