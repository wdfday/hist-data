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

// dailyFileRe matches: SYMBOL_TF_YYYY-MM-DD_to_YYYY-MM-DD.parquet
var dailyFileRe = regexp.MustCompile(
	`^(.+)_([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.parquet$`,
)

type monthKey struct {
	symbol string
	tf     string
	year   int
	month  time.Month
}

// CompactMonthly walks providerDir and compacts daily Parquet files from
// completed months into one monthly file per (symbol, tf, month).
//
// Expected structure: {providerDir}/{tf}/{symbol}/*.parquet
// Monthly output:     {providerDir}/{tf}/{symbol}/{symbol}_{TF}_{YYYY-MM}.parquet
//
// Skips the current month. Skips groups where a monthly file already exists.
func CompactMonthly(providerDir string, saver PacketSaver) error {
	if _, err := os.Stat(providerDir); os.IsNotExist(err) {
		return nil
	}

	now := time.Now().UTC()
	currentYM := now.Format("2006-01")

	// Collect daily files grouped by (symbol, tf, year-month) → dir
	type entry struct {
		dir   string
		files []string // full paths
	}
	groups := map[monthKey]*entry{}

	err := filepath.WalkDir(providerDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		m := dailyFileRe.FindStringSubmatch(d.Name())
		if m == nil {
			return nil
		}
		symbol, tf, fromStr, toStr := m[1], m[2], m[3], m[4]

		from, err := time.Parse("2006-01-02", fromStr)
		if err != nil {
			return nil
		}
		to, err := time.Parse("2006-01-02", toStr)
		if err != nil {
			return nil
		}

		// Only compact files within the same month that is not the current month
		sameMonth := from.Year() == to.Year() && from.Month() == to.Month()
		ym := from.Format("2006-01")
		if !sameMonth || ym == currentYM {
			return nil
		}

		k := monthKey{symbol: symbol, tf: tf, year: from.Year(), month: from.Month()}
		if _, ok := groups[k]; !ok {
			groups[k] = &entry{dir: filepath.Dir(path)}
		}
		groups[k].files = append(groups[k].files, path)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk %s: %w", providerDir, err)
	}

	for k, e := range groups {
		ym := fmt.Sprintf("%04d-%02d", k.year, int(k.month))
		monthlyName := fmt.Sprintf("%s_%s_%s.parquet", k.symbol, k.tf, ym)
		monthlyPath := filepath.Join(e.dir, monthlyName)

		if _, err := os.Stat(monthlyPath); err == nil {
			slog.Debug("compact: monthly file exists, skipping",
				"symbol", k.symbol, "tf", k.tf, "month", ym)
			continue
		}

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
			continue
		}

		sort.Slice(all, func(i, j int) bool { return all[i].Timestamp < all[j].Timestamp })

		if err := saver.Save(all, monthlyPath); err != nil {
			slog.Error("compact: write failed", "path", monthlyPath, "err", err)
			continue
		}

		for _, f := range e.files {
			if err := os.Remove(f); err != nil {
				slog.Warn("compact: remove daily file failed", "path", f, "err", err)
			}
		}

		slog.Info("compact: month done",
			"symbol", k.symbol, "tf", k.tf, "month", ym,
			"files", len(e.files), "bars", len(all), "out", monthlyPath)
	}

	return nil
}
