package saver

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"hist-data/internal/model"
)

func SaveFrameSet(provider, sourceFrame string, sinkFrames []string, dir, symbol string, from, to time.Time, bars []model.Bar, packetSaver PacketSaver) {
	if dir == "" || packetSaver == nil || len(bars) == 0 {
		return
	}

	providerDir := filepath.Dir(dir)
	for _, sinkFrame := range sinkFrames {
		sinkBars, err := barsForFrame(sourceFrame, sinkFrame, bars)
		if err != nil {
			slog.Error(provider+" save: aggregate failed", "symbol", symbol, "source", sourceFrame, "sink", sinkFrame, "err", err)
			continue
		}
		if len(sinkBars) == 0 {
			continue
		}
		sinkDir := filepath.Join(providerDir, sinkFrame)
		SaveBars(provider, sinkFrame, sinkDir, symbol, sinkBars, packetSaver)
	}
}

// intradayMaxRank is the rank of H4 — frames at or below this rank are
// intraday and get partitioned by calendar month.
const intradayMaxRank = 11 // H12

// SaveBars persists bars for a single frame directly (no aggregation).
// Intraday frames (M1–H4) are partitioned by calendar month; daily and
// higher frames are saved as a single range file.
//
// Partitioning strategy for intraday frames:
//   - Closed months  → one file per month: SYMBOL_TF_YYYY-MM.parquet
//   - Current month  → one file per day:   SYMBOL_TF_YYYY-MM-DD.parquet
//
// Month-end compaction (merging daily files → monthly file) is handled by
// saver.CompactMonthly, called separately by the scheduler.
func SaveBars(provider, frame, dir, symbol string, bars []model.Bar, packetSaver PacketSaver) {
	if len(bars) == 0 {
		return
	}
	rank := frameRank[strings.ToUpper(strings.TrimSpace(frame))]
	if rank > 0 && rank <= intradayMaxRank {
		saveBarsByMonth(provider, frame, dir, symbol, bars, packetSaver)
		return
	}
	saveBarsSingleFile(provider, frame, dir, symbol, bars, packetSaver)
}

func saveBarsByMonth(provider, frame, dir, symbol string, bars []model.Bar, packetSaver PacketSaver) {
	tickerDir := filepath.Join(dir, symbol)
	if err := os.MkdirAll(tickerDir, 0o755); err != nil {
		slog.Error(provider+" save: mkdir failed", "symbol", symbol, "dir", tickerDir, "err", err)
		return
	}
	ext := packetSaver.Extension()
	frameUp := strings.ToUpper(frame)

	now := time.Now().UTC()
	curYear, curMonth := now.Year(), now.Month()

	// partitionKey returns "YYYY-MM-DD" for current month bars (daily partition)
	// and "YYYY-MM" for closed month bars (monthly partition).
	partitionKey := func(ts int64) string {
		t := time.UnixMilli(ts).UTC()
		if t.Year() == curYear && t.Month() == curMonth {
			return t.Format("2006-01-02")
		}
		return t.Format("2006-01")
	}

	start := 0
	for i := 1; i <= len(bars); i++ {
		curKey := partitionKey(bars[start].Timestamp)
		if i < len(bars) && partitionKey(bars[i].Timestamp) == curKey {
			continue
		}
		chunk := bars[start:i]
		name := fmt.Sprintf("%s_%s_%s.%s", symbol, frameUp, curKey, ext)
		path := filepath.Join(tickerDir, name)

		if len(curKey) == len("2006-01") {
			// Closed month: delete any leftover daily files for this month so
			// herald bootstrap doesn't load duplicates alongside the new monthly file.
			deleteDailyFiles(tickerDir, symbol, frameUp, ext, curKey)
		} else {
			// Current month (daily key): delete stale monthly file if it exists
			// (transition from old behaviour where current month was one big file).
			monthlyName := fmt.Sprintf("%s_%s_%s.%s", symbol, frameUp,
				time.UnixMilli(bars[start].Timestamp).UTC().Format("2006-01"), ext)
			if monthlyPath := filepath.Join(tickerDir, monthlyName); monthlyPath != path {
				_ = os.Remove(monthlyPath)
			}
		}

		if err := packetSaver.Save(chunk, path); err != nil {
			slog.Error(provider+" save: write failed", "symbol", symbol, "path", path, "err", err)
		} else {
			slog.Info(provider+" save ok", "symbol", symbol, "frame", frame, "bars", len(chunk), "path", path)
		}
		start = i
	}
}

// deleteDailyFiles removes SYMBOL_TF_YYYY-MM-DD.parquet files for the given
// month (ym = "YYYY-MM") from tickerDir. Called when writing the official
// monthly file so no daily fragments remain alongside it.
func deleteDailyFiles(tickerDir, symbol, frameUp, ext, ym string) {
	prefix := fmt.Sprintf("%s_%s_%s-", symbol, frameUp, ym)
	entries, err := os.ReadDir(tickerDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if !strings.HasPrefix(n, prefix) || !strings.HasSuffix(n, "."+ext) {
			continue
		}
		// Confirm it's a daily file: remainder after prefix should be "DD.ext"
		rest := strings.TrimPrefix(n, prefix)
		rest = strings.TrimSuffix(rest, "."+ext)
		if len(rest) == 2 { // "DD"
			p := filepath.Join(tickerDir, n)
			if err := os.Remove(p); err == nil {
				slog.Info("saver: removed daily fragment", "path", p)
			}
		}
	}
}

func saveBarsSingleFile(provider, frame, dir, symbol string, bars []model.Bar, packetSaver PacketSaver) {
	tickerDir := filepath.Join(dir, symbol)
	if err := os.MkdirAll(tickerDir, 0o755); err != nil {
		slog.Error(provider+" save: mkdir failed", "symbol", symbol, "dir", tickerDir, "err", err)
		return
	}
	from := time.UnixMilli(bars[0].Timestamp).UTC()
	to := time.UnixMilli(bars[len(bars)-1].Timestamp).UTC()
	ext := packetSaver.Extension()
	name := fmt.Sprintf("%s_%s_%s_to_%s.%s",
		symbol,
		strings.ToUpper(frame),
		from.Format("2006-01-02"),
		to.Format("2006-01-02"),
		ext,
	)
	path := filepath.Join(tickerDir, name)
	if err := packetSaver.Save(bars, path); err != nil {
		slog.Error(provider+" save: write failed", "symbol", symbol, "path", path, "err", err)
		return
	}
	slog.Info(provider+" save ok", "symbol", symbol, "frame", frame, "bars", len(bars), "path", path)
}

// frameRank defines the hierarchy for sink validation.
// A sink frame must have a higher rank than its source.
var frameRank = map[string]int{
	"M1": 1, "M3": 2, "M5": 3, "M15": 4, "M30": 5,
	"H1": 6, "H2": 7, "H4": 8, "H6": 9, "H8": 10, "H12": 11,
	"D1": 12, "W1": 13, "MN": 14,
}

func barsForFrame(sourceFrame, sinkFrame string, bars []model.Bar) ([]model.Bar, error) {
	sourceFrame = strings.ToUpper(strings.TrimSpace(sourceFrame))
	sinkFrame = strings.ToUpper(strings.TrimSpace(sinkFrame))
	if sinkFrame == sourceFrame {
		return bars, nil
	}

	srcRank, srcOk := frameRank[sourceFrame]
	dstRank, dstOk := frameRank[sinkFrame]
	if !srcOk {
		return nil, fmt.Errorf("unknown source frame %q", sourceFrame)
	}
	if !dstOk {
		return nil, fmt.Errorf("unknown sink frame %q", sinkFrame)
	}
	if dstRank <= srcRank {
		return nil, fmt.Errorf("sink frame %q must be higher than source %q", sinkFrame, sourceFrame)
	}

	bucket := bucketFn(sinkFrame)

	var out []model.Bar
	var current model.Bar
	var bucketKey int64 = -1

	for _, bar := range bars {
		key := bucket(time.UnixMilli(bar.Timestamp).UTC())
		if key != bucketKey {
			if bucketKey >= 0 {
				out = append(out, current)
			}
			bucketKey = key
			current = model.Bar{
				Timestamp:    key,
				Open:         bar.Open,
				High:         bar.High,
				Low:          bar.Low,
				Close:        bar.Close,
				Volume:       bar.Volume,
				VWAP:         bar.VWAP,
				Transactions: bar.Transactions,
			}
			continue
		}
		if bar.High > current.High {
			current.High = bar.High
		}
		if bar.Low < current.Low {
			current.Low = bar.Low
		}
		current.Close = bar.Close
		current.Volume += bar.Volume
		current.Transactions += bar.Transactions
		current.VWAP = mergeVWAP(current.VWAP, current.Volume-bar.Volume, bar.VWAP, bar.Volume, current.Close)
	}
	if bucketKey >= 0 {
		out = append(out, current)
	}
	return out, nil
}

// bucketFn returns a function that maps a UTC timestamp to its bucket start (UnixMilli).
func bucketFn(frame string) func(time.Time) int64 {
	switch frame {
	case "M3":
		return func(t time.Time) int64 { return t.Truncate(3 * time.Minute).UnixMilli() }
	case "M5":
		return func(t time.Time) int64 { return t.Truncate(5 * time.Minute).UnixMilli() }
	case "M15":
		return func(t time.Time) int64 { return t.Truncate(15 * time.Minute).UnixMilli() }
	case "M30":
		return func(t time.Time) int64 { return t.Truncate(30 * time.Minute).UnixMilli() }
	case "H1":
		return func(t time.Time) int64 { return t.Truncate(time.Hour).UnixMilli() }
	case "H2":
		return func(t time.Time) int64 { return t.Truncate(2 * time.Hour).UnixMilli() }
	case "H4":
		return func(t time.Time) int64 { return t.Truncate(4 * time.Hour).UnixMilli() }
	case "H6":
		return func(t time.Time) int64 { return t.Truncate(6 * time.Hour).UnixMilli() }
	case "H8":
		return func(t time.Time) int64 { return t.Truncate(8 * time.Hour).UnixMilli() }
	case "H12":
		return func(t time.Time) int64 { return t.Truncate(12 * time.Hour).UnixMilli() }
	case "D1":
		return func(t time.Time) int64 { return t.Truncate(24 * time.Hour).UnixMilli() }
	case "W1":
		return func(t time.Time) int64 {
			// truncate to Monday 00:00 UTC
			d := int(t.Weekday())
			if d == 0 {
				d = 7 // Sunday → 7 so Monday offset = d-1 = 6
			}
			return t.AddDate(0, 0, -(d - 1)).Truncate(24 * time.Hour).UnixMilli()
		}
	case "MN":
		return func(t time.Time) int64 {
			return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC).UnixMilli()
		}
	default:
		// fallback: daily bucket
		return func(t time.Time) int64 { return t.Truncate(24 * time.Hour).UnixMilli() }
	}
}

func mergeVWAP(leftVWAP float64, leftVol int64, rightVWAP float64, rightVol int64, fallbackClose float64) float64 {
	totalVol := leftVol + rightVol
	if totalVol <= 0 {
		return fallbackClose
	}
	leftValue := leftVWAP * float64(leftVol)
	if leftVol == 0 {
		leftValue = 0
	}
	rightValue := rightVWAP * float64(rightVol)
	if rightVol == 0 {
		rightValue = 0
	}
	if leftValue == 0 && leftVol > 0 {
		leftValue = fallbackClose * float64(leftVol)
	}
	if rightValue == 0 && rightVol > 0 {
		rightValue = fallbackClose * float64(rightVol)
	}
	return (leftValue + rightValue) / float64(totalVol)
}
