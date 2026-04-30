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
const intradayMaxRank = 6 // H4

// SaveBars persists bars for a single frame directly (no aggregation).
// Intraday frames (M1–H4) are partitioned by calendar month; daily and
// higher frames are saved as a single range file.
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

	start := 0
	for i := 1; i <= len(bars); i++ {
		var curKey, nextKey [2]int // [year, month]
		t := time.UnixMilli(bars[start].Timestamp).UTC()
		curKey = [2]int{t.Year(), int(t.Month())}
		if i < len(bars) {
			t2 := time.UnixMilli(bars[i].Timestamp).UTC()
			nextKey = [2]int{t2.Year(), int(t2.Month())}
		}
		if i == len(bars) || nextKey != curKey {
			chunk := bars[start:i]
			name := fmt.Sprintf("%s_%s_%04d-%02d.%s", symbol, frameUp, curKey[0], curKey[1], ext)
			path := filepath.Join(tickerDir, name)
			if err := packetSaver.Save(chunk, path); err != nil {
				slog.Error(provider+" save: write failed", "symbol", symbol, "path", path, "err", err)
			} else {
				slog.Info(provider+" save ok", "symbol", symbol, "frame", frame, "bars", len(chunk), "path", path)
			}
			start = i
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
	"M1": 1, "M5": 2, "M15": 3, "M30": 4,
	"H1": 5, "H4": 6, "D1": 7, "W1": 8, "MN": 9,
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
	case "M5":
		return func(t time.Time) int64 { return t.Truncate(5 * time.Minute).UnixMilli() }
	case "M15":
		return func(t time.Time) int64 { return t.Truncate(15 * time.Minute).UnixMilli() }
	case "M30":
		return func(t time.Time) int64 { return t.Truncate(30 * time.Minute).UnixMilli() }
	case "H1":
		return func(t time.Time) int64 { return t.Truncate(time.Hour).UnixMilli() }
	case "H4":
		return func(t time.Time) int64 { return t.Truncate(4 * time.Hour).UnixMilli() }
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
