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
		saveBars(provider, sinkFrame, sinkDir, symbol, sinkBars, packetSaver)
	}
}

func saveBars(provider, frame, dir, symbol string, bars []model.Bar, packetSaver PacketSaver) {
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

func barsForFrame(sourceFrame, sinkFrame string, bars []model.Bar) ([]model.Bar, error) {
	sourceFrame = strings.ToUpper(strings.TrimSpace(sourceFrame))
	sinkFrame = strings.ToUpper(strings.TrimSpace(sinkFrame))
	if sinkFrame == sourceFrame {
		return bars, nil
	}
	if sourceFrame != "M1" {
		return nil, fmt.Errorf("aggregation only supported from M1 source")
	}

	duration, err := frameDuration(sinkFrame)
	if err != nil {
		return nil, err
	}

	var out []model.Bar
	var current model.Bar
	var bucketStart time.Time
	var inBucket bool

	flush := func() {
		if inBucket {
			out = append(out, current)
		}
	}

	for _, bar := range bars {
		ts := time.UnixMilli(bar.Timestamp).UTC()
		start := ts.Truncate(duration)
		if !inBucket || !start.Equal(bucketStart) {
			flush()
			bucketStart = start
			current = model.Bar{
				Timestamp:    start.UnixMilli(),
				Open:         bar.Open,
				High:         bar.High,
				Low:          bar.Low,
				Close:        bar.Close,
				Volume:       bar.Volume,
				VWAP:         bar.VWAP,
				Transactions: bar.Transactions,
			}
			inBucket = true
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

	flush()
	return out, nil
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

func frameDuration(frame string) (time.Duration, error) {
	switch frame {
	case "M5":
		return 5 * time.Minute, nil
	case "M15":
		return 15 * time.Minute, nil
	case "M30":
		return 30 * time.Minute, nil
	case "H1":
		return time.Hour, nil
	case "H4":
		return 4 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported sink frame %q", frame)
	}
}
