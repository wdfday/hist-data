package vci

import (
	"fmt"
	"time"

	"hist-data/internal/model"
	"hist-data/internal/saver"
)

// MaxBarsPerRequest limits one gap-chart request to avoid 502 (doc §7).
const MaxBarsPerRequest = 30000

// Crawler fetches OHLCV bars from VCI (Vietcap) and persists them.
// Supports ONE_DAY and ONE_MINUTE; ONE_HOUR can be added if needed.
type Crawler struct {
	client        *Client
	TimeFrame     string // ONE_DAY | ONE_MINUTE | ONE_HOUR
	SavePacketDir string
	PacketSaver   saver.PacketSaver
	FrameLabel    string
	SinkFrames    []string
	// ChunkDelay is the sleep between paginated requests for a single symbol.
	// VCI has an undocumented rate limit; 500ms between chunks avoids 429s.
	ChunkDelay time.Duration
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
//
// `from` may be the zero time (full-history mode); we treat that as "fetch as
// many bars as the API will give back" — backward pagination breaks naturally
// when VCI returns an empty page or stops advancing.
func (c *Crawler) FetchBars(symbol, _ string, from, to time.Time) ([]model.Bar, error) {
	var all []model.Bar
	toSec := to.Unix()
	if !from.IsZero() && to.Before(from) {
		return nil, fmt.Errorf("vci: from must be before to")
	}

	switch c.TimeFrame {
	case TimeFrameDay:
		days := 5000 // doc: ~5k bars max for daily; use the cap when from is zero
		if !from.IsZero() {
			days = int(to.Sub(from).Hours()/24) + 1
			if days > 5000 {
				days = 5000
			}
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
		first := true
		for {
			if !first && c.ChunkDelay > 0 {
				time.Sleep(c.ChunkDelay)
			}
			first = false
			bars, err := c.client.GetOHLC(c.TimeFrame, []string{symbol}, curTo, chunk)
			if err != nil {
				return nil, err
			}
			if len(bars) == 0 {
				break
			}
			filtered := filterBarsInRange(bars, from, to)
			all = append(all, filtered...)
			// VCI returns bars in ascending order (oldest first).
			// Use bars[0] (oldest) to advance curTo backward.
			oldestTs := bars[0].Timestamp
			if oldestTs/1000 >= curTo {
				break
			}
			curTo = oldestTs/1000 - 1
			if !from.IsZero() && curTo < from.Unix() {
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
	toMs := to.UnixMilli()
	var fromMs int64
	if !from.IsZero() {
		fromMs = from.UnixMilli()
	}
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
	frameLabel := c.FrameLabel
	if frameLabel == "" {
		switch c.TimeFrame {
		case TimeFrameMinute:
			frameLabel = "M1"
		case TimeFrameHour:
			frameLabel = "H1"
		default:
			frameLabel = "D1"
		}
	}
	saver.SaveFrameSet("vci", frameLabel, c.SinkFrames, dir, symbol, from, to, bars, c.PacketSaver)
}
