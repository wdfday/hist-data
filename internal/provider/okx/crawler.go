package okx

import (
	"fmt"
	"strings"
	"time"

	"hist-data/internal/model"
	"hist-data/internal/saver"
)

// intervalDuration maps OKX bar strings to time.Duration for chunking.
var intervalDuration = map[string]time.Duration{
	"1m":  time.Minute,
	"3m":  3 * time.Minute,
	"5m":  5 * time.Minute,
	"15m": 15 * time.Minute,
	"30m": 30 * time.Minute,
	"1H":  time.Hour,
	"2H":  2 * time.Hour,
	"4H":  4 * time.Hour,
	"6H":  6 * time.Hour,
	"12H": 12 * time.Hour,
	"1D":  24 * time.Hour,
	"1W":  7 * 24 * time.Hour,
}

// Crawler fetches OHLCV bars from OKX public market data API and persists them.
// No API key required for public endpoints.
type Crawler struct {
	client        *Client
	SavePacketDir string
	PacketSaver   saver.PacketSaver
	Interval      string // OKX bar string: "1m", "4H", "1D", etc.
	FrameLabel    string
	SinkFrames    []string
}

// NewCrawler creates an OKX Crawler.
// interval is an OKX bar string (e.g. "1m", "4H", "1D").
// saveDir is the root output directory (e.g. "data/OKX").
func NewCrawler(baseURL, saveDir, interval string, ps saver.PacketSaver) (*Crawler, error) {
	if _, ok := intervalDuration[interval]; !ok {
		return nil, fmt.Errorf("unsupported OKX interval %q (valid: 1m, 5m, 15m, 30m, 1H, 4H, 1D, ...)", interval)
	}
	return &Crawler{
		client:        NewClient(baseURL),
		SavePacketDir: saveDir,
		PacketSaver:   ps,
		Interval:      interval,
	}, nil
}

// FetchBars retrieves OHLCV bars for instId (e.g. "BTC-USDT") over [from, to].
// apiKey is ignored — OKX market data is public.
// Paginates via before/after cursors since OKX returns at most 100–300 bars per request.
//
// `from` may be the zero time, meaning full-history mode (config
// backfillYears=0). We try to resolve the listing time via /public/instruments
// to avoid burning requests on pre-listing periods; if that fails we still
// paginate backward and let an empty batch terminate the loop naturally. When
// `from` is non-zero we clamp up to listTime when known.
func (c *Crawler) FetchBars(instId, _ string, from, to time.Time) ([]model.Bar, error) {
	barDur, ok := intervalDuration[c.Interval]
	if !ok {
		return nil, fmt.Errorf("unknown interval %q", c.Interval)
	}

	listTime, _ := c.client.GetListTime(instId)
	switch {
	case from.IsZero() && !listTime.IsZero():
		from = listTime
	case from.IsZero():
		// listTime lookup failed: use a far-past anchor; backward pagination
		// breaks naturally on first empty batch.
		from = time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
	case !listTime.IsZero() && from.Before(listTime):
		from = listTime
	}

	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()

	// OKX returns candles newest-first; paginate backward using `after` cursor.
	// Start with the recent `candles` endpoint; when it returns empty (its 1440-
	// entry hard cap is exhausted) switch transparently to `history-candles`.
	var collected []model.Bar
	cursor := toMs + 1 // exclusive upper bound
	useHistory := false
	emptyStreak := 0
	const maxEmptyRetries = 5

	for {
		batch, err := c.client.GetCandles(instId, c.Interval, cursor, useHistory)
		if err != nil {
			return nil, fmt.Errorf("fetch %s [%s, %s]: %w",
				instId, from.Format(time.RFC3339), to.Format(time.RFC3339), err)
		}

		if len(batch) == 0 {
			// If candles endpoint is exhausted, switch to history-candles once.
			if !useHistory {
				useHistory = true
				continue
			}
			emptyStreak++
			if emptyStreak > maxEmptyRetries {
				break // real end of available data
			}
			wait := time.Duration(1<<emptyStreak) * time.Second
			if wait > 16*time.Second {
				wait = 16 * time.Second
			}
			time.Sleep(wait)
			continue
		}
		emptyStreak = 0

		for _, bar := range batch {
			if bar.Timestamp >= fromMs && bar.Timestamp <= toMs {
				collected = append(collected, bar)
			}
		}

		// Oldest bar in this batch is last (descending order)
		oldest := batch[len(batch)-1].Timestamp
		if oldest <= fromMs {
			break
		}
		cursor = oldest // next page: candles strictly older than this
		// OKX public limit: 40 req/2 s (20 req/s) per IP.
		// 50 ms per request = 20 req/s — saturates the budget with 1 worker.
		time.Sleep(50 * time.Millisecond)
	}

	// Reverse to ascending order
	for i, j := 0, len(collected)-1; i < j; i, j = i+1, j-1 {
		collected[i], collected[j] = collected[j], collected[i]
	}

	// Deduplicate by timestamp (in case pagination boundary overlaps)
	seen := make(map[int64]struct{}, len(collected))
	deduped := collected[:0]
	for _, bar := range collected {
		if _, ok := seen[bar.Timestamp]; !ok {
			seen[bar.Timestamp] = struct{}{}
			deduped = append(deduped, bar)
		}
	}

	_ = barDur // used for documentation; chunking handled by cursor pagination
	return deduped, nil
}

// SaveBars persists bars to {dir}/{instId}/ using the configured saver.
func (c *Crawler) SaveBars(dir, instId string, from, to time.Time, bars []model.Bar) {
	frameLabel := c.FrameLabel
	if frameLabel == "" {
		frameLabel = strings.ToUpper(c.Interval)
	}
	saver.SaveFrameSet("okx", frameLabel, c.SinkFrames, dir, instId, from, to, bars, c.PacketSaver)
}

// EarliestAvailable returns the first known listing time for instId.
func (c *Crawler) EarliestAvailable(instId string) (time.Time, error) {
	return c.client.GetListTime(instId)
}
