package polygon

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"hist-data/internal/model"
	"hist-data/internal/saver"
)

const (
	polygonBaseURL = "https://api.polygon.io"

	// maxLimit is the Polygon hard cap on results per request.
	maxLimit = 50000

	// KeyCooldownSec: Polygon 5 req/min => 12s between requests per key
	KeyCooldownSec = 12
)

// barsPerDayBase is the maximum number of bars per calendar day for each
// supported timespan at multiplier=1. Values are the worst-case upper bound
// across all asset classes so chunk sizes never exceed maxLimit (50 000).
//
//   - minute: 1 440 = 24 × 60 — crypto/forex trade 24 h; stocks only use 960
//     (extended hours), but we must size chunks for the densest asset class
//     to avoid silent truncation on Polygon responses.
//   - hour/day/week/month: straightforward calendar units.
//
// For multiplier N, the effective bars/day = barsPerDayBase[ts] / N.
var barsPerDayBase = map[string]int{
	"minute": 1440, // 24 × 60 — upper bound for crypto/forex
	"hour":   24,
	"day":    1,
	"week":   1,
	"month":  1,
}

// timespanSuffix maps each supported timespan to its file-name suffix.
// The numeric multiplier is prepended at runtime: "5" + "min" → "5min".
var timespanSuffix = map[string]string{
	"minute": "min",
	"hour":   "h",
	"day":    "d",
	"week":   "wk",
	"month":  "mo",
}

// Crawler is responsible for fetching bar aggregates from the Polygon API
// and optionally persisting raw packets to disk.
type Crawler struct {
	client        *http.Client
	SavePacketDir string
	PacketSaver   saver.PacketSaver // When non-nil, used to persist raw packets.
	SavePerDay    bool              // When true, saves one file per day; otherwise a single range file.
	Timespan      string            // minute | hour | day | week | month (default: minute)
	Multiplier    int               // timeframe multiplier, e.g. 1, 5, 15 (default: 1)
}

func (c *Crawler) timespan() string {
	if ts := strings.ToLower(strings.TrimSpace(c.Timespan)); ts != "" {
		return ts
	}
	return "minute"
}

func (c *Crawler) multiplier() int {
	if c.Multiplier > 0 {
		return c.Multiplier
	}
	return 1
}

// timespanLabel returns a compact label for use in file names.
//
//	minute/1  → "1min"   minute/5  → "5min"
//	hour/1    → "1h"     day/1     → "1d"
//	week/1    → "1wk"    month/1   → "1mo"
func (c *Crawler) timespanLabel() string {
	suffix, ok := timespanSuffix[c.timespan()]
	if !ok {
		suffix = c.timespan()
	}
	return fmt.Sprintf("%d%s", c.multiplier(), suffix)
}

// maxChunkDays is the upper bound for a single API request window.
// 730 = 2 years, matching the default backfillYears. Any chunk larger than
// the job range is harmless (Polygon trims the response), but there is no
// reason to request more than the longest possible job window.
const maxChunkDays = 730

// maxDaysPerChunk returns the maximum calendar days per API request so that
// the bar count stays under maxLimit.
//
//	days = floor(maxLimit × multiplier / barsPerDayBase[timespan])
//
// The result is capped at maxChunkDays (730) so chunks never exceed the
// default 2-year backfill window.
func (c *Crawler) maxDaysPerChunk() int {
	base, ok := barsPerDayBase[c.timespan()]
	if !ok || base <= 0 {
		return maxChunkDays
	}
	d := maxLimit * c.multiplier() / base
	if d < 1 {
		d = 1
	}
	if d > maxChunkDays {
		d = maxChunkDays
	}
	return d
}

// estimatedBars returns a pre-allocation capacity for [from, to] to avoid slice growth.
//
// barsPerDayBase already uses worst-case density (1440 for minute = crypto/forex),
// so stocks (~960 bars/day) naturally get ~50% headroom before the +20% buffer.
// This intentional generosity avoids reallocs without a separate cap formula.
//
//	1-min  →  ~1.25 M    5-min  → ~250 k
//	1-hour →   ~21 k     1-day  →    ~880
func (c *Crawler) estimatedBars(from, to time.Time) int {
	if !from.Before(to) && !from.Equal(to) {
		return 0
	}
	days := int(to.Sub(from).Hours()/24) + 1
	if days < 1 {
		days = 1
	}
	base, ok := barsPerDayBase[c.timespan()]
	if !ok || base <= 0 {
		base = 1
	}
	barsPerDay := base / c.multiplier()
	if barsPerDay < 1 {
		barsPerDay = 1
	}
	return days*barsPerDay + days*barsPerDay/5 // +20% buffer
}

// Close closes connections
func (c *Crawler) Close() error {
	return nil
}

// SaveBars persists bars into dir/ticker/ using the configured PacketSaver.
// dir is the asset-class-specific directory (e.g. data/Polygon/stocks).
// If dir is empty or PacketSaver is nil, the call is a no-op.
//
// File name format: {ticker}_{timespan}_{from}.{ext}  (per-day)
//
//	{ticker}_{timespan}_{from}_to_{to}.{ext}  (range)
func (c *Crawler) SaveBars(dir, ticker string, from, to time.Time, bars []model.Bar) {
	if dir == "" || c.PacketSaver == nil || len(bars) == 0 {
		return
	}
	tickerDir := filepath.Join(dir, ticker)
	if err := os.MkdirAll(tickerDir, 0755); err != nil {
		slog.Error("save: mkdir failed", "ticker", ticker, "dir", tickerDir, "err", err)
		return
	}
	ext := c.PacketSaver.Extension()
	ts := c.timespanLabel()
	var name string
	if c.SavePerDay {
		name = fmt.Sprintf("%s_%s_%s.%s", ticker, ts, from.Format("2006-01-02"), ext)
	} else {
		name = fmt.Sprintf("%s_%s_%s_to_%s.%s", ticker, ts, from.Format("2006-01-02"), to.Format("2006-01-02"), ext)
	}
	packetPath := filepath.Join(tickerDir, name)
	if err := c.PacketSaver.Save(bars, packetPath); err != nil {
		slog.Error("save: write failed", "ticker", ticker, "path", packetPath, "err", err)
	} else {
		slog.Info("save ok", "ticker", ticker, "format", ext, "path", packetPath, "bars", len(bars))
	}
}

// splitDateRangeIntoChunks splits [from, to] into day chunks so each request stays under ~maxLimit bars
func splitDateRangeIntoChunks(from, to time.Time, maxDays int) [][2]time.Time {
	var chunks [][2]time.Time
	start := from.UTC()
	end := to.UTC()

	if !start.Before(end) && !start.Equal(end) {
		return chunks
	}

	for currentStart := start; !currentStart.After(end); {
		currentEnd := currentStart.AddDate(0, 0, maxDays-1)
		if currentEnd.After(end) {
			currentEnd = end
		}

		chunks = append(chunks, [2]time.Time{currentStart, currentEnd})

		if currentEnd.Equal(end) {
			break
		}

		currentStart = currentEnd.AddDate(0, 0, 1)
	}

	return chunks
}

// adjustLastChunkToAvoidDelayed returns chunkTo unchanged, or end of previous day if chunkTo is today/future (avoids DELAYED).
func adjustLastChunkToAvoidDelayed(chunkTo time.Time, isLastChunk bool) time.Time {
	if !isLastChunk {
		return chunkTo
	}
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	chunkToDate := time.Date(chunkTo.Year(), chunkTo.Month(), chunkTo.Day(), 0, 0, 0, 0, time.UTC)
	if chunkToDate.Equal(today) || chunkToDate.After(today) {
		return today.AddDate(0, 0, -1).Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	}
	return chunkTo
}

const maxRetries = 3
const retryDelay = 15 * time.Second

// buildAggregatesRequest builds a GET request for bar aggregates using the
// configured Timespan and Multiplier (e.g. range/1/minute, range/5/minute, range/1/day).
func (c *Crawler) buildAggregatesRequest(ticker string, fromMillis, toMillis int64, apiKey string) (*http.Request, error) {
	rawURL := fmt.Sprintf("%s/v2/aggs/ticker/%s/range/%d/%s/%d/%d",
		polygonBaseURL, ticker, c.multiplier(), c.timespan(), fromMillis, toMillis)
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse URL: %w", err)
	}
	q := u.Query()
	q.Set("adjusted", "true")
	q.Set("limit", strconv.Itoa(maxLimit))
	q.Set("sort", "asc")
	q.Set("apiKey", apiKey)
	u.RawQuery = q.Encode()
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Connection", "close")
	return req, nil
}

// doAggregatesRequest runs one GET request with retries. On 429 calls on429 before retry.
// Returns (nil, nil) when status is DELAYED (caller should skip chunk); (nil, err) on error; (resp, nil) on success.
func (c *Crawler) doAggregatesRequest(client *http.Client, req *http.Request, on429 func()) (*AggregatesResponse, error) {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := client.Do(req)
		if err != nil {
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return nil, fmt.Errorf("API call failed after %d attempts: %w", maxRetries, err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 429 {
				if attempt < maxRetries {
					time.Sleep(retryDelay)
					if on429 != nil {
						on429()
					}
					continue
				}
				return nil, fmt.Errorf("API rate limit (429) after %d attempts: %s", maxRetries, string(body))
			}
			return nil, fmt.Errorf("API status %d: %s", resp.StatusCode, string(body))
		}

		var result AggregatesResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return nil, fmt.Errorf("parse JSON: %w", err)
		}
		resp.Body.Close()

		if result.Status != "OK" {
			if result.Status == "DELAYED" {
				return nil, nil // caller skips chunk
			}
			return nil, fmt.Errorf("API status not OK: %s", result.Status)
		}
		return &result, nil
	}
	return nil, fmt.Errorf("no response")
}

// LastTradingDay returns the most recent NYSE trading day by fetching
// the latest 1-day bar for SPY. Uses the first available API key.
// Returns zero time if the key is empty or the request fails.
func LastTradingDay(apiKey string) (time.Time, error) {
	if apiKey == "" {
		return time.Time{}, fmt.Errorf("polygon: no API key for last trading day query")
	}
	now := time.Now().UTC()
	from := now.AddDate(0, 0, -7).UnixMilli()
	to := now.UnixMilli()
	rawURL := fmt.Sprintf("%s/v2/aggs/ticker/SPY/range/1/day/%d/%d?sort=desc&limit=1&apiKey=%s",
		polygonBaseURL, from, to, apiKey)
	resp, err := http.Get(rawURL) //nolint:noctx
	if err != nil {
		return time.Time{}, fmt.Errorf("polygon last trading day: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("polygon last trading day: HTTP %d", resp.StatusCode)
	}
	var result AggregatesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return time.Time{}, fmt.Errorf("polygon last trading day decode: %w", err)
	}
	if len(result.Results) == 0 {
		return time.Time{}, nil
	}
	ts := time.UnixMilli(result.Results[0].Timestamp).UTC()
	return time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC), nil
}

// CrawlBarsWithKey fetches bar aggregates for the given ticker and time range using
// the provided API key. The timeframe is determined by Crawler.Timespan and Crawler.Multiplier.
// Callers are responsible for API-key rotation and rate limiting.
func (c *Crawler) CrawlBarsWithKey(ticker, apiKey string, from, to time.Time) ([]model.Bar, error) {
	client := c.client
	if client == nil {
		client = http.DefaultClient
	}

	allBars := make([]model.Bar, 0, c.estimatedBars(from, to))
	chunks := splitDateRangeIntoChunks(from, to, c.maxDaysPerChunk())
	if len(chunks) == 0 {
		slog.Debug("no chunks in date range",
			"ticker", ticker, "from", from.Format("2006-01-02"), "to", to.Format("2006-01-02"))
		return allBars, nil
	}

	keyPfx := apiKey
	if len(apiKey) > 8 {
		keyPfx = apiKey[:8] + "…"
	}
	slog.Debug("fetch split",
		"ticker", ticker, "chunks", len(chunks), "timespan", c.timespanLabel(), "key", keyPfx)

	for chunkIndex, ch := range chunks {
		if chunkIndex > 0 {
			slog.Debug("rate cooldown",
				"ticker", ticker, "chunk", chunkIndex+1, "of", len(chunks),
				"wait_s", KeyCooldownSec, "key", keyPfx)
			time.Sleep(KeyCooldownSec * time.Second)
		}

		chunkFrom := ch[0]
		chunkTo := adjustLastChunkToAvoidDelayed(ch[1], chunkIndex == len(chunks)-1)

		req, err := c.buildAggregatesRequest(ticker, chunkFrom.UnixMilli(), chunkTo.UnixMilli(), apiKey)
		if err != nil {
			return nil, err
		}
		response, err := c.doAggregatesRequest(client, req, nil)
		if err != nil {
			return nil, err
		}
		if response == nil {
			continue // DELAYED
		}

		for _, barRaw := range response.Results {
			allBars = append(allBars, barRaw.ToBar())
		}

		// Post-last-chunk cooldown: key must be rested before the caller returns it to the pool.
		if chunkIndex == len(chunks)-1 {
			slog.Debug("rate cooldown (post-fetch)",
				"ticker", ticker, "wait_s", KeyCooldownSec, "key", keyPfx)
			time.Sleep(KeyCooldownSec * time.Second)
		}
	}
	return allBars, nil
}
