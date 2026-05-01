package okx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"hist-data/internal/model"
)

const (
	defaultBaseURL     = "https://www.okx.com"
	historyCandlesPath = "/api/v5/market/history-candles"
	recentCandlesPath  = "/api/v5/market/candles"
	instrumentsPath    = "/api/v5/public/instruments"
	maxHistoryPerReq   = 100 // OKX history-candles limit
	maxRecentPerReq    = 300 // OKX candles limit

	// recentCutoffDuration controls the endpoint switch.
	// OKX `candles` has a hard cap of 1440 entries total (= 24 h for 1m bars).
	// Using a 2-day window ensures `candles` only covers unconfirmed/very recent
	// bars; `history-candles` handles all bulk backfill.
	recentCutoffDuration = 2 * 24 * time.Hour
)

type okxResponse struct {
	Code string              `json:"code"`
	Msg  string              `json:"msg"`
	Data [][]json.RawMessage `json:"data"`
}

// Client is a minimal OKX REST client for public candlestick data.
// No API key required for market data endpoints.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// GetListTime returns the listing timestamp of a SPOT instrument (e.g. "BTC-USDT").
// Used to clamp backfill ranges to the actual data window — OKX rejects /candles
// requests aimed at periods before the instrument was listed, and walking
// pagination from year 2000 would burn thousands of empty requests and trip the
// public rate-limit. Returns the zero time when OKX omits the field so callers
// can fall back to the originally requested range.
func (c *Client) GetListTime(instId string) (time.Time, error) {
	url := fmt.Sprintf("%s%s?instType=SPOT&instId=%s", c.baseURL, instrumentsPath, instId)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return time.Time{}, fmt.Errorf("okx GET instruments: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return time.Time{}, fmt.Errorf("okx instruments HTTP %d: %s", resp.StatusCode, body)
	}

	var raw struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			InstId   string `json:"instId"`
			ListTime string `json:"listTime"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return time.Time{}, fmt.Errorf("okx instruments decode: %w", err)
	}
	if raw.Code != "0" {
		return time.Time{}, fmt.Errorf("okx instruments error %s: %s", raw.Code, raw.Msg)
	}
	if len(raw.Data) == 0 || raw.Data[0].ListTime == "" {
		return time.Time{}, nil
	}
	ms, err := strconv.ParseInt(raw.Data[0].ListTime, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("okx instruments listTime parse: %w", err)
	}
	return time.UnixMilli(ms).UTC(), nil
}

// GetCandles fetches up to limit OHLCV bars for instId with timestamp < afterMs.
// Returns bars in descending order (newest first) — caller reverses for ascending.
// Starts with the candles (recent) endpoint; switches to history-candles when
// candles returns empty (it has a hard cap of ~1440 entries regardless of cursor).
//
// Note on OKX semantics (counter-intuitive!):
//   - `after=ts`  → returns bars OLDER than ts (use this for backward pagination)
//   - `before=ts` → returns bars NEWER than ts (forward pagination)
//
// Transient HTTP 429 and 5xx are retried with exponential backoff.
func (c *Client) GetCandles(instId, bar string, afterMs int64, useHistory bool) ([]model.Bar, error) {
	const maxAttempts = 5
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		bars, err := c.getCandlesOnce(instId, bar, afterMs, useHistory)
		if err == nil {
			return bars, nil
		}
		if !shouldRetry(err) {
			return nil, err
		}
		lastErr = err
		wait := time.Duration(500*(1<<attempt)) * time.Millisecond
		if wait > 8*time.Second {
			wait = 8 * time.Second
		}
		time.Sleep(wait)
	}
	return nil, fmt.Errorf("okx candles: exhausted retries: %w", lastErr)
}

func (c *Client) getCandlesOnce(instId, bar string, afterMs int64, useHistory bool) ([]model.Bar, error) {
	cutoff := time.Now().Add(-recentCutoffDuration).UnixMilli()

	endpoint := historyCandlesPath
	limit := maxHistoryPerReq
	if !useHistory && afterMs > cutoff {
		endpoint = recentCandlesPath
		limit = maxRecentPerReq
	}

	// `after=ts` requests bars OLDER than ts (backward pagination).
	url := fmt.Sprintf("%s%s?instId=%s&bar=%s&after=%d&limit=%d",
		c.baseURL, endpoint, instId, bar, afterMs, limit)

	slog.Debug("okx candles request", "url", url)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("okx GET candles: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Debug("okx candles non-200", "status", resp.StatusCode, "body", string(body), "url", url)
		return nil, &httpError{status: resp.StatusCode, body: string(body)}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("okx candles read: %w", err)
	}
	var okxResp okxResponse
	if err := json.Unmarshal(body, &okxResp); err != nil {
		slog.Debug("okx candles decode failed", "body", string(body), "url", url)
		return nil, fmt.Errorf("okx candles decode: %w", err)
	}
	slog.Debug("okx candles response",
		"code", okxResp.Code, "msg", okxResp.Msg, "rows", len(okxResp.Data),
		"endpoint", endpoint, "instId", instId, "after_ms", afterMs)
	if okxResp.Code != "0" {
		slog.Debug("okx candles error body", "body", string(body), "url", url)
		return nil, fmt.Errorf("okx candles error %s: %s", okxResp.Code, okxResp.Msg)
	}

	bars := make([]model.Bar, 0, len(okxResp.Data))
	for _, row := range okxResp.Data {
		if len(row) < 6 {
			continue
		}
		// Only include confirmed candles (confirm field = "1") when present
		if len(row) >= 9 {
			var confirm string
			if err := json.Unmarshal(row[8], &confirm); err == nil && confirm == "0" {
				continue // skip forming candle
			}
		}
		bar, err := parseCandleRow(row)
		if err != nil {
			return nil, fmt.Errorf("okx candle parse: %w", err)
		}
		bars = append(bars, bar)
	}
	return bars, nil
}

type httpError struct {
	status int
	body   string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("okx candles HTTP %d: %s", e.status, e.body)
}

func shouldRetry(err error) bool {
	var he *httpError
	if !errors.As(err, &he) {
		return false
	}
	return he.status == http.StatusTooManyRequests || he.status >= 500
}

// parseCandleRow converts one OKX candle array to model.Bar.
// Format: [ts, o, h, l, c, vol(base), volCcy, volCcyQuote, confirm]
func parseCandleRow(row []json.RawMessage) (model.Bar, error) {
	parseStr := func(idx int) (string, error) {
		var s string
		return s, json.Unmarshal(row[idx], &s)
	}
	parseFloat := func(idx int) (float64, error) {
		s, err := parseStr(idx)
		if err != nil {
			return 0, err
		}
		return strconv.ParseFloat(s, 64)
	}

	tsStr, err := parseStr(0)
	if err != nil {
		return model.Bar{}, fmt.Errorf("ts: %w", err)
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return model.Bar{}, fmt.Errorf("ts parse: %w", err)
	}

	o, err := parseFloat(1)
	if err != nil {
		return model.Bar{}, fmt.Errorf("open: %w", err)
	}
	h, err := parseFloat(2)
	if err != nil {
		return model.Bar{}, fmt.Errorf("high: %w", err)
	}
	l, err := parseFloat(3)
	if err != nil {
		return model.Bar{}, fmt.Errorf("low: %w", err)
	}
	cl, err := parseFloat(4)
	if err != nil {
		return model.Bar{}, fmt.Errorf("close: %w", err)
	}
	vol, err := parseFloat(5)
	if err != nil {
		return model.Bar{}, fmt.Errorf("volume: %w", err)
	}

	return model.Bar{
		Timestamp: ts,
		Open:      o,
		High:      h,
		Low:       l,
		Close:     cl,
		Volume:    int64(vol * 1e6), // scale to avoid float precision loss (same as Binance)
	}, nil
}
