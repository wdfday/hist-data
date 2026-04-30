package binance

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"hist-data/internal/model"
)

const (
	defaultBaseURL  = "https://api.binance.com"
	klinesEndpoint  = "/api/v3/klines"
	exchangeInfoAPI = "/api/v3/exchangeInfo"
	maxKlinesPerReq = 1000 // Binance API max limit
)

// Client is a minimal Binance REST client for public klines data.
// No API key required — klines are part of the public market data API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a Binance REST client.
func NewClient(baseURL string) *Client {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetSymbolStartTime resolves the earliest practical fetch time for a symbol.
// Tries exchangeInfo's onboardDate first; if that's missing (some regions/
// gateways strip the field), falls back to probing /klines with startTime=0
// limit=1 — Binance returns the very first available bar, whose openTime is
// the listing time.
func (c *Client) GetSymbolStartTime(symbol string) (time.Time, error) {
	if t, ok := c.onboardFromExchangeInfo(symbol); ok {
		return t, nil
	}
	return c.onboardFromFirstKline(symbol)
}

// onboardFromExchangeInfo returns (t, true) only when exchangeInfo gives a
// usable onboardDate. Any failure (HTTP, decode, missing field) returns
// (zero, false) so the caller can fall back to the kline probe.
func (c *Client) onboardFromExchangeInfo(symbol string) (time.Time, bool) {
	url := fmt.Sprintf("%s%s?symbol=%s", c.baseURL, exchangeInfoAPI, symbol)

	slog.Debug("binance exchangeInfo request", "url", url)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		slog.Debug("binance exchangeInfo failed", "symbol", symbol, "err", err)
		return time.Time{}, false
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		slog.Debug("binance exchangeInfo non-200", "status", resp.StatusCode, "symbol", symbol)
		return time.Time{}, false
	}

	var raw struct {
		Symbols []struct {
			Symbol      string `json:"symbol"`
			OnboardDate int64  `json:"onboardDate"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return time.Time{}, false
	}
	if len(raw.Symbols) == 0 || raw.Symbols[0].OnboardDate <= 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(raw.Symbols[0].OnboardDate).UTC(), true
}

// onboardFromFirstKline asks /klines for the earliest bar of `symbol` by
// passing startTime=0 limit=1. Binance returns the listing-time kline.
func (c *Client) onboardFromFirstKline(symbol string) (time.Time, error) {
	url := fmt.Sprintf("%s%s?symbol=%s&interval=1d&startTime=0&limit=1",
		c.baseURL, klinesEndpoint, symbol)

	slog.Debug("binance first-kline probe", "url", url)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return time.Time{}, fmt.Errorf("binance first-kline probe: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("binance first-kline probe HTTP %d: %s", resp.StatusCode, body)
	}

	var raw [][]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return time.Time{}, fmt.Errorf("binance first-kline probe decode: %w", err)
	}
	if len(raw) == 0 || len(raw[0]) == 0 {
		return time.Time{}, fmt.Errorf("binance %s: first-kline probe returned no bars", symbol)
	}
	var openTime int64
	if err := json.Unmarshal(raw[0][0], &openTime); err != nil {
		return time.Time{}, fmt.Errorf("binance first-kline openTime parse: %w", err)
	}
	return time.UnixMilli(openTime).UTC(), nil
}

// GetKlines fetches OHLCV bars for symbol in [startMs, endMs).
// interval is a Binance interval string: "1m", "5m", "15m", "1h", "4h", "1d".
// Returns at most maxKlinesPerReq bars per call; caller must chunk for larger ranges.
func (c *Client) GetKlines(symbol, interval string, startMs, endMs int64) ([]model.Bar, error) {
	url := fmt.Sprintf("%s%s?symbol=%s&interval=%s&startTime=%d&endTime=%d&limit=%d",
		c.baseURL, klinesEndpoint, symbol, interval, startMs, endMs, maxKlinesPerReq)

	slog.Debug("binance klines request", "url", url)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("binance GET klines: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Debug("binance klines non-200", "status", resp.StatusCode, "body", string(body), "url", url)
		return nil, fmt.Errorf("binance klines HTTP %d: %s", resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("binance klines read: %w", err)
	}
	// Binance returns a 2D JSON array:
	// [[openTime, open, high, low, close, volume, closeTime, ...], ...]
	var raw [][]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		slog.Debug("binance klines decode failed", "body", string(body), "url", url)
		return nil, fmt.Errorf("binance klines decode: %w", err)
	}
	slog.Debug("binance klines response",
		"rows", len(raw), "symbol", symbol, "interval", interval, "start_ms", startMs, "end_ms", endMs)

	bars := make([]model.Bar, 0, len(raw))
	for _, row := range raw {
		if len(row) < 6 {
			continue
		}
		bar, err := parseKlineRow(row)
		if err != nil {
			return nil, fmt.Errorf("binance klines parse: %w", err)
		}
		bars = append(bars, bar)
	}
	return bars, nil
}

// parseKlineRow converts one Binance kline array row to model.Bar.
func parseKlineRow(row []json.RawMessage) (model.Bar, error) {
	// Index 0: open time (ms), 1: open, 2: high, 3: low, 4: close, 5: volume
	var openTimeRaw int64
	if err := json.Unmarshal(row[0], &openTimeRaw); err != nil {
		return model.Bar{}, fmt.Errorf("open time: %w", err)
	}

	parseFloat := func(idx int) (float64, error) {
		var s string
		if err := json.Unmarshal(row[idx], &s); err != nil {
			return 0, err
		}
		return strconv.ParseFloat(s, 64)
	}

	open, err := parseFloat(1)
	if err != nil {
		return model.Bar{}, fmt.Errorf("open: %w", err)
	}
	high, err := parseFloat(2)
	if err != nil {
		return model.Bar{}, fmt.Errorf("high: %w", err)
	}
	low, err := parseFloat(3)
	if err != nil {
		return model.Bar{}, fmt.Errorf("low: %w", err)
	}
	close_, err := parseFloat(4)
	if err != nil {
		return model.Bar{}, fmt.Errorf("close: %w", err)
	}
	vol, err := parseFloat(5)
	if err != nil {
		return model.Bar{}, fmt.Errorf("volume: %w", err)
	}

	return model.Bar{
		Timestamp: openTimeRaw,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     close_,
		Volume:    int64(vol * 1e6), // store as scaled int to avoid float precision loss
	}, nil
}
