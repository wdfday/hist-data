package binance

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"hist-data/internal/model"
)

const (
	defaultBaseURL  = "https://api.binance.com"
	klinesEndpoint  = "/api/v3/klines"
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

// GetKlines fetches OHLCV bars for symbol in [startMs, endMs).
// interval is a Binance interval string: "1m", "5m", "15m", "1h", "4h", "1d".
// Returns at most maxKlinesPerReq bars per call; caller must chunk for larger ranges.
func (c *Client) GetKlines(symbol, interval string, startMs, endMs int64) ([]model.Bar, error) {
	url := fmt.Sprintf("%s%s?symbol=%s&interval=%s&startTime=%d&endTime=%d&limit=%d",
		c.baseURL, klinesEndpoint, symbol, interval, startMs, endMs, maxKlinesPerReq)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("binance GET klines: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("binance klines HTTP %d: %s", resp.StatusCode, body)
	}

	// Binance returns a 2D JSON array:
	// [[openTime, open, high, low, close, volume, closeTime, ...], ...]
	var raw [][]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("binance klines decode: %w", err)
	}

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
