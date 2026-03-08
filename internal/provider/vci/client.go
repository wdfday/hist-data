package vci

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"hist-data/internal/model"
)

const (
	defaultBaseURL   = "https://trading.vietcap.com.vn/api"
	gapChartPath     = "/chart/OHLCChart/gap-chart"
	symbolsByGroupPath = "/price/symbols/getByGroup"
)

// Client is an HTTP client for VCI (Vietcap) trading API.
// No API key required; browser-like headers are used.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a VCI REST client.
func NewClient(baseURL string) *Client {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// defaultHeaders returns headers required by VCI (doc §2).
func defaultHeaders() map[string]string {
	return map[string]string{
		"Accept":       "application/json, text/plain, */*",
		"Content-Type": "application/json",
		"Origin":       "https://trading.vietcap.com.vn",
		"Referer":      "https://trading.vietcap.com.vn/",
		"User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	}
}

// GetOHLC fetches OHLCV bars via gap-chart. toSec is Unix seconds (end of range), countBack is number of bars.
// Returns bars for the first symbol in the request (VCI returns one series per symbol in order).
func (c *Client) GetOHLC(timeFrame string, symbols []string, toSec int64, countBack int) ([]model.Bar, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("vci: symbols required")
	}
	body := GapChartRequest{
		TimeFrame:  timeFrame,
		Symbols:    symbols,
		To:         toSec,
		CountBack:  countBack,
	}
	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("vci marshal request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, c.baseURL+gapChartPath, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(reqBody))

	for k, v := range defaultHeaders() {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vci gap-chart: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vci gap-chart HTTP %d: %s", resp.StatusCode, string(b))
	}

	var series []GapChartSeries
	if err := json.NewDecoder(resp.Body).Decode(&series); err != nil {
		return nil, fmt.Errorf("vci gap-chart decode: %w", err)
	}
	if len(series) == 0 {
		return nil, nil
	}
	// Response is array per symbol; we requested one symbol → first element.
	s := series[0]
	return seriesToBars(s), nil
}

// seriesToBars converts VCI series (t in seconds) to model.Bar (Timestamp in ms).
func seriesToBars(s GapChartSeries) []model.Bar {
	n := len(s.T)
	if n == 0 {
		return nil
	}
	// Lengths may differ; use minimum.
	for _, arr := range [][]float64{s.O, s.H, s.L, s.C} {
		if len(arr) < n {
			n = len(arr)
		}
	}
	if len(s.V) < n {
		n = len(s.V)
	}
	bars := make([]model.Bar, n)
	for i := 0; i < n; i++ {
		tsSec := s.T[i].Int64()
		vol := int64(0)
		if i < len(s.V) {
			vol = s.V[i].Int64()
		}
		bars[i] = model.Bar{
			Timestamp: tsSec * 1000, // VCI uses seconds; model uses ms
			Open:      s.O[i],
			High:      s.H[i],
			Low:       s.L[i],
			Close:     s.C[i],
			Volume:    vol,
		}
	}
	return bars
}

// GetSymbolsByGroup returns symbols for a group (e.g. "VN30", "HOSE").
func (c *Client) GetSymbolsByGroup(group string) ([]string, error) {
	url := fmt.Sprintf("%s%s?group=%s", c.baseURL, symbolsByGroupPath, group)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range defaultHeaders() {
		req.Header.Set(k, v)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vci getByGroup: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vci getByGroup HTTP %d: %s", resp.StatusCode, string(b))
	}
	var list []struct {
		Symbol string `json:"symbol"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("vci getByGroup decode: %w", err)
	}
	symbols := make([]string, 0, len(list))
	for _, item := range list {
		if item.Symbol != "" {
			symbols = append(symbols, item.Symbol)
		}
	}
	return symbols, nil
}