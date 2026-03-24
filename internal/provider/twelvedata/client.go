package twelvedata

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"hist-data/internal/model"
)

const (
	defaultBaseURL = "https://api.twelvedata.com"
	maxOutputSize  = 5000
)

// intervalDuration maps TwelveData interval strings to time.Duration for chunk sizing.
var intervalDuration = map[string]time.Duration{
	"1min":  time.Minute,
	"5min":  5 * time.Minute,
	"15min": 15 * time.Minute,
	"30min": 30 * time.Minute,
	"45min": 45 * time.Minute,
	"1h":    time.Hour,
	"2h":    2 * time.Hour,
	"4h":    4 * time.Hour,
	"8h":    8 * time.Hour,
	"1day":  24 * time.Hour,
	"1week": 7 * 24 * time.Hour,
}

// Client calls the TwelveData REST API.
// Free tier: 8 req/min, 800 credits/day (1 credit per time_series call).
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a TwelveData API client.
func NewClient(baseURL, apiKey string) *Client {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	return &Client{
		baseURL:    baseURL,
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// timeSeriesResponse is the JSON envelope returned by /time_series.
type timeSeriesResponse struct {
	Status  string   `json:"status"`
	Message string   `json:"message"` // populated on error
	Values  []rawBar `json:"values"`
}

type rawBar struct {
	Datetime string `json:"datetime"`
	Open     string `json:"open"`
	High     string `json:"high"`
	Low      string `json:"low"`
	Close    string `json:"close"`
	Volume   string `json:"volume"`
}

// GetBars fetches up to maxOutputSize bars for symbol in [from, to].
// Returns bars in ascending (oldest-first) order.
func (c *Client) GetBars(symbol, interval string, from, to time.Time) ([]model.Bar, error) {
	params := url.Values{
		"symbol":     {symbol},
		"interval":   {interval},
		"start_date": {from.UTC().Format("2006-01-02 15:04:05")},
		"end_date":   {to.UTC().Format("2006-01-02 15:04:05")},
		"apikey":     {c.apiKey},
		"format":     {"JSON"},
		"outputsize": {strconv.Itoa(maxOutputSize)},
		"order":      {"ASC"},
	}

	resp, err := c.httpClient.Get(c.baseURL + "/time_series?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("twelvedata GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("twelvedata HTTP %d", resp.StatusCode)
	}

	var result timeSeriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("twelvedata decode: %w", err)
	}
	if result.Status != "ok" {
		return nil, fmt.Errorf("twelvedata API error: %s", result.Message)
	}

	bars := make([]model.Bar, 0, len(result.Values))
	for _, v := range result.Values {
		bar, err := parseRawBar(v)
		if err != nil {
			continue // skip malformed rows
		}
		bars = append(bars, bar)
	}
	return bars, nil
}

func parseRawBar(v rawBar) (model.Bar, error) {
	var ts time.Time
	var err error
	if len(v.Datetime) > 10 {
		ts, err = time.Parse("2006-01-02 15:04:05", v.Datetime)
	} else {
		ts, err = time.Parse("2006-01-02", v.Datetime)
	}
	if err != nil {
		return model.Bar{}, fmt.Errorf("parse datetime %q: %w", v.Datetime, err)
	}

	parseF := func(s string) (float64, error) {
		var f float64
		_, scanErr := fmt.Sscanf(s, "%f", &f)
		return f, scanErr
	}

	o, err := parseF(v.Open)
	if err != nil {
		return model.Bar{}, fmt.Errorf("parse open: %w", err)
	}
	h, err := parseF(v.High)
	if err != nil {
		return model.Bar{}, fmt.Errorf("parse high: %w", err)
	}
	l, err := parseF(v.Low)
	if err != nil {
		return model.Bar{}, fmt.Errorf("parse low: %w", err)
	}
	cl, err := parseF(v.Close)
	if err != nil {
		return model.Bar{}, fmt.Errorf("parse close: %w", err)
	}

	var vol int64
	fmt.Sscanf(v.Volume, "%d", &vol) // volume=0 for forex — parse but don't fail

	return model.Bar{
		Timestamp: ts.UnixMilli(),
		Open:      o,
		High:      h,
		Low:       l,
		Close:     cl,
		Volume:    vol,
	}, nil
}
