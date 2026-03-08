package histdata

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"hist-data/internal/model"
)

const (
	defaultBaseURL = "https://www.histdata.com"

	// Step 1: GET this page to obtain the tk token from the hidden form field.
	pageURLTemplate = "/download-free-forex-historical-data/?/ascii/1-minute-bar-quotes/%s/%d/%02d"

	// Step 2: POST to this endpoint with form data to trigger the actual zip download.
	downloadEndpoint = "/get.php"
)

// tokenRegexp matches the hidden input field containing the download token.
// <input type="hidden" name="tk" value="abc123...">
var tokenRegexp = regexp.MustCompile(`(?i)<input[^>]+name=["']?tk["']?[^>]+value=["']([^"']+)["']`)

// Client downloads and parses HistData monthly CSV files.
// No API key required — data is freely available.
// Download requires a 2-step process: GET page → POST get.php with token.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a HistData client.
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

// GetBars downloads and parses 1-minute OHLCV bars for pair/year/month.
// pair is uppercase without slash, e.g. "EURUSD".
// Returns nil, nil for future months (404).
func (c *Client) GetBars(pair string, year, month int) ([]model.Bar, error) {
	pair = strings.ToUpper(pair)
	pageURL := c.baseURL + fmt.Sprintf(pageURLTemplate, pair, year, month)

	// Step 1: GET the page and extract the tk token.
	tk, err := c.extractToken(pageURL)
	if err != nil {
		return nil, fmt.Errorf("histdata token %s/%04d-%02d: %w", pair, year, month, err)
	}
	if tk == "" {
		// Page likely 404 or no token — month probably doesn't exist yet.
		return nil, nil
	}

	// Step 2: POST to get.php to download the zip.
	data, err := c.downloadZip(pair, year, month, tk, pageURL)
	if err != nil {
		return nil, fmt.Errorf("histdata download %s/%04d-%02d: %w", pair, year, month, err)
	}
	if data == nil {
		return nil, nil
	}

	return parseZipCSV(data, pair, year, month)
}

// extractToken fetches the download page and returns the tk token value.
// Returns ("", nil) if page is 404 (month not yet available).
func (c *Client) extractToken(pageURL string) (string, error) {
	resp, err := c.httpClient.Get(pageURL)
	if err != nil {
		return "", fmt.Errorf("GET page: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("page HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024)) // 512 KB max for HTML
	if err != nil {
		return "", fmt.Errorf("read page: %w", err)
	}

	matches := tokenRegexp.FindSubmatch(body)
	if len(matches) < 2 {
		// Try alternate attribute order: value before name.
		alt := regexp.MustCompile(`(?i)<input[^>]+value=["']([^"']+)["'][^>]+name=["']?tk["']?`)
		if m := alt.FindSubmatch(body); len(m) >= 2 {
			return string(m[1]), nil
		}
		return "", nil // token not found — page may be empty / captcha
	}
	return string(matches[1]), nil
}

// downloadZip POSTs to get.php and returns the raw zip bytes.
func (c *Client) downloadZip(pair string, year, month int, tk, referer string) ([]byte, error) {
	form := url.Values{
		"tk":        {tk},
		"date":      {fmt.Sprintf("%04d%02d", year, month)},
		"datemonth": {fmt.Sprintf("%02d", month)},
		"platform":  {"ASCII"},
		"timeframe": {"M1"},
		"fxpair":    {pair},
		"type":      {"1"}, // 1 = 1-minute bars
		"format":    {"ascii"},
	}

	req, err := http.NewRequest("POST", c.baseURL+downloadEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", referer)
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; hist-data-crawler/1.0)")
	req.Header.Set("Origin", c.baseURL)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST get.php: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download HTTP %d", resp.StatusCode)
	}

	// Verify we got a zip (Content-Type: application/zip or application/x-zip)
	ct := resp.Header.Get("Content-Type")
	if ct != "" && !strings.Contains(ct, "zip") && !strings.Contains(ct, "octet-stream") {
		// Got HTML back — likely CAPTCHA or error page
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("expected zip, got %q: %.200s", ct, body)
	}

	return io.ReadAll(resp.Body)
}

// ── CSV parsing ───────────────────────────────────────────────────────────

// parseZipCSV unzips the downloaded payload and parses all CSV rows inside.
func parseZipCSV(data []byte, pair string, year, month int) ([]model.Bar, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("histdata unzip %s/%d-%02d: %w", pair, year, month, err)
	}

	var bars []model.Bar
	for _, f := range zr.File {
		if !strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("histdata open csv %s: %w", f.Name, err)
		}
		parsed, err := parseCSVRows(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("histdata parse csv %s: %w", f.Name, err)
		}
		bars = append(bars, parsed...)
	}
	return bars, nil
}

// parseCSVRows reads HistData 1-minute bar CSV.
// Format: YYYYMMDD HHmmSS;open;high;low;close;volume
// Volume is always 0 for forex — stored as 0.
func parseCSVRows(r io.Reader) ([]model.Bar, error) {
	reader := csv.NewReader(r)
	reader.Comma = ';'
	reader.FieldsPerRecord = 6

	var bars []model.Bar
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		dt, err := time.Parse("20060102 150405", strings.TrimSpace(row[0]))
		if err != nil {
			continue // skip malformed rows
		}

		parseF := func(s string) (float64, error) {
			var f float64
			_, err := fmt.Sscanf(strings.TrimSpace(s), "%f", &f)
			return f, err
		}

		o, err := parseF(row[1])
		if err != nil {
			continue
		}
		h, err := parseF(row[2])
		if err != nil {
			continue
		}
		l, err := parseF(row[3])
		if err != nil {
			continue
		}
		cl, err := parseF(row[4])
		if err != nil {
			continue
		}

		bars = append(bars, model.Bar{
			Timestamp: dt.UnixMilli(),
			Open:      o,
			High:      h,
			Low:       l,
			Close:     cl,
			Volume:    0, // forex has no real volume
		})
	}
	return bars, nil
}
