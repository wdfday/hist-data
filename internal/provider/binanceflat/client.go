// Package binanceflat implements a BarFetcher backed by Binance Vision flat-files
// (https://data.binance.vision). No API key, no rate limit beyond CDN
// bandwidth, ZIPs of CSV klines partitioned by symbol/interval/date.
package binanceflat

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"hist-data/internal/model"
)

const defaultBaseURL = "https://data.binance.vision"

// ErrNotFound is returned when a Vision file does not exist (HTTP 404).
// Callers treat it as "no data for this period" and skip silently.
var ErrNotFound = errors.New("binanceflat: file not found")

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
		httpClient: &http.Client{Timeout: 5 * time.Minute},
	}
}

// FetchMonthly downloads <symbol>-<interval>-<YYYY-MM>.zip and parses klines.
func (c *Client) FetchMonthly(symbol, interval string, year int, month time.Month) ([]model.Bar, error) {
	url := fmt.Sprintf("%s/data/spot/monthly/klines/%s/%s/%s-%s-%04d-%02d.zip",
		c.baseURL, symbol, interval, symbol, interval, year, int(month))
	return c.fetchZip(url)
}

// FetchDaily downloads <symbol>-<interval>-<YYYY-MM-DD>.zip and parses klines.
func (c *Client) FetchDaily(symbol, interval string, day time.Time) ([]model.Bar, error) {
	url := fmt.Sprintf("%s/data/spot/daily/klines/%s/%s/%s-%s-%s.zip",
		c.baseURL, symbol, interval, symbol, interval, day.Format("2006-01-02"))
	return c.fetchZip(url)
}

func (c *Client) fetchZip(url string) ([]model.Bar, error) {
	slog.Debug("binanceflat fetch", "url", url)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("binanceflat GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("binanceflat HTTP %d: %s", resp.StatusCode, body)
	}

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("binanceflat read zip: %w", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		return nil, fmt.Errorf("binanceflat zip open: %w", err)
	}
	if len(zr.File) == 0 {
		return nil, fmt.Errorf("binanceflat zip empty: %s", url)
	}

	f, err := zr.File[0].Open()
	if err != nil {
		return nil, fmt.Errorf("binanceflat csv open: %w", err)
	}
	defer f.Close()

	return parseCSV(f)
}

// parseCSV parses Vision kline CSV. Columns 0..5 are openTime, O, H, L, C, V.
// Vision switched openTime from milliseconds to microseconds around 2025-01;
// we detect the magnitude and normalize back to milliseconds.
func parseCSV(r io.Reader) ([]model.Bar, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	var bars []model.Bar
	row := 0
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("csv row %d: %w", row, err)
		}
		row++
		if len(rec) < 6 {
			continue
		}

		ts, err := strconv.ParseInt(rec[0], 10, 64)
		if err != nil {
			// First row may be header (Vision recently added one) — skip.
			if row == 1 {
				continue
			}
			return nil, fmt.Errorf("csv ts row %d: %w", row, err)
		}
		// μs → ms (Vision file format change late-2024).
		if ts > 1e14 {
			ts /= 1000
		}

		o, _ := strconv.ParseFloat(rec[1], 64)
		h, _ := strconv.ParseFloat(rec[2], 64)
		l, _ := strconv.ParseFloat(rec[3], 64)
		cl, _ := strconv.ParseFloat(rec[4], 64)
		v, _ := strconv.ParseFloat(rec[5], 64)

		bars = append(bars, model.Bar{
			Timestamp: ts,
			Open:      o,
			High:      h,
			Low:       l,
			Close:     cl,
			Volume:    int64(v * 1e6),
		})
	}
	return bars, nil
}
