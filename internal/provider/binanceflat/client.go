// Package binanceflat implements a BarFetcher backed by Binance Vision flat-files
// (https://data.binance.vision). No API key, no rate limit beyond CDN
// bandwidth, ZIPs of CSV klines partitioned by symbol/interval/date.
package binanceflat

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
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

	if err := c.verifyChecksum(url, buf); err != nil {
		return nil, err
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

// verifyChecksum fetches <url>.CHECKSUM and compares the SHA256 of buf.
// If the checksum file returns 404 the check is skipped (older files may not
// have one). Any other fetch or mismatch error is returned as a hard error.
func (c *Client) verifyChecksum(zipURL string, buf []byte) error {
	csResp, err := c.httpClient.Get(zipURL + ".CHECKSUM")
	if err != nil {
		return fmt.Errorf("binanceflat checksum fetch: %w", err)
	}
	defer csResp.Body.Close()

	if csResp.StatusCode == http.StatusNotFound {
		return nil // checksum not available; skip
	}
	if csResp.StatusCode != http.StatusOK {
		return fmt.Errorf("binanceflat checksum HTTP %d", csResp.StatusCode)
	}

	csBody, err := io.ReadAll(io.LimitReader(csResp.Body, 256))
	if err != nil {
		return fmt.Errorf("binanceflat checksum read: %w", err)
	}

	// Format: "sha256hash  filename\n"
	want := strings.Fields(string(csBody))[0]
	got := sha256Hex(buf)
	if got != want {
		return fmt.Errorf("binanceflat checksum mismatch: got %s want %s url=%s", got, want, zipURL)
	}
	return nil
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// parseCSV parses Vision kline CSV.
//
// Binance Vision columns:
//
//	0: openTime   1: O   2: H   3: L   4: C   5: volume
//	6: closeTime  7: quoteAssetVolume  8: numberOfTrades
//	9: takerBuyBaseVol  10: takerBuyQuoteVol  11: ignore
//
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

		var vwap float64
		if len(rec) > 7 {
			quoteVol, _ := strconv.ParseFloat(rec[7], 64)
			if v > 0 {
				vwap = quoteVol / v
			}
		}

		var txns int64
		if len(rec) > 8 {
			txns, _ = strconv.ParseInt(rec[8], 10, 64)
		}

		bars = append(bars, model.Bar{
			Timestamp:    ts,
			Open:         o,
			High:         h,
			Low:          l,
			Close:        cl,
			Volume:       int64(v * 1e6),
			VWAP:         vwap,
			Transactions: txns,
		})
	}
	return bars, nil
}
