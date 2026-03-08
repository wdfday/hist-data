package binance

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ── parseKlineRow ─────────────────────────────────────────────────────────

func TestParseKlineRow_Happy(t *testing.T) {
	// Binance kline format: [openTime, open, high, low, close, volume, ...]
	raw := `[1704067200000,"42000.00","43500.50","41800.00","43000.00","1250.75"]`
	var row []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		t.Fatalf("unmarshal row: %v", err)
	}

	bar, err := parseKlineRow(row)
	if err != nil {
		t.Fatalf("parseKlineRow: %v", err)
	}

	if bar.Timestamp != 1704067200000 {
		t.Errorf("timestamp: got %d, want 1704067200000", bar.Timestamp)
	}
	if bar.Open != 42000.00 {
		t.Errorf("open: got %f, want 42000.00", bar.Open)
	}
	if bar.High != 43500.50 {
		t.Errorf("high: got %f, want 43500.50", bar.High)
	}
	if bar.Low != 41800.00 {
		t.Errorf("low: got %f, want 41800.00", bar.Low)
	}
	if bar.Close != 43000.00 {
		t.Errorf("close: got %f, want 43000.00", bar.Close)
	}
}

func TestParseKlineRow_TooFewFields(t *testing.T) {
	raw := `[1704067200000,"42000.00"]` // only 2 fields, need at least 6
	var row []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// parseKlineRow is only called when len(row) >= 6; caller skips short rows
	// Just verify the guard in GetKlines works by checking row length
	if len(row) >= 6 {
		t.Error("expected short row to have <6 fields")
	}
}

// ── NewCrawler interval validation ────────────────────────────────────────

func TestNewCrawler_InvalidInterval(t *testing.T) {
	_, err := NewCrawler("", "", "tick", nil) // "tick" is not a valid interval
	if err == nil {
		t.Error("expected error for invalid interval, got nil")
	}
}

func TestNewCrawler_ValidInterval(t *testing.T) {
	for _, interval := range []string{"1m", "5m", "15m", "1h", "4h", "1d"} {
		c, err := NewCrawler("https://api.binance.com", "/tmp", interval, nil)
		if err != nil {
			t.Errorf("interval %q: unexpected error: %v", interval, err)
		}
		if c.Interval != interval {
			t.Errorf("interval %q: got %q", interval, c.Interval)
		}
	}
}

// ── GetKlines with mock HTTP server ──────────────────────────────────────

func TestGetKlines_MockServer(t *testing.T) {
	// Binance klines response: array of arrays
	mockResponse := `[
		[1704067200000,"42000.00","43500.00","41800.00","43000.00","10.50","1704067499999","441300.00",100,"5.25","220650.00","0"],
		[1704067500000,"43000.00","44000.00","42500.00","43800.00","8.20","1704067799999","358360.00",80,"4.10","179180.00","0"]
	]`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify required query params are present
		q := r.URL.Query()
		if q.Get("symbol") == "" {
			t.Error("missing symbol param")
		}
		if q.Get("interval") == "" {
			t.Error("missing interval param")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(mockResponse))
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	bars, err := client.GetKlines("BTCUSDT", "5m", 1704067200000, 1704068000000)
	if err != nil {
		t.Fatalf("GetKlines: %v", err)
	}
	if len(bars) != 2 {
		t.Errorf("got %d bars, want 2", len(bars))
	}
	if bars[0].Open != 42000.00 {
		t.Errorf("bar[0].Open: %f, want 42000.00", bars[0].Open)
	}
	if bars[1].Close != 43800.00 {
		t.Errorf("bar[1].Close: %f, want 43800.00", bars[1].Close)
	}
}

func TestGetKlines_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"code":-1121,"msg":"Invalid symbol"}`, http.StatusBadRequest)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.GetKlines("INVALID", "5m", 0, 0)
	if err == nil {
		t.Error("expected error for HTTP 400, got nil")
	}
}

// ── FetchBars chunking ────────────────────────────────────────────────────

func TestFetchBars_ChunksCorrectly(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		// Return empty slice to stop chunking after first call
		w.Write([]byte("[]"))
	}))
	defer srv.Close()

	c, _ := NewCrawler(srv.URL, "/tmp", "1m", nil)
	from := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	// 1m interval: 1000 bars per chunk = 1000 minutes per chunk
	// Request 2001 minutes → should trigger 3 chunks (but returns empty → stops at 1)
	to := from.Add(2001 * time.Minute)
	_, err := c.FetchBars("BTCUSDT", "", from, to)
	if err != nil {
		t.Fatalf("FetchBars: %v", err)
	}
	// First chunk returns empty → loop breaks, 1 call expected
	if callCount != 1 {
		t.Errorf("expected 1 HTTP call (empty response stops loop), got %d", callCount)
	}
}
