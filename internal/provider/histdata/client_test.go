package histdata

import (
	"archive/zip"
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ── parseCSVRows ──────────────────────────────────────────────────────────

func TestParseCSVRows_Happy(t *testing.T) {
	csv := strings.NewReader(
		"20230102 000100;1.07012;1.07021;1.07008;1.07018;0\n" +
			"20230102 000200;1.07018;1.07030;1.07010;1.07025;0\n",
	)

	bars, err := parseCSVRows(csv)
	if err != nil {
		t.Fatalf("parseCSVRows: %v", err)
	}
	if len(bars) != 2 {
		t.Fatalf("got %d bars, want 2", len(bars))
	}

	// First bar
	b := bars[0]
	if b.Open != 1.07012 {
		t.Errorf("bar[0].Open: got %f, want 1.07012", b.Open)
	}
	if b.Close != 1.07018 {
		t.Errorf("bar[0].Close: got %f, want 1.07018", b.Close)
	}
	if b.Volume != 0 {
		t.Errorf("bar[0].Volume: got %d, want 0 (forex has no real volume)", b.Volume)
	}

	// Timestamp should parse correctly
	expected := time.Date(2023, 1, 2, 0, 1, 0, 0, time.UTC).UnixMilli()
	if b.Timestamp != expected {
		t.Errorf("bar[0].Timestamp: got %d, want %d", b.Timestamp, expected)
	}
}

func TestParseCSVRows_MalformedRowsSkipped(t *testing.T) {
	csv := strings.NewReader(
		"INVALID_ROW;bad;data;here;x;y\n" + // malformed datetime → skip
			"20230102 000100;1.07012;1.07021;1.07008;1.07018;0\n", // good row
	)

	bars, err := parseCSVRows(csv)
	if err != nil {
		t.Fatalf("parseCSVRows: %v", err)
	}
	// Malformed row skipped, only 1 good bar
	if len(bars) != 1 {
		t.Errorf("got %d bars, want 1 (malformed row should be skipped)", len(bars))
	}
}

func TestParseCSVRows_EmptyInput(t *testing.T) {
	bars, err := parseCSVRows(strings.NewReader(""))
	if err != nil {
		t.Fatalf("parseCSVRows on empty: %v", err)
	}
	if len(bars) != 0 {
		t.Errorf("expected 0 bars from empty input, got %d", len(bars))
	}
}

// ── parseZipCSV ───────────────────────────────────────────────────────────

func makeTestZip(csvContent string) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	f, _ := zw.Create("DAT_ASCII_EURUSD_M1_202301.csv")
	f.Write([]byte(csvContent))
	zw.Close()
	return buf.Bytes()
}

func TestParseZipCSV_Happy(t *testing.T) {
	csv := "20230102 000100;1.07012;1.07021;1.07008;1.07018;0\n"
	data := makeTestZip(csv)

	bars, err := parseZipCSV(data, "EURUSD", 2023, 1)
	if err != nil {
		t.Fatalf("parseZipCSV: %v", err)
	}
	if len(bars) != 1 {
		t.Errorf("got %d bars, want 1", len(bars))
	}
}

func TestParseZipCSV_InvalidZip(t *testing.T) {
	_, err := parseZipCSV([]byte("not a zip file"), "EURUSD", 2023, 1)
	if err == nil {
		t.Error("expected error for invalid zip, got nil")
	}
}

// ── GetBars with mock HTTP server ─────────────────────────────────────────

func TestGetBars_MockServer(t *testing.T) {
	csvContent := "20230102 000100;1.07012;1.07021;1.07008;1.07018;0\n" +
		"20230102 000200;1.07018;1.07030;1.07010;1.07025;0\n"
	zipData := makeTestZip(csvContent)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET":
			// Step 1: serve HTML page with tk token
			w.Header().Set("Content-Type", "text/html")
			w.Write([]byte(`<input type="hidden" name="tk" value="test-token-abc123">`))
		case r.Method == "POST":
			// Step 2: verify form fields and return zip
			r.ParseForm()
			if r.FormValue("tk") == "" {
				t.Error("missing tk token in POST")
			}
			if r.FormValue("fxpair") == "" {
				t.Error("missing fxpair in POST")
			}
			if r.FormValue("timeframe") != "M1" {
				t.Errorf("expected timeframe M1, got %q", r.FormValue("timeframe"))
			}
			w.Header().Set("Content-Type", "application/zip")
			w.Write(zipData)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	bars, err := client.GetBars("EURUSD", 2023, 1)
	if err != nil {
		t.Fatalf("GetBars: %v", err)
	}
	if len(bars) != 2 {
		t.Errorf("got %d bars, want 2", len(bars))
	}
}

func TestGetBars_404ReturnsEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Both GET and POST return 404
		http.NotFound(w, r)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	bars, err := client.GetBars("EURUSD", 2099, 1)
	if err != nil {
		t.Fatalf("expected nil error for 404, got: %v", err)
	}
	if bars != nil {
		t.Errorf("expected nil bars for 404, got %d", len(bars))
	}
}

// ── FetchBars month iteration ─────────────────────────────────────────────

func TestFetchBars_IteratesMonths(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			// Step 1: return page with tk token
			w.Header().Set("Content-Type", "text/html")
			w.Write([]byte(`<input type="hidden" name="tk" value="tok123">`))
			return
		}
		// Step 2: POST — return a bar mid-month to avoid boundary issues
		callCount++
		csvContent := fmt.Sprintf("202301%02d 120000;1.07;1.08;1.06;1.07;0\n", 10+callCount)
		w.Header().Set("Content-Type", "application/zip")
		w.Write(makeTestZip(csvContent))
	}))
	defer srv.Close()

	c := NewCrawler(srv.URL, "/tmp", nil)
	from := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2023, 3, 31, 23, 59, 59, 0, time.UTC)

	_, err := c.FetchBars("EURUSD", "", from, to)
	if err != nil {
		t.Fatalf("FetchBars: %v", err)
	}
	// 3 months → 3 POST calls
	if callCount != 3 {
		t.Errorf("expected 3 POST calls (Jan/Feb/Mar), got %d", callCount)
	}
}

func TestFetchBars_FiltersToRange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "text/html")
			w.Write([]byte(`<input type="hidden" name="tk" value="tok123">`))
			return
		}
		// Return 3 bars: Jan 1 (before from), Jan 15, Jan 25 (within range)
		csv := "20230101 000000;1.07;1.08;1.06;1.07;0\n" +
			"20230115 000000;1.07;1.08;1.06;1.07;0\n" +
			"20230125 000000;1.07;1.08;1.06;1.07;0\n"
		w.Header().Set("Content-Type", "application/zip")
		w.Write(makeTestZip(csv))
	}))
	defer srv.Close()

	c := NewCrawler(srv.URL, "/tmp", nil)
	from := time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC)
	to := time.Date(2023, 1, 31, 23, 59, 59, 0, time.UTC)

	bars, err := c.FetchBars("EURUSD", "", from, to)
	if err != nil {
		t.Fatalf("FetchBars: %v", err)
	}
	if len(bars) != 2 {
		t.Errorf("expected 2 bars (filtered to range), got %d", len(bars))
	}
}
