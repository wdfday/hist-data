package binanceflat

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// buildZipCSV creates a ZIP archive containing one CSV file with the given rows.
func buildZipCSV(t *testing.T, rows []string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	f, err := zw.Create("klines.csv")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		fmt.Fprintln(f, r)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func checksumOf(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// mockServer serves a ZIP at /zip and its CHECKSUM at /zip.CHECKSUM.
// Set corrupt=true to serve a wrong checksum; set noChecksum=true for 404 on checksum.
type mockServer struct {
	zipBytes   []byte
	corrupt    bool
	noChecksum bool
	missingZip bool
}

func (m *mockServer) serve(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/zip.CHECKSUM":
			if m.noChecksum {
				http.NotFound(w, r)
				return
			}
			cs := checksumOf(m.zipBytes)
			if m.corrupt {
				cs = "0000000000000000000000000000000000000000000000000000000000000000"
			}
			fmt.Fprintf(w, "%s  klines.zip\n", cs)
		case r.URL.Path == "/zip":
			if m.missingZip {
				http.NotFound(w, r)
				return
			}
			w.Write(m.zipBytes)
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestFetchZip_HappyPath(t *testing.T) {
	rows := []string{
		"1704067200000,42000.0,43000.0,41500.0,42500.0,100",
		"1704153600000,42500.0,44000.0,42000.0,43800.0,120",
	}
	zip := buildZipCSV(t, rows)
	ms := &mockServer{zipBytes: zip}
	srv := ms.serve(t)
	defer srv.Close()

	client := NewClient(srv.URL)
	bars, err := client.fetchZip(srv.URL + "/zip")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(bars) != 2 {
		t.Fatalf("want 2 bars, got %d", len(bars))
	}
	if bars[0].Open != 42000.0 {
		t.Errorf("want open 42000, got %v", bars[0].Open)
	}
}

func TestFetchZip_ChecksumMismatch(t *testing.T) {
	zip := buildZipCSV(t, []string{"1704067200000,1,2,3,4,5"})
	ms := &mockServer{zipBytes: zip, corrupt: true}
	srv := ms.serve(t)
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.fetchZip(srv.URL + "/zip")
	if err == nil {
		t.Fatal("expected checksum error, got nil")
	}
}

func TestFetchZip_ChecksumNotFound_Skipped(t *testing.T) {
	zip := buildZipCSV(t, []string{"1704067200000,1,2,3,4,5"})
	ms := &mockServer{zipBytes: zip, noChecksum: true}
	srv := ms.serve(t)
	defer srv.Close()

	client := NewClient(srv.URL)
	bars, err := client.fetchZip(srv.URL + "/zip")
	if err != nil {
		t.Fatalf("expected success when checksum absent, got: %v", err)
	}
	if len(bars) != 1 {
		t.Fatalf("want 1 bar, got %d", len(bars))
	}
}

func TestFetchZip_NotFound(t *testing.T) {
	ms := &mockServer{zipBytes: []byte{}, missingZip: true}
	srv := ms.serve(t)
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.fetchZip(srv.URL + "/zip")
	if err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestParseCSV_WithHeader(t *testing.T) {
	csv := "open_time,open,high,low,close,volume\n" +
		"1704067200000,42000.0,43000.0,41500.0,42500.0,100\n"
	bars, err := parseCSV(bytes.NewReader([]byte(csv)))
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 1 {
		t.Fatalf("want 1 bar (header skipped), got %d", len(bars))
	}
}

func TestParseCSV_MicrosecondTimestamp(t *testing.T) {
	tsMs := int64(1704067200000)
	tsUs := tsMs * 1000
	row := fmt.Sprintf("%d,42000,43000,41500,42500,100", tsUs)
	bars, err := parseCSV(bytes.NewReader([]byte(row + "\n")))
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 1 {
		t.Fatalf("want 1 bar, got %d", len(bars))
	}
	if bars[0].Timestamp != tsMs {
		t.Errorf("want ts %d (ms), got %d", tsMs, bars[0].Timestamp)
	}
}

func TestFetchMonthly_Integration(t *testing.T) {
	rows := []string{
		"1704067200000,42000,43000,41500,42500,100",
	}
	zipData := buildZipCSV(t, rows)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path[len(r.URL.Path)-9:] == ".CHECKSUM" {
			fmt.Fprintf(w, "%s  file.zip\n", checksumOf(zipData))
			return
		}
		w.Write(zipData)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	bars, err := client.FetchMonthly("BTCUSDT", "1d", 2024, time.January)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(bars) != 1 {
		t.Fatalf("want 1 bar, got %d", len(bars))
	}
}
