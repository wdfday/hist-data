package histdata

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hist-data/internal/model"
	"hist-data/internal/saver"
)

// ── SaveBars ──────────────────────────────────────────────────────────────

func TestSaveBars_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	ps := saver.NewPacketSaver("csv")
	c := NewCrawler("", dir, ps)

	from := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2023, 1, 31, 0, 0, 0, 0, time.UTC)
	bars := []model.Bar{
		{Timestamp: from.UnixMilli(), Open: 1.07, High: 1.08, Low: 1.06, Close: 1.075},
		{Timestamp: to.UnixMilli(), Open: 1.08, High: 1.09, Low: 1.07, Close: 1.085},
	}

	c.SaveBars(dir, "EURUSD", from, to, bars)

	// Verify directory created: {dir}/EURUSD/
	pairDir := filepath.Join(dir, "EURUSD")
	entries, err := os.ReadDir(pairDir)
	if err != nil {
		t.Fatalf("expected pair dir %q to exist: %v", pairDir, err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 file in pair dir, got %d", len(entries))
	}

	name := entries[0].Name()
	// Expected: EURUSD_1m_2023-01-01_to_2023-01-31.csv
	if !strings.HasPrefix(name, "EURUSD_1m_") {
		t.Errorf("unexpected filename prefix: %q", name)
	}
	if !strings.HasSuffix(name, ".csv") {
		t.Errorf("unexpected filename extension: %q", name)
	}
	if !strings.Contains(name, "2023-01-01") {
		t.Errorf("filename missing from date: %q", name)
	}
	if !strings.Contains(name, "2023-01-31") {
		t.Errorf("filename missing to date: %q", name)
	}
}

func TestSaveBars_EmptyBarsNoFile(t *testing.T) {
	dir := t.TempDir()
	ps := saver.NewPacketSaver("csv")
	c := NewCrawler("", dir, ps)

	from := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2023, 1, 31, 0, 0, 0, 0, time.UTC)

	// Empty bars — SaveBars should be a no-op
	c.SaveBars(dir, "EURUSD", from, to, nil)
	c.SaveBars(dir, "EURUSD", from, to, []model.Bar{})

	// No directory should be created
	pairDir := filepath.Join(dir, "EURUSD")
	if _, err := os.Stat(pairDir); !os.IsNotExist(err) {
		t.Errorf("expected no pair dir for empty bars, but it exists")
	}
}

func TestSaveBars_NilSaverNoOp(t *testing.T) {
	dir := t.TempDir()
	c := NewCrawler("", dir, nil) // nil saver
	from := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2023, 1, 31, 0, 0, 0, 0, time.UTC)
	bars := []model.Bar{{Timestamp: from.UnixMilli(), Open: 1.07, Close: 1.08}}

	// Should not panic
	c.SaveBars(dir, "EURUSD", from, to, bars)
}

// ── CSV parse benchmark ────────────────────────────────────────────────────

// makeMonthCSV generates N rows of realistic 1-minute OHLCV CSV data.
func makeMonthCSV(rows int) []byte {
	var b bytes.Buffer
	base := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	for i := 0; i < rows; i++ {
		t := base.Add(time.Duration(i) * time.Minute)
		b.WriteString(t.Format("20060102 150405"))
		b.WriteString(";1.07012;1.07021;1.07008;1.07018;0\n")
	}
	return b.Bytes()
}

func makeMonthZip(rows int) []byte {
	csv := makeMonthCSV(rows)
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	f, _ := zw.Create("DAT_ASCII_EURUSD_M1_202301.csv")
	f.Write(csv)
	zw.Close()
	return buf.Bytes()
}

// BenchmarkCSVParse_FullMonth benchmarks parsing a full month of 1-minute bars.
// A 30-day month has 43 200 bars (30 × 24 × 60).
// go test -bench=BenchmarkCSVParse -benchmem ./internal/provider/histdata/
func BenchmarkCSVParse_FullMonth(b *testing.B) {
	const rowsPerMonth = 30 * 24 * 60 // 43 200
	csv := makeMonthCSV(rowsPerMonth)
	b.SetBytes(int64(len(csv)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		bars, err := parseCSVRows(strings.NewReader(string(csv)))
		if err != nil {
			b.Fatal(err)
		}
		_ = bars
	}
}

// BenchmarkZipDownloadParse benchmarks the full unzip + parse pipeline.
// go test -bench=BenchmarkZipDownloadParse -benchmem ./internal/provider/histdata/
func BenchmarkZipDownloadParse_FullMonth(b *testing.B) {
	const rowsPerMonth = 30 * 24 * 60
	data := makeMonthZip(rowsPerMonth)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		bars, err := parseZipCSV(data, "EURUSD", 2023, 1)
		if err != nil {
			b.Fatal(err)
		}
		_ = bars
	}
}
