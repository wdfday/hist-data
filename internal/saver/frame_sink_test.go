package saver

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"hist-data/internal/model"
)

// --- helpers ---

func ms(t time.Time) int64 { return t.UnixMilli() }

func bar(ts time.Time, o, h, l, c float64, v int64) model.Bar {
	return model.Bar{Timestamp: ms(ts), Open: o, High: h, Low: l, Close: c, Volume: v}
}

func barVWAP(ts time.Time, o, h, l, c float64, v int64, vwap float64, txns int64) model.Bar {
	return model.Bar{Timestamp: ms(ts), Open: o, High: h, Low: l, Close: c, Volume: v, VWAP: vwap, Transactions: txns}
}

// mockSaver captures saves for test assertions.
type mockSaver struct {
	saved []savedCall
}

type savedCall struct {
	bars []model.Bar
	path string
}

func (m *mockSaver) Save(bars []model.Bar, path string) error {
	m.saved = append(m.saved, savedCall{bars: bars, path: path})
	return nil
}

func (m *mockSaver) Extension() string { return "mock" }

// --- barsForFrame ---

func TestBarsForFrame_SameFrame_ReturnedAsIs(t *testing.T) {
	input := []model.Bar{bar(time.Unix(0, 0), 1, 2, 0.5, 1.5, 100)}
	got, err := barsForFrame("M1", "M1", input)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
}

func TestBarsForFrame_NonM1Source_Error(t *testing.T) {
	input := []model.Bar{bar(time.Unix(0, 0), 1, 2, 0.5, 1.5, 100)}
	_, err := barsForFrame("D1", "M5", input)
	if err == nil {
		t.Fatal("expected error for non-M1 source")
	}
}

func TestBarsForFrame_SinkLowerThanSource_Error(t *testing.T) {
	input := []model.Bar{bar(time.Unix(0, 0), 1, 2, 0.5, 1.5, 100)}
	_, err := barsForFrame("H1", "M5", input)
	if err == nil {
		t.Fatal("expected error: sink M5 lower than source H1")
	}
}

func TestBarsForFrame_M1_to_M5_BasicAggregation(t *testing.T) {
	base := time.Date(2024, 1, 2, 9, 30, 0, 0, time.UTC)

	// 6 M1 bars → 1 complete M5 bar + 1 partial M5 bar (1 bar)
	bars := []model.Bar{
		bar(base.Add(0*time.Minute), 100, 102, 99, 101, 10),
		bar(base.Add(1*time.Minute), 101, 103, 100, 102, 20),
		bar(base.Add(2*time.Minute), 102, 104, 101, 103, 15),
		bar(base.Add(3*time.Minute), 103, 105, 102, 104, 25),
		bar(base.Add(4*time.Minute), 104, 106, 103, 105, 30),
		bar(base.Add(5*time.Minute), 105, 107, 104, 106, 12),
	}

	got, err := barsForFrame("M1", "M5", bars)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}

	first := got[0]
	// Open = first bar open, High = max, Low = min, Close = last bar close
	if first.Open != 100 {
		t.Errorf("open = %v, want 100", first.Open)
	}
	if first.High != 106 {
		t.Errorf("high = %v, want 106", first.High)
	}
	if first.Low != 99 {
		t.Errorf("low = %v, want 99", first.Low)
	}
	if first.Close != 105 {
		t.Errorf("close = %v, want 105", first.Close)
	}
	if first.Volume != 100 {
		t.Errorf("volume = %v, want 100", first.Volume)
	}
	// Timestamp = start of M5 bucket
	wantTs := base.Truncate(5 * time.Minute).UnixMilli()
	if first.Timestamp != wantTs {
		t.Errorf("timestamp = %v, want %v", first.Timestamp, wantTs)
	}

	second := got[1]
	if second.Open != 105 {
		t.Errorf("second open = %v, want 105", second.Open)
	}
	if second.Volume != 12 {
		t.Errorf("second volume = %v, want 12", second.Volume)
	}
}

func TestBarsForFrame_M1_to_M15_BucketAlignment(t *testing.T) {
	base := time.Date(2024, 1, 2, 9, 0, 0, 0, time.UTC) // 9:00 UTC

	// 16 bars spanning two M15 buckets
	var bars []model.Bar
	for i := 0; i < 16; i++ {
		bars = append(bars, bar(base.Add(time.Duration(i)*time.Minute), float64(100+i), float64(101+i), float64(99+i), float64(100+i), int64(10+i)))
	}

	got, err := barsForFrame("M1", "M15", bars)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}

	// First bucket: 9:00–9:14
	if got[0].Timestamp != base.Truncate(15*time.Minute).UnixMilli() {
		t.Errorf("first bucket ts mismatch")
	}
	// Second bucket: 9:15
	wantSecond := base.Add(15 * time.Minute).Truncate(15 * time.Minute)
	if got[1].Timestamp != wantSecond.UnixMilli() {
		t.Errorf("second bucket ts mismatch")
	}
}

func TestBarsForFrame_M1_to_H1_VolumeSum(t *testing.T) {
	base := time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC)

	// 60 M1 bars, all volume=1 → H1 volume=60
	var bars []model.Bar
	for i := 0; i < 60; i++ {
		bars = append(bars, bar(base.Add(time.Duration(i)*time.Minute), 100, 101, 99, 100, 1))
	}

	got, err := barsForFrame("M1", "H1", bars)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Volume != 60 {
		t.Errorf("volume = %d, want 60", got[0].Volume)
	}
	if got[0].Open != 100 {
		t.Errorf("open = %v, want 100", got[0].Open)
	}
}

func TestBarsForFrame_M1_to_H4_BucketBoundaries(t *testing.T) {
	// H4 buckets in UTC: 0h, 4h, 8h, 12h, 16h, 20h
	b0 := time.Date(2024, 1, 2, 3, 59, 0, 0, time.UTC) // bucket 0h
	b4 := time.Date(2024, 1, 2, 4, 0, 0, 0, time.UTC)  // bucket 4h
	b8 := time.Date(2024, 1, 2, 8, 1, 0, 0, time.UTC)  // bucket 8h

	bars := []model.Bar{
		bar(b0, 10, 12, 9, 11, 5),
		bar(b4, 20, 22, 19, 21, 8),
		bar(b8, 30, 32, 29, 31, 3),
	}

	got, err := barsForFrame("M1", "H4", bars)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].Timestamp != b0.Truncate(4*time.Hour).UnixMilli() {
		t.Errorf("bucket[0] ts wrong")
	}
	if got[1].Timestamp != b4.Truncate(4*time.Hour).UnixMilli() {
		t.Errorf("bucket[1] ts wrong")
	}
	if got[2].Timestamp != b8.Truncate(4*time.Hour).UnixMilli() {
		t.Errorf("bucket[2] ts wrong")
	}
}

func TestBarsForFrame_M1_to_M30_HighLowCorrect(t *testing.T) {
	base := time.Date(2024, 1, 2, 9, 0, 0, 0, time.UTC)

	bars := []model.Bar{
		bar(base.Add(0*time.Minute), 100, 150, 50, 110, 10), // high=150, low=50
		bar(base.Add(1*time.Minute), 110, 120, 80, 115, 20), // high=120, low=80
		bar(base.Add(2*time.Minute), 115, 200, 10, 120, 30), // high=200, low=10 ← extremes
	}

	got, err := barsForFrame("M1", "M30", bars)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].High != 200 {
		t.Errorf("high = %v, want 200", got[0].High)
	}
	if got[0].Low != 10 {
		t.Errorf("low = %v, want 10", got[0].Low)
	}
}

func TestBarsForFrame_EmptyInput(t *testing.T) {
	got, err := barsForFrame("M1", "M5", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %d bars", len(got))
	}
}

func TestBarsForFrame_SingleBar(t *testing.T) {
	base := time.Date(2024, 1, 2, 10, 3, 0, 0, time.UTC)
	bars := []model.Bar{bar(base, 100, 101, 99, 100, 5)}

	got, err := barsForFrame("M1", "M5", bars)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	// Timestamp should be truncated to M5 bucket
	if got[0].Timestamp != base.Truncate(5*time.Minute).UnixMilli() {
		t.Errorf("timestamp mismatch")
	}
}

func TestBarsForFrame_TransactionsSummed(t *testing.T) {
	base := time.Date(2024, 1, 2, 9, 0, 0, 0, time.UTC)
	bars := []model.Bar{
		barVWAP(base.Add(0*time.Minute), 100, 101, 99, 100, 10, 100.5, 3),
		barVWAP(base.Add(1*time.Minute), 100, 102, 98, 101, 20, 101.0, 7),
	}

	got, err := barsForFrame("M1", "M5", bars)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Transactions != 10 {
		t.Errorf("transactions = %d, want 10", got[0].Transactions)
	}
}

// --- mergeVWAP ---

func TestMergeVWAP_BasicWeightedAverage(t *testing.T) {
	// left: price=100, vol=10 → value=1000
	// right: price=200, vol=10 → value=2000
	// weighted avg = 3000/20 = 150
	got := mergeVWAP(100, 10, 200, 10, 99)
	if got != 150 {
		t.Errorf("mergeVWAP = %v, want 150", got)
	}
}

func TestMergeVWAP_ZeroTotalVolume_FallsBackToClose(t *testing.T) {
	got := mergeVWAP(0, 0, 0, 0, 42.5)
	if got != 42.5 {
		t.Errorf("mergeVWAP zero vol = %v, want 42.5", got)
	}
}

func TestMergeVWAP_LeftZeroVWAP_WithVolume_FallsBackToClose(t *testing.T) {
	// leftVWAP=0 but leftVol>0 → fallback to close for left side
	got := mergeVWAP(0, 5, 100, 5, 50.0)
	// left value = 50*5=250, right value = 100*5=500, total=750/10=75
	if got != 75 {
		t.Errorf("mergeVWAP = %v, want 75", got)
	}
}

func TestMergeVWAP_RightZeroVWAP_WithVolume_FallsBackToClose(t *testing.T) {
	got := mergeVWAP(100, 5, 0, 5, 50.0)
	// right value = 50*5=250, left value = 100*5=500, total=750/10=75
	if got != 75 {
		t.Errorf("mergeVWAP = %v, want 75", got)
	}
}

// --- SaveFrameSet ---

func TestSaveFrameSet_NoOp_OnEmptyBars(t *testing.T) {
	ms := &mockSaver{}
	SaveFrameSet("test", "M1", []string{"M1", "M5"}, "/some/dir/M1", "AAPL", time.Now(), time.Now(), nil, ms)
	if len(ms.saved) != 0 {
		t.Errorf("expected no saves for empty bars, got %d", len(ms.saved))
	}
}

func TestSaveFrameSet_SavesEachSinkFrame(t *testing.T) {
	dir := t.TempDir()
	sourceDir := filepath.Join(dir, "M1")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2024, 1, 2, 9, 0, 0, 0, time.UTC)
	var bars []model.Bar
	for i := 0; i < 10; i++ {
		bars = append(bars, bar(base.Add(time.Duration(i)*time.Minute), 100, 101, 99, 100, 5))
	}

	ms := &mockSaver{}
	SaveFrameSet("test", "M1", []string{"M1", "M5"}, sourceDir, "AAPL",
		base, base.Add(10*time.Minute), bars, ms)

	// Should save M1 (passthrough) and M5 (aggregated)
	if len(ms.saved) != 2 {
		t.Fatalf("saves = %d, want 2", len(ms.saved))
	}
}

func TestSaveFrameSet_M1PassthroughMatchesInput(t *testing.T) {
	dir := t.TempDir()
	sourceDir := filepath.Join(dir, "M1")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2024, 1, 2, 9, 0, 0, 0, time.UTC)
	bars := []model.Bar{
		bar(base, 100, 101, 99, 100, 5),
		bar(base.Add(time.Minute), 100, 102, 98, 101, 8),
	}

	ms := &mockSaver{}
	SaveFrameSet("test", "M1", []string{"M1"}, sourceDir, "AAPL",
		base, base.Add(2*time.Minute), bars, ms)

	if len(ms.saved) != 1 {
		t.Fatalf("saves = %d, want 1", len(ms.saved))
	}
	if len(ms.saved[0].bars) != 2 {
		t.Errorf("passthrough bars = %d, want 2", len(ms.saved[0].bars))
	}
}

func TestSaveFrameSet_SinkDirIsProviderLevelNotSourceFrame(t *testing.T) {
	dir := t.TempDir()
	providerDir := filepath.Join(dir, "Binance")
	sourceDir := filepath.Join(providerDir, "M1")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2024, 1, 2, 9, 0, 0, 0, time.UTC)
	var bars []model.Bar
	for i := 0; i < 5; i++ {
		bars = append(bars, bar(base.Add(time.Duration(i)*time.Minute), 100, 101, 99, 100, 1))
	}

	ms := &mockSaver{}
	SaveFrameSet("binance", "M1", []string{"M5"}, sourceDir, "BTCUSDT",
		base, base.Add(5*time.Minute), bars, ms)

	if len(ms.saved) != 1 {
		t.Fatalf("saves = %d, want 1", len(ms.saved))
	}
	// M5 dir should be Binance/M5/BTCUSDT/...
	wantDir := filepath.Join(providerDir, "M5", "BTCUSDT")
	gotDir := filepath.Dir(ms.saved[0].path)
	if gotDir != wantDir {
		t.Errorf("save dir = %q, want %q", gotDir, wantDir)
	}
}
