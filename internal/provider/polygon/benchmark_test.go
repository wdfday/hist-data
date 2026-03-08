package polygon

import (
	"sync"
	"testing"
	"time"

	"hist-data/internal/model"
)

// ────────────────────────────────────────────────────────────────────────────
// Constants
// ────────────────────────────────────────────────────────────────────────────

const (
	// US stock market: 252 trading days × 6.5h × 12 bars (5-min) ≈ ~19 700 bars/year
	// 2 years of 5-min bars per ticker
	bench5MinBars2Y = 252 * 2 * 6 * 60 / 5 // ≈ 181 440

	// 2 years of 1-min bars per ticker (crypto/forex worst case: 24h × 365.25 × 2)
	bench1MinBars2Y = int(365.25 * 2 * 24 * 60) // ≈ 1 051 920
)

// ────────────────────────────────────────────────────────────────────────────
// Mock helpers
// ────────────────────────────────────────────────────────────────────────────

// mockFetch simulates one ticker fetch: sleep cooldown, return n bars.
func mockFetch(n int, cooldown time.Duration, prealloc bool) func(string, string) ([]model.Bar, error) {
	return func(ticker, key string) ([]model.Bar, error) {
		time.Sleep(cooldown)
		var bars []model.Bar
		if prealloc {
			bars = make([]model.Bar, 0, n)
		}
		for j := 0; j < n; j++ {
			bars = append(bars, model.Bar{
				Timestamp: int64(j), Open: 100, High: 101, Low: 99, Close: 100.5,
				Volume: 1000, VWAP: 100.2, Transactions: 50,
			})
		}
		return bars, nil
	}
}

// runPool simulates the Runner worker-pool pattern:
// N goroutines share a key pool, drain a ticker channel.
func runPool(tickers []string, keys []string, fetch func(string, string) ([]model.Bar, error)) (ok, fail int) {
	tickCh := make(chan string, len(tickers))
	for _, t := range tickers {
		tickCh <- t
	}
	close(tickCh)

	keyPool := make(chan string, len(keys))
	for _, k := range keys {
		keyPool <- k
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(keys))
	for range keys {
		go func() {
			defer wg.Done()
			for ticker := range tickCh {
				key := <-keyPool
				bars, err := fetch(ticker, key)
				keyPool <- key
				mu.Lock()
				if err != nil || len(bars) == 0 {
					fail++
				} else {
					ok++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return
}

// ────────────────────────────────────────────────────────────────────────────
// Benchmarks — worker concurrency
// ────────────────────────────────────────────────────────────────────────────

var tickers9 = []string{"AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA", "BRK.B", "JPM"}
var keys3 = []string{"key1", "key2", "key3"}

// BenchmarkWorkerPool_Quick — 12ms cooldown to simulate 12s API cooldown at 1000× speed.
// go test -bench=BenchmarkWorkerPool_Quick -benchtime=5s ./internal/provider/polygon/
func BenchmarkWorkerPool_Quick(b *testing.B) {
	fetch := mockFetch(bench5MinBars2Y, 12*time.Millisecond, true)
	b.ResetTimer()
	for range b.N {
		runPool(tickers9, keys3, fetch)
	}
}

// BenchmarkWorkerPool_Real — real 12s cooldown; run with -benchtime=1x.
// go test -bench=BenchmarkWorkerPool_Real -benchtime=1x -timeout=120s ./internal/provider/polygon/
func BenchmarkWorkerPool_Real(b *testing.B) {
	fetch := mockFetch(bench5MinBars2Y, 12*time.Second, true)
	b.ResetTimer()
	for range b.N {
		runPool(tickers9, keys3, fetch)
	}
}

// BenchmarkWorkerPool_KeyScaling compares throughput for 1/2/3 API keys.
// go test -bench=BenchmarkWorkerPool_KeyScaling -benchtime=3s ./internal/provider/polygon/
func BenchmarkWorkerPool_KeyScaling(b *testing.B) {
	fetch := mockFetch(bench5MinBars2Y, 12*time.Millisecond, true)
	for _, keys := range [][]string{
		{"key1"},
		{"key1", "key2"},
		{"key1", "key2", "key3"},
	} {
		b.Run("keys="+string(rune('0'+len(keys))), func(b *testing.B) {
			for range b.N {
				runPool(tickers9, keys, fetch)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Benchmarks — slice pre-allocation
// ────────────────────────────────────────────────────────────────────────────

// BenchmarkPrealloc compares pre-allocated vs dynamic slice growth.
// Useful for tuning estimatedBars() headroom.
// go test -bench=BenchmarkPrealloc -benchmem ./internal/provider/polygon/
func BenchmarkPrealloc(b *testing.B) {
	c := &Crawler{Timespan: "minute", Multiplier: 5}
	from := time.Now().AddDate(-2, 0, 0)
	to := time.Now()
	cap := c.estimatedBars(from, to)

	b.Run("Prealloc_5min_2y", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			bars := make([]model.Bar, 0, cap)
			for j := 0; j < bench5MinBars2Y; j++ {
				bars = append(bars, model.Bar{Timestamp: int64(j), Open: 100, Close: 100.5})
			}
			_ = bars
		}
	})

	b.Run("NoPrealloc_5min_2y", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			var bars []model.Bar
			for j := 0; j < bench5MinBars2Y; j++ {
				bars = append(bars, model.Bar{Timestamp: int64(j), Open: 100, Close: 100.5})
			}
			_ = bars
		}
	})

	b.Run("Prealloc_1min_2y", func(b *testing.B) {
		b.ReportAllocs()
		c1 := &Crawler{Timespan: "minute", Multiplier: 1}
		cap1 := c1.estimatedBars(from, to)
		for range b.N {
			bars := make([]model.Bar, 0, cap1)
			for j := 0; j < bench1MinBars2Y; j++ {
				bars = append(bars, model.Bar{Timestamp: int64(j), Open: 100, Close: 100.5})
			}
			_ = bars
		}
	})
}

// ────────────────────────────────────────────────────────────────────────────
// Benchmarks — chunk calculation
// ────────────────────────────────────────────────────────────────────────────

// BenchmarkChunkCalc measures overhead of splitDateRangeIntoChunks.
func BenchmarkChunkCalc(b *testing.B) {
	from := time.Now().AddDate(-2, 0, 0)
	to := time.Now()
	c := &Crawler{Timespan: "minute", Multiplier: 5}
	b.ResetTimer()
	for range b.N {
		chunks := splitDateRangeIntoChunks(from, to, c.maxDaysPerChunk())
		_ = chunks
	}
}
