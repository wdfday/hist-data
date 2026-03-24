package crawl

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Runner orchestrates one full crawl cycle.
//
//	ProgressProducer → <-chan Job → workers (one per key) → results + logs channels
//	                                                       ↓               ↓
//	                                                result collector   log writer
//
// Separation of concerns:
//   - Producer : reads progress once, resolves from/to per target, pushes fully-resolved Jobs
//   - Worker   : pure compute — sends JobResult and LogEntry through channels, never calls slog
//   - Log writer: single goroutine drains the log channel and calls slog (ordered output)
type Runner struct {
	Fetcher       BarFetcher
	APIKeys       []string
	Targets       []Job
	ProgressPath  string
	SaveBaseDir   string
	BackfillYears int           // passed to ProgressProducer
	AsOf          time.Time     // last date to include; zero = yesterday
	RateLimit     time.Duration // minimum delay between requests per worker; 0 = no limit
}

// Run starts one crawl cycle asynchronously and returns a channel that receives
// Done when the cycle finishes (or context is cancelled).
func (r *Runner) Run(ctx context.Context) <-chan Done {
	done := make(chan Done, 1)
	go r.execute(ctx, done)
	return done
}

func (r *Runner) execute(ctx context.Context, done chan<- Done) {
	defer func() {
		if rc := recover(); rc != nil {
			slog.Error("runner panicked", "targets", len(r.Targets), "panic", rc)
		}
		done <- Done{}
	}()

	start := time.Now().UTC()
	slog.Info("cycle start", "targets", len(r.Targets), "workers", len(r.APIKeys))

	// Each runner owns its progress writer — no shared channel needed.
	progressUpdates := make(chan ProgressUpdate, 256)
	go RunProgressWriter(r.ProgressPath, progressUpdates)
	defer close(progressUpdates)

	// Bootstrap must run before producer reads progress, so every target has an entry.
	BootstrapProgress(r.ProgressPath, r.Targets, start, r.BackfillYears)

	producer := NewProgressProducer(r.Targets, r.ProgressPath, r.BackfillYears)
	producer.AsOf = r.AsOf
	jobCh := producer.Start(ctx)
	successes, failures := r.runWorkers(ctx, jobCh, progressUpdates)

	slog.Info("cycle done",
		"success", len(successes), "failed", len(failures),
		"duration", time.Since(start).Round(time.Second))

	if len(successes) > 0 || len(failures) > 0 {
		if err := writeRunReport(r.SaveBaseDir, successes, failures); err != nil {
			slog.Warn("run report write failed", "err", err)
		}
	}
}

// runWorkers fans jobs out to N workers (one per API key) and collects results.
// Each worker owns its key — no shared key pool needed.
// Workers communicate exclusively through channels — no direct slog calls.
func (r *Runner) runWorkers(ctx context.Context, jobCh <-chan Job, progressUpdates chan<- ProgressUpdate) (successes []string, failures []failedEntry) {
	results := make(chan JobResult, 256)
	logs := make(chan LogEntry, 512)

	// --- log writer: drains log channel, calls slog sequentially ---
	var logWg sync.WaitGroup
	logWg.Add(1)
	go func() {
		defer logWg.Done()
		for e := range logs {
			slog.Log(context.Background(), e.Level, e.Msg, e.Args...)
		}
	}()

	// --- result collector ---
	var (
		mu            sync.Mutex
		successCount  int
		failedCount   int
		barsPerTicker = make(map[string]int)
		barsPerKey    = make(map[string]int)
	)
	var resWg sync.WaitGroup
	resWg.Add(1)
	go func() {
		defer resWg.Done()
		for res := range results {
			mu.Lock()
			if res.Ok {
				successCount++
				successes = appendUniq(successes, res.Ticker)
				barsPerTicker[res.Ticker] += res.Bars
				barsPerKey[res.KeyPrefix] += res.Bars
			} else {
				failedCount++
				failures = append(failures, failedEntry{
					Ticker: res.Ticker, DateRange: res.DateRange, Reason: res.Reason,
				})
			}
			mu.Unlock()
		}
	}()

	// --- heartbeat: background ticker, reads shared counters ---
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go runHeartbeat(hbCtx, 15*time.Minute, &mu, &successCount, &failedCount, barsPerTicker)

	// --- workers: each goroutine owns its API key ---
	var wg sync.WaitGroup
	wg.Add(len(r.APIKeys))
	for _, key := range r.APIKeys {
		key := key
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobCh:
					if !ok {
						return
					}
					func() {
						defer func() {
							if rc := recover(); rc != nil {
								logs <- LogEntry{slog.LevelError, "worker panicked", []any{
									"ticker", job.Ticker, "panic", rc,
								}}
								results <- JobResult{Ok: false, Ticker: job.Ticker, Reason: "panic"}
							}
						}()
						r.processJob(job, key, results, logs, progressUpdates)
					}()
					if r.RateLimit > 0 {
						select {
						case <-time.After(r.RateLimit):
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}()
	}

	wg.Wait()
	close(results)
	close(logs)
	resWg.Wait()
	logWg.Wait()

	logSummary(barsPerTicker, barsPerKey, failures, successCount, failedCount)
	return
}

// processJob fetches and saves bars for a fully-resolved Job.
// All output goes through channels — results to resultCh, logs to logs.
// This goroutine never calls slog directly.
func (r *Runner) processJob(job Job, key string, results chan<- JobResult, logs chan<- LogEntry, progressUpdates chan<- ProgressUpdate) {
	fromStr := job.From.Format("2006-01-02")
	toStr := job.To.Format("2006-01-02")

	keyPfx := key
	if len(key) > 8 {
		keyPfx = key[:8] + "…"
	}

	logs <- LogEntry{slog.LevelInfo, "fetch start", []any{
		"ticker", job.Ticker, "class", job.Class,
		"from", fromStr, "to", toStr, "key", keyPfx,
	}}

	bars, err := r.Fetcher.FetchBars(job.Ticker, key, job.From, job.To)

	switch {
	case err != nil:
		logs <- LogEntry{slog.LevelError, "fetch error", []any{
			"ticker", job.Ticker, "class", job.Class,
			"from", fromStr, "to", toStr, "key", keyPfx, "err", err,
		}}
		results <- JobResult{
			Ok: false, Ticker: job.Ticker,
			DateRange: fromStr + ".." + toStr, Reason: err.Error(),
		}

	case len(bars) == 0:
		logs <- LogEntry{slog.LevelWarn, "fetch empty", []any{
			"ticker", job.Ticker, "class", job.Class,
			"from", fromStr, "to", toStr,
		}}
		results <- JobResult{
			Ok: false, Ticker: job.Ticker,
			DateRange: fromStr + ".." + toStr, Reason: "no data",
		}

	default:
		r.Fetcher.SaveBars(job.SaveDir, job.Ticker, job.From, job.To, bars)
		logs <- LogEntry{slog.LevelInfo, "fetch ok", []any{
			"ticker", job.Ticker, "class", job.Class,
			"from", fromStr, "to", toStr, "bars", len(bars), "key", keyPfx,
		}}
		results <- JobResult{
			Ok: true, Ticker: job.Ticker,
			DateRange: fromStr + ".." + toStr, Bars: len(bars), KeyPrefix: keyPfx,
		}

		select {
		case progressUpdates <- ProgressUpdate{
			Source: job.Source, Class: job.Class,
			Ticker: job.Ticker, Date: toStr,
		}:
		default:
			logs <- LogEntry{slog.LevelWarn, "progress update dropped", []any{
				"ticker", job.Ticker,
			}}
		}
	}
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func logSummary(barsPerTicker, barsPerKey map[string]int, failures []failedEntry, success, failed int) {
	var totalBars int
	for _, n := range barsPerTicker {
		totalBars += n
	}
	slog.Info("cycle summary",
		"total_bars", totalBars, "tickers_ok", success, "tickers_failed", failed)
	for _, t := range sortedKeys(barsPerTicker) {
		slog.Debug("ticker bars", "ticker", t, "bars", barsPerTicker[t])
	}
	for _, k := range sortedKeys(barsPerKey) {
		slog.Debug("key bars", "key", k, "bars", barsPerKey[k])
	}
	if len(failures) > 0 {
		slog.Warn("failed tickers",
			"count", len(failures), "detail", joinFailedReasons(failures))
	}
}

func appendUniq(list []string, ticker string) []string {
	for _, t := range list {
		if t == ticker {
			return list
		}
	}
	return append(list, ticker)
}

func runHeartbeat(ctx context.Context, interval time.Duration, mu *sync.Mutex, success, failed *int, barsPerTicker map[string]int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastDone := -1 // sentinel: always log on first tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mu.Lock()
			s, f := *success, *failed
			var total int
			for _, n := range barsPerTicker {
				total += n
			}
			mu.Unlock()
			done := s + f
			if done == lastDone {
				continue // no progress since last tick — skip
			}
			lastDone = done
			slog.Info("cycle heartbeat",
				"done", done, "success", s, "failed", f, "bars", total)
		}
	}
}
