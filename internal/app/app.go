package app

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"hist-data/internal/crawl"
)

// Run is the top-level scheduler loop.
// It accepts a map of provider name → BarFetcher and corresponding job lists,
// running each provider's jobs in sequence within each scheduled cycle.
func Run(cfg *Config, providers map[string]crawl.BarFetcher, jobsByProvider map[string][]crawl.Job) {
	progressUpdates := make(chan crawl.ProgressUpdate, 256)
	go crawl.RunProgressWriter(cfg.ProgressPath(), progressUpdates)
	defer close(progressUpdates)

	// Build runners — one per provider
	runners := make([]*crawl.Runner, 0, len(providers))
	for name, fetcher := range providers {
		jobs, ok := jobsByProvider[name]
		if !ok || len(jobs) == 0 {
			slog.Info("no jobs for provider, skipping", "provider", name)
			continue
		}
		keys := providerKeys(cfg, name)
		if len(keys) == 0 {
			slog.Warn("provider has 0 workers, skipping", "provider", name)
			continue
		}
		runners = append(runners, &crawl.Runner{
			Fetcher:         fetcher,
			APIKeys:         keys,
			Targets:         jobs,
			SaveBaseDir:     cfg.ProviderSaveDir(name),
			ProgressPath:    cfg.ProgressPath(),
			ProgressUpdates: progressUpdates,
			BackfillYears:   cfg.Data.BackfillYears,
		})
		slog.Info("runner ready", "provider", name, "jobs", len(jobs), "workers", len(keys))
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	for {
		exiting := triggerAll(runners, signals)
		if exiting {
			return
		}
		nextRun := nextCrawlRunTime(cfg)
		waitDur := time.Until(nextRun)
		if waitDur <= 0 {
			slog.Info("scheduler: next run already past, starting immediately",
				"next_run", nextRun.Format("2006-01-02 15:04 UTC"))
			continue
		}
		slog.Info("scheduler: waiting for next run",
			"wait", waitDur.Round(time.Second),
			"next_run", nextRun.Format("2006-01-02 15:04 UTC"))
		timer := time.NewTimer(waitDur)
		select {
		case <-timer.C:
		case sig := <-signals:
			timer.Stop()
			slog.Info("scheduler: shutting down", "signal", sig)
			return
		}
	}
}

// triggerAll runs all runners concurrently in one cycle.
// Each provider is fully independent (different APIs, different output dirs),
// so they can run in parallel without coordination.
// Returns true if the process should exit due to signal.
func triggerAll(runners []*crawl.Runner, signals <-chan os.Signal) (shouldExit bool) {
	ctx, cancel := context.WithCancel(context.Background())

	// Launch all providers concurrently
	doneChs := make([]<-chan crawl.Done, len(runners))
	for i, runner := range runners {
		doneChs[i] = runner.Run(ctx)
	}

	// Wait for all to finish, or cancel all on signal
	allDone := mergeAll(doneChs)
	select {
	case <-allDone:
		cancel()
		return false
	case sig := <-signals:
		slog.Info("cycle: signal received, cancelling all providers", "signal", sig)
		cancel()
		<-allDone // drain
		return true
	}
}

// mergeAll returns a channel that closes once all input channels have received.
func mergeAll(chs []<-chan crawl.Done) <-chan struct{} {
	merged := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(chs))
	for _, ch := range chs {
		ch := ch
		go func() {
			<-ch
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(merged)
	}()
	return merged
}

func nextCrawlRunTime(cfg *Config) time.Time {
	now := time.Now().UTC()
	hour, min := cfg.Schedule.RunHour, cfg.Schedule.RunMinute
	today := time.Date(now.Year(), now.Month(), now.Day(), hour, min, 0, 0, time.UTC)
	if now.Before(today) {
		return today
	}
	tomorrow := now.AddDate(0, 0, 1)
	return time.Date(tomorrow.Year(), tomorrow.Month(), tomorrow.Day(), hour, min, 0, 0, time.UTC)
}

// providerKeys returns the "key pool" for a provider.
// For Massive/Polygon: real API keys (worker count = number of keys).
// For Binance/HistData: N empty strings (key is ignored by the provider;
// the count controls how many parallel goroutines Runner spawns).
func providerKeys(cfg *Config, provider string) []string {
	switch provider {
	case "binance":
		n := cfg.Binance.Workers
		if n <= 0 {
			n = 1
		}
		return make([]string, n) // slice of N empty strings
	case "histdata":
		n := cfg.HistData.Workers
		if n <= 0 {
			n = 1
		}
		return make([]string, n)
	case "vci":
		n := cfg.VCI.Workers
		if n <= 0 {
			n = 1
		}
		return make([]string, n)
	default: // massive / polygon
		return cfg.API.Keys
	}
}
