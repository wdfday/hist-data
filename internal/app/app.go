package app

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"us-data/internal/crawl"
)

// Run is the top-level scheduler loop.
//
// Responsibility: schedule + OS signal handling only.
// It has no knowledge of tickers, API keys, or crawl internals —
// those are encapsulated in crawl.Runner.
func Run(cfg *Config, fetcher crawl.BarFetcher, targets []crawl.Job) {
	progressUpdates := make(chan crawl.ProgressUpdate, 256)
	go crawl.RunProgressWriter(cfg.ProgressPath(), progressUpdates)
	defer close(progressUpdates) // signals RunProgressWriter to drain and exit

	runner := &crawl.Runner{
		Fetcher:         fetcher,
		APIKeys:         cfg.API.Keys,
		Targets:         targets,
		SaveBaseDir:     cfg.SaveBaseDir(),
		ProgressPath:    cfg.ProgressPath(),
		ProgressUpdates: progressUpdates,
		BackfillYears:   cfg.Data.BackfillYears,
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	for {
		exiting := trigger(runner, signals)
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

// trigger starts one crawl run and blocks until it finishes or a signal arrives.
// Returns true if the process should exit.
func trigger(runner *crawl.Runner, signals <-chan os.Signal) (shouldExit bool) {
	ctx, cancel := context.WithCancel(context.Background())
	doneCh := runner.Run(ctx)

	select {
	case <-doneCh:
		cancel()
		return false
	case sig := <-signals:
		slog.Info("cycle: signal received, finishing current jobs", "signal", sig)
		cancel()
		<-doneCh
		return true
	}
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
