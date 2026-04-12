package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"hist-data/internal/crawl"
)

func runGroup(ctx context.Context, _ *Config, g providerGroup) {
	var lastRan time.Time
	var processedAsOf time.Time

	for {
		if ctx.Err() != nil {
			return
		}

		if !waitUntilNextRun(ctx, g.policy, lastRan) {
			return
		}

		active, newAsOf := g.policy.gateRunners(g.runners, processedAsOf)
		if len(active) == 0 {
			lastRan = time.Now().UTC()
			continue
		}

		runRunners(ctx, active, g.policy.runSequential)

		lastRan = time.Now().UTC()
		if !newAsOf.IsZero() {
			processedAsOf = newAsOf
		}
	}
}

func waitUntilNextRun(ctx context.Context, policy providerPolicy, lastRan time.Time) bool {
	next := policy.nextRunTime(lastRan)
	if now := time.Now().UTC(); now.Before(next) {
		wait := time.Until(next)
		slog.Info("provider: waiting for next run",
			"provider", policy.name,
			"next_run", next.Format("2006-01-02 15:04 UTC"),
			"wait", wait.Round(time.Second))
		timer := time.NewTimer(wait)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

func runRunners(ctx context.Context, runners []*crawl.Runner, sequential bool) {
	if sequential {
		runSerial(ctx, runners)
		return
	}
	runConcurrent(ctx, runners)
}

func runSerial(ctx context.Context, runners []*crawl.Runner) {
	for _, runner := range runners {
		if ctx.Err() != nil {
			return
		}
		<-runner.Run(ctx)
	}
}

func runConcurrent(ctx context.Context, runners []*crawl.Runner) {
	doneChs := make([]<-chan crawl.Done, len(runners))
	for i, runner := range runners {
		doneChs[i] = runner.Run(ctx)
	}
	<-mergeAll(doneChs)
}

func mergeAll(chs []<-chan crawl.Done) <-chan struct{} {
	merged := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(chs))
	for _, ch := range chs {
		go func(done <-chan crawl.Done) {
			<-done
			wg.Done()
		}(ch)
	}
	go func() {
		wg.Wait()
		close(merged)
	}()
	return merged
}
