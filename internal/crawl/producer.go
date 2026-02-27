package crawl

import (
	"context"
	"log/slog"
	"time"
)

// ProgressProducer streams fully-resolved Jobs into a channel.
//
// The producer owns the date-range logic: it reads progress once per cycle,
// computes from/to for each target via resolveJobRange, and skips targets that
// are already up to date. Up-to-date skips never reach the worker.
//
// The worker receives a Job with From/To already set and only needs to fetch.
type ProgressProducer struct {
	Targets       []Job
	ProgressPath  string
	BackfillYears int // years of history to fetch on first run (default: 2)
}

// NewProgressProducer constructs a ProgressProducer.
func NewProgressProducer(targets []Job, progressPath string, backfillYears int) *ProgressProducer {
	return &ProgressProducer{Targets: targets, ProgressPath: progressPath, BackfillYears: backfillYears}
}

// Start resolves date ranges for all targets and streams pending Jobs into the
// returned channel. The channel is closed when all targets are processed or ctx
// is cancelled. Targets already up to date are silently dropped.
//
// The progress file is read once before the loop — not once per target.
func (p *ProgressProducer) Start(ctx context.Context) <-chan Job {
	out := make(chan Job, 64)
	go func() {
		defer close(out)
		now := time.Now().UTC()
		m := loadProgress(p.ProgressPath) // single read for all targets
		pending, skipped := 0, 0
		for _, target := range p.Targets {
			from, to, skip := resolveJobRange(target, m, now, p.BackfillYears)
			if skip {
				skipped++
				continue
			}
			job := target
			job.From, job.To = from, to
			select {
			case out <- job:
				pending++
			case <-ctx.Done():
				slog.Info("producer stopped early", "reason", "context cancelled")
				return
			}
		}
		slog.Info("jobs queued", "count", pending, "skipped", skipped)
	}()
	return out
}
