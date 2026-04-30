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
	Targets      []Job
	ProgressPath string
	FromDate     time.Time // fixed start date (e.g. 2018-01-01); zero = full history
	AsOf         time.Time // last date to include; zero = yesterday (UTC)
}

// NewProgressProducer constructs a ProgressProducer.
func NewProgressProducer(targets []Job, progressPath string, fromDate time.Time) *ProgressProducer {
	return &ProgressProducer{Targets: targets, ProgressPath: progressPath, FromDate: fromDate}
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
		asOf := p.AsOf
		if asOf.IsZero() {
			asOf = date(time.Now().UTC()).AddDate(0, 0, -1) // default: yesterday
		}
		m := loadProgress(p.ProgressPath) // single read for all targets
		pending, skipped := 0, 0
		for i, target := range p.Targets {
			from, to, skip := resolveJobRange(target, m, asOf, p.FromDate)
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
				slog.Info("shutdown signal received; producer stops enqueueing new jobs",
					"queued_so_far", pending,
					"skipped_so_far", skipped,
					"remaining_targets", len(p.Targets)-i,
					"message", "workers will keep draining already queued jobs")
				return
			}
		}
		slog.Info("jobs queued", "count", pending, "skipped", skipped)
	}()
	return out
}
