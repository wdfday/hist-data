package app

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"hist-data/internal/crawl"
	"hist-data/internal/provider/polygon"
	"hist-data/internal/provider/vci"
)

// App holds all wired dependencies for the historical data crawling service.
type App struct {
	cfg            *Config
	providers      map[string]crawl.BarFetcher
	jobsByProvider map[string][]crawl.Job
}

// NewApp constructs a fully wired App from config.
func NewApp(cfg *Config) (*App, error) {
	ps, err := buildPacketSaver(cfg)
	if err != nil {
		return nil, err
	}

	providers, err := buildProviders(cfg, ps)
	if err != nil {
		return nil, err
	}
	for name := range providers {
		slog.Info("provider ready", "name", name)
	}

	jobsByProvider, err := ResolveTargetsByProvider(cfg)
	if err != nil {
		return nil, err
	}

	return &App{
		cfg:            cfg,
		providers:      providers,
		jobsByProvider: jobsByProvider,
	}, nil
}

// Run starts all provider schedulers and waits until a signal is received.
// Each provider gets its own independent goroutine with its own scheduler loop,
// gate logic, and state — no shared scheduling between providers.
func (a *App) Run() {
	groups := buildGroups(a.cfg, a.providers, a.jobsByProvider)
	if len(groups) == 0 {
		slog.Error("no provider groups configured")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		sig := <-signals
		slog.Info("scheduler: shutting down", "signal", sig)
		cancel()
	}()

	var wg sync.WaitGroup
	for _, g := range groups {
		wg.Add(1)
		g := g
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					slog.Error("provider goroutine panicked", "provider", g.provider, "panic", r)
				}
			}()
			runGroup(ctx, a.cfg, g)
		}()
	}
	wg.Wait()
}

// yesterday returns the start of yesterday in UTC.
func yesterday() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
}

// providerGroup holds all runners belonging to one provider.
type providerGroup struct {
	provider string
	runners  []*crawl.Runner
}

// buildGroups constructs one Runner per asset-key and groups them by provider.
func buildGroups(cfg *Config, providers map[string]crawl.BarFetcher, jobsByProvider map[string][]crawl.Job) []providerGroup {
	byProvider := make(map[string][]*crawl.Runner)
	for key, fetcher := range providers {
		jobs, ok := jobsByProvider[key]
		if !ok || len(jobs) == 0 {
			slog.Info("no jobs for asset, skipping", "key", key)
			continue
		}
		prov := providerFromKey(key)
		keys := providerKeys(cfg, prov)
		if len(keys) == 0 {
			slog.Warn("asset has 0 workers, skipping", "key", key)
			continue
		}
		backfill := providerBackfillYears(cfg, prov)
		if a := assetForKey(cfg, key); a != nil && a.BackfillYears > 0 {
			backfill = a.BackfillYears
		}
		r := &crawl.Runner{
			Fetcher:       fetcher,
			APIKeys:       keys,
			Targets:       jobs,
			SaveBaseDir:   cfg.ProviderSaveDir(prov),
			ProgressPath:  cfg.ProviderProgressPath(prov),
			BackfillYears: backfill,
			RateLimit:     providerRateLimit(cfg, prov),
		}
		byProvider[prov] = append(byProvider[prov], r)
		slog.Info("runner ready", "key", key, "jobs", len(jobs), "workers", len(keys))
	}

	var groups []providerGroup
	for prov, runners := range byProvider {
		groups = append(groups, providerGroup{provider: prov, runners: runners})
	}
	return groups
}

// runGroup is the per-provider scheduler loop.
func runGroup(ctx context.Context, cfg *Config, g providerGroup) {
	var lastRan time.Time
	var processedAsOf time.Time

	for {
		if ctx.Err() != nil {
			return
		}

		next := nextProviderRunTime(cfg, g.provider, lastRan)
		if now := time.Now().UTC(); now.Before(next) {
			slog.Info("provider: waiting for next run",
				"provider", g.provider,
				"next_run", next.Format("2006-01-02 15:04 UTC"),
				"wait", time.Until(next).Round(time.Second))
			timer := time.NewTimer(time.Until(next))
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}

		active, newAsOf := gateGroup(ctx, cfg, g.provider, g.runners, processedAsOf)
		if len(active) == 0 {
			lastRan = time.Now().UTC()
			continue
		}

		if g.provider == "vci" {
			runSerial(ctx, active)
		} else {
			runConcurrent(ctx, active)
		}

		lastRan = time.Now().UTC()
		if !newAsOf.IsZero() {
			processedAsOf = newAsOf
		}
	}
}

// gateGroup checks whether new data is available since processedAsOf.
func gateGroup(_ context.Context, cfg *Config, provider string, runners []*crawl.Runner, processedAsOf time.Time) ([]*crawl.Runner, time.Time) {
	var lastDay time.Time
	var err error

	switch provider {
	case "massive":
		if len(cfg.Massive.Keys) == 0 {
			return runners, time.Time{}
		}
		lastDay, err = polygon.LastTradingDay(cfg.Massive.Keys[0])
		if err != nil {
			slog.Warn("massive: last trading day query failed, running anyway", "err", err)
			return runners, time.Time{}
		}
	case "vci":
		lastDay, err = vci.NewClient(cfg.VCI.BaseURL).LastTradingDay()
		if err != nil {
			slog.Warn("vci: last trading day query failed, running anyway", "err", err)
			return runners, time.Time{}
		}
		if yest := yesterday(); lastDay.After(yest) {
			lastDay = yest
		}
	default:
		lastDay = yesterday()
	}

	if lastDay.IsZero() || !lastDay.After(processedAsOf) {
		slog.Info("provider: no new trading day, skipping",
			"provider", provider,
			"last_day", lastDay.Format("2006-01-02"),
			"processed_asof", processedAsOf.Format("2006-01-02"))
		return nil, time.Time{}
	}

	slog.Info("provider: new trading day available",
		"provider", provider, "last_day", lastDay.Format("2006-01-02"))
	for _, r := range runners {
		r.AsOf = lastDay
	}
	return runners, lastDay
}

func runSerial(ctx context.Context, runners []*crawl.Runner) {
	for _, r := range runners {
		if ctx.Err() != nil {
			return
		}
		<-r.Run(ctx)
	}
}

func runConcurrent(ctx context.Context, runners []*crawl.Runner) {
	doneChs := make([]<-chan crawl.Done, len(runners))
	for i, r := range runners {
		doneChs[i] = r.Run(ctx)
	}
	<-mergeAll(doneChs)
}

func mergeAll(chs []<-chan crawl.Done) <-chan struct{} {
	merged := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(chs))
	for _, ch := range chs {
		go func(c <-chan crawl.Done) {
			<-c
			wg.Done()
		}(ch)
	}
	go func() {
		wg.Wait()
		close(merged)
	}()
	return merged
}

func nextProviderRunTime(cfg *Config, provider string, lastRan time.Time) time.Time {
	if lastRan.IsZero() {
		return time.Time{}
	}
	h, m := providerSchedule(cfg, provider)
	base := lastRan.UTC()
	t := time.Date(base.Year(), base.Month(), base.Day(), h, m, 0, 0, time.UTC)
	if !t.After(lastRan) {
		t = t.AddDate(0, 0, 1)
	}
	return t
}

func providerSchedule(cfg *Config, provider string) (hour, minute int) {
	var h, m int
	switch provider {
	case "massive":
		h, m = cfg.Massive.Schedule.RunHour, cfg.Massive.Schedule.RunMinute
	case "binance":
		h, m = cfg.Binance.Schedule.RunHour, cfg.Binance.Schedule.RunMinute
	case "twelvedata":
		h, m = cfg.TwelveData.Schedule.RunHour, cfg.TwelveData.Schedule.RunMinute
	case "vci":
		h, m = cfg.VCI.Schedule.RunHour, cfg.VCI.Schedule.RunMinute
	}
	if h == 0 && m == 0 {
		return cfg.Schedule.RunHour, cfg.Schedule.RunMinute
	}
	return h, m
}

func providerBackfillYears(cfg *Config, provider string) int {
	switch provider {
	case "massive":
		return cfg.Massive.BackfillYears
	case "binance":
		return cfg.Binance.BackfillYears
	case "twelvedata":
		return cfg.TwelveData.BackfillYears
	case "vci":
		return cfg.VCI.BackfillYears
	}
	return 0
}

func assetForKey(cfg *Config, key string) *AssetConfig {
	for i := range cfg.Assets {
		if cfg.Assets[i].Enabled && AssetKey(cfg.Assets[i]) == key {
			return &cfg.Assets[i]
		}
	}
	return nil
}

// providerFromKey extracts the provider name from a "provider:class" key.
func providerFromKey(key string) string {
	prov, _, _ := strings.Cut(key, ":")
	return prov
}

// providerKeys returns the worker "key pool" for a provider.
func providerKeys(cfg *Config, provider string) []string {
	switch provider {
	case "binance":
		n := cfg.Binance.Workers
		if n <= 0 {
			n = 1
		}
		return make([]string, n)
	case "twelvedata":
		n := cfg.TwelveData.Workers
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
		keys := cfg.Massive.Keys
		if len(keys) == 0 {
			return nil
		}
		n := cfg.Massive.Workers
		if n <= 0 || n == len(keys) {
			return keys
		}
		workers := make([]string, n)
		for i := range workers {
			workers[i] = keys[i%len(keys)]
		}
		return workers
	}
}

func providerRateLimit(cfg *Config, provider string) time.Duration {
	var sec int
	switch provider {
	case "massive":
		sec = cfg.Massive.RateLimitSec
	case "binance":
		sec = cfg.Binance.RateLimitSec
	case "twelvedata":
		sec = cfg.TwelveData.RateLimitSec
	case "vci":
		sec = cfg.VCI.RateLimitSec
	}
	if sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}
