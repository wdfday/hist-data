package app

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"hist-data/internal/crawl"
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

// Run wires the application and blocks until ctx is cancelled.
// Initialises logging, loads config, constructs all providers, then starts
// per-provider scheduler goroutines. Intended to be called from main with a
// signal-aware context.
func Run(ctx context.Context) {
	InitLogger()

	cfg, err := LoadConfig()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}
	defer cfg.ApplyLogger()()

	a, err := NewApp(cfg)
	if err != nil {
		slog.Error("init failed", "error", err)
		os.Exit(1)
	}
	a.run(ctx)
}

// run starts all provider schedulers and blocks until ctx is cancelled.
// Each provider gets its own independent goroutine with its own scheduler loop,
// gate logic, and state — no shared scheduling between providers.
func (a *App) run(ctx context.Context) {
	groups := buildGroups(a.cfg, a.providers, a.jobsByProvider)
	if len(groups) == 0 {
		slog.Error("no provider groups configured")
		return
	}

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
