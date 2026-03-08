package main

import (
	"log/slog"
	"os"

	"hist-data/internal/app"
)

func main() {
	app.InitLogger()

	cfg, err := app.ProvideConfig()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}
	defer cfg.ApplyLogger()()

	ps, err := app.ProvidePacketSaver(cfg)
	if err != nil {
		slog.Error("saver init failed", "error", err)
		os.Exit(1)
	}

	// Build all providers needed by enabled assets
	providers, err := app.ProvideProviders(cfg, ps)
	if err != nil {
		slog.Error("provider init failed", "error", err)
		os.Exit(1)
	}
	for name := range providers {
		slog.Info("provider ready", "name", name)
	}

	// Resolve jobs grouped by provider
	jobsByProvider, err := app.ResolveTargetsByProvider(cfg)
	if err != nil {
		slog.Error("bootstrap failed", "error", err)
		os.Exit(1)
	}

	app.Run(cfg, providers, jobsByProvider)
}
