package main

import (
	"log/slog"
	"os"

	"us-data/internal/app"
)

func main() {
	a, err := InitializeApp()
	if err != nil {
		slog.Error("init failed", "error", err)
		os.Exit(1)
	}
	defer a.DP.Close()

	defer a.Config.ApplyLogger()() // apply level + format + file; defer closes log file
	slog.Info("provider", "name", a.DP.GetName(), "workers", len(a.Config.API.Keys))

	targets, err := app.ResolveTargets(a.Config)
	if err != nil {
		slog.Error("bootstrap failed", "error", err)
		os.Exit(1)
	}

	app.Run(a.Config, a.DP, targets)
}
