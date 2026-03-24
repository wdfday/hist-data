package main

import (
	"log/slog"
	"os"

	"hist-data/internal/app"
)

func main() {
	app.InitLogger()

	cfg, err := app.LoadConfig()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}
	defer cfg.ApplyLogger()()

	a, err := app.NewApp(cfg)
	if err != nil {
		slog.Error("init failed", "error", err)
		os.Exit(1)
	}
	a.Run()
}
