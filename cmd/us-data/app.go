package main

import (
	"us-data/internal/app"
	"us-data/internal/provider"
)

// App holds the application's top-level dependencies.
type App struct {
	Config *app.Config
	DP     *provider.PolygonProvider
}

// InitializeApp wires and returns App. Caller must call a.DP.Close() when done.
func InitializeApp() (*App, error) {
	app.InitLogger()

	cfg, err := app.ProvideConfig()
	if err != nil {
		return nil, err
	}
	ps, err := app.ProvidePacketSaver(cfg)
	if err != nil {
		return nil, err
	}
	dp, err := app.ProvidePolygonProvider(cfg, ps)
	if err != nil {
		return nil, err
	}
	return &App{Config: cfg, DP: dp}, nil
}
