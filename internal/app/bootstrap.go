package app

import (
	"fmt"
	"log/slog"
	"os"

	"us-data/internal/crawl"
	"us-data/internal/provider/polygon"
)

// ResolveTargets resolves tickers for all enabled asset classes and returns
// a flat list of crawl Jobs ready to be handed to Run.
// It also creates the root data directory if it doesn't exist.
func ResolveTargets(cfg *Config) ([]crawl.Job, error) {
	apiKey := ""
	if len(cfg.API.Keys) > 0 {
		apiKey = cfg.API.Keys[0]
	}

	byClass := make(map[crawl.AssetClass][]string)
	for _, asset := range cfg.EnabledAssets() {
		class := crawl.AssetClass(asset.Class)
		slog.Info("resolving tickers",
			"class", asset.Class, "groups", asset.Groups, "explicit", len(asset.Tickers))

		syms, err := polygon.ResolveAssetTickers(apiKey, polygon.AssetTickerSpec{
			Class:    asset.Class,
			Groups:   asset.Groups,
			Tickers:  asset.Tickers,
			Validate: asset.Validate,
		})
		if err != nil {
			return nil, fmt.Errorf("resolve %s tickers: %w", asset.Class, err)
		}
		byClass[class] = syms
		slog.Info("tickers resolved", "class", class, "count", len(syms))
	}

	var targets []crawl.Job
	for class, tickers := range byClass {
		targets = append(targets, crawl.BuildTargets(tickers, cfg.SaveBaseDir(), cfg.Provider, class)...)
	}
	slog.Info("total jobs", "count", len(targets))

	if err := os.MkdirAll(cfg.Data.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %q: %w", cfg.Data.Dir, err)
	}
	slog.Info("storage configured", "dir", cfg.SaveBaseDir(), "format", cfg.Data.Format)

	return targets, nil
}
