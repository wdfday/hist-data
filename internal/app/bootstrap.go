package app

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"hist-data/internal/crawl"
	"hist-data/internal/provider/polygon"
	"hist-data/internal/provider/vci"
)

// ResolveTargetsByProvider resolves tickers for all enabled assets and returns
// jobs grouped by provider name. Each provider key maps to a slice of Jobs
// that its BarFetcher should handle.
func ResolveTargetsByProvider(cfg *Config) (map[string][]crawl.Job, error) {
	apiKey := ""
	if len(cfg.API.Keys) > 0 {
		apiKey = cfg.API.Keys[0]
	}

	result := make(map[string][]crawl.Job)

	for _, asset := range cfg.EnabledAssets() {
		// Determine which provider handles this asset
		prov := strings.ToLower(strings.TrimSpace(asset.Provider))
		if prov == "" {
			prov = "massive" // default
		}

		saveDir := cfg.ProviderSaveDir(prov)
		class := crawl.AssetClass(asset.Class)

		var tickers []string
		var err error

		switch prov {
		case "binance", "histdata":
			// These providers use explicit tickers only — no group resolution
			tickers = asset.Tickers
			slog.Info("tickers from config",
				"provider", prov, "class", asset.Class, "count", len(tickers))

		case "vci":
			tickers, err = resolveVCITickers(cfg.VCI.BaseURL, asset.Groups, asset.Tickers)
			if err != nil {
				return nil, fmt.Errorf("resolve vci tickers: %w", err)
			}
			slog.Info("tickers resolved", "provider", prov, "class", asset.Class, "count", len(tickers))

		default: // massive / polygon
			slog.Info("resolving tickers via Polygon",
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
			tickers = syms
			slog.Info("tickers resolved", "provider", prov, "class", class, "count", len(tickers))
		}

		jobs := crawl.BuildTargets(tickers, saveDir, prov, class)
		result[prov] = append(result[prov], jobs...)
	}

	total := 0
	for _, jobs := range result {
		total += len(jobs)
	}
	slog.Info("total jobs", "count", total, "providers", len(result))

	if err := os.MkdirAll(cfg.Data.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %q: %w", cfg.Data.Dir, err)
	}

	return result, nil
}

// resolveVCITickers returns symbols from VCI getByGroup (if groups set) plus explicit tickers.
func resolveVCITickers(baseURL string, groups, explicitTickers []string) ([]string, error) {
	cl := vci.NewClient(baseURL)
	seen := make(map[string]bool)
	var out []string
	for _, g := range groups {
		if g == "" {
			continue
		}
		syms, err := cl.GetSymbolsByGroup(g)
		if err != nil {
			return nil, fmt.Errorf("vci getByGroup %q: %w", g, err)
		}
		for _, s := range syms {
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	for _, t := range explicitTickers {
		if t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out, nil
}

// ResolveTargets is kept for backward compatibility with the Polygon-only flow.
// Deprecated: use ResolveTargetsByProvider.
func ResolveTargets(cfg *Config) ([]crawl.Job, error) {
	byProvider, err := ResolveTargetsByProvider(cfg)
	if err != nil {
		return nil, err
	}
	var all []crawl.Job
	for _, jobs := range byProvider {
		all = append(all, jobs...)
	}
	return all, nil
}
