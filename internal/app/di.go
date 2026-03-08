package app

import (
	"fmt"
	"strings"

	"hist-data/internal/crawl"
	"hist-data/internal/provider/binance"
	"hist-data/internal/provider/histdata"
	"hist-data/internal/provider/polygon"
	"hist-data/internal/provider/vci"
	"hist-data/internal/saver"
)

// ProvideConfig loads application config.
func ProvideConfig() (*Config, error) {
	return LoadConfig()
}

// ProvidePacketSaver constructs the bar persistence backend from config.
func ProvidePacketSaver(cfg *Config) (saver.PacketSaver, error) {
	ps := saver.NewPacketSaver(cfg.Data.Format)
	if ps == nil {
		return nil, fmt.Errorf("unsupported data.format %q (allowed: csv, parquet, json)", cfg.Data.Format)
	}
	return ps, nil
}

// ProvideProviders builds the map of provider name → BarFetcher based on
// which providers have enabled assets in the config.
// Crawlers implement crawl.BarFetcher directly — no wrapper layer needed (DIP).
func ProvideProviders(cfg *Config, ps saver.PacketSaver) (map[string]crawl.BarFetcher, error) {
	needed := neededProviders(cfg)
	providers := make(map[string]crawl.BarFetcher, len(needed))

	for _, name := range needed {
		switch name {
		case "massive":
			if len(cfg.API.Keys) == 0 {
				return nil, fmt.Errorf("massive provider requires API keys (set POLYGON_API_KEYS)")
			}
			c, err := polygon.NewCrawler()
			if err != nil {
				return nil, fmt.Errorf("polygon crawler: %w", err)
			}
			c.SavePacketDir = cfg.ProviderSaveDir("massive")
			c.PacketSaver = ps
			c.Timespan = cfg.Data.Timespan
			c.Multiplier = cfg.Data.Multiplier
			// polygon.Crawler uses CrawlBarsWithKey (legacy name); wrap with adapter
			providers[name] = &polygon.BarFetcherAdapter{Crawler: c}

		case "binance":
			c, err := binance.NewCrawler(
				cfg.Binance.BaseURL,
				cfg.ProviderSaveDir("binance"),
				cfg.Binance.Interval,
				ps,
			)
			if err != nil {
				return nil, fmt.Errorf("binance crawler: %w", err)
			}
			providers[name] = c

		case "histdata":
			providers[name] = histdata.NewCrawler(
				cfg.HistData.BaseURL,
				cfg.ProviderSaveDir("histdata"),
				ps,
			)

		case "vci":
			c, err := vci.NewCrawler(
				cfg.VCI.BaseURL,
				cfg.ProviderSaveDir("vci"),
				cfg.VCI.TimeFrame,
				ps,
			)
			if err != nil {
				return nil, fmt.Errorf("vci crawler: %w", err)
			}
			providers[name] = c
		}
	}
	return providers, nil
}

// neededProviders returns deduplicated provider names from enabled assets.
func neededProviders(cfg *Config) []string {
	seen := make(map[string]bool)
	var out []string
	for _, a := range cfg.EnabledAssets() {
		prov := strings.ToLower(strings.TrimSpace(a.Provider))
		if prov == "" {
			prov = "massive"
		}
		if !seen[prov] {
			seen[prov] = true
			out = append(out, prov)
		}
	}
	return out
}
