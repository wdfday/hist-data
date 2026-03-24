package app

import (
	"fmt"
	"strings"
	"time"

	"hist-data/internal/crawl"
	"hist-data/internal/provider/binance"
	"hist-data/internal/provider/polygon"
	"hist-data/internal/provider/twelvedata"
	"hist-data/internal/provider/vci"
	"hist-data/internal/saver"
)

// buildPacketSaver constructs the bar persistence backend from config.
func buildPacketSaver(cfg *Config) (saver.PacketSaver, error) {
	ps := saver.NewPacketSaver(cfg.Data.Format)
	if ps == nil {
		return nil, fmt.Errorf("unsupported data.format %q (allowed: csv, parquet, json)", cfg.Data.Format)
	}
	return ps, nil
}

// buildProviders creates one BarFetcher per enabled asset.
//
// Map key: "provider:class"  (e.g. "massive:stocks", "binance:crypto", "vci:vn").
// Two assets using the same provider but different intervals each get their own crawler.
func buildProviders(cfg *Config, ps saver.PacketSaver) (map[string]crawl.BarFetcher, error) {
	providers := make(map[string]crawl.BarFetcher)
	for _, a := range cfg.EnabledAssets() {
		key := AssetKey(a)
		prov := ProviderName(a)
		var (
			fetcher crawl.BarFetcher
			err     error
		)
		switch prov {
		case "massive":
			fetcher, err = buildMassiveFetcher(cfg, a, ps)
		case "binance":
			fetcher, err = buildBinanceFetcher(cfg, a, ps)
		case "twelvedata":
			fetcher, err = buildTwelveDataFetcher(cfg, a, ps)
		case "vci":
			fetcher, err = buildVCIFetcher(cfg, a, ps)
		default:
			return nil, fmt.Errorf("unknown provider %q for asset %q", prov, a.Class)
		}
		if err != nil {
			return nil, fmt.Errorf("build fetcher for %s: %w", key, err)
		}
		providers[key] = fetcher
	}
	return providers, nil
}

func buildMassiveFetcher(cfg *Config, a AssetConfig, ps saver.PacketSaver) (crawl.BarFetcher, error) {
	timespan := "minute"
	multiplier := 1
	if a.Timespan != "" {
		timespan = a.Timespan
	}
	if a.Multiplier > 0 {
		multiplier = a.Multiplier
	}
	c, err := polygon.NewCrawler()
	if err != nil {
		return nil, err
	}
	c.SavePacketDir = cfg.ProviderSaveDir("massive")
	c.PacketSaver = ps
	c.Timespan = timespan
	c.Multiplier = multiplier
	return &polygon.BarFetcherAdapter{Crawler: c}, nil
}

func buildBinanceFetcher(cfg *Config, a AssetConfig, ps saver.PacketSaver) (crawl.BarFetcher, error) {
	interval := "1m"
	if a.Interval != "" {
		interval = a.Interval
	}
	return binance.NewCrawler(cfg.Binance.BaseURL, cfg.ProviderSaveDir("binance"), interval, ps)
}

func buildTwelveDataFetcher(cfg *Config, a AssetConfig, ps saver.PacketSaver) (crawl.BarFetcher, error) {
	interval := "1min"
	if a.Interval != "" {
		interval = a.Interval
	}
	return twelvedata.NewCrawler(cfg.TwelveData.BaseURL, cfg.TwelveData.APIKey, cfg.ProviderSaveDir("twelvedata"), interval, ps)
}

func buildVCIFetcher(cfg *Config, a AssetConfig, ps saver.PacketSaver) (crawl.BarFetcher, error) {
	timeFrame := "ONE_DAY"
	if a.TimeFrame != "" {
		timeFrame = a.TimeFrame
	}
	c, err := vci.NewCrawler(cfg.VCI.BaseURL, cfg.ProviderSaveDir("vci"), timeFrame, ps)
	if err != nil {
		return nil, err
	}
	if rl := providerRateLimit(cfg, "vci"); rl > 0 {
		c.ChunkDelay = rl / 2
	} else {
		c.ChunkDelay = 500 * time.Millisecond
	}
	return c, nil
}

// AssetKey returns the unique pipeline key for an asset: "provider:class".
// Examples: "massive:stocks", "binance:crypto", "vci:vn".
func AssetKey(a AssetConfig) string {
	return ProviderName(a) + ":" + strings.ToLower(strings.TrimSpace(a.Class))
}

// ProviderName returns the normalized provider name (empty → "massive").
func ProviderName(a AssetConfig) string {
	p := strings.ToLower(strings.TrimSpace(a.Provider))
	if p == "" {
		return "massive"
	}
	return p
}
