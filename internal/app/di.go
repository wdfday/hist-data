package app

import (
	"fmt"
	"strings"
	"time"

	"hist-data/internal/crawl"
	"hist-data/internal/provider/binance"
	"hist-data/internal/provider/binanceflat"
	"hist-data/internal/provider/okx"
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
// Map key: "provider:class:frame" (e.g. "massive:stocks:1xminute", "binance:crypto:1m").
// Multi-frame config expands into one fetcher per frame.
func buildProviders(cfg *Config, ps saver.PacketSaver) (map[string]crawl.BarFetcher, error) {
	providers := make(map[string]crawl.BarFetcher)
	for _, a := range cfg.ExpandedAssets() {
		key := AssetKey(a)
		if _, exists := providers[key]; exists {
			return nil, fmt.Errorf("duplicate asset pipeline key %q", key)
		}
		prov := ProviderName(a.AssetConfig)
		var (
			fetcher crawl.BarFetcher
			err     error
		)
		switch prov {
		case "massive":
			fetcher, err = buildMassiveFetcher(cfg, a, ps)
		case "binance":
			fetcher, err = buildBinanceFetcher(cfg, a, ps)
		case "binanceflat":
			fetcher, err = buildBinanceFlatFetcher(cfg, a, ps)
		case "twelvedata":
			fetcher, err = buildTwelveDataFetcher(cfg, a, ps)
		case "vci":
			fetcher, err = buildVCIFetcher(cfg, a, ps)
		case "okx":
			fetcher, err = buildOKXFetcher(cfg, a, ps)
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

func buildMassiveFetcher(cfg *Config, a resolvedAsset, ps saver.PacketSaver) (crawl.BarFetcher, error) {
	frame, err := providerFrameSpec("massive", a.Frame.Name)
	if err != nil {
		return nil, err
	}
	crawler, err := polygon.NewCrawler(cfg.ProviderSaveDir("massive"), frame.MassiveTimespan, frame.MassiveMultiplier, ps)
	if err != nil {
		return nil, err
	}
	crawler.FrameLabel = a.Frame.Name
	crawler.SinkFrames = a.SinkFrames
	return crawler, nil
}

func buildBinanceFetcher(cfg *Config, a resolvedAsset, ps saver.PacketSaver) (crawl.BarFetcher, error) {
	frame, err := providerFrameSpec("binance", a.Frame.Name)
	if err != nil {
		return nil, err
	}
	crawler, err := binance.NewCrawler(cfg.Binance.BaseURL, cfg.ProviderSaveDir("binance"), frame.ProviderInterval, ps)
	if err != nil {
		return nil, err
	}
	crawler.FrameLabel = a.Frame.Name
	crawler.SinkFrames = a.SinkFrames
	return crawler, nil
}

func buildBinanceFlatFetcher(cfg *Config, a resolvedAsset, ps saver.PacketSaver) (crawl.BarFetcher, error) {
	frame, err := providerFrameSpec("binance", a.Frame.Name)
	if err != nil {
		return nil, err
	}
	crawler, err := binanceflat.NewCrawler(cfg.BinanceFlat.BaseURL, cfg.ProviderSaveDir("binanceflat"), frame.ProviderInterval, ps)
	if err != nil {
		return nil, err
	}
	crawler.FrameLabel = a.Frame.Name
	return crawler, nil
}

func buildTwelveDataFetcher(cfg *Config, a resolvedAsset, ps saver.PacketSaver) (crawl.BarFetcher, error) {
	frame, err := providerFrameSpec("twelvedata", a.Frame.Name)
	if err != nil {
		return nil, err
	}
	crawler, err := twelvedata.NewCrawler(cfg.TwelveData.BaseURL, cfg.TwelveData.APIKey, cfg.ProviderSaveDir("twelvedata"), frame.ProviderInterval, ps)
	if err != nil {
		return nil, err
	}
	crawler.FrameLabel = a.Frame.Name
	crawler.SinkFrames = a.SinkFrames
	return crawler, nil
}

func buildVCIFetcher(cfg *Config, a resolvedAsset, ps saver.PacketSaver) (crawl.BarFetcher, error) {
	frame, err := providerFrameSpec("vci", a.Frame.Name)
	if err != nil {
		return nil, err
	}
	c, err := vci.NewCrawler(cfg.VCI.BaseURL, cfg.ProviderSaveDir("vci"), frame.ProviderInterval, ps)
	if err != nil {
		return nil, err
	}
	c.FrameLabel = a.Frame.Name
	c.SinkFrames = a.SinkFrames
	if rl := providerRateLimit(cfg, "vci"); rl > 0 {
		c.ChunkDelay = rl / 2
	} else {
		c.ChunkDelay = 500 * time.Millisecond
	}
	return c, nil
}

func buildOKXFetcher(cfg *Config, a resolvedAsset, ps saver.PacketSaver) (crawl.BarFetcher, error) {
	frame, err := providerFrameSpec("okx", a.Frame.Name)
	if err != nil {
		return nil, err
	}
	crawler, err := okx.NewCrawler(cfg.OKX.BaseURL, cfg.ProviderSaveDir("okx"), frame.ProviderInterval, ps)
	if err != nil {
		return nil, err
	}
	crawler.FrameLabel = a.Frame.Name
	crawler.SinkFrames = a.SinkFrames
	return crawler, nil
}

// AssetKey returns the unique pipeline key for a resolved asset: "provider:class:frame".
func AssetKey(a resolvedAsset) string {
	return ProviderName(a.AssetConfig) +
		":" + strings.ToLower(strings.TrimSpace(a.Class)) +
		":" + assetFrameLabel(a)
}

// ProviderName returns the normalized provider name (empty → "massive").
func ProviderName(a AssetConfig) string {
	p := strings.ToLower(strings.TrimSpace(a.Provider))
	if p == "" {
		return "massive"
	}
	return p
}

func assetFrameLabel(a resolvedAsset) string {
	if a.Frame.Name == "" {
		return "default"
	}
	return strings.ToUpper(strings.TrimSpace(a.Frame.Name))
}

type providerFrame struct {
	MassiveTimespan   string
	MassiveMultiplier int
	ProviderInterval  string
}

func providerFrameSpec(provider, frame string) (providerFrame, error) {
	frame = strings.ToUpper(strings.TrimSpace(frame))
	switch provider {
	case "massive":
		switch frame {
		case "M1":
			return providerFrame{MassiveTimespan: "minute", MassiveMultiplier: 1}, nil
		case "M5":
			return providerFrame{MassiveTimespan: "minute", MassiveMultiplier: 5}, nil
		case "M15":
			return providerFrame{MassiveTimespan: "minute", MassiveMultiplier: 15}, nil
		case "M30":
			return providerFrame{MassiveTimespan: "minute", MassiveMultiplier: 30}, nil
		case "H1":
			return providerFrame{MassiveTimespan: "hour", MassiveMultiplier: 1}, nil
		case "H4":
			return providerFrame{MassiveTimespan: "hour", MassiveMultiplier: 4}, nil
		case "D1":
			return providerFrame{MassiveTimespan: "day", MassiveMultiplier: 1}, nil
		case "W1":
			return providerFrame{MassiveTimespan: "week", MassiveMultiplier: 1}, nil
		case "MN":
			return providerFrame{MassiveTimespan: "month", MassiveMultiplier: 1}, nil
		}
	case "binance":
		switch frame {
		case "M1":
			return providerFrame{ProviderInterval: "1m"}, nil
		case "M5":
			return providerFrame{ProviderInterval: "5m"}, nil
		case "M15":
			return providerFrame{ProviderInterval: "15m"}, nil
		case "M30":
			return providerFrame{ProviderInterval: "30m"}, nil
		case "H1":
			return providerFrame{ProviderInterval: "1h"}, nil
		case "H4":
			return providerFrame{ProviderInterval: "4h"}, nil
		case "D1":
			return providerFrame{ProviderInterval: "1d"}, nil
		case "W1":
			return providerFrame{ProviderInterval: "1w"}, nil
		case "MN":
			return providerFrame{ProviderInterval: "1M"}, nil
		}
	case "twelvedata":
		switch frame {
		case "M1":
			return providerFrame{ProviderInterval: "1min"}, nil
		case "M5":
			return providerFrame{ProviderInterval: "5min"}, nil
		case "M15":
			return providerFrame{ProviderInterval: "15min"}, nil
		case "M30":
			return providerFrame{ProviderInterval: "30min"}, nil
		case "H1":
			return providerFrame{ProviderInterval: "1h"}, nil
		case "H4":
			return providerFrame{ProviderInterval: "4h"}, nil
		case "D1":
			return providerFrame{ProviderInterval: "1day"}, nil
		case "W1":
			return providerFrame{ProviderInterval: "1week"}, nil
		case "MN":
			return providerFrame{ProviderInterval: "1month"}, nil
		}
	case "vci":
		switch frame {
		case "M1":
			return providerFrame{ProviderInterval: vci.TimeFrameMinute}, nil
		case "H1":
			return providerFrame{ProviderInterval: vci.TimeFrameHour}, nil
		case "D1":
			return providerFrame{ProviderInterval: vci.TimeFrameDay}, nil
		}
	case "okx":
		// OKX bar strings: minute=lowercase m, hour/day/week/month=uppercase
		switch frame {
		case "M1":
			return providerFrame{ProviderInterval: "1m"}, nil
		case "M5":
			return providerFrame{ProviderInterval: "5m"}, nil
		case "M15":
			return providerFrame{ProviderInterval: "15m"}, nil
		case "M30":
			return providerFrame{ProviderInterval: "30m"}, nil
		case "H1":
			return providerFrame{ProviderInterval: "1H"}, nil
		case "H4":
			return providerFrame{ProviderInterval: "4H"}, nil
		case "D1":
			return providerFrame{ProviderInterval: "1D"}, nil
		case "W1":
			return providerFrame{ProviderInterval: "1W"}, nil
		}
	}

	return providerFrame{}, fmt.Errorf("provider %q does not support frame %q", provider, frame)
}
