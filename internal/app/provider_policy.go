package app

import (
	"log/slog"
	"strings"
	"time"

	"hist-data/internal/crawl"
	"hist-data/internal/provider/polygon"
	"hist-data/internal/provider/vci"
)

type providerPolicy struct {
	name           string
	lastDayPath    string
	runHour        int
	runMinute      int
	rateLimit      time.Duration
	workerKeys     []string
	runSequential  bool
	resolveLastDay func() (time.Time, error)
}

func newProviderPolicy(cfg *Config, provider string) providerPolicy {
	switch provider {
	case "binance":
		return providerPolicy{
			name:           provider,
			lastDayPath:    cfg.ProviderLastDayPath(provider),
			runHour:        scheduledAt(cfg.Binance.Schedule.RunHour, cfg.Binance.Schedule.RunMinute, cfg).hour,
			runMinute:      scheduledAt(cfg.Binance.Schedule.RunHour, cfg.Binance.Schedule.RunMinute, cfg).minute,
			rateLimit:      secondsToDuration(cfg.Binance.RateLimitSec),
			workerKeys:     workerSlots(cfg.Binance.Workers),
			resolveLastDay: func() (time.Time, error) { return yesterday(), nil },
		}
	case "binanceflat":
		return providerPolicy{
			name:           provider,
			lastDayPath:    cfg.ProviderLastDayPath(provider),
			runHour:        scheduledAt(cfg.BinanceFlat.Schedule.RunHour, cfg.BinanceFlat.Schedule.RunMinute, cfg).hour,
			runMinute:      scheduledAt(cfg.BinanceFlat.Schedule.RunHour, cfg.BinanceFlat.Schedule.RunMinute, cfg).minute,
			rateLimit:      0, // CDN flat-file — no rate limit
			workerKeys:     workerSlots(cfg.BinanceFlat.Workers),
			resolveLastDay: func() (time.Time, error) { return yesterday(), nil },
		}
	case "twelvedata":
		return providerPolicy{
			name:           provider,
			lastDayPath:    cfg.ProviderLastDayPath(provider),
			runHour:        scheduledAt(cfg.TwelveData.Schedule.RunHour, cfg.TwelveData.Schedule.RunMinute, cfg).hour,
			runMinute:      scheduledAt(cfg.TwelveData.Schedule.RunHour, cfg.TwelveData.Schedule.RunMinute, cfg).minute,
			rateLimit:      secondsToDuration(cfg.TwelveData.RateLimitSec),
			workerKeys:     workerSlots(cfg.TwelveData.Workers),
			resolveLastDay: func() (time.Time, error) { return yesterday(), nil },
		}
	case "okx":
		return providerPolicy{
			name:           provider,
			lastDayPath:    cfg.ProviderLastDayPath(provider),
			runHour:        scheduledAt(cfg.OKX.Schedule.RunHour, cfg.OKX.Schedule.RunMinute, cfg).hour,
			runMinute:      scheduledAt(cfg.OKX.Schedule.RunHour, cfg.OKX.Schedule.RunMinute, cfg).minute,
			rateLimit:      secondsToDuration(cfg.OKX.RateLimitSec),
			workerKeys:     workerSlots(cfg.OKX.Workers),
			runSequential:  true, // share IP rate-limit budget across frames
			resolveLastDay: func() (time.Time, error) { return yesterday(), nil },
		}
	case "vci":
		return providerPolicy{
			name:          provider,
			lastDayPath:   cfg.ProviderLastDayPath(provider),
			runHour:       scheduledAt(cfg.VCI.Schedule.RunHour, cfg.VCI.Schedule.RunMinute, cfg).hour,
			runMinute:     scheduledAt(cfg.VCI.Schedule.RunHour, cfg.VCI.Schedule.RunMinute, cfg).minute,
			rateLimit:     secondsToDuration(cfg.VCI.RateLimitSec),
			workerKeys:    workerSlots(cfg.VCI.Workers),
			runSequential: true,
			resolveLastDay: func() (time.Time, error) {
				lastDay, err := vci.NewClient(cfg.VCI.BaseURL).LastTradingDay()
				if err != nil {
					return time.Time{}, err
				}
				if yest := yesterday(); lastDay.After(yest) {
					lastDay = yest
				}
				return lastDay, nil
			},
		}
	default:
		return providerPolicy{
			name:          "massive",
			lastDayPath:   cfg.ProviderLastDayPath("massive"),
			runHour:       scheduledAt(cfg.Massive.Schedule.RunHour, cfg.Massive.Schedule.RunMinute, cfg).hour,
			runMinute:     scheduledAt(cfg.Massive.Schedule.RunHour, cfg.Massive.Schedule.RunMinute, cfg).minute,
			rateLimit:     secondsToDuration(cfg.Massive.RateLimitSec),
			workerKeys:    massiveWorkerKeys(cfg.Massive.Keys, cfg.Massive.Workers),
			runSequential: true, // share rate-limit budget across frames (D1, M1, …)
			resolveLastDay: func() (time.Time, error) {
				if len(cfg.Massive.Keys) == 0 {
					return time.Time{}, nil
				}
				return polygon.LastTradingDay(cfg.Massive.Keys[0])
			},
		}
	}
}

func (p providerPolicy) nextRunTime(lastRan time.Time) time.Time {
	if lastRan.IsZero() {
		return time.Time{}
	}
	base := lastRan.UTC()
	next := time.Date(base.Year(), base.Month(), base.Day(), p.runHour, p.runMinute, 0, 0, time.UTC)
	if !next.After(lastRan) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

func (p providerPolicy) gateRunners(runners []*crawl.Runner, processedAsOf time.Time) ([]*crawl.Runner, time.Time) {
	lastDay, err := p.resolveLastDay()
	if err != nil {
		slog.Warn(p.name+": last trading day query failed, running anyway", "err", err)
		return runners, time.Time{}
	}
	if lastDay.IsZero() {
		return runners, time.Time{}
	}
	if !lastDay.After(processedAsOf) {
		slog.Info("provider: no new trading day, skipping",
			"provider", p.name,
			"last_day", lastDay.Format("2006-01-02"),
			"processed_asof", processedAsOf.Format("2006-01-02"))
		return nil, time.Time{}
	}

	slog.Info("provider: new trading day available",
		"provider", p.name, "last_day", lastDay.Format("2006-01-02"))
	_ = crawl.WriteProviderLastDay(p.lastDayPath, p.name, lastDay)
	for _, runner := range runners {
		runner.AsOf = lastDay
	}
	return runners, lastDay
}

func assetForKey(cfg *Config, key string) *resolvedAsset {
	for _, asset := range cfg.ExpandedAssets() {
		if AssetKey(asset) == key {
			asset := asset
			return &asset
		}
	}
	return nil
}

func providerFromKey(key string) string {
	provider, _, _ := strings.Cut(key, ":")
	return provider
}

type scheduledClock struct {
	hour   int
	minute int
}

func scheduledAt(hour, minute int, cfg *Config) scheduledClock {
	if hour == 0 && minute == 0 {
		return scheduledClock{hour: cfg.Schedule.RunHour, minute: cfg.Schedule.RunMinute}
	}
	return scheduledClock{hour: hour, minute: minute}
}

func secondsToDuration(sec int) time.Duration {
	if sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}

func workerSlots(n int) []string {
	if n <= 0 {
		n = 1
	}
	return make([]string, n)
}

func massiveWorkerKeys(keys []string, workers int) []string {
	if len(keys) == 0 {
		return nil
	}
	if workers <= 0 || workers == len(keys) {
		return keys
	}
	out := make([]string, workers)
	for i := range out {
		out[i] = keys[i%len(keys)]
	}
	return out
}

func providerRateLimit(cfg *Config, provider string) time.Duration {
	return newProviderPolicy(cfg, provider).rateLimit
}
