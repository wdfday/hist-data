package app

import (
	"log/slog"
	"time"

	"hist-data/internal/crawl"
)

type providerGroup struct {
	provider string
	policy   providerPolicy
	runners  []*crawl.Runner
}

func buildGroups(cfg *Config, providers map[string]crawl.BarFetcher, jobsByProvider map[string][]crawl.Job) []providerGroup {
	byProvider := make(map[string]*providerGroup)
	for key, fetcher := range providers {
		jobs, ok := jobsByProvider[key]
		if !ok || len(jobs) == 0 {
			slog.Info("no jobs for asset, skipping", "key", key)
			continue
		}

		provider := providerFromKey(key)
		group := byProvider[provider]
		if group == nil {
			group = &providerGroup{
				provider: provider,
				policy:   newProviderPolicy(cfg, provider),
			}
			byProvider[provider] = group
		}
		if len(group.policy.workerKeys) == 0 {
			slog.Warn("asset has 0 workers, skipping", "key", key)
			continue
		}

		var fromDate time.Time
		frameName := ""
		if asset := assetForKey(cfg, key); asset != nil {
			frameName = asset.Frame.Name
			fromDate = asset.Frame.fromDate()
		}
		runner := &crawl.Runner{
			Fetcher:      fetcher,
			APIKeys:      group.policy.workerKeys,
			Targets:      jobs,
			SaveBaseDir:  cfg.ProviderSaveDir(provider),
			ProgressPath: cfg.ProviderFrameProgressPath(provider, frameName),
			LegacyPath:   cfg.ProviderLastDayPath(provider),
			FromDate:     fromDate,
			RateLimit:    group.policy.rateLimit,
		}
		group.runners = append(group.runners, runner)
		slog.Info("runner ready", "key", key, "jobs", len(jobs), "workers", len(group.policy.workerKeys))
	}

	groups := make([]providerGroup, 0, len(byProvider))
	for _, group := range byProvider {
		groups = append(groups, *group)
	}
	return groups
}

func yesterday() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
}
