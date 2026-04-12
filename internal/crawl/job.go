package crawl

import (
	"sort"
	"time"
)

// BuildTargets stamps a flat ticker list into typed Job targets,
// filling Source, Class, and SaveDir for each entry.
func BuildTargets(tickers []string, saveDir, source string, class AssetClass) []Job {
	out := make([]Job, 0, len(tickers))
	for _, t := range tickers {
		out = append(out, Job{
			Source:  source,
			Class:   class,
			Ticker:  t,
			SaveDir: saveDir,
		})
	}
	return out
}

// resolveJobRange computes the date range [from, to] that still needs to be
// fetched for target, given a pre-loaded progress map.
//
// asOf is the last calendar date to include (e.g. last trading day or yesterday).
//
// Rules:
//   - no progress entry → backfill backfillYears of history ending asOf
//   - has entry         → fetch from lastday+1 to asOf
//   - already up to date → skip=true
//
// Chunk splitting and rate-limiting are handled inside CrawlBarsWithKey.
func resolveJobRange(target Job, m map[string]string, asOf time.Time, backfillYears int) (from, to time.Time, skip bool) {
	asOf = date(asOf)
	endOfAsOf := asOf.Add(24*time.Hour - time.Millisecond)

	key := progressKey(target.Source, target.Class, target.Ticker)
	last, ok := m[key]
	if !ok {
		if legacy, legacyOk := m[target.Ticker]; legacyOk {
			last = legacy
			ok = true
		}
	}

	if !ok {
		from = time.Date(asOf.Year()-backfillYears, asOf.Month(), asOf.Day(), 0, 0, 0, 0, time.UTC)
		return from, endOfAsOf, false
	}

	lastDay, _ := time.ParseInLocation("2006-01-02", last, time.UTC)
	startDate := date(lastDay).AddDate(0, 0, 1)
	if !startDate.Before(asOf) && !startDate.Equal(asOf) {
		return time.Time{}, time.Time{}, true // already up to date
	}
	return startDate, endOfAsOf, false
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

func date(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
