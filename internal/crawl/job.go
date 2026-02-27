package crawl

import (
	"path/filepath"
	"sort"
	"time"
)

// ClassSaveDir returns the per-class subdirectory under saveBaseDir.
//
//	ClassSaveDir("data/Polygon", AssetStocks) → "data/Polygon/stocks"
func ClassSaveDir(saveBaseDir string, class AssetClass) string {
	return filepath.Join(saveBaseDir, string(class))
}

// BuildTargets stamps a flat ticker list into typed Job targets,
// filling Source, Class, and SaveDir for each entry.
func BuildTargets(tickers []string, saveBaseDir, source string, class AssetClass) []Job {
	dir := ClassSaveDir(saveBaseDir, class)
	out := make([]Job, 0, len(tickers))
	for _, t := range tickers {
		out = append(out, Job{
			Source:  source,
			Class:   class,
			Ticker:  t,
			SaveDir: dir,
		})
	}
	return out
}

// resolveJobRange computes the date range [from, to] that still needs to be
// fetched for target, given a pre-loaded progress map.
//
// Rules:
//   - no progress entry → backfill backfillYears of history ending yesterday
//   - has entry         → fetch from lastday+1 to yesterday
//   - already up to date → skip=true
//
// Chunk splitting and rate-limiting are handled inside CrawlBarsWithKey.
func resolveJobRange(target Job, m map[string]string, now time.Time, backfillYears int) (from, to time.Time, skip bool) {
	yesterday := date(now).AddDate(0, 0, -1)
	endOfYesterday := yesterday.Add(24*time.Hour - time.Millisecond)

	key := progressKey(target.Source, target.Class, target.Ticker)
	last, ok := m[key]
	if !ok {
		if legacy, legacyOk := m[target.Ticker]; legacyOk {
			last = legacy
			ok = true
		}
	}

	if !ok {
		if backfillYears <= 0 {
			backfillYears = 2
		}
		from = time.Date(now.Year()-backfillYears, now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		return from, endOfYesterday, false
	}

	lastDay, _ := time.ParseInLocation("2006-01-02", last, time.UTC)
	startDate := date(lastDay).AddDate(0, 0, 1)
	if !startDate.Before(yesterday) && !startDate.Equal(yesterday) {
		return time.Time{}, time.Time{}, true // already up to date
	}
	return startDate, endOfYesterday, false
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
