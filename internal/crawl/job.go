package crawl

import (
	"sort"
	"time"
)

// BuildTargets stamps a flat ticker list into typed Job targets,
// filling Source, Class, Frame, and SaveDir for each entry.
func BuildTargets(tickers []string, saveDir, source, frame string, class AssetClass) []Job {
	out := make([]Job, 0, len(tickers))
	for _, t := range tickers {
		out = append(out, Job{
			Source:  source,
			Class:   class,
			Frame:   frame,
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
//   - has entry              → fetch from lastSuccess+1 to asOf
//   - no entry, fromDate set → use fromDate as start
//   - no entry, fromDate zero → from = zero time; crawler self-resolves
//     (e.g. via EarliestAvailable / backward pagination until empty)
//   - already up to date     → skip=true
func resolveJobRange(target Job, m map[string]string, asOf time.Time, fromDate time.Time) (from, to time.Time, skip bool) {
	asOf = date(asOf)
	endOfAsOf := asOf.Add(24*time.Hour - time.Millisecond)

	key := progressKey(target.Source, target.Class, target.Frame, target.Ticker)
	last, ok := m[key]
	if !ok {
		if legacy, legacyOk := m[target.Ticker]; legacyOk {
			last = legacy
			ok = true
		}
	}

	if !ok {
		if !fromDate.IsZero() {
			from = date(fromDate)
		}
		// fromDate zero → from stays zero; crawler resolves.
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
