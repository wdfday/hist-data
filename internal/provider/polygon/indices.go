package polygon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrNotAuthorized is returned when the API responds with HTTP 403.
// Callers can decide whether to skip or abort.
var ErrNotAuthorized = errors.New("not authorized (plan upgrade required)")

// ---------------------------------------------------------------------------
// Known index groups → ETF proxy ticker used to fetch constituents via the
// ETF Global API ( GET /etf-global/v1/constituents?ticker=<ETF> )
// ---------------------------------------------------------------------------

var knownGroups = map[string]string{
	"sp500":    "SPY", // SPDR S&P 500 ETF Trust
	"nasdaq100": "QQQ", // Invesco QQQ Trust
	"dji":      "DIA", // SPDR Dow Jones Industrial Average ETF
	"russell2000": "IWM", // iShares Russell 2000 ETF
}

// KnownGroupNames returns the list of supported named groups.
func KnownGroupNames() []string {
	names := make([]string, 0, len(knownGroups))
	for k := range knownGroups {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// ---------------------------------------------------------------------------
// File-based loader (kept for STOCK_SELECTION=file fallback)
// ---------------------------------------------------------------------------

// LoadTickersFromFile reads a ticker list from a .txt or .json file.
func LoadTickersFromFile(path string) ([]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("open file %s: %w", path, err)
	}
	var tickers []string
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		if err := json.Unmarshal(content, &tickers); err != nil {
			return nil, fmt.Errorf("parse JSON: %w", err)
		}
	case ".txt":
		tickers = parseTickersFromText(string(content))
	default:
		return nil, fmt.Errorf("unsupported extension %q (use .txt or .json)", filepath.Ext(path))
	}
	out := dedup(tickers)
	slog.Info("tickers loaded from file", "path", path, "count", len(out))
	return out, nil
}

func parseTickersFromText(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Polygon / Massive reference API loader
// ---------------------------------------------------------------------------

type polygonTicker struct {
	Ticker string `json:"ticker"`
	Active bool   `json:"active"`
	Market string `json:"market"`
}

type polygonTickersResponse struct {
	Results []polygonTicker `json:"results"`
	NextURL string          `json:"next_url"`
}

// LoadTickersFromPolygon fetches all active tickers for one or more markets
// from the Massive/Polygon reference API, paginating via next_url.
func LoadTickersFromPolygon(apiKey string, markets []string) ([]string, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("API key required")
	}
	if len(markets) == 0 {
		markets = []string{"stocks"}
	}

	client := refHTTPClient()
	seen := make(map[string]struct{})
	var all []string

	for _, market := range markets {
		market = strings.ToLower(strings.TrimSpace(market))
		if market == "" {
			continue
		}
		pageURL := fmt.Sprintf(
			"%s/v3/reference/tickers?market=%s&active=true&limit=1000&order=asc",
			polygonBaseURL, url.QueryEscape(market),
		)
		for pageURL != "" {
			results, next, err := fetchTickerPage(client, pageURL, apiKey)
			if err != nil {
				return nil, fmt.Errorf("market %q: %w", market, err)
			}
			for _, sym := range results {
				if _, ok := seen[sym]; !ok {
					seen[sym] = struct{}{}
					all = append(all, sym)
				}
			}
			pageURL = next
		}
	}

	sort.Strings(all)
	slog.Info("tickers loaded from API", "markets", markets, "count", len(all))
	return all, nil
}

// ---------------------------------------------------------------------------
// Group-based loader (index constituents via ETF proxy)
// ---------------------------------------------------------------------------

type etfConstituentResponse struct {
	Results []struct {
		Ticker string `json:"ticker"`
	} `json:"results"`
	NextURL string `json:"next_url"`
}

// LoadTickersForGroup fetches constituent tickers for a named index group.
// Supported groups: sp500, nasdaq100, dji (free), russell2000 (paid only).
//
// Strategy:
//  1. Try a free public source (GitHub CSV / Wikipedia).
//  2. If the free source is unavailable for this group, fall back to the
//     Massive/Polygon ETF API (requires Starter+ plan).
func LoadTickersForGroup(apiKey, group string) ([]string, error) {
	group = strings.ToLower(strings.TrimSpace(group))

	if _, ok := knownGroups[group]; !ok {
		return nil, fmt.Errorf("unknown group %q; supported: %v", group, KnownGroupNames())
	}

	// 1. Try free source first.
	tickers, found, err := loadGroupFree(group)
	if found {
		if err != nil {
			slog.Warn("free source failed, falling back to ETF API",
				"group", group, "err", err)
		} else {
			sort.Strings(tickers)
			slog.Info("group constituents loaded (free source)",
				"group", group, "count", len(tickers))
			return tickers, nil
		}
	}

	// 2. Fall back to paid ETF API.
	if apiKey == "" {
		return nil, fmt.Errorf("group %q: no free source available and no API key provided", group)
	}

	etfTicker := knownGroups[group]
	client := refHTTPClient()
	pageURL := fmt.Sprintf("%s/etf-global/v1/constituents?ticker=%s", polygonBaseURL, etfTicker)

	var all []string
	seen := make(map[string]struct{})

	for pageURL != "" {
		u, err := url.Parse(pageURL)
		if err != nil {
			return nil, fmt.Errorf("parse URL: %w", err)
		}
		q := u.Query()
		if q.Get("apiKey") == "" {
			q.Set("apiKey", apiKey)
		}
		u.RawQuery = q.Encode()

		req, _ := http.NewRequest("GET", u.String(), nil)
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch group %q: %w", group, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("group %q (ETF=%s): %w — upgrade at https://massive.com/pricing",
				group, etfTicker, ErrNotAuthorized)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("ETF constituents status %d for group %q (ETF=%s): %s",
				resp.StatusCode, group, etfTicker, string(body))
		}

		var parsed etfConstituentResponse
		if err := json.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("parse ETF response for group %q: %w", group, err)
		}
		for _, r := range parsed.Results {
			sym := strings.ToUpper(strings.TrimSpace(r.Ticker))
			if sym != "" {
				if _, ok := seen[sym]; !ok {
					seen[sym] = struct{}{}
					all = append(all, sym)
				}
			}
		}
		pageURL = parsed.NextURL
	}

	sort.Strings(all)
	slog.Info("group constituents loaded (ETF API)",
		"group", group, "etf", etfTicker, "count", len(all))
	return all, nil
}

// ---------------------------------------------------------------------------
// Ticker validation
// ---------------------------------------------------------------------------

type tickerDetailResponse struct {
	Status  string `json:"status"`
	Results struct {
		Ticker string `json:"ticker"`
		Active bool   `json:"active"`
	} `json:"results"`
}

// ValidateTickers checks each ticker against GET /v3/reference/tickers/{ticker}.
// Returns (valid, invalid, error). Runs validations concurrently (max 8).
func ValidateTickers(apiKey string, tickers []string) (valid, invalid []string, err error) {
	if apiKey == "" {
		return nil, nil, fmt.Errorf("API key required for validation")
	}
	if len(tickers) == 0 {
		return nil, nil, nil
	}

	type result struct {
		ticker  string
		isValid bool
	}

	client := refHTTPClient()
	sem := make(chan struct{}, 8) // max 8 concurrent
	results := make(chan result, len(tickers))
	var wg sync.WaitGroup

	for _, t := range tickers {
		t := t
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			u := fmt.Sprintf("%s/v3/reference/tickers/%s?apiKey=%s",
				polygonBaseURL, url.PathEscape(t), apiKey)
			req, _ := http.NewRequest("GET", u, nil)
			resp, e := client.Do(req)
			if e != nil {
				results <- result{t, false}
				return
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				results <- result{t, false}
				return
			}
			if resp.StatusCode != http.StatusOK {
				slog.Warn("ticker validation unexpected status",
					"ticker", t, "status", resp.StatusCode, "body", string(body))
				results <- result{t, false}
				return
			}
			var detail tickerDetailResponse
			if err := json.Unmarshal(body, &detail); err != nil {
				results <- result{t, false}
				return
			}
			results <- result{t, detail.Status == "OK" && detail.Results.Active}
		}()
	}

	wg.Wait()
	close(results)

	for r := range results {
		if r.isValid {
			valid = append(valid, r.ticker)
		} else {
			invalid = append(invalid, r.ticker)
		}
	}
	sort.Strings(valid)
	sort.Strings(invalid)

	if len(invalid) > 0 {
		slog.Warn("tickers skipped (invalid/inactive)", "count", len(invalid), "tickers", invalid)
	}
	slog.Info("ticker validation done", "valid", len(valid), "skipped", len(invalid))
	return valid, invalid, nil
}

// ---------------------------------------------------------------------------
// ResolveAssetTickers resolves the full ticker list for a single AssetConfig:
//   1. Load from each group (index constituents via ETF API)
//   2. Merge explicit tickers
//   3. Dedup
//   4. Validate if cfg.Validate = true
// ---------------------------------------------------------------------------

// AssetTickerSpec is a minimal interface so polygon/indices.go doesn't import app.
type AssetTickerSpec struct {
	Class    string
	Groups   []string
	Tickers  []string
	Validate bool
}

func ResolveAssetTickers(apiKey string, spec AssetTickerSpec) ([]string, error) {
	seen := make(map[string]struct{})
	var all []string

	add := func(sym string) {
		sym = strings.ToUpper(strings.TrimSpace(sym))
		if sym == "" {
			return
		}
		if _, ok := seen[sym]; !ok {
			seen[sym] = struct{}{}
			all = append(all, sym)
		}
	}

	// Load from groups
	for _, group := range spec.Groups {
		group = strings.ToLower(strings.TrimSpace(group))
		if group == "" {
			continue
		}
		if group == "all" {
			market := classToMarket(spec.Class)
			tickers, err := LoadTickersFromPolygon(apiKey, []string{market})
			if err != nil {
				if errors.Is(err, ErrNotAuthorized) {
					slog.Warn("group \"all\" skipped: plan upgrade required",
						"class", spec.Class, "hint", "use explicit tickers[] in config.yaml instead")
					continue
				}
				return nil, fmt.Errorf("group \"all\" for class %q: %w", spec.Class, err)
			}
			for _, t := range tickers {
				add(t)
			}
			continue
		}
		tickers, err := LoadTickersForGroup(apiKey, group)
		if err != nil {
			if errors.Is(err, ErrNotAuthorized) {
				slog.Warn("group skipped: plan upgrade required",
					"group", group, "hint", "use explicit tickers[] in config.yaml instead")
				continue
			}
			return nil, fmt.Errorf("group %q: %w", group, err)
		}
		for _, t := range tickers {
			add(t)
		}
	}

	// Add explicit individual tickers
	for _, t := range spec.Tickers {
		add(t)
	}

	if len(all) == 0 {
		return nil, fmt.Errorf("class %q: no tickers resolved (groups=%v, tickers=%v)",
			spec.Class, spec.Groups, spec.Tickers)
	}

	// Validate individual explicit tickers if requested
	// (group-loaded tickers come from the reference API so they're implicitly valid)
	if spec.Validate && len(spec.Tickers) > 0 {
		explicit := dedup(spec.Tickers)
		valid, invalid, err := ValidateTickers(apiKey, explicit)
		if err != nil {
			return nil, fmt.Errorf("validate tickers for class %q: %w", spec.Class, err)
		}
		if len(invalid) > 0 {
			slog.Warn("invalid tickers removed", "class", spec.Class, "tickers", invalid)
		}
		// Rebuild all: keep group-loaded ones + only valid explicit ones
		validSet := make(map[string]struct{}, len(valid))
		for _, v := range valid {
			validSet[v] = struct{}{}
		}
		explicitSet := make(map[string]struct{}, len(explicit))
		for _, e := range explicit {
			explicitSet[e] = struct{}{}
		}
		var filtered []string
		for _, sym := range all {
			if _, isExplicit := explicitSet[sym]; isExplicit {
				if _, isValid := validSet[sym]; isValid {
					filtered = append(filtered, sym)
				}
			} else {
				filtered = append(filtered, sym)
			}
		}
		all = filtered
	}

	sort.Strings(all)
	slog.Info("asset tickers resolved",
		"class", spec.Class, "count", len(all),
		"groups", spec.Groups, "explicit", len(spec.Tickers))
	return all, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func classToMarket(class string) string {
	switch strings.ToLower(class) {
	case "crypto":
		return "crypto"
	case "forex":
		return "fx"
	case "indices":
		return "indices"
	default:
		return "stocks"
	}
}

func refHTTPClient() *http.Client {
	return &http.Client{Timeout: 20 * time.Second}
}

func fetchTickerPage(client *http.Client, pageURL, apiKey string) (tickers []string, nextURL string, err error) {
	u, err := url.Parse(pageURL)
	if err != nil {
		return nil, "", fmt.Errorf("parse URL: %w", err)
	}
	q := u.Query()
	if q.Get("apiKey") == "" {
		q.Set("apiKey", apiKey)
	}
	u.RawQuery = q.Encode()

	req, _ := http.NewRequest("GET", u.String(), nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("request: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	var parsed polygonTickersResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, "", fmt.Errorf("parse JSON: %w", err)
	}
	for _, t := range parsed.Results {
		if t.Active {
			if sym := strings.ToUpper(strings.TrimSpace(t.Ticker)); sym != "" {
				tickers = append(tickers, sym)
			}
		}
	}
	return tickers, parsed.NextURL, nil
}

func dedup(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	var out []string
	for _, s := range in {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}
