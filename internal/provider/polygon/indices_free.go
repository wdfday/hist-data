package polygon

// Free public data sources for index constituents.
// These are tried before the paid Massive/Polygon ETF API.
//
// Sources:
//   - S&P 500  : GitHub-hosted CSV (datasets/s-and-p-500-companies)
//   - NASDAQ100: Wikipedia wikitext API (section "Current components")
//   - DJI      : Wikipedia wikitext API (section "Components")

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	sp500CSV = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"

	// Wikipedia API: fetch wikitext for a specific section.
	// section=12 → "Current components" for Nasdaq-100
	// section=1  → "Components" for Dow Jones
	wikiAPINasdaq100 = "https://en.wikipedia.org/w/api.php?action=parse&page=Nasdaq-100&prop=wikitext&section=12&format=json"
	wikiAPIDJI       = "https://en.wikipedia.org/w/api.php?action=parse&page=Dow_Jones_Industrial_Average&prop=wikitext&section=1&format=json"
)

// freeGroupLoaders maps known group names to their free-source loader.
var freeGroupLoaders = map[string]func() ([]string, error){
	"sp500":    loadSP500Free,
	"nasdaq100": loadNasdaq100Free,
	"dji":      loadDJIFree,
}

// loadGroupFree tries to load constituents from a free public source.
// Returns (tickers, true, nil) on success.
// Returns (nil, false, nil) when no free source exists for the group.
// Returns (nil, true, err) when the free source is found but fails.
func loadGroupFree(group string) (tickers []string, found bool, err error) {
	loader, ok := freeGroupLoaders[group]
	if !ok {
		return nil, false, nil
	}
	tickers, err = loader()
	return tickers, true, err
}

// ---------------------------------------------------------------------------
// S&P 500 — GitHub raw CSV
// ---------------------------------------------------------------------------

func loadSP500Free() ([]string, error) {
	slog.Debug("fetching S&P 500 from GitHub CSV")
	return fetchCSVColumn(sp500CSV, "Symbol")
}

// fetchCSVColumn downloads a CSV and returns all values in the named column.
func fetchCSVColumn(url, column string) ([]string, error) {
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: HTTP %d", url, resp.StatusCode)
	}

	r := csv.NewReader(resp.Body)
	headers, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV header: %w", err)
	}
	colIdx := -1
	for i, h := range headers {
		if strings.EqualFold(strings.TrimSpace(h), column) {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return nil, fmt.Errorf("column %q not found (headers: %v)", column, headers)
	}

	var out []string
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if colIdx < len(row) {
			if sym := strings.ToUpper(strings.TrimSpace(row[colIdx])); sym != "" {
				out = append(out, sym)
			}
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// NASDAQ 100 + DJI — Wikipedia wikitext API
// ---------------------------------------------------------------------------

type wikiParseResp struct {
	Parse struct {
		Wikitext struct {
			Content string `json:"*"`
		} `json:"wikitext"`
	} `json:"parse"`
}

// Nasdaq-100 wikitext: each component row starts with "| TICKER ||"
var nasdaq100Re = regexp.MustCompile(`(?m)^\| ([A-Z0-9]+) \|\|`)

// DJI wikitext: tickers appear as {{NYSE link|MMM}} or {{NASDAQ link|AMGN}}
var djiRe = regexp.MustCompile(`\{\{(?:NYSE|NASDAQ) link\|([A-Z.]+)\}\}`)

func loadNasdaq100Free() ([]string, error) {
	slog.Debug("fetching NASDAQ-100 from Wikipedia")
	wikitext, err := fetchWikitext(wikiAPINasdaq100)
	if err != nil {
		return nil, err
	}
	return extractMatches(wikitext, nasdaq100Re, 1)
}

func loadDJIFree() ([]string, error) {
	slog.Debug("fetching DJI from Wikipedia")
	wikitext, err := fetchWikitext(wikiAPIDJI)
	if err != nil {
		return nil, err
	}
	return extractMatches(wikitext, djiRe, 1)
}

func fetchWikitext(apiURL string) (string, error) {
	client := &http.Client{Timeout: 20 * time.Second}
	req, _ := http.NewRequest("GET", apiURL, nil)
	req.Header.Set("User-Agent", "us-data-crawler/1.0 (github.com/us-data; contact@example.com)")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("wikipedia fetch: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var parsed wikiParseResp
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("wikipedia JSON parse: %w", err)
	}
	return parsed.Parse.Wikitext.Content, nil
}

func extractMatches(text string, re *regexp.Regexp, group int) ([]string, error) {
	seen := make(map[string]struct{})
	var out []string
	for _, m := range re.FindAllStringSubmatch(text, -1) {
		if sym := strings.ToUpper(strings.TrimSpace(m[group])); sym != "" {
			if _, ok := seen[sym]; !ok {
				seen[sym] = struct{}{}
				out = append(out, sym)
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no tickers extracted (pattern: %s)", re)
	}
	return out, nil
}
