package app

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// logLevel is a package-level LevelVar so the log level can be changed at
// runtime (after config is loaded) without recreating the handler.
var logLevel = new(slog.LevelVar) // defaults to Info

// AssetConfig describes one asset class to crawl.
type AssetConfig struct {
	Class    string   `mapstructure:"class"`
	Provider string   `mapstructure:"provider"` // massive | binance | twelvedata | vci | okx
	Enabled  bool     `mapstructure:"enabled"`
	Groups   []string `mapstructure:"groups"`   // sp500 | nasdaq100 | dji | all (Polygon only)
	Tickers  []string `mapstructure:"tickers"`  // explicit individual symbols
	Validate bool     `mapstructure:"validate"` // verify tickers against reference API (Polygon only)

	// Frames is the preferred timeframe model across all providers.
	// Examples: M1, M5, M15, M30, H1, H4, D1.
	// Set sinkFrames per frame to derive additional timeframes from the fetched source.
	// Example: M1 with sinkFrames [M1, M5, M15, M30, H1, H4] saves all derived bars.
	Frames []FrameSpec `mapstructure:"frames"`

	// Legacy per-provider frame fields kept for backward compatibility.
	Timespans  []string `mapstructure:"timespan"`   // massive: minute|hour|day|week|month
	Multiplier int      `mapstructure:"multiplier"` // massive: e.g. 1, 5, 15
	Intervals  []string `mapstructure:"interval"`   // binance: 1m|5m|1h|1d|...
	TimeFrames []string `mapstructure:"timeFrame"`  // vci: ONE_DAY|ONE_MINUTE|ONE_HOUR
}

type FrameSpec struct {
	Name       string   `mapstructure:"name"`
	From       string   `mapstructure:"from"`      // "YYYY", "YYYY-MM", or "YYYY-MM-DD"; 0/empty = full history
	FromYear   int      `mapstructure:"fromYear"`  // legacy: use From instead
	FromMonth  int      `mapstructure:"fromMonth"` // legacy: use From instead
	SinkFrames []string `mapstructure:"sinkFrames"`
}

// fromDate parses the From string ("YYYY", "YYYY-MM", "YYYY-MM-DD").
// Falls back to FromYear/FromMonth for backward compatibility.
// Returns zero time when no start date is configured (= full history).
func (f FrameSpec) fromDate() time.Time {
	if f.From != "" {
		for _, layout := range []string{"2006-01-02", "2006-01", "2006"} {
			if t, err := time.Parse(layout, strings.TrimSpace(f.From)); err == nil {
				return t.UTC()
			}
		}
	}
	if f.FromYear > 0 {
		month := time.Month(f.FromMonth)
		if month < 1 || month > 12 {
			month = time.January
		}
		return time.Date(f.FromYear, month, 1, 0, 0, 0, 0, time.UTC)
	}
	return time.Time{}
}

// Config is the application configuration loaded from config.yaml with env overrides.
type Config struct {
	Provider string `mapstructure:"provider"` // default provider (massive)

	// Massive / Polygon — US stocks and indices (requires API key)
	Massive struct {
		BaseURL      string   `mapstructure:"baseUrl"`   // default: https://api.polygon.io
		Keys         []string `mapstructure:"keys"`      // set via POLYGON_API_KEYS env
		Workers      int      `mapstructure:"workers"`   // parallel goroutines; 0 = one per key (default)
		RateLimitSec int      `mapstructure:"rateLimit"` // seconds between requests per worker; 0 = no limit
		Schedule     struct {
			RunHour   int `mapstructure:"runHour"`
			RunMinute int `mapstructure:"runMinute"`
		} `mapstructure:"schedule"`
	} `mapstructure:"massive"`

	Data struct {
		Dir    string `mapstructure:"dir"`    // root output directory; supports relative (../data) and absolute paths
		Format string `mapstructure:"format"` // parquet | csv | json
	} `mapstructure:"data"`

	// Binance — crypto klines via REST API (no key required)
	Binance struct {
		BaseURL      string `mapstructure:"baseUrl"`   // default: https://api.binance.com
		Workers      int    `mapstructure:"workers"`   // parallel download goroutines
		RateLimitSec int    `mapstructure:"rateLimit"` // seconds between requests per worker; 0 = no limit
		Schedule     struct {
			RunHour   int `mapstructure:"runHour"`
			RunMinute int `mapstructure:"runMinute"`
		} `mapstructure:"schedule"`
	} `mapstructure:"binance"`

	// BinanceFlat — crypto klines via flat-file CDN (data.binance.vision, no key, no rate limit)
	BinanceFlat struct {
		BaseURL  string `mapstructure:"baseUrl"` // default: https://data.binance.vision
		Workers  int    `mapstructure:"workers"` // parallel ZIP downloads; CDN has no rate limit
		Schedule struct {
			RunHour   int `mapstructure:"runHour"`
			RunMinute int `mapstructure:"runMinute"`
		} `mapstructure:"schedule"`
	} `mapstructure:"binanceflat"`

	// TwelveData — forex, stocks, ETFs, indices, crypto (requires API key)
	// Free tier: 8 req/min, 800 credits/day. Set workers: 1 on free plan.
	TwelveData struct {
		BaseURL      string `mapstructure:"baseUrl"`   // default: https://api.twelvedata.com
		APIKey       string `mapstructure:"apiKey"`    // set via TWELVEDATA_API_KEY env
		Workers      int    `mapstructure:"workers"`   // 1 on free tier, up to 8 on paid
		RateLimitSec int    `mapstructure:"rateLimit"` // seconds between requests per worker; 0 = no limit
		Schedule     struct {
			RunHour   int `mapstructure:"runHour"`
			RunMinute int `mapstructure:"runMinute"`
		} `mapstructure:"schedule"`
	} `mapstructure:"twelvedata"`

	// VCI (Vietcap) — Vietnamese stocks (no key required)
	VCI struct {
		BaseURL      string `mapstructure:"baseUrl"`   // default: https://trading.vietcap.com.vn/api
		Workers      int    `mapstructure:"workers"`   // parallel goroutines
		RateLimitSec int    `mapstructure:"rateLimit"` // seconds between requests per worker; 0 = no limit
		Schedule     struct {
			RunHour   int `mapstructure:"runHour"`
			RunMinute int `mapstructure:"runMinute"`
		} `mapstructure:"schedule"`
	} `mapstructure:"vci"`

	// OKX — crypto klines (public API, no key required)
	OKX struct {
		BaseURL      string `mapstructure:"baseUrl"`   // default: https://www.okx.com
		Workers      int    `mapstructure:"workers"`   // parallel download goroutines
		RateLimitSec int    `mapstructure:"rateLimit"` // seconds between requests per worker; 0 = no limit
		Schedule     struct {
			RunHour   int `mapstructure:"runHour"`
			RunMinute int `mapstructure:"runMinute"`
		} `mapstructure:"schedule"`
	} `mapstructure:"okx"`

	// Schedule is the global fallback run time (UTC).
	// Each provider can override via its own schedule block.
	Schedule struct {
		RunHour   int `mapstructure:"runHour"`
		RunMinute int `mapstructure:"runMinute"`
	} `mapstructure:"schedule"`

	Log struct {
		Level  string `mapstructure:"level"`
		Format string `mapstructure:"format"` // text | json  (default: text)
		File   string `mapstructure:"file"`   // optional path; empty = stderr only
	} `mapstructure:"log"`

	Assets []AssetConfig `mapstructure:"assets"`
}

// LoadConfig reads config.yaml (or CONFIG_FILE env) then overlays secrets from env.
func LoadConfig() (*Config, error) {
	v := viper.New()

	// Locate config file
	cfgFile := os.Getenv("CONFIG_FILE")
	if cfgFile == "" {
		cfgFile = "config.yaml"
	}
	v.SetConfigFile(cfgFile)

	// Defaults
	v.SetDefault("provider", "massive")
	v.SetDefault("massive.baseUrl", "https://api.polygon.io")
	v.SetDefault("data.dir", "data")
	v.SetDefault("data.format", "parquet")
	v.SetDefault("schedule.runHour", 0)
	v.SetDefault("schedule.runMinute", 30)
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "text")
	v.SetDefault("binance.baseUrl", "https://api.binance.com")
	v.SetDefault("binance.workers", 1)
	v.SetDefault("binanceflat.baseUrl", "https://data.binance.vision")
	v.SetDefault("binanceflat.workers", 5)
	v.SetDefault("twelvedata.baseUrl", "https://api.twelvedata.com")
	v.SetDefault("twelvedata.workers", 1)
	v.SetDefault("vci.baseUrl", "https://trading.vietcap.com.vn/api")
	v.SetDefault("vci.workers", 2)
	v.SetDefault("okx.baseUrl", "https://www.okx.com")
	v.SetDefault("okx.workers", 3)

	// Bind env vars — picked up automatically by Unmarshal.
	// Secrets are never in YAML; they live only in env.
	_ = v.BindEnv("twelvedata.apiKey", "TWELVEDATA_API_KEY")
	_ = v.BindEnv("log.level", "LOG_LEVEL")
	_ = v.BindEnv("data.dir", "DATA_DIR")
	_ = v.BindEnv("data.format", "SAVE_FORMAT")

	// POLYGON_API_KEYS needs special handling: comma-split + singular fallback.
	if keys := parseAPIKeysFromEnv(); len(keys) > 0 {
		v.Set("massive.keys", keys)
	}

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config file %q: %w", cfgFile, err)
	}
	slog.Info("loaded config", "file", v.ConfigFileUsed())

	var cfg Config
	if err := v.Unmarshal(&cfg, viper.DecodeHook(stringOrSliceHook())); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	cfg.Data.Dir = resolveDataDir(cfg.Data.Dir, cfgFile)

	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// resolveDataDir anchors a relative data dir to the directory of the config
// file (which is the repo root in this project). Absolute paths are kept as-is
// so Docker builds with `/mallow/hist-data/data` still work. An empty value
// defaults to `<repo-root>/data`.
func resolveDataDir(dir, cfgFile string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		dir = "data"
	}
	if filepath.IsAbs(dir) {
		return filepath.Clean(dir)
	}
	cfgAbs, err := filepath.Abs(cfgFile)
	if err != nil {
		return filepath.Clean(dir)
	}
	return filepath.Clean(filepath.Join(filepath.Dir(cfgAbs), dir))
}

var supportedFrameNames = map[string]bool{
	"M1":  true,
	"M5":  true,
	"M15": true,
	"M30": true,
	"H1":  true,
	"H4":  true,
	"D1":  true,
	"W1":  true,
	"MN":  true,
}

func validateConfig(cfg *Config) error {
	format := strings.ToLower(cfg.Data.Format)
	if format != "parquet" && format != "csv" && format != "json" {
		return fmt.Errorf("unsupported data.format %q (allowed: parquet, csv, json)", cfg.Data.Format)
	}

	enabled := 0
	for _, a := range cfg.Assets {
		if !a.Enabled {
			continue
		}
		enabled++
		prov := strings.ToLower(strings.TrimSpace(a.Provider))
		if prov == "" {
			prov = "massive"
		}
		switch prov {
		case "massive":
			if len(cfg.Massive.Keys) == 0 {
				return fmt.Errorf("asset %q uses massive provider but no API keys found: set POLYGON_API_KEYS env", a.Class)
			}
			if a.Multiplier < 0 {
				return fmt.Errorf("asset %q: multiplier must be >= 1, got %d", a.Class, a.Multiplier)
			}
		}

		frames := a.frameSpecs()
		if len(frames) == 0 {
			return fmt.Errorf("asset %q: no frames configured", a.Class)
		}
		for _, frame := range frames {
			if frame.Name == "" {
				return fmt.Errorf("asset %q: frame name must not be empty", a.Class)
			}
			if !supportedFrameNames[frame.Name] {
				return fmt.Errorf("asset %q: unsupported frame %q (allowed: M1, M5, M15, M30, H1, H4, D1, W1, MN)", a.Class, frame.Name)
			}
			if frame.From != "" {
				valid := false
				for _, layout := range []string{"2006-01-02", "2006-01", "2006"} {
					if _, err := time.Parse(layout, strings.TrimSpace(frame.From)); err == nil {
						valid = true
						break
					}
				}
				if !valid {
					return fmt.Errorf("asset %q frame %q: invalid from %q (expected YYYY, YYYY-MM, or YYYY-MM-DD)", a.Class, frame.Name, frame.From)
				}
			} else if frame.FromYear < 0 {
				return fmt.Errorf("asset %q frame %q: fromYear must be >= 0 (0 = full history)", a.Class, frame.Name)
			}
			if _, err := providerFrameSpec(prov, frame.Name); err != nil {
				return fmt.Errorf("asset %q: %w", a.Class, err)
			}
		}

		for _, asset := range a.expand() {
			if err := validateSinkFrames(asset); err != nil {
				return fmt.Errorf("asset %q source %q: %w", a.Class, asset.Frame.Name, err)
			}
		}
	}
	if enabled == 0 {
		return fmt.Errorf("no assets enabled in config.yaml; set at least one assets[].enabled: true")
	}
	return nil
}

func parseAPIKeysFromEnv() []string {
	s := os.Getenv("POLYGON_API_KEYS")
	if s == "" {
		s = os.Getenv("POLYGON_API_KEY")
	}
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	var keys []string
	for _, p := range parts {
		if k := strings.TrimSpace(p); k != "" {
			keys = append(keys, k)
		}
	}
	return keys
}

func stringOrSliceHook() mapstructure.DecodeHookFunc {
	return func(from reflect.Type, to reflect.Type, data any) (any, error) {
		if to != reflect.TypeOf([]string{}) {
			return data, nil
		}

		switch from.Kind() {
		case reflect.String:
			value := strings.TrimSpace(data.(string))
			if value == "" {
				return []string{}, nil
			}
			return []string{value}, nil
		case reflect.Slice:
			if from.Elem().Kind() == reflect.String {
				return data, nil
			}

			items, ok := data.([]any)
			if !ok {
				return data, nil
			}

			out := make([]string, 0, len(items))
			for _, item := range items {
				value, ok := item.(string)
				if !ok {
					return data, nil
				}
				value = strings.TrimSpace(value)
				if value != "" {
					out = append(out, value)
				}
			}
			return out, nil
		default:
			return data, nil
		}
	}
}

type resolvedAsset struct {
	AssetConfig
	Frame      FrameSpec
	SinkFrames []string
}

func (a AssetConfig) expand() []resolvedAsset {
	frames := a.frameSpecs()
	if len(frames) == 0 {
		frames = []FrameSpec{{}}
	}

	out := make([]resolvedAsset, 0, len(frames))
	for _, frame := range frames {
		normalized := normalizeFrameSpec(frame)
		out = append(out, resolvedAsset{
			AssetConfig: a,
			Frame:       normalized,
			SinkFrames:  normalizeSinkFrames(normalized.SinkFrames, normalized.Name),
		})
	}
	return out
}

// normalizeSinkFrames deduplicates sink frame names.
// If sinks is empty, defaults to [sourceFrame] (save only the fetched frame).
func normalizeSinkFrames(sinks []string, sourceFrame string) []string {
	sourceFrame = strings.ToUpper(strings.TrimSpace(sourceFrame))
	if len(sinks) == 0 {
		return []string{sourceFrame}
	}

	out := make([]string, 0, len(sinks))
	seen := make(map[string]struct{}, len(sinks))
	for _, sink := range sinks {
		sink = strings.ToUpper(strings.TrimSpace(sink))
		if sink == "" {
			continue
		}
		if _, ok := seen[sink]; ok {
			continue
		}
		seen[sink] = struct{}{}
		out = append(out, sink)
	}
	return out
}

func (a AssetConfig) frameSpecs() []FrameSpec {
	if len(a.Frames) > 0 {
		out := make([]FrameSpec, 0, len(a.Frames))
		for _, frame := range a.Frames {
			out = append(out, normalizeFrameSpec(frame))
		}
		return out
	}

	return a.legacyFrameSpecs()
}

func (a AssetConfig) legacyFrameSpecs() []FrameSpec {
	provider := ProviderName(a)
	switch provider {
	case "massive":
		if len(a.Timespans) == 0 {
			return nil
		}
		out := make([]FrameSpec, 0, len(a.Timespans))
		for _, timespan := range a.Timespans {
			out = append(out, FrameSpec{
				Name: legacyMassiveFrameName(strings.TrimSpace(timespan), a.Multiplier),
			})
		}
		return out
	case "vci":
		if len(a.TimeFrames) == 0 {
			return nil
		}
		out := make([]FrameSpec, 0, len(a.TimeFrames))
		for _, timeFrame := range a.TimeFrames {
			out = append(out, FrameSpec{
				Name: legacyVCIFrameName(strings.TrimSpace(timeFrame)),
			})
		}
		return out
	default:
		if len(a.Intervals) == 0 {
			return nil
		}
		out := make([]FrameSpec, 0, len(a.Intervals))
		for _, interval := range a.Intervals {
			out = append(out, FrameSpec{
				Name: legacyIntervalFrameName(strings.TrimSpace(interval)),
			})
		}
		return out
	}
}

func normalizeFrameSpec(frame FrameSpec) FrameSpec {
	frame.Name = strings.ToUpper(strings.TrimSpace(frame.Name))
	frame.From = strings.TrimSpace(frame.From)
	if frame.FromYear < 0 {
		frame.FromYear = 0
	}
	// Normalize sink frame names in-place (uppercase, trim).
	for i, s := range frame.SinkFrames {
		frame.SinkFrames[i] = strings.ToUpper(strings.TrimSpace(s))
	}
	return frame
}

// knownFrames mirrors saver.frameRank — all valid frame names.
var knownFrames = map[string]int{
	"M1": 1, "M5": 2, "M15": 3, "M30": 4,
	"H1": 5, "H4": 6, "D1": 7, "W1": 8, "MN": 9,
}

func validateSinkFrames(asset resolvedAsset) error {
	source := asset.Frame.Name
	if source == "" {
		return fmt.Errorf("source frame is required")
	}
	if len(asset.SinkFrames) == 0 {
		return fmt.Errorf("at least one sink frame is required")
	}

	srcRank, srcOk := knownFrames[source]
	if !srcOk {
		return fmt.Errorf("unknown source frame %q", source)
	}

	for _, sink := range asset.SinkFrames {
		dstRank, dstOk := knownFrames[sink]
		if !dstOk {
			return fmt.Errorf("unknown sink frame %q", sink)
		}
		if dstRank < srcRank {
			return fmt.Errorf("sink frame %q (rank %d) must be >= source frame %q (rank %d)", sink, dstRank, source, srcRank)
		}
	}
	return nil
}

func legacyMassiveFrameName(timespan string, multiplier int) string {
	if multiplier <= 0 {
		multiplier = 1
	}
	switch strings.ToLower(timespan) {
	case "minute":
		return fmt.Sprintf("M%d", multiplier)
	case "hour":
		return fmt.Sprintf("H%d", multiplier)
	case "day":
		return fmt.Sprintf("D%d", multiplier)
	case "week":
		return fmt.Sprintf("W%d", multiplier)
	case "month":
		return fmt.Sprintf("MO%d", multiplier)
	default:
		return ""
	}
}

func legacyIntervalFrameName(interval string) string {
	switch strings.ToLower(interval) {
	case "1m":
		return "M1"
	case "5m":
		return "M5"
	case "15m":
		return "M15"
	case "30m":
		return "M30"
	case "1h":
		return "H1"
	case "4h":
		return "H4"
	case "1d", "1day":
		return "D1"
	case "1w", "1week":
		return "W1"
	case "1month", "1mo", "1mth", "1M":
		return "MN"
	default:
		return ""
	}
}

func legacyVCIFrameName(timeFrame string) string {
	switch strings.ToUpper(timeFrame) {
	case "ONE_MINUTE":
		return "M1"
	case "ONE_HOUR":
		return "H1"
	case "ONE_DAY":
		return "D1"
	default:
		return ""
	}
}

// ProviderSaveDir returns the output directory for a given provider
// rooted at data.dir. Supports relative (../data) and absolute paths.
func (c *Config) ProviderSaveDir(provider string) string {
	switch provider {
	case "binance":
		return filepath.Join(c.Data.Dir, "Binance")
	case "binanceflat":
		return filepath.Join(c.Data.Dir, "BinanceFlat")
	case "twelvedata":
		return filepath.Join(c.Data.Dir, "TwelveData")
	case "vci":
		return filepath.Join(c.Data.Dir, "VCI")
	case "okx":
		return filepath.Join(c.Data.Dir, "OKX")
	default: // massive / polygon
		return filepath.Join(c.Data.Dir, "Polygon")
	}
}

// ProgressPath returns the Polygon progress file path (kept for compatibility).
func (c *Config) ProgressPath() string {
	return c.ProviderProgressPath("massive")
}

// ProviderProgressPath returns the per-ticker last-success checkpoint file for a provider.
// This file is used to resolve the next crawl start (lastSuccess + 1 day).
func (c *Config) ProviderProgressPath(provider string) string {
	return filepath.Join(c.ProviderSaveDir(provider), ".lastrun.success.json")
}

// ProviderFrameProgressPath returns a frame-scoped progress file so that
// multiple frames of the same provider (e.g. D1 and M1) do not race on a
// shared file. Falls back to ProviderProgressPath when frame is empty.
func (c *Config) ProviderFrameProgressPath(provider, frame string) string {
	if frame == "" {
		return c.ProviderProgressPath(provider)
	}
	return filepath.Join(c.ProviderSaveDir(provider), strings.ToUpper(frame), ".lastrun.success.json")
}

// ProviderLastDayPath returns the provider-level last trading day snapshot.
// This is market metadata (not per-ticker success progress).
func (c *Config) ProviderLastDayPath(provider string) string {
	return filepath.Join(c.ProviderSaveDir(provider), ".lastday.json")
}

// InitLogger installs the bootstrap logger (Info level, text format) before
// config is loaded. Call ApplyLogger after loading config to apply the
// configured level and format.
func InitLogger() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	})))
}

// ApplyLogger applies the configured log level, format, and optional file output.
// Returns a cleanup function the caller must defer to flush and close the log file.
//
// If log.file is set, logs are written to both stderr and the file (io.MultiWriter).
// If the file cannot be opened, a warning is logged and stderr-only is used.
func (c *Config) ApplyLogger() (cleanup func()) {
	logLevel.Set(parseLevel(c.Log.Level))
	cleanup = func() {}

	w := io.Writer(os.Stderr)
	if path := strings.TrimSpace(c.Log.File); path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			slog.Warn("log file dir create failed", "path", path, "err", err)
		} else if f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err != nil {
			slog.Warn("log file open failed", "path", path, "err", err)
		} else {
			w = io.MultiWriter(os.Stderr, f)
			cleanup = func() { _ = f.Close() }
			slog.Info("log file opened", "path", path)
		}
	}

	opts := &slog.HandlerOptions{Level: logLevel}
	var handler slog.Handler
	if strings.ToLower(strings.TrimSpace(c.Log.Format)) == "json" {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}
	slog.SetDefault(slog.New(handler))
	return cleanup
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// EnabledAssets returns only the asset configs that are enabled.
func (c *Config) EnabledAssets() []AssetConfig {
	var out []AssetConfig
	for _, a := range c.Assets {
		if a.Enabled {
			out = append(out, a)
		}
	}
	return out
}

// ExpandedAssets flattens multi-frame asset config into one runtime asset per frame.
func (c *Config) ExpandedAssets() []resolvedAsset {
	var out []resolvedAsset
	for _, asset := range c.EnabledAssets() {
		out = append(out, asset.expand()...)
	}
	return out
}
