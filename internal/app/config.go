package app

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// logLevel is a package-level LevelVar so the log level can be changed at
// runtime (after config is loaded) without recreating the handler.
var logLevel = new(slog.LevelVar) // defaults to Info

// AssetConfig describes one asset class to crawl.
type AssetConfig struct {
	Class    string   `mapstructure:"class"`
	Provider string   `mapstructure:"provider"` // massive | binance | histdata
	Enabled  bool     `mapstructure:"enabled"`
	Groups   []string `mapstructure:"groups"`   // sp500 | nasdaq100 | dji | all (Polygon only)
	Tickers  []string `mapstructure:"tickers"`  // explicit individual symbols
	Validate bool     `mapstructure:"validate"` // verify tickers against reference API (Polygon only)
}

// Config is the application configuration loaded from config.yaml with env overrides.
type Config struct {
	Provider string `mapstructure:"provider"` // default provider (massive)

	API struct {
		Keys []string `mapstructure:"keys"`
	} `mapstructure:"api"`

	Data struct {
		Dir           string `mapstructure:"dir"`
		Format        string `mapstructure:"format"`
		Timespan      string `mapstructure:"timespan"`      // minute | hour | day | week | month
		Multiplier    int    `mapstructure:"multiplier"`    // e.g. 1, 5, 15
		BackfillYears int    `mapstructure:"backfillYears"` // years of history on first run
	} `mapstructure:"data"`

	// Binance-specific config (public API, no key required)
	Binance struct {
		BaseURL  string `mapstructure:"baseUrl"`  // default: https://api.binance.com
		Interval string `mapstructure:"interval"` // e.g. "1m", "5m", "1h", "1d"
		Workers  int    `mapstructure:"workers"`  // number of parallel download goroutines
	} `mapstructure:"binance"`

	// HistData-specific config (CSV download, no key required)
	HistData struct {
		BaseURL string `mapstructure:"baseUrl"` // default: https://www.histdata.com
		Workers int    `mapstructure:"workers"` // number of parallel download goroutines
	} `mapstructure:"histdata"`

	// VCI (Vietcap) — Vietnamese stocks, no API key
	VCI struct {
		BaseURL   string `mapstructure:"baseUrl"`   // default: https://trading.vietcap.com.vn/api
		TimeFrame string `mapstructure:"timeFrame"` // ONE_DAY | ONE_MINUTE | ONE_HOUR
		Workers   int    `mapstructure:"workers"`  // parallel goroutines
	} `mapstructure:"vci"`

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
	v.SetDefault("data.dir", "data")
	v.SetDefault("data.format", "parquet")
	v.SetDefault("data.timespan", "minute")
	v.SetDefault("data.multiplier", 1)
	v.SetDefault("data.backfillYears", 2)
	v.SetDefault("schedule.runHour", 0)
	v.SetDefault("schedule.runMinute", 30)
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "text")
	v.SetDefault("binance.baseUrl", "https://api.binance.com")
	v.SetDefault("binance.interval", "5m")
	v.SetDefault("binance.workers", 3)
	v.SetDefault("histdata.baseUrl", "https://www.histdata.com")
	v.SetDefault("histdata.workers", 2)
	v.SetDefault("vci.baseUrl", "https://trading.vietcap.com.vn/api")
	v.SetDefault("vci.timeFrame", "ONE_DAY")
	v.SetDefault("vci.workers", 2)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config file %q: %w", cfgFile, err)
	}
	slog.Info("loaded config", "file", v.ConfigFileUsed())

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	// Env overlay: API keys (secrets never live in YAML)
	if keys := parseAPIKeysFromEnv(); len(keys) > 0 {
		cfg.API.Keys = keys
	}

	// Env overrides for convenience
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.Log.Level = v
	}
	if v := os.Getenv("DATA_DIR"); v != "" {
		cfg.Data.Dir = v
	}
	if v := os.Getenv("SAVE_FORMAT"); v != "" {
		cfg.Data.Format = v
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

var validTimespans = map[string]bool{
	"minute": true, "hour": true, "day": true, "week": true, "month": true,
}

func validateConfig(cfg *Config) error {
	// Polygon API key required only when at least one asset uses massive provider
	needsMassive := false
	for _, a := range cfg.Assets {
		if a.Enabled && (a.Provider == "" || a.Provider == "massive") {
			needsMassive = true
			break
		}
	}
	if needsMassive && len(cfg.API.Keys) == 0 {
		return fmt.Errorf("no API keys found: set POLYGON_API_KEYS env or api.keys in config.yaml")
	}
	format := strings.ToLower(cfg.Data.Format)
	if format != "parquet" && format != "csv" && format != "json" {
		return fmt.Errorf("unsupported data.format %q (allowed: parquet, csv, json)", cfg.Data.Format)
	}
	if !validTimespans[strings.ToLower(cfg.Data.Timespan)] {
		return fmt.Errorf("unsupported data.timespan %q (allowed: minute, hour, day, week, month)", cfg.Data.Timespan)
	}
	if cfg.Data.Multiplier <= 0 {
		return fmt.Errorf("data.multiplier must be >= 1, got %d", cfg.Data.Multiplier)
	}
	if cfg.Data.BackfillYears <= 0 {
		return fmt.Errorf("data.backfillYears must be >= 1, got %d", cfg.Data.BackfillYears)
	}
	enabled := 0
	for _, a := range cfg.Assets {
		if a.Enabled {
			enabled++
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

// SaveBaseDir returns the root output directory for a given provider.
// e.g. data/Binance, data/HistData, data/Polygon
func (c *Config) SaveBaseDir() string {
	return filepath.Join(c.Data.Dir, "Polygon")
}

// ProviderSaveDir returns the provider-specific output directory.
func (c *Config) ProviderSaveDir(provider string) string {
	switch provider {
	case "binance":
		return filepath.Join(c.Data.Dir, "Binance")
	case "histdata":
		return filepath.Join(c.Data.Dir, "HistData")
	case "vci":
		return filepath.Join(c.Data.Dir, "VCI")
	default: // massive / polygon
		return filepath.Join(c.Data.Dir, "Polygon")
	}
}

// ProgressPath returns the path to the per-ticker progress file.
func (c *Config) ProgressPath() string {
	return filepath.Join(c.SaveBaseDir(), ".lastday.json")
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
