package app

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-viper/mapstructure/v2"
)

func TestStringOrSliceHookSupportsLegacyScalarAndSlice(t *testing.T) {
	type sample struct {
		Interval []string `mapstructure:"interval"`
	}

	tests := []struct {
		name string
		in   map[string]any
		want []string
	}{
		{
			name: "scalar",
			in:   map[string]any{"interval": "1m"},
			want: []string{"1m"},
		},
		{
			name: "slice",
			in:   map[string]any{"interval": []any{"1m", "1d"}},
			want: []string{"1m", "1d"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sample
			decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
				DecodeHook: stringOrSliceHook(),
				Result:     &got,
			})
			if err != nil {
				t.Fatalf("new decoder: %v", err)
			}
			if err := decoder.Decode(tt.in); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(got.Interval, tt.want) {
				t.Fatalf("interval = %v, want %v", got.Interval, tt.want)
			}
		})
	}
}

func TestLoadConfigReadsPolygonKeysFromEnv(t *testing.T) {
	tmp := t.TempDir()
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir temp: %v", err)
	}
	defer func() {
		if err := os.Chdir(oldwd); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	}()

	restoreSingle := unsetEnv("POLYGON_API_KEY")
	defer restoreSingle()
	restoreMulti := setEnv("POLYGON_API_KEYS", "key1,key2")
	defer restoreMulti()

	config := []byte(`
data:
  dir: data
  format: parquet
assets:
  - class: stocks
    provider: massive
    enabled: true
    frames:
      - name: M1
        backfillYears: 1
`)
	if err := os.WriteFile("config.yaml", config, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !reflect.DeepEqual(cfg.Massive.Keys, []string{"key1", "key2"}) {
		t.Fatalf("keys = %v, want [key1 key2]", cfg.Massive.Keys)
	}
}

func setEnv(key, value string) func() {
	old, ok := os.LookupEnv(key)
	_ = os.Setenv(key, value)
	return func() {
		if ok {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	}
}

func TestResolveDataDirAnchorsRelativeToConfigDir(t *testing.T) {
	tmp := t.TempDir()
	cfgFile := filepath.Join(tmp, "config.yaml")

	got := resolveDataDir("data", cfgFile)
	want := filepath.Join(tmp, "data")
	if got != want {
		t.Fatalf("relative dir: got %q, want %q", got, want)
	}

	got = resolveDataDir("", cfgFile)
	if got != want {
		t.Fatalf("empty dir: got %q, want %q", got, want)
	}

	abs := filepath.Join(tmp, "external", "store")
	if got := resolveDataDir(abs, cfgFile); got != abs {
		t.Fatalf("absolute dir: got %q, want %q", got, abs)
	}
}

func unsetEnv(key string) func() {
	old, ok := os.LookupEnv(key)
	_ = os.Unsetenv(key)
	return func() {
		if ok {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	}
}

func TestExpandedAssetsCreatesOnePipelinePerFrame(t *testing.T) {
	cfg := &Config{
		Assets: []AssetConfig{
			{
				Class:    "crypto",
				Provider: "binance",
				Enabled:  true,
				Frames: []FrameSpec{
					{Name: "M1", FromYear: 2022},
					{Name: "D1", FromYear: 2000},
				},
				Tickers: []string{"BTCUSDT"},
			},
		},
	}

	assets := cfg.ExpandedAssets()
	if len(assets) != 2 {
		t.Fatalf("expanded assets = %d, want 2", len(assets))
	}

	gotKeys := []string{AssetKey(assets[0]), AssetKey(assets[1])}
	wantKeys := []string{"binance:crypto:M1", "binance:crypto:D1"}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("keys = %v, want %v", gotKeys, wantKeys)
	}
	if assets[1].Frame.FromYear != 2000 {
		t.Fatalf("D1 fromYear = %d, want 2000", assets[1].Frame.FromYear)
	}
}

func TestExpandedAssetsFallsBackToLegacyFrameFields(t *testing.T) {
	cfg := &Config{
		Assets: []AssetConfig{
			{
				Class:      "vn",
				Provider:   "vci",
				Enabled:    true,
				TimeFrames: []string{"ONE_MINUTE", "ONE_DAY"},
				Tickers:    []string{"FPT"},
			},
		},
	}

	assets := cfg.ExpandedAssets()
	gotKeys := []string{AssetKey(assets[0]), AssetKey(assets[1])}
	wantKeys := []string{"vci:vn:M1", "vci:vn:D1"}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("keys = %v, want %v", gotKeys, wantKeys)
	}
}

func TestProviderFrameSpecRejectsUnsupportedProviderFramePair(t *testing.T) {
	if _, err := providerFrameSpec("vci", "M5"); err == nil {
		t.Fatal("expected unsupported frame error")
	}
}

func TestValidateConfigRejectsNegativeFromYear(t *testing.T) {
	cfg := &Config{}
	cfg.Binance.Workers = 1
	cfg.Assets = []AssetConfig{
		{
			Class:    "crypto",
			Provider: "binance",
			Enabled:  true,
			Frames: []FrameSpec{
				{Name: "M1", FromYear: -1},
			},
			Tickers: []string{"BTCUSDT"},
		},
	}

	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected fromYear validation error for negative value")
	}
}

func TestValidateConfigAcceptsZeroBackfillAsFullHistory(t *testing.T) {
	cfg := &Config{}
	cfg.Data.Format = "parquet"
	cfg.Binance.Workers = 1
	cfg.Assets = []AssetConfig{
		{
			Class:    "crypto",
			Provider: "binance",
			Enabled:  true,
			Frames: []FrameSpec{
				{Name: "M1"},
			},
			Tickers: []string{"BTCUSDT"},
		},
	}

	if err := validateConfig(cfg); err != nil {
		t.Fatalf("expected zero backfillYears to be accepted as full history, got %v", err)
	}
}

func TestResolveTargetsByProviderUsesFrameFirstSaveDir(t *testing.T) {
	cfg := &Config{}
	cfg.Data.Dir = "data"
	cfg.Assets = []AssetConfig{
		{
			Class:    "crypto",
			Provider: "binance",
			Enabled:  true,
			Frames: []FrameSpec{
				{Name: "M1", FromYear: 2021, SinkFrames: []string{"M1", "M5"}},
			},
			Tickers: []string{"BTCUSDT"},
		},
	}

	targets, err := ResolveTargetsByProvider(cfg)
	if err != nil {
		t.Fatalf("ResolveTargetsByProvider: %v", err)
	}

	jobs := targets["binance:crypto:M1"]
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}

	want := filepath.Join("data", "Binance", "M1")
	if jobs[0].SaveDir != want {
		t.Fatalf("SaveDir = %q, want %q", jobs[0].SaveDir, want)
	}
}

func TestExpandedAssetsSinkFramesPerFrame(t *testing.T) {
	cfg := &Config{
		Assets: []AssetConfig{
			{
				Class:    "vn",
				Provider: "vci",
				Enabled:  true,
				Frames: []FrameSpec{
					{Name: "M1", FromYear: 2022, SinkFrames: []string{"M1", "M5", "H1"}},
					{Name: "D1", FromYear: 2000},
				},
			},
		},
	}

	assets := cfg.ExpandedAssets()
	if len(assets) != 2 {
		t.Fatalf("len = %d, want 2", len(assets))
	}
	// M1 uses its own declared sinkFrames
	if !reflect.DeepEqual(assets[0].SinkFrames, []string{"M1", "M5", "H1"}) {
		t.Fatalf("M1 sinkFrames = %v", assets[0].SinkFrames)
	}
	// D1 has no sinkFrames declared → defaults to [D1]
	if !reflect.DeepEqual(assets[1].SinkFrames, []string{"D1"}) {
		t.Fatalf("D1 sinkFrames = %v, want [D1]", assets[1].SinkFrames)
	}
}
