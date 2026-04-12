package app

import (
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

func TestExpandedAssetsCreatesOnePipelinePerFrame(t *testing.T) {
	cfg := &Config{
		Assets: []AssetConfig{
			{
				Class:    "crypto",
				Provider: "binance",
				Enabled:  true,
				Frames: []FrameSpec{
					{Name: "M1", BackfillYears: 3},
					{Name: "D1", BackfillYears: 25},
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
	if assets[1].Frame.BackfillYears != 25 {
		t.Fatalf("D1 backfill = %d, want 25", assets[1].Frame.BackfillYears)
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

func TestValidateConfigRequiresDataLevelBackfill(t *testing.T) {
	cfg := &Config{}
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

	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected backfillYears validation error")
	}
}

func TestResolveTargetsByProviderUsesFrameFirstSaveDir(t *testing.T) {
	cfg := &Config{}
	cfg.Data.Dir = "data"
	cfg.Assets = []AssetConfig{
		{
			Class:         "crypto",
			Provider:      "binance",
			Enabled:       true,
			BackfillYears: 4,
			Frames: []FrameSpec{
				{Name: "M1", BackfillYears: 4, SinkFrames: []string{"M1", "M5"}},
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
				Class:         "vn",
				Provider:      "vci",
				Enabled:       true,
				BackfillYears: 3,
				Frames: []FrameSpec{
					{Name: "M1", BackfillYears: 3, SinkFrames: []string{"M1", "M5", "H1"}},
					{Name: "D1", BackfillYears: 25},
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
