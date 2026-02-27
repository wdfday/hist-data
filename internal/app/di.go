package app

import (
	"fmt"

	"us-data/internal/provider"
	"us-data/internal/saver"
)

// ProvideConfig loads application config. Used by Wire.
func ProvideConfig() (*Config, error) {
	return LoadConfig()
}

// ProvidePacketSaver constructs the bar persistence backend from config. Used by Wire.
func ProvidePacketSaver(cfg *Config) (saver.PacketSaver, error) {
	ps := saver.NewPacketSaver(cfg.Data.Format)
	if ps == nil {
		return nil, fmt.Errorf("unsupported data.format %q (allowed: csv, parquet, json)", cfg.Data.Format)
	}
	return ps, nil
}

// ProvidePolygonProvider constructs a fully configured PolygonProvider. Used by Wire.
func ProvidePolygonProvider(cfg *Config, ps saver.PacketSaver) (*provider.PolygonProvider, error) {
	if len(cfg.API.Keys) == 0 {
		return nil, fmt.Errorf("no API keys configured")
	}
	return provider.NewPolygonProvider(cfg.SaveBaseDir(), ps, cfg.Data.Timespan, cfg.Data.Multiplier)
}
