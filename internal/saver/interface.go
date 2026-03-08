package saver

import (
	"strings"
	"hist-data/internal/model"
)

// PacketSaver là abstraction cho lưu từng packet (chunk) bars.
// High-level (main) inject implementation; low-level (crawler) chỉ phụ thuộc interface — DIP.
type PacketSaver interface {
	Save(bars []model.Bar, path string) error
	Extension() string
}

// NewPacketSaver creates implementation by format (csv, parquet, json).
// Returns nil if format not supported.
func NewPacketSaver(format string) PacketSaver {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "csv":
		return CSVSaver{}
	case "parquet":
		return ParquetSaver{}
	case "json":
		return JSONSaver{}
	default:
		return nil
	}
}
