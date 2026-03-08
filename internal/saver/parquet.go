package saver

import (
	"github.com/parquet-go/parquet-go"

	"hist-data/internal/model"
)

// ParquetSaver lưu packet dưới dạng Parquet.
type ParquetSaver struct{}

func (ParquetSaver) Extension() string { return "parquet" }

func (ParquetSaver) Save(bars []model.Bar, path string) error {
	return parquet.WriteFile(path, bars)
}
