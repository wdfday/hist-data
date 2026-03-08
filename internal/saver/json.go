package saver

import (
	"encoding/json"
	"os"

	"hist-data/internal/model"
)

// JSONSaver lưu packet dưới dạng JSON (array, indent).
type JSONSaver struct{}

func (JSONSaver) Extension() string { return "json" }

func (JSONSaver) Save(bars []model.Bar, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(bars)
}
