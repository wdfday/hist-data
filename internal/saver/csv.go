package saver

import (
	"encoding/csv"
	"os"
	"strconv"

	"hist-data/internal/model"
)

// CSVSaver lưu packet dưới dạng CSV (header: t,o,h,l,c,v,vw,n).
type CSVSaver struct{}

func (CSVSaver) Extension() string { return "csv" }

func (CSVSaver) Save(bars []model.Bar, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"t", "o", "h", "l", "c", "v", "vw", "n"}); err != nil {
		return err
	}
	for _, b := range bars {
		if err := w.Write([]string{
			strconv.FormatInt(b.Timestamp, 10),
			floatStr(b.Open),
			floatStr(b.High),
			floatStr(b.Low),
			floatStr(b.Close),
			strconv.FormatInt(b.Volume, 10),
			floatStr(b.VWAP),
			strconv.FormatInt(b.Transactions, 10),
		}); err != nil {
			return err
		}
	}
	return nil
}

func floatStr(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
