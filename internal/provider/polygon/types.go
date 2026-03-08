package polygon

import (
	"encoding/json"
	"fmt"
	"strconv"
	"hist-data/internal/model"
)

// BarRaw is raw bar for JSON with FlexibleInt64 for Volume and Transactions
type BarRaw struct {
	Timestamp    int64         `json:"t"` // Unix timestamp in milliseconds
	Open         float64       `json:"o"`
	High         float64       `json:"h"`
	Low          float64       `json:"l"`
	Close        float64       `json:"c"`
	Volume       FlexibleInt64 `json:"v"`
	VWAP         float64       `json:"vw,omitempty"`
	Transactions FlexibleInt64 `json:"n,omitempty"`
}

// ToBar converts BarRaw to model.Bar
func (br BarRaw) ToBar() model.Bar {
	return model.Bar{
		Timestamp:    br.Timestamp,
		Open:         br.Open,
		High:         br.High,
		Low:          br.Low,
		Close:        br.Close,
		Volume:       br.Volume.Int64(), // Convert FlexibleInt64 -> int64
		VWAP:         br.VWAP,
		Transactions: br.Transactions.Int64(), // Convert FlexibleInt64 -> int64
	}
}

// AggregatesResponse is Polygon API response with next_url
type AggregatesResponse struct {
	Ticker       string   `json:"ticker"`
	QueryCount   int      `json:"queryCount"`
	ResultsCount int      `json:"resultsCount"`
	Adjusted     bool     `json:"adjusted"`
	Results      []BarRaw `json:"results"`
	Status       string   `json:"status"`
	RequestID    string   `json:"request_id"`
	Count        int      `json:"count"`
	NextURL      string   `json:"next_url,omitempty"`
}

// FlexibleInt64 parses int or float (scientific notation) to int64
type FlexibleInt64 int64

// UnmarshalJSON parses int or float
func (f *FlexibleInt64) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		val, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return err
		}
		*f = FlexibleInt64(int64(val))
		return nil
	}

	var floatVal float64
	if err := json.Unmarshal(data, &floatVal); err == nil {
		*f = FlexibleInt64(int64(floatVal))
		return nil
	}

	var intVal int64
	if err := json.Unmarshal(data, &intVal); err == nil {
		*f = FlexibleInt64(intVal)
		return nil
	}

	return fmt.Errorf("cannot parse as int64: %s", string(data))
}

// Int64 returns int64 value
func (f FlexibleInt64) Int64() int64 {
	return int64(f)
}
