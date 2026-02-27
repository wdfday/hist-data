package model

// Bar represents one OHLCV bar (minute/daily etc.).
// Shared across provider, saver, and serialization layers (json, parquet).
type Bar struct {
	Timestamp    int64   `json:"t" parquet:"t"` // Unix timestamp in milliseconds
	Open         float64 `json:"o" parquet:"o"`
	High         float64 `json:"h" parquet:"h"`
	Low          float64 `json:"l" parquet:"l"`
	Close        float64 `json:"c" parquet:"c"`
	Volume       int64   `json:"v" parquet:"v"`
	VWAP         float64 `json:"vw,omitempty" parquet:"vw,optional"` // Volume weighted average price
	Transactions int64   `json:"n,omitempty" parquet:"n,optional"`   // Number of transactions
}
