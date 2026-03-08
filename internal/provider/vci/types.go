package vci

import "encoding/json"

// TimeFrame is the VCI gap-chart timeframe.
// API only supports: ONE_MINUTE, ONE_HOUR, ONE_DAY.
const (
	TimeFrameMinute = "ONE_MINUTE"
	TimeFrameHour   = "ONE_HOUR"
	TimeFrameDay    = "ONE_DAY"
)

// GapChartRequest is the body for POST /api/chart/OHLCChart/gap-chart.
type GapChartRequest struct {
	TimeFrame string   `json:"timeFrame"` // ONE_MINUTE | ONE_HOUR | ONE_DAY
	Symbols   []string `json:"symbols"`
	To        int64    `json:"to"`        // Unix timestamp (seconds) — end of range
	CountBack int      `json:"countBack"` // number of bars backward from To
}

// GapChartSeries is one symbol's OHLCV arrays in the response.
// VCI may return t and v as numbers or strings — FlexNum parses both.
type GapChartSeries struct {
	T []FlexNum `json:"t"` // timestamp (seconds)
	O []float64 `json:"o"`
	H []float64 `json:"h"`
	L []float64 `json:"l"`
	C []float64 `json:"c"`
	V []FlexNum `json:"v"` // volume
}

// FlexNum unmarshals JSON number or string (e.g. "1735689600") to int64.
type FlexNum int64

// UnmarshalJSON implements json.Unmarshaler.
func (f *FlexNum) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		var n int64
		for _, c := range s {
			if c >= '0' && c <= '9' {
				n = n*10 + int64(c-'0')
			}
		}
		*f = FlexNum(n)
		return nil
	}
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	*f = FlexNum(n)
	return nil
}

// Int64 returns the value as int64.
func (f FlexNum) Int64() int64 { return int64(f) }
