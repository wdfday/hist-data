package crawl

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

type failedEntry struct {
	Ticker    string `json:"ticker"`
	DateRange string `json:"date_range"`
	Reason    string `json:"reason"`
}

func writeRunReport(saveBaseDir string, successList []string, failedList []failedEntry) error {
	if err := os.MkdirAll(saveBaseDir, 0755); err != nil {
		return err
	}
	if len(successList) > 0 {
		p := filepath.Join(saveBaseDir, ".lastrun.success.json")
		data, err := json.MarshalIndent(successList, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(p, data, 0644); err != nil {
			return err
		}
		slog.Info("report wrote success", "path", p, "tickers", len(successList))
	}
	if len(failedList) > 0 {
		p := filepath.Join(saveBaseDir, ".lastrun.failed.json")
		data, err := json.MarshalIndent(failedList, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(p, data, 0644); err != nil {
			return err
		}
		slog.Info("report wrote failed", "path", p, "count", len(failedList))
	}
	return nil
}

func joinFailedReasons(failedList []failedEntry) string {
	if len(failedList) == 0 {
		return ""
	}
	var b strings.Builder
	for i, f := range failedList {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(f.Ticker)
		b.WriteString(": ")
		b.WriteString(f.Reason)
		if i >= 4 && len(failedList) > 6 {
			b.WriteString(fmt.Sprintf(" (+%d more)", len(failedList)-5))
			break
		}
	}
	return b.String()
}
