package polygon

import (
	"hist-data/internal/saver"
	"net/http"
	"time"
)

// baseTransportConfig returns the shared HTTP transport configuration used by Polygon clients.
func baseTransportConfig() *http.Transport {
	return &http.Transport{
		ResponseHeaderTimeout: 10 * time.Minute,
		IdleConnTimeout:       0,
		TLSHandshakeTimeout:   10 * time.Second,
		DisableKeepAlives:     true,
		MaxIdleConns:          0,
		MaxIdleConnsPerHost:   0,
	}
}

// newHTTPClient creates an HTTP client configured for Polygon requests.
func newHTTPClient() *http.Client {
	return &http.Client{
		Transport: baseTransportConfig(),
		Timeout:   10 * time.Minute,
	}
}

func NewCrawler(saveDir, timespan string, multiplier int, ps saver.PacketSaver) (*Crawler, error) {
	return &Crawler{
		client:        newHTTPClient(),
		SavePacketDir: saveDir,
		PacketSaver:   ps,
		Timespan:      timespan,
		Multiplier:    multiplier,
	}, nil
}
