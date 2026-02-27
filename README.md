# us-data

Go crawler that fetches minute OHLCV bars for financial instruments from the
[Massive/Polygon API](https://massive.com/docs) and persists them locally as
Parquet, CSV, or JSON.

Supports multi-asset classes (stocks, crypto, forex, indices), full 2-year
historical backfill on first run, and incremental daily gap-fill thereafter.

## Quick start

```bash
cp .env.example .env
# Set POLYGON_API_KEYS in .env
go run ./cmd/us-data/
```

## Docker

```bash
cp .env.example .env
# Set POLYGON_API_KEYS in .env
docker compose up --build -d
docker compose logs -f crawler
```

Data lands in `./data` (configurable via `DATA_DIR_HOST` in `.env`).
Progress and reports persist across restarts via Docker volume.

## Configuration

All settings live in `config.yaml`. Secrets and overrides are supplied via
environment variables — never put API keys in the config file.

| Env variable       | Description                                          |
|--------------------|------------------------------------------------------|
| `POLYGON_API_KEYS` | Comma-separated API keys. One worker per key.        |
| `POLYGON_API_KEY`  | Alternative single-key form.                         |
| `LOG_LEVEL`        | `debug` / `info` / `warn` / `error` (overrides YAML)|
| `DATA_DIR`         | Root data directory (overrides YAML)                 |
| `SAVE_FORMAT`      | `parquet` / `csv` / `json` (overrides YAML)          |
| `DATA_DIR_HOST`    | Host path for Docker volume mount (default `./data`) |

Key `config.yaml` sections:

```yaml
schedule:
  runHour: 4       # UTC — 4:00 AM UTC = 11:00 AM Vietnam (UTC+7)
  runMinute: 0     # 3h+ buffer after US extended session close (8 PM ET)

log:
  level: info      # debug | info | warn | error
  format: text     # text (dev) | json (production / Docker log drivers)
  file: ""         # optional path, e.g. logs/app.log
                   # if set: logs go to both stderr AND the file

assets:
  - class: stocks
    enabled: true
    groups:
      - sp500      # free: GitHub CSV
      - nasdaq100  # free: Wikipedia
    tickers: []    # additional explicit symbols
    validate: false
```

## Asset groups

| Group        | Source                  | Plan required |
|--------------|-------------------------|---------------|
| `sp500`      | GitHub CSV              | free          |
| `nasdaq100`  | Wikipedia wikitext API  | free          |
| `dji`        | Wikipedia wikitext API  | free          |
| `russell2000`| Polygon ETF API         | Starter+      |
| `all`        | Polygon reference API   | Starter+      |

## Output layout

```
data/Polygon/
├── stocks/
│   └── AAPL/
│       └── AAPL_2024-02-26_to_2026-02-25.parquet
├── crypto/
│   └── X:BTCUSD/
│       └── X:BTCUSD_2024-02-26_to_2026-02-25.parquet
├── .lastday.json          # progress: source:class:TICKER → last fetched date
├── .lastrun.success.json  # tickers fetched successfully in last cycle
└── .lastrun.failed.json   # tickers that failed with reason
```

## Architecture

```
cmd/us-data/
  main.go     entry point, defers: DP.Close(), log file flush
  app.go      App struct, InitializeApp()

internal/
  app/
    config.go     Config struct, LoadConfig (Viper), InitLogger, ApplyLogger
    di.go         ProvideConfig, ProvidePacketSaver, ProvidePolygonProvider
    bootstrap.go  ResolveTargets: ticker resolution per asset class
    app.go        Run: scheduler loop + OS signal handling + graceful shutdown

  crawl/
    types.go      Job, JobResult, LogEntry, AssetClass, Done
    interfaces.go BarFetcher interface (DIP boundary)
    job.go        BuildTargets, resolveJobRange
    progress.go   .lastday.json read/write, BootstrapProgress
    producer.go   ProgressProducer: reads progress once, streams resolved Jobs
    runner.go     Runner: worker pool, log channel, result channel, heartbeat
    report.go     .lastrun.*.json

  provider/
    polygon_provider.go   PolygonProvider (implements BarFetcher)
    polygon/
      crawler.go          CrawlMinuteBarsWithKey, SaveBars
      transport.go        HTTP client config
      types.go            BarRaw, AggregatesResponse, FlexibleInt64
      indices.go          ResolveAssetTickers, ETF API fallback
      indices_free.go     GitHub CSV (S&P 500), Wikipedia (NASDAQ-100, DJI)

  model/  bar.go   Bar struct (OHLCV + VWAP + Transactions)
  saver/  *.go     PacketSaver: Parquet, CSV, JSON
```

### Concurrency model

```
ProgressProducer goroutine
  reads .lastday.json once → resolves from/to per target → chan<- Job

Worker goroutines (one per API key)
  receive Job → FetchBars (chunked, rate-limited) → SaveBars
  chan<- JobResult      → result collector goroutine
  chan<- LogEntry       → log writer goroutine → slog (sequential output)
  chan<- ProgressUpdate → RunProgressWriter goroutine → .lastday.json

Heartbeat goroutine
  fires every 15 min; skips tick if done count unchanged
```

### Graceful shutdown

```
SIGINT / SIGTERM
  │
  ├─ during wait between cycles → exit immediately
  │
  └─ during active cycle
       cancel(ctx)
         workers stop accepting new jobs
         in-flight FetchBars() completes naturally (not interrupted)
       wg.Wait() → close(results/logs) → collectors drain → done <- Done{}
       close(progressUpdates) → RunProgressWriter drains and exits
       signal.Stop(signals)
       defer a.Config.ApplyLogger()() → close log file
       defer a.DP.Close()             → close HTTP connections
```

### DIP

`crawl.Runner` depends only on `crawl.BarFetcher` — it imports no provider
package. `PolygonProvider` lives in `internal/provider/` and satisfies the
interface defined by the consumer (`internal/crawl/interfaces.go`).

## Debug

```bash
go run -race ./cmd/us-data/          # race detector
LOG_LEVEL=debug go run ./cmd/us-data/ # verbose logs
GODEBUG=gctrace=1 go run ./cmd/us-data/
```

See [docs/DEBUG.md](docs/DEBUG.md) for Docker debug commands and GODEBUG reference.

## License

MIT
