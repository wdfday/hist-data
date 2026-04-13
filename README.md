# hist-data

Multi-provider historical market data crawler. Fetches OHLCV bars, saves as Parquet/CSV/JSON for backtesting.

## Providers

| Provider | Asset class | Auth | Min granularity | Session filter |
| --- | --- | --- | --- | --- |
| **Massive / Polygon** | US Stocks, Indices | API key | M1 | Regular hours only (9:30–16:00 ET) |
| **Binance** | Crypto | None | M1 | 24/7 |
| **TwelveData** | Stocks, Forex, Crypto | API key | M1 | As returned by API |
| **VCI (Vietcap)** | Vietnamese stocks | None | M1 | As returned by API |

> No tick data — minimum granularity is 1-minute across all providers.

---

## Quick start

```bash
# Set secrets via env — never in config files
export POLYGON_API_KEYS=key1,key2   # Polygon/Massive
export TWELVEDATA_API_KEY=your_key  # TwelveData
# Binance and VCI need no credentials

go run ./cmd/hist-data/
```

### Docker

```bash
docker compose up --build -d
docker compose logs -f
```

---

## Configuration

All settings in `config.yaml`. The root data directory supports relative and absolute paths:

```yaml
data:
  dir: ../data      # relative to working dir, or /mnt/storage/hist etc.
  format: parquet   # parquet | csv | json
```

### Environment variables

| Env var | Description |
| --- | --- |
| `POLYGON_API_KEYS` | Comma-separated Polygon keys. One worker per key. |
| `POLYGON_API_KEY` | Single-key alternative. |
| `TWELVEDATA_API_KEY` | TwelveData API key. |
| `DATA_DIR` | Override `data.dir`. |
| `SAVE_FORMAT` | Override `data.format`. |
| `LOG_LEVEL` | `debug` / `info` / `warn` / `error` |
| `CONFIG_FILE` | Override config file path (default: `config.yaml`) |

### Asset pipelines

Each entry in `assets` is one independent crawl pipeline. Each `frames` entry expands into its own runtime pipeline.

```yaml
assets:
  - class: stocks
    provider: massive
    backfillYears: 2
    frames:
      - name: M1
        sinkFrames: [M1, M5, M15, M30, H1, H4]   # derived from M1 in one pass
    enabled: true
    groups: [sp500, nasdaq100]

  - class: crypto
    provider: binance
    backfillYears: 4
    frames:
      - name: M1
        sinkFrames: [M1, M5, M15, M30, H1, H4]
      - name: D1
        backfillYears: 7                           # override per frame
    enabled: true
    tickers: [BTCUSDT, ETHUSDT, SOLUSDT]

  - class: vn
    provider: vci
    backfillYears: 3
    frames:
      - name: M1
        sinkFrames: [M1, M5, M15, M30, H1, H4]
      - name: D1
        backfillYears: 25
    enabled: true
    groups: [VN30]
```

**`sinkFrames`** — set on a frame to derive additional timeframes from M1 source in one pass. Only valid on M1 source. Supported sinks: `M1 M5 M15 M30 H1 H4`.

**`backfillYears`** — required at the asset level; each frame can override it.

### Supported frames per provider

| Provider | Frames |
| --- | --- |
| `massive` | M1 M5 M15 M30 H1 H4 D1 W1 MO1 |
| `binance` | M1 M5 M15 M30 H1 H4 D1 W1 MO1 |
| `twelvedata` | M1 M5 M15 M30 H1 H4 D1 W1 MO1 |
| `vci` | M1 H1 D1 |

### Polygon groups

| Group | Source | Plan |
| --- | --- | --- |
| `sp500` | GitHub CSV | Free |
| `nasdaq100` | Wikipedia | Free |
| `dji` | Wikipedia | Free |
| `russell2000` | Polygon ETF API | Starter+ |
| `all` | Polygon reference API | Starter+ |

### VCI groups

| Group | Description |
| --- | --- |
| `VN30` | VN30 index constituents |
| `HOSE` | All HOSE-listed stocks |
| `HNX` | All HNX-listed stocks |

### Schedules

Each provider has its own `schedule` block (UTC). The global `schedule` block is the fallback.

```yaml
massive:
  schedule:
    runHour: 4      # 04:00 UTC = 11:00 VN; US markets close ~21:00 UTC
    runMinute: 0

binance:
  schedule:
    runHour: 0      # midnight UTC; crypto trades 24/7
    runMinute: 30
```

---

## Output layout

```
data/
├── Polygon/
│   ├── M1/AAPL/AAPL_M1_2024-01-01_to_2026-01-01.parquet
│   ├── M5/AAPL/AAPL_M5_2024-01-01_to_2026-01-01.parquet
│   └── ...
├── Binance/
│   ├── M1/BTCUSDT/BTCUSDT_M1_2024-01-01_to_2026-01-01.parquet
│   └── D1/BTCUSDT/BTCUSDT_D1_2020-01-01_to_2026-01-01.parquet
├── TwelveData/
│   └── M1/EUR_USD/EUR_USD_M1_2016-01-01_to_2026-01-01.parquet
├── VCI/
│   ├── M1/FPT/FPT_M1_2023-01-01_to_2026-01-01.parquet
│   └── D1/FPT/FPT_D1_2001-01-01_to_2026-01-01.parquet
├── .lastday.json           # progress: last fetched date per ticker
├── .lastrun.success.json
└── .lastrun.failed.json
```

Progress is tracked per `provider:class:ticker` key — incremental runs only fetch missing data.

---

## Architecture

```
crawl/interfaces.go   BarFetcher interface (DIP boundary)
provider/*/crawler.go implements BarFetcher — fetch + filter + save
saver/frame_sink.go   SaveFrameSet: aggregate M1 → derived frames, persist
crawl/runner.go       Runner: worker pool, key rotation, progress tracking
app/                  config loading, DI wiring, per-provider scheduler loops
```

### Concurrency model

```
ProgressProducer  reads .lastday.json once → resolves from/to per ticker → chan Job

Workers
  Polygon:     1 goroutine per API key
  Binance:     binance.workers goroutines (no key needed)
  TwelveData:  twelvedata.workers goroutines
  VCI:         vci.workers goroutines

Each worker:  Job → FetchBars → SaveBars (filter + aggregate + persist) → progress update

Log writer    drains log channel → slog (serialised output)
Result collector  aggregates JobResult → run report
Progress writer   drains ProgressUpdate → .lastday.json
```

### Worker sizing

```
Bar struct = 64 bytes (8 fields × 8 bytes)
M1 bars, 2 years, 1 ticker ≈ 700k bars × 64 B ≈ 45 MB
                              + chunking buffers ≈ ~60 MB peak per worker

Rule of thumb:
  max_workers = available_RAM / 60 MB
  Binance: also capped by 1200 req/min weight budget → workers ≤ 10
```

---

## Testing

```bash
go test ./...
go test -race ./...
go test -bench=. ./internal/provider/polygon/...
go build ./... && go vet ./...
```

---

## Debug

```bash
LOG_LEVEL=debug go run ./cmd/hist-data/
```
