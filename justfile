set dotenv-load := true

dc := "docker compose"

# List all recipes
default:
    @just --list

# ── Local dev ─────────────────────────────────────────────────────────────────

# Build binary
build:
    go build -o bin/hist-data ./cmd/hist-data/

# Run locally (uses go run)
run:
    go run ./cmd/hist-data/

# Build then exec binary directly (cleaner signal handling than go run)
start: build
    exec ./bin/hist-data

# Kill any hist-data processes (including go run orphans)
stop:
    -pkill -f '/bin/hist-data$|cmd/hist-data'
    -pkill -f 'go run.*cmd/hist-data'

# Run tests
test:
    go test ./...

# Format source files
fmt:
    gofmt -w $(rg --files cmd internal | rg '\.go$')

# Regenerate Wire DI
wire:
    go generate ./cmd/hist-data/

# ── Docker ────────────────────────────────────────────────────────────────────

# Build + start hist-data container
up:
    {{dc}} up -d --build hist-data

# Start hist-data without rebuild
start-docker:
    {{dc}} up -d hist-data

# Stop all containers
down:
    {{dc}} down

# Tail hist-data logs
logs:
    {{dc}} logs -f hist-data

# Restart hist-data
restart:
    {{dc}} restart hist-data

# ── Utilities ─────────────────────────────────────────────────────────────────

# Remove build artifacts
clean:
    rm -f bin/hist-data
    {{dc}} down -v
