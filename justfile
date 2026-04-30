# Tự nạp .env vào env của recipe (giúp `just run` thấy POLYGON_API_KEYS, v.v.)
set dotenv-load := true

# Chạy `just` không đối số → in danh sách recipes
default:
    @just --list

# Build Go binary locally
build:
    go build -o main ./cmd/hist-data/

# Run locally (uses `go run` — kill/Ctrl+C có thể để grandchild chạy tiếp tới hết cycle)
run:
    go run ./cmd/hist-data/

# Run locally — build rồi exec luôn binary, kill/Ctrl+C đi thẳng vào 1 PID duy nhất
start: build
    exec ./main

# Diệt mọi tiến trình hist-data đang còn (bao gồm grandchild orphan của `go run`)
stop:
    -pkill -f '/exe/hist-data$|^./main$|cmd/hist-data'
    -pkill -f 'go run.*cmd/hist-data'

# Docker: build images
docker-build:
    docker-compose build

# Docker: start containers
docker-up:
    docker-compose up -d

# Docker: build and start (recommended)
docker-up-build:
    docker-compose up -d --build

# Alias ngắn gọn cho docker-up-build
up: docker-up-build

# Docker: stop containers
docker-down:
    docker-compose down

# Docker: xem logs crawler
docker-logs:
    docker-compose logs -f crawler

# Docker: restart crawler
docker-restart:
    docker-compose restart crawler

# Build Docker image (tag us-data-crawler:latest)
docker-image:
    docker build -t us-data-crawler:latest .

# Chạy container bằng tay (--rm, mount data + indices, host network)
docker-run:
    docker run --rm \
        --env-file .env \
        -v $(pwd)/data:/app/data \
        -v $(pwd)/indices:/app/indices:ro \
        --network host \
        us-data-crawler:latest

# Clean build artifacts và Docker
clean:
    rm -f main
    docker-compose down -v
    docker rmi us-data-crawler:latest 2>/dev/null || true
