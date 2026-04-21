# Chạy `just` không đối số → in danh sách recipes
default:
    @just --list

# Build Go binary locally
build:
    go build -o main ./cmd/us-data/

# Run locally
run:
    go run ./cmd/us-data/

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
