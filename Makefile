.PHONY: build test lint docker-up docker-down clean coverage coverage-report ci

build:
	go build -o bin/zeroraft ./cmd/zeroraft

test:
	go test -race ./...

lint:
	golangci-lint run --timeout=3m

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down

clean:
	rm -rf bin/

# Run tests with coverage (excluding generated files)
coverage:
	go test -race -coverprofile=coverage.out ./internal/...
	@echo "=== Coverage by package ==="
	@go tool cover -func=coverage.out | grep -E "internal/(raft|client|transport)" | grep -v ".pb.go" | grep -v "codec_selector\|json_codec\|protobuf_codec"
	@echo ""
	@echo "=== Total coverage ==="
	@go tool cover -func=coverage.out | grep total

# Generate HTML coverage report
coverage-report: coverage
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report saved to coverage.html"

# Full CI check
ci: lint test build
	@echo "✅ CI passed!"