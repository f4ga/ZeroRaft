.PHONY: build test lint docker-up docker-down

build:
	go build -o bin/zeroraft ./cmd/zeroraft

test:
	go test -race ./...

lint:
	golangci-lint run

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down