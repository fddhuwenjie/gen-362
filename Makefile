SHELL := /bin/bash

.PHONY: build build-server build-cli docker-up docker-down test clean fmt vet

build: build-server build-cli

build-server:
	go build -o bin/temporal-lite-server ./cmd/server

build-cli:
	go build -o bin/temporal-lite ./cmd/cli

docker-up:
	docker-compose up -d --build

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f temporal-lite

test:
	go test -v ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

clean:
	rm -rf bin/

start-db:
	docker-compose up -d postgres

stop-db:
	docker-compose stop postgres

migrate: build-cli
	bin/temporal-lite migrate

replay: build-cli
	@echo "Usage: make replay WORKFLOW_ID=xxx RUN_ID=xxx"
	@test -n "$(WORKFLOW_ID)" -a -n "$(RUN_ID)" || (echo "WORKFLOW_ID and RUN_ID required"; exit 1)
	bin/temporal-lite replay --workflow-id $(WORKFLOW_ID) --run-id $(RUN_ID) --verbose

install:
	go install ./cmd/cli
	go install ./cmd/server
