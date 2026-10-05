SHELL := /bin/sh
ORDER_API ?= http://localhost:8080

.PHONY: help build test test-race cover vet fmt fmt-check lint up up-obs down logs ps seed smoke integration migrate clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Compile all binaries
	go build ./...

test: ## Run unit tests
	go test ./...

test-race: ## Run unit tests with the race detector
	go test -race ./...

cover: ## Run tests with a coverage report
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

vet: ## Run go vet
	go vet ./...

fmt: ## Format the codebase
	gofmt -w .

fmt-check: ## Fail if the codebase is not gofmt-clean
	@files="$$(gofmt -l .)"; if [ -n "$$files" ]; then echo "gofmt required for:"; echo "$$files"; exit 1; fi

lint: vet fmt-check ## Run vet and formatting checks

up: ## Start the local stack (broker, databases, services)
	docker compose up -d --build

up-obs: ## Start the stack plus Jaeger, Prometheus, and Grafana
	docker compose -f docker-compose.yml -f deploy/docker-compose.observability.yml up -d --build

down: ## Stop the local stack and remove volumes
	docker compose down -v

logs: ## Tail service logs
	docker compose logs -f --tail=100

ps: ## Show service status
	docker compose ps

seed: ## Seed development inventory data
	docker compose run --rm seed

smoke: ## Create an order and print its timeline
	@id=$$(curl -sS -X POST $(ORDER_API)/v1/orders \
		-H 'Content-Type: application/json' \
		-H "Idempotency-Key: smoke-$$(date +%s)" \
		-d '{"customer_id":"cust-smoke","currency":"USD","items":[{"sku":"SKU-WIDGET","quantity":2,"unit_price_cents":1500}]}' \
		| sed -n 's/.*"id":"\([^"]*\)".*/\1/p'); \
	echo "order id: $$id"; \
	sleep 3; \
	curl -sS $(ORDER_API)/v1/orders/$$id/timeline

integration: ## Run integration and end-to-end tests (requires docker compose stack)
	ORBIT_TEST_INTEGRATION=1 go test -tags=integration -count=1 ./test/...

migrate: ## Apply migrations for one service: make migrate SERVICE=order DB=...
	ORBIT_DATABASE_URL=$(DB) go run ./cmd/orbitctl migrate --service $(SERVICE)

clean: ## Remove build and coverage artifacts
	rm -f coverage.out coverage.html
