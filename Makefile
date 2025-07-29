.PHONY: up down destroy-data test test-integration test-integration-local lint build demo bench reconcile explain

up:
	docker compose up --build -d

down:
	docker compose down --remove-orphans

# Explicitly destructive: removes the persistent development database volume.
destroy-data:
	docker compose down --volumes --remove-orphans

test:
	go test -race ./internal/... ./cmd/... ./migrations

# An isolated temporary DB; never runs fault injection against the development DB.
test-integration:
	docker compose --profile test run --build --rm integration

# Caller supplies all LEDGER_TEST_* URLs and LEDGER_REQUIRE_INTEGRATION=1.
test-integration-local:
	go test -race -count=1 ./tests

lint:
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

build:
	go build -trimpath -o bin/ ./cmd/api ./cmd/ledgerctl

demo:
	docker compose --profile tools run --rm ledgerctl demo

bench:
	docker compose --profile tools run --rm ledgerctl bench -n 1024

reconcile:
	docker compose --profile tools run --rm ledgerctl reconcile

explain:
	docker compose --profile tools run --rm ledgerctl explain
