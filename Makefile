.PHONY: up down destroy-data generate test test-integration test-integration-local lint build demo bench demo-legacy bench-legacy reconcile explain

ifeq ($(OS),Windows_NT)
GENERATE = powershell -NoProfile -ExecutionPolicy Bypass -File scripts/generate.ps1
DEMO = powershell -NoProfile -ExecutionPolicy Bypass -File scripts/demo-events.ps1
BENCH = powershell -NoProfile -ExecutionPolicy Bypass -File scripts/bench-events.ps1
else
GENERATE = sh scripts/generate.sh
DEMO = sh scripts/demo-events.sh
BENCH = sh scripts/bench-events.sh
endif

up:
	docker compose up --build -d --wait --wait-timeout 180

down:
	docker compose --profile test --profile tools --profile compat down --remove-orphans

# Explicitly destructive: removes the persistent development database volume.
destroy-data:
	docker compose --profile test --profile tools --profile compat down --volumes --remove-orphans

generate:
	$(GENERATE)

test:
	go test -p 1 -race ./internal/... ./cmd/... ./migrations

# Dedicated test databases and unique topics; no development data is deleted.
test-integration:
	docker compose --profile test run --build --rm integration

# Caller supplies all LEDGER_TEST_* URLs and LEDGER_REQUIRE_INTEGRATION=1.
test-integration-local:
	go test -p 1 -race -count=1 ./...

lint:
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

build:
	go build -trimpath -o bin/ ./cmd/...

demo:
	$(DEMO)

bench:
	$(BENCH)

demo-legacy:
	docker compose --profile tools --profile compat run --rm ledgerctl-legacy demo

bench-legacy:
	docker compose --profile tools --profile compat run --rm ledgerctl-legacy bench -n 1024

reconcile:
	docker compose --profile tools run --rm ledgerctl reconcile
	docker compose --profile tools run --rm ledgerctl reconcile-activity

explain:
	docker compose --profile tools run --rm ledgerctl explain
