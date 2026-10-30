# Extension baseline audit

Actual date: 2026-10-30 Europe/Moscow. Base branch: main. Base SHA: 4a4ebd17158341f4740c31377a6d15951bc8e46f.
Working branch: feature/events-grpc-kafka-redis. Initial working tree: clean.

The source is the existing LedgerCore repository, with checked money, READ COMMITTED ordered account locks, transactionally saved idempotency outcomes, append-only journal, deferred commit constraints, snapshot reconciliation and fourteen real PostgreSQL integration scenarios. HTTP compatibility is retained through the existing application Store; financial logic will not be duplicated.

Extension boundaries: Gateway calls LedgerCore and Activity over real gRPC; Redis limits authenticated write requests across gateways; LedgerCore commits a versioned outbox event with every new transfer/reversal; publisher sends outside SQL locks; Activity stores inbox and projection in a distinct database and commits offsets only after SQL commit. Activity is eventually consistent and is not a funds authority. New events start at migration, with no implicit historical backfill.

New commits use actual dates and configured identity. Existing historical commits remain untouched. No automatic push for this extension.

This records the initial audit, not a statement of current runtime readiness.
Final extension checks are recorded in [acceptance.md](acceptance.md).

## Executed baseline checks

- PASS: original `go test -race ./... -count=1` and `go vet ./...` before extension packages (integration tests skip without URLs in that first command).
- PASS: original `go test -race ./tests -count=1` with actual ledger_app/ledger_fixture/ledger_migrator logins and native PostgreSQL17.9 in the isolated ledgercore_acceptance_test DB; package24.314s, all14 financial scenarios passed.
- Docker Desktop clean restart succeeded during the initial audit. Existing unrelated containers on8080/5432 were preserved. Initial dedicated infrastructure used55433/19092/16379 and Gateway18080/18081, with separate databases/roles/topics. The final default Compose keeps infrastructure private; its opt-in host-test override uses55434/19093/16380.
- Pinned library choices verified from official release pages and Go module registry: grpc-go1.85.0, protoc36.2, protoc-gen-go1.36.12, protoc-gen-go-grpc1.6.2, franz-go1.22.2, go-redis9.23.0. Existing Go1.26.2,pgx5.11.0 and PostgreSQL17.9 are retained. Kafka4.3.2 and Redis8.2.11 images are pinned by tag and digest in Compose.
