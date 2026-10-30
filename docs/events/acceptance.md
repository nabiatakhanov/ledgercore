# Events extension acceptance

Implementation date:2026-10-30 (Europe/Moscow). Repository is the existing
LedgerCore, branch `feature/events-grpc-kafka-redis`. Original base:
`4a4ebd17158341f4740c31377a6d15951bc8e46f`. The initial working tree was clean;
original history and financial logic are preserved. New commits use actual dates
and configured Git authorship. This extension has not been pushed.

## Implemented

- Gateway reuses the existing HTTP contract and application operations through real
  versioned Ledger/Activity gRPC. Two Gateway processes share Redis Lua admission.
- Ledger commits immutable JSON events in the financial transaction. No broker or
  Redis call holds financial SQL locks; no event for rollback/refusal/funding/replay.
- Publisher uses short SKIP LOCKED leases with fresh ownership tokens, fenced
  acknowledgments and bounded retries. Kafka acks=all/protocol idempotence does not
  remove the outbox crash-window duplicates; delivery is at least once.
- Activity stores inbox, two account entries and NUMERIC summaries atomically in a
  separate database. Partition processing is sequential, partitions have bounded
  concurrency, and offsets follow SQL commit or acknowledged DLQ publication.
- Replay, independent snapshot audits, bounded shutdown, operational metrics,
  pinned non-root images, Compose, CI and repeatable fault/measurement scripts.

Code: `internal/{events,outbox,activity,rpc,gateway,ratelimit,eventaudit}`;
executables: `cmd/{gateway,ledger,publisher,activity,ledgerctl}`. The old `cmd/api`
uses the same Store and is retained in a private compatibility Compose profile.
Contracts: `proto/{ledger,activity}/v1`, generated `gen`,
[schema](../../schemas/ledger-operation-v1.schema.json),
[OpenAPI](../openapi.yaml), migrations002/003 plus Activity's own migration.
Operations predating migration002 are not automatically backfilled.

## Executed checks

All checks below actually ran against this local implementation; an unconfigured
integration test skip is not counted as a real infrastructure test.

| Check | Result / scope |
|---|---|
| Original baseline unit/race and vet | PASS before extension; original14 financial PostgreSQL scenarios PASS (24.314s) |
| Original financial suite plus atomic outbox constraints | PASS with real PostgreSQL roles, race detector (22.195s) |
| Real TCP gRPC | PASS: auth, deadlines/cancellation, message bounds, ErrorInfo, int64 string preservation, request IDs, committed lost-reply replay, unkeyed create not retried |
| Real Redis admission | PASS: atomic common quota, two clients/Gateways, first-only TTL, expiry,429 versus outage503 |
| Activity PostgreSQL | PASS: duplicate races, arbitrary-precision totals, conflicting event IDs, privileges, late reversal/order, stable cursor, independent snapshot/corruption checks (race6.749s) |
| Real Kafka consumer | PASS: two-member rebalance, replay, DLQ, transient SQL failure with no offset skip, failed DLQ with no source commit, subprocess death after SQL commit before offset |
| Publisher/complete path | PASS: two publishers, stale-owner fencing, lease recovery, Kafka-ack-before-outbox-mark killed subprocess, projection dedup, source/outbox drift detection |
| Actual host container faults | PASS with real Kafka and Redis stop/start through two Gateways; separate isolated test DBs/topics, financial and projection audits |
| Linux Compose full integration | PASS: `go test -p 1 -race -count=1 ./...` with PostgreSQL/Kafka/Redis and real gRPC; Activity25.167s, financial10.633s, events10.490s in the first full run |
| Build, vet, Staticcheck0.7.0 | PASS locally; multi-stage Linux image builds and full primary stack becomes healthy |
| Pinned Protobuf regeneration | PASS on Windows: four generated files byte-identical |
| OpenAPI | PASS YAML/duplicate keys,9 operations,81 local references, path parameters/responses; full meta-schema validator NOT RUN |
| Event JSON Schema | PASS draft2020-12 meta-schema check,3 valid and11 invalid examples; application semantic validation is tested separately |

Test cases live in `tests/integration_test.go`, `tests/outbox_atomic_test.go`,
`tests/events/events_test.go`, `internal/activity/*_test.go`, `internal/rpc/*_test.go`,
`internal/gateway/*_test.go`, `internal/ratelimit/*_test.go` and `internal/outbox/*_test.go`.
Fault barriers exist only in tests; there is no production crash endpoint.

## Corrections found during final review

- Kafka ProduceSync completion order need not equal input order. Publisher matches
  acknowledgments by record pointer; a reversed mixed-success unit test covers it.
- Activity listener, Kafka client and pool shutdown share one absolute10-second
  deadline; publisher closes the producer promptly during a broker outage.
- Docker service-name gRPC could wait for a DNS TXT service-config query after the
  A record had resolved. Client now uses WithDisableServiceConfig, retaining DNS
  addresses and explicit local retry/deadline policy. Real full-demo Activity read
  passed after this fix. [gRPC service-config behavior](https://grpc.io/docs/guides/service-config/).
- A persistent Kafka fetch error deliberately stops Activity without advancing
  offsets. Full demo exposed this during a broker outage; recovery explicitly
  starts Activity after broker readiness, as documented in the runbook. An exited
  process does not recover without a supervisor/operator restart.

Independent final code review found no blocking defect in SQL boundaries, lease
fencing, offset commits, partition sequencing, internal authentication, retries or
shutdown. Sandbox RPC tests initially hit Windows loopback socket restrictions;
the successful checks used actual unrestricted TCP or Linux Compose.

## Reproduction and outstanding result collection

```sh
make up
make generate
make test
make lint
make build
make test-integration
make demo
make bench
make reconcile
make down
```

On Windows without Make, use Docker Compose and the PowerShell scripts shown in
[demo](../demo.md), [benchmarks](../benchmarks.md) and [runbook](../runbook.md).
`make down` keeps data. Full-demo fault evidence and official finite benchmark
measurements are being collected against the final committed source and will be
recorded here with raw artifacts. No provisional data is a throughput claim.

NOT RUN: hosted GitHub Actions for the extension (no push requested), long-duration
soak/capacity tests, multi-broker failover/node or disk loss, external TLS/mTLS and
production deployment. The one-broker local topology cannot establish those claims.

## Limits and interview material

Private Docker plaintext gRPC plus a shared internal token is a local trust model.
The single broker uses replication factor1 and minISR1. Kafka retention limits
replay history; inbox/outbox are not automatically deleted. Full restore requires
retained records or a separately designed source backfill/archive. Activity is
statistics/history and is never an authoritative available balance. Redis loss can
reset quota state; boundary bursts are possible. The limiter cannot replace the
financial invariants. Strict cross-database source/projection audit is for quiescent
writers because two databases do not share a transaction snapshot.

[Ten implementation-specific interview answers and two honest CV bullets](interview.md)
update the existing LedgerCore project. Historical benchmark evidence remains
[separate](../benchmarks-ledger-v1.md). No speed-improvement percentage or distributed
exactly-once guarantee is claimed.
