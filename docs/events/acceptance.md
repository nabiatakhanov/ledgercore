# Events extension acceptance

Implementation date:2026-10-30 (Europe/Moscow). Repository is the existing
LedgerCore, branch `feature/events-grpc-kafka-redis`. Original base:
`4a4ebd17158341f4740c31377a6d15951bc8e46f`. The initial working tree was clean;
original history and financial logic are preserved. New commits use actual dates
and configured Git authorship. This local acceptance snapshot was completed at
`c16b1efd79f75b3380e5736c9c67e6269eaa3181` before publication. The user subsequently
explicitly requested the GitHub update. Current hosted verification is available
in [GitHub Actions](https://github.com/nabiatakhanov/ledgercore/actions/workflows/ci.yml);
the raw evidence below remains the original local measurements.

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
| Original baseline unit/race and vet | PASS before extension; original14 financial PostgreSQL scenarios PASS (24.314 s) |
| Original financial suite plus atomic outbox constraints | PASS with real PostgreSQL roles, race detector (22.195 s) |
| Real TCP gRPC | PASS: auth, deadlines/cancellation, message bounds, ErrorInfo, int64 string preservation, request IDs, committed lost-reply replay, unkeyed create not retried |
| Real Redis admission | PASS: atomic common quota, two clients/Gateways, first-only TTL, expiry, 429 versus outage503 |
| Activity PostgreSQL | PASS: duplicate races, arbitrary-precision totals, conflicting event IDs, privileges, late reversal/order, stable cursor, independent snapshot/corruption checks (race6.749 s) |
| Real Kafka consumer | PASS: two-member rebalance, replay, DLQ, transient SQL failure with no offset skip, failed DLQ with no source commit, subprocess death after SQL commit before offset |
| Publisher/complete path | PASS: two publishers, stale-owner fencing, lease recovery, Kafka-ack-before-outbox-mark killed subprocess, projection dedup, source/outbox drift detection |
| Actual host container faults | PASS with real Kafka and Redis stop/start through two Gateways; separate isolated test DBs/topics, financial and projection audits |
| Linux Compose full integration | PASS: `go test -p 1 -race -count=1 ./...` with PostgreSQL/Kafka/Redis and real gRPC; Activity 25.167 s, financial10.633 s, events 10.490 s in the first full run |
| Build, vet, Staticcheck 0.7.0 | PASS locally; multi-stage Linux image builds and full primary stack becomes healthy |
| Pinned Protobuf regeneration | PASS on Windows and with pinned Linux binaries in an ephemeral container: four generated files byte-identical |
| OpenAPI | PASS YAML/duplicate keys, 9 operations, 81 local references, path parameters/responses; full meta-schema validator NOT RUN |
| Event JSON Schema | PASS draft2020-12 meta-schema check, 3 valid and 11 invalid examples; application semantic validation is tested separately |

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

## Final acceptance and measurements

The final full Linux check on clean source
`05ecfef624e3ae36d200efd402fac5fa1dd78875` passed with exit 0:
`docker compose --profile test run --build --rm integration`. It runs
`go test -p 1 -race -count=1 ./...` against real PostgreSQL, Kafka, Redis and gRPC.
Latest package times: Activity 20.381 s, financial tests 11.403 s, events 9.808 s,
RPC 1.149 s. Native `go vet ./...` and Staticcheck 0.7.0 also passed on that source.
[Raw test output](evidence/integration.log),
[execution receipt](evidence/checks-receipt.json),
[Linux generation output](evidence/proto-linux.log).

Two full fault demonstrations passed on clean `0400fcf` in 219.785 s/210.828 s,
with identical source-content hashes, stable normal transfer/reversal IDs and
exactly 2 demo funding keys. Both restored services healthy. The later `05ecfef`
commit only adds bounded raw measurement capture to the CLI/scripts; financial
services, RPC, outbox and consumer code are unchanged from those demonstrations.
Each demo retained 20 unpublished events during Kafka outage and 20 unprojected
events during Activity outage, then converged and passed both audits. Replay
left 65/130 and 106/212 event/entry counts unchanged. Both shared limiter bursts
returned 60 successes/20 HTTP 429 from 80 requests; Redis outage returned 503 with
no SQL reservation. All SIGTERM checks exited 0, including publisher during outage.

Official benchmark: 256 requests × 3 repeats × 3 client counts = **2304 successful
transfers**, 0 business refusals, 0 limiter responses, 0 infrastructure errors.
The 3-repeat throughput median and pooled 768-sample p95 latency per client count:

| Clients | Median req/s | HTTP p95, ms | Commit→projection p95, ms |
|---:|---:|---:|---:|
| 1 | 81.88 | 15.256 | 43.868 |
| 8 | 290.53 | 63.636 | 1469.698 |
| 32 | 326.23 | 173.335 | 1315.712 |

All 4608 raw HTTP/commit samples and their quantiles were independently checked;
all 2304 operation IDs are unique. Final source/inbox counts 2410/2410, Activity
entries 4820, unpublished/pending 0; both audits pass. The dataset includes 246 new
fixture funding transactions outside the timed requests. A short local burst is
not an SLA, and no speed improvement over the old HTTP-only path is claimed.

[Full conditions, p50/p95/p99, all 9 cases, recovery times and dataset counts](../benchmarks.md).
[Raw evidence index and SHA256 hashes](evidence/README.md).

## Reproduction

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
`make down` keeps data. The default demo is repeatable and does not reset shared
Redis keys or re-fund its original wallets. Full scripts record their own run IDs,
source revision/dirty status, content hash, timings and raw output.

Hosted GitHub Actions had not run when the local evidence was collected; later
publication runs are tracked separately in the Actions link above.

NOT RUN: long-duration soak/capacity tests, multi-broker failover/node or disk loss,
external TLS/mTLS and production deployment. The one-broker local topology cannot
establish those claims.

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

## Actual commit history

Base `4a4ebd17158341f4740c31377a6d15951bc8e46f` remains an ancestor. All 9 original
historical commits passed `scripts/verify-history.ps1`; none were rewritten.
The following implementation commits use configured author GyBJluHv2 and actual
2026-10-30 dates with matching author/committer timestamps (+03:00):

| SHA | Message |
|---|---|
| `8ede65f8355c27f09e864177699c243772fb1309` | docs: audit ledger baseline and define event extension boundaries |
| `3ac9a986c33b19b487d793271c7a29c26d0fa5fa` | feat: commit immutable operation events atomically with ledger writes |
| `887a3d0ee5c74d20e58d0dc3a3955d7b2c470d95` | feat: materialize Kafka activity with transactional inbox and safe replay |
| `c2fb9aa74886eb308cc02931ab426e567b9a95ba` | feat: route authenticated gateways through gRPC and shared Redis admission |
| `9d28adc0050d2107824177ef8e464b0ce41ab7d3` | feat: fence outbox publishers and verify recovery across real service failures |
| `0400fcff1ed0a5fd56409572660b47cd9c3f5679` | feat: ship reproducible events stack, fault demo and measured-path tooling |
| `05ecfef624e3ae36d200efd402fac5fa1dd78875` | feat: retain bounded raw HTTP and projection commit samples |
| `c16b1efd79f75b3380e5736c9c67e6269eaa3181` | docs: record verified event failures, raw samples and benchmark limits |

The documentation/evidence commit changes no runtime code. The working tree was
checked clean at completion of that local acceptance, and no automatic push was
performed then. Later publication was explicitly authorized by the user; the
current repository revision is obtained with `git log -1 --format='%H %s'`.
