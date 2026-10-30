# Gateway/gRPC/events measurements

The events path is measured separately from the original HTTP-only LedgerCore.
[Historical baseline](benchmarks-ledger-v1.md) contains the earlier native benchmark;
its approximately 2248 transfers/s is not a measurement of Gateway/gRPC/Redis/Kafka.
Adding asynchronous history is not claimed to accelerate financial writes.

## Reproduce

```sh
make up
make demo
make bench
```

PowerShell equivalent for the benchmark:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/bench-events.ps1 -Requests 256 -Repeats 3
```

The Bash entry point is `sh scripts/bench-events.sh 256 3`. Both implementations
build/start the stack, temporarily configure both Gateways with a shared quota of
1,000,000 requests/minute, record raw data and restore the original quota even on
failure. Limiter correctness is measured separately by the full demo:80 requests,
two Gateways, one credential, 60 allowed and 20 HTTP 429. No limiter key is deleted.

## Method

`ledgerctl events-bench` measures256 new1-kopeck transfers per case at 1,8 and 32
concurrent clients, with three independent repetitions per client count. Each worker
uses its own funded account pair. Requests alternate between two Gateway URLs and
traverse real Redis admission and Ledger gRPC before the PostgreSQL commit. Each
request has a unique operation key. Setup/funding, catch-up and audits are outside
the timed HTTP loop. The fixture and load-generator database credentials belong to
an administrative CLI, not the Gateway or Activity runtime.

Throughput is all attempted requests divided by wall time of the HTTP worker loop.
Responses are separately classified as successful, business refusals, limited or
infrastructure errors. HTTP latency uses a monotonic client clock and includes the
request/response and commit. Percentiles use nearest rank over the per-case samples.
There is no warm-up exclusion and no coordinated-omission correction; this is a
closed-loop finite local experiment, not a sustained-load capacity test or SLA.

For each successful source ID, the CLI waits for the Activity inbox then reads
`pg_xact_commit_timestamp` for the source header creation XID and Activity inbox XID.
Their difference measures **SQL commit to projection SQL commit**, on the same
PostgreSQL server clock. It does not include a later client HTTP read/poll. The
stack enables `track_commit_timestamp`; absence of these timestamps is an error,
not silently replaced with event `occurred_at` or precommit `projected_at` values.
Each sample count and p50/p95/p99/max is retained. Commit-timestamp history is a
local measurement aid, not a permanent business timestamp guarantee.

The JSON also retains every request's latency, classified status and successful
operation ID, plus each event's source/projection commit timestamps and delay.
Statistics can be recalculated from those samples. Total samples across all
cases are limited to 100,000 to bound CLI memory; reduce requests/repeats for
larger parameter combinations. A status0 sample denotes a transport/invalid
response classified as an infrastructure error, not an HTTP status code.

After each case, source financial and independent projection audits must pass.
The demo separately records backlog during Kafka/Activity outages, catch-up wait,
replay before/after counts, shared limiter outcomes and SIGTERM exit codes.
Catch-up wait starts when the verification CLI begins after the component start
command; start/readiness and full command durations are retained separately, so
an already-caught-up projection is not presented as the entire recovery duration.

Raw reports and step output are saved under `.runtime/events-bench/<run-id>/` and
`.runtime/events-demo/<run-id>/`. Reports include source HEAD, dirty flag, content
manifest hash, Docker hardware, service CPU/memory limits and image IDs. Docker
stats before/after are point samples, not peak resource or CPU-integral measurements.
Selected credential-free evidence and actual results are retained in
[the acceptance report](events/acceptance.md).

## Runtime bounds

Two Gateway processes, one Ledger, one publisher, one Activity consumer; three
partitions. Each Go application is limited to 1 CPU and 256 MiB with GOMEMLIMIT192 MiB.
Ledger pool 16, publisher pool 4, Activity pool 16, each Gateway Redis pool 16 and hard
maximum16 connections. Kafka 4.3.2 is a single KRaft broker, replication factor1,
min.insync.replicas1, producer acks=all with protocol idempotence, 7-day and 256 MiB
per-partition retention limits. This topology does not protect against disk/node
loss. PostgreSQL 17.9 uses200 max_connections and 1 GiB/2 CPU. Kafka 1 GiB/2 CPU;
Redis 8.2.11 uses256 MiB/1 CPU, 64 MiB noeviction policy and AOF everysec. Inbox/outbox
are retained without automatic deletion. See Compose and runbook for disk/backlog
limits. Package/image versions are pinned; protobuf generation versions are in
[scripts](../scripts/generate.sh).

## Recorded results:2026-10-30

Benchmark run `f48768cb-e751-4377-87e0-279e6fc2edbc` used clean commit
`05ecfef624e3ae36d200efd402fac5fa1dd78875`. The workflow took107.990 s,
including image build/readiness and restoring the original Gateway quota. The
load CLI command took28.82 s including fixtures, catch-up and audits. Load ran
inside the Compose tools container (1 CPU/256 MiB) over its private HTTP network;
host-to-Docker NAT and an external client connection are outside these timings.
Hardware and limits are in [environment](events/environment.md).

All 9 cases completed256/256 transfers: **2304 successes, 0 business refusals,
0 rate-limited responses and 0 infrastructure errors**. All per-case audits passed.
The higher throughput quota was temporary; the separate limiter test used60/min.
The dataset grows between cases; there is no warm-up exclusion or random case order.

The summary uses median throughput across3 repetitions. Latency quantiles below
are pooled over all 768 raw samples for each client count; they are not medians of
the 3 case quantiles. Raw reports also retain both forms.

| Clients | Median req/s | Req/s range | HTTP p50 / p95 / p99, ms | Commit→projection p50 / p95 / p99, ms |
|---:|---:|---:|---:|---:|
| 1 | 81.88 | 75.33–84.43 | 11.884 / 15.256 / 18.631 | 26.231 / 43.868 / 69.683 |
| 8 | 290.53 | 283.81–304.22 | 18.389 / 63.636 / 78.385 | 900.793 / 1469.698 / 1560.924 |
| 32 | 326.23 | 272.84–360.78 | 99.609 / 173.335 / 191.832 | 975.346 / 1315.712 / 1414.063 |

All individual HTTP cases:

| Clients | Repeat | Timed loop, s | Req/s | p50, ms | p95, ms | p99, ms | Max, ms |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 3.398 | 75.333 | 12.554 | 16.660 | 23.173 | 33.641 |
| 1 | 2 | 3.127 | 81.877 | 11.659 | 15.294 | 17.868 | 38.702 |
| 1 | 3 | 3.032 | 84.425 | 11.500 | 13.790 | 14.894 | 16.713 |
| 8 | 1 | 0.881 | 290.533 | 18.493 | 63.861 | 83.138 | 83.369 |
| 8 | 2 | 0.841 | 304.221 | 19.497 | 56.403 | 61.613 | 68.059 |
| 8 | 3 | 0.902 | 283.806 | 16.620 | 70.602 | 77.545 | 82.944 |
| 32 | 1 | 0.938 | 272.838 | 108.043 | 186.845 | 199.732 | 211.417 |
| 32 | 2 | 0.710 | 360.781 | 97.052 | 161.546 | 171.817 | 172.837 |
| 32 | 3 | 0.785 | 326.234 | 95.939 | 122.296 | 169.850 | 177.820 |

Commit-to-projection cases (256 actual commit timestamp pairs per row):

| Clients | Repeat | p50, ms | p95, ms | p99, ms | Max, ms | Wait after HTTP loop, s |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 28.678 | 46.021 | 75.498 | 106.736 | 0.022 |
| 1 | 2 | 26.190 | 53.657 | 76.864 | 98.956 | 0.041 |
| 1 | 3 | 24.736 | 28.498 | 36.505 | 42.339 | 0.042 |
| 8 | 1 | 903.537 | 1539.221 | 1572.473 | 1582.045 | 1.591 |
| 8 | 2 | 730.587 | 1336.326 | 1360.680 | 1368.929 | 1.376 |
| 8 | 3 | 955.070 | 1203.330 | 1229.201 | 1263.905 | 1.280 |
| 32 | 1 | 1075.885 | 1393.728 | 1435.503 | 1444.932 | 1.472 |
| 32 | 2 | 958.093 | 1263.006 | 1331.255 | 1349.516 | 1.371 |
| 32 | 3 | 927.733 | 1295.331 | 1335.711 | 1360.144 | 1.376 |

Projection latency rises under the concurrent bursts, even though writes commit
earlier. This run does not isolate one bottleneck. The overlapping throughput
ranges and short cases do not justify a capacity/SLA claim or an improvement
percentage over the old native HTTP benchmark. No infrastructure optimization
or speed improvement is asserted.

Independent recomputation verified all 2304 HTTP samples, 2304 source/projection
timestamp pairs, 2304 unique operation IDs, every case percentile/max and each
throughput. Largest floating-point discrepancy when recomputing timestamp deltas
was2.3e-13 ms. [Raw benchmark](events/evidence/benchmark-raw.json),
[workflow report](events/evidence/benchmark-report.json) and
[sample audit](events/evidence/sample-audit.json) retain the actual values.

## Outage, replay and shared limiter experiments

Two complete clean-source demos passed on `0400fcf` in 219.785 s and 210.828 s.
Both explicitly held Kafka and Activity outages for10 s, independently. The
observed outage includes stop/enqueue/hold/start commands and health waits:

| Demo | Stopped component | Observed backlog | Explicit hold, s | Stop→ready span, s | Recovery command start→verified audit, s |
|---:|---|---|---:|---:|---:|
| 1 | Kafka | 20 unpublished outbox | 10 | 35.824 | 29.487 |
| 1 | Activity | 20 unprojected; outbox0 | 10 | 21.612 | 8.775 |
| 2 | Kafka | 20 unpublished outbox | 10 | 49.645 | 45.268 |
| 2 | Activity | 20 unprojected; outbox0 | 10 | 21.701 | 8.654 |

Kafka recovery includes explicit publisher and Activity starts after broker
readiness. Activity stops safely on persistent fetch failure and needs an operator
or supervisor restart. The final verification work itself took roughly 34–53 ms
after components became ready; that is **not the entire backlog recovery time**.
The recovery spans above include process readiness, CLI overhead and both audits.

Replay in demo1 left 65 inbox events/130 entries unchanged; demo2 left 106/212
unchanged. A new group read retained records into the same projection, with no
extra effects. Each run returned 60 successful requests and 20 HTTP 429 responses
for an80-request concurrent burst through 2 Gateways, with 0 infrastructure errors.
The normal request key was reused;60 successful responses did not mean60 new
financial operations. Real TTL expiry was observed without deleting shared keys.
Redis outage returned 503 `rate_limiter_unavailable` and 0 SQL reservations for
the tested key in both runs.

All normal SIGTERM checks exited 0; Docker stop command spans across both runs
were 0.877–1.736 s. Publisher also exited 0 during Kafka outage in 2.700 s/3.702 s.
The application cleanup deadline is10 s; these observed idle/small-backlog exits
do not establish a universal shutdown latency. Both stacks were restored healthy.

Normal transfer/reversal IDs remained `411500f6-98db-4f0c-b4ca-eef275015d8e` /
`bbb8f4cd-4728-44af-9228-c3a2f9567576`. Demo funding keys remained exactly 2
before and after repeat demonstrations and the benchmark.
[Demo1](events/evidence/demo-1.json), [demo2](events/evidence/demo-2.json).

## Dataset before/after the benchmark

| Count / bytes | Before | After |
|---|---:|---:|
| Ledger accounts | 3 | 249 |
| Journal transactions | 108 | 2658 |
| Postings | 216 | 5316 |
| Idempotency records | 108 | 2658 |
| Outbox events | 106 | 2410 |
| Unpublished outbox | 0 | 0 |
| Activity inbox | 106 | 2410 |
| Activity entries | 212 | 4820 |
| Activity summaries | 2 | 248 |
| Demo funding keys | 2 | 2 |
| Ledger DB bytes | 8631987 | 15251123 |
| Activity DB bytes | 8115891 | 9983667 |

The delta is exactly 2304 benchmark transfers plus 246 separate fixture funding
transactions; fixtures are outside the HTTP timing. All new transfers produced
one event and two projection entries. Final financial and projection audits passed
with 2410/2410 source/inbox events, 4820 entries and 0 pending. These are actual
preexisting-plus-benchmark datasets, not an initially empty database.
[Before](events/evidence/dataset-before.json), [after](events/evidence/dataset-after.json).

[Evidence index](events/evidence/README.md) includes immutable raw copies, resource
point samples, final test output and hashes. No hosted CI or production deployment
was run for the extension; there was no automatic push.
