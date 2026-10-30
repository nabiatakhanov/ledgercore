# Gateway/gRPC/events measurements

The events path is measured separately from the original HTTP-only LedgerCore.
[Historical baseline](benchmarks-ledger-v1.md) contains the earlier native benchmark;
its approximately2248 transfers/s is not a measurement of Gateway/gRPC/Redis/Kafka.
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
two Gateways, one credential,60 allowed and20 HTTP429. No limiter key is deleted.

## Method

`ledgerctl events-bench` measures256 new1-kopeck transfers per case at1,8 and32
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
cases are limited to100,000 to bound CLI memory; reduce requests/repeats for
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
partitions. Each Go application is limited to1 CPU and256MiB with GOMEMLIMIT192MiB.
Ledger pool16, publisher pool4, Activity pool16, each Gateway Redis pool16 and hard
maximum16 connections. Kafka4.3.2 is a single KRaft broker, replication factor1,
min.insync.replicas1, producer acks=all with protocol idempotence,7-day and256MiB
per-partition retention limits. This topology does not protect against disk/node
loss. PostgreSQL17.9 uses200 max_connections and1GiB/2 CPU. Kafka1GiB/2 CPU;
Redis8.2.11 uses256MiB/1 CPU,64MiB noeviction policy and AOF everysec. Inbox/outbox
are retained without automatic deletion. See Compose and runbook for disk/backlog
limits. Package/image versions are pinned; protobuf generation versions are in
[scripts](../scripts/generate.sh).

Actual result tables and their source/evidence IDs are added after the committed
source has passed the full demo; no provisional runtime output is a performance claim.
