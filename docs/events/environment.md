# Local events runtime

Observed on 2026-10-30, Windows host with Docker Desktop Linux containers. CPU:
AMD Ryzen 7 5800HS, 8 physical cores / 16 logical processors. Host physical RAM:
16,539,389,952 bytes. Docker Engine 28.1.1 reports 16 CPUs and 8,011,022,336 bytes
of VM memory; these are shared resources, not a dedicated benchmark host.

Go 1.26.2; PostgreSQL 17.9; Kafka 4.3.2 in single-node KRaft; Redis 8.2.11.
The module graph pins grpc-go 1.85.0, protobuf 1.36.12, franz-go 1.22.2,
kadm 1.17.2, go-redis 9.23.0, pgx 5.11.0, Prometheus client 1.25.0.
protoc 36.2 and protoc-gen-go-grpc 1.6.2 are pinned in generation scripts.
Staticcheck 0.7.0 is the applicable linter. Image digests live in Compose/Dockerfile.

The `ledgercore-events` project is separate from original LedgerCore volumes and
other applications on this host. Default public addresses are loopback 18080/18081
for HTTP, 19090/19091 for Gateway metrics. Infrastructure and gRPC ports remain
private. The optional `compose.test-host.yaml` exposes loopback-only infrastructure
for the isolated host fault tests. No Docker socket is mounted into applications.

Each Go service: 1 CPU, 256 MiB, non-root UID 65532, read-only root filesystem,
capabilities dropped, no-new-privileges, `GOMEMLIMIT=192MiB`. Ledger/Activity pools
allow 16 connections each; publisher allows 4. Each Gateway has at most 16 Redis
connections, 4 concurrent dials and a 300 ms admission timeout. One Activity
consumer processes 3 topic partitions with at most 3 partition workers; tests
separately exercised two consumer-group members. gRPC has a 4-second overall call
deadline, 64 concurrent server admissions, 64 streams per connection, 1 MiB
messages and 16 KiB headers.

PostgreSQL: 1 GiB / 2 CPU, `max_connections=200`, `track_commit_timestamp=on` for
the local experiment. Kafka: 1 GiB / 2 CPU, JVM 384 MiB, 3 partitions, replication
factor 1, minISR 1, max message 2 MiB, 7-day / 256 MiB per-partition retention.
Redis: 256 MiB / 1 CPU, 64 MiB noeviction, AOF everysec. Ordinary down/up retains
persistent data.

Integration profile uses dedicated databases and unique Kafka topics/group IDs;
it runs Linux race tests with a 2 GiB / 2 CPU limit. Native Windows race builds
require local MSYS2 GCC and an ASCII task-local temporary path on this machine.
No global compiler, proxy, execution policy or Go configuration was changed.

Benchmark reports capture their own hardware, image IDs, source revision and
container resource settings. Docker before/after stats are samples, not peaks.
See [measurements](../benchmarks.md) and [acceptance](acceptance.md).
