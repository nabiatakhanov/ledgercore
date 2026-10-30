# Local events runtime

Observed on2026-10-30, Windows host with Docker Desktop Linux containers. CPU:
AMD Ryzen7 5800HS,8 physical cores/16 logical processors. Host physical RAM:
16,539,389,952 bytes. Docker Engine28.1.1 reports16 CPUs and8,011,022,336 bytes
of VM memory; these are shared resources, not a dedicated benchmark host.

Go1.26.2; PostgreSQL17.9; Kafka4.3.2 in single-node KRaft; Redis8.2.11.
The module graph pins grpc-go1.85.0, protobuf1.36.12, franz-go1.22.2,
kadm1.17.2, go-redis9.23.0, pgx5.11.0, Prometheus client1.25.0.
protoc36.2 and protoc-gen-go-grpc1.6.2 are pinned in generation scripts.
Staticcheck0.7.0 is the applicable linter. Image digests live in Compose/Dockerfile.

The `ledgercore-events` project is separate from original LedgerCore volumes and
other applications on this host. Default public addresses are loopback18080/18081
for HTTP,19090/19091 for Gateway metrics. Infrastructure and gRPC ports remain
private. The optional `compose.test-host.yaml` exposes loopback-only infrastructure
for the isolated host fault tests. No Docker socket is mounted into applications.

Each Go service:1 CPU,256MiB, non-root UID65532, read-only root filesystem,
capabilities dropped, no-new-privileges, GOMEMLIMIT192MiB. Ledger/Activity pools16,
publisher4. Both Gateways have16 Redis connections each,4 concurrent dials and
300ms admission timeout. One Activity consumer processes3 topic partitions with
at most3 partition workers; tests separately exercised two consumer-group members.
gRPC has a4-second overall call deadline,64 concurrent server admissions,
64 streams per connection,1MiB messages and16KiB headers.

PostgreSQL:1GiB/2 CPU, max_connections200, track_commit_timestamp enabled for the
local experiment. Kafka:1GiB/2 CPU, JVM384MiB,3 partitions, replication factor1,
minISR1, max message2MiB,7-day/256MiB per-partition retention. Redis:256MiB/1 CPU,
64MiB noeviction, AOF everysec. Persisted data is retained by ordinary down/up.

Integration profile uses dedicated databases and unique Kafka topics/group IDs;
it runs Linux race tests with a2GiB/2 CPU limit. Native Windows race builds require
the local MSYS2 GCC and an ASCII task-local temporary path on this machine. No
global compiler, proxy, execution policy or Go configuration was changed.

Benchmark reports capture their own hardware, image IDs, source revision and
container resource settings. Docker before/after stats are samples, not peaks.
See [measurements](../benchmarks.md) and [acceptance](acceptance.md).
