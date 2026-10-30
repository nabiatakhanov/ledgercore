# Actual toolchain and source references

Baseline environment/source audit only. Current Kafka/gRPC/Redis versions and
executed extension checks are recorded in [events/baseline.md](events/baseline.md)
and [events/acceptance.md](events/acceptance.md).

Implementation session: 2026-10-29, Europe/Moscow. Git historical dates are user-specified metadata, not claims about development time or version availability.

Pinned baseline: Go 1.26.2, pgx/v5 v5.11.0, Prometheus client_golang v1.25.0, Staticcheck v0.7.0 and PostgreSQL 17.9. Current image digests are in Compose/Dockerfile; this dated audit does not claim a later security-update review.

Official references checked on 2026-10-29:
- https://go.dev/doc/devel/release and https://go.dev/dl/?mode=json
- https://github.com/jackc/pgx/releases/tag/v5.11.0
- https://github.com/prometheus/client_golang/releases/tag/v1.25.0
- https://github.com/dominikh/go-tools/releases/tag/2026.1
- https://www.postgresql.org/support/versioning/
- https://www.postgresql.org/docs/current/transaction-iso.html
- https://www.postgresql.org/docs/current/explicit-locking.html
- https://www.postgresql.org/docs/current/ddl-constraints.html
- https://docs.tigerbeetle.com/coding/financial-accounting/

Runtime constraints and executed checks are recorded separately; availability is never inferred from configuration alone.
