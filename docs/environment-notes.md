# Local verification environment

Archived environment notes for the original HTTP/PostgreSQL acceptance. Docker
NOT RUN statements below describe 2026-10-29 only; current extension execution is
recorded separately in [events/acceptance.md](events/acceptance.md).

Actual verification date: 2026-10-29 (Europe/Moscow). Historical Git dates are
user-selected metadata and do not describe when these tool versions existed.

- Host: Windows amd64; AMD Ryzen 7 5800HS, 16 logical processors,
  16,539,389,952 bytes physical RAM.
- Go: `go1.26.2 windows/amd64`; `CGO_ENABLED=1`.
- C compiler for `go test -race`: MSYS2 UCRT64 GCC 16.1.0.
- Git: 2.47.1.windows.1. Existing user identity was preserved.
- GNU Make is installed as `C:\msys64\ucrt64\bin\mingw32-make.exe`;
  the command `make` is absent from PATH. This executable accepts the Makefile.
- Docker CLI: 28.1.1; installed Docker Desktop: 4.41.2.
  The engine was initially stopped. An attempted startup failed in Docker's
  Inference manager while accessing its `dockerInference` Unix socket.
  Docker Desktop was then stopped normally; no reset, data deletion or global
  configuration change was attempted.

## Real PostgreSQL fallback

PostgreSQL **17.9** was run natively, as a temporary isolated cluster, bound only
to `127.0.0.1:55432`. Server reports:
`PostgreSQL 17.9 on x86_64-windows, compiled by msvc-19.44.35222, 64-bit`.
Defaults: `max_connections=100`, `shared_buffers=128MB`, data page checksums off;
ordinary durable WAL/fsync settings were retained.

The [official Windows download page](https://www.postgresql.org/download/windows/)
links to EDB's portable binary archives. The source archive used here was
`https://get.enterprisedb.com/postgresql/postgresql-17.9-1-windows-x64-binaries.zip`
(334,313,473 bytes). Only server `bin`, `lib` and `share` files were fetched using
HTTP ranges and extracted with ZIP CRC validation. The adjacent official SHA256
file was unavailable, so no independent publisher checksum is claimed.

Native `initdb` initially failed under the Cyrillic profile path with an invalid
UTF8 byte-sequence error. Running through the profile's existing ASCII 8.3 short
path solved it. The cluster and downloaded files are in OS temp, outside Git.
Passwords were randomly generated and saved only in temporary connection files.

The bootstrap superuser created three distinct non-superuser LOGIN roles:
`ledger_migrator`, `ledger_app`, and `ledger_fixture`. Databases `ledgercore`,
`ledgercore_test`, and `ledgercore_bench_test` are owned by `ledger_migrator`.
Integration tests and controlled corruption use the database whose name contains
`test`. The application never receives the bootstrap/migrator credentials.

Native reproduction after downloading/extracting the official archive into an
ASCII path:

```powershell
# Use your own local passwords in a private pwfile and environment variables.
initdb -D C:\temp\ledgercore\data --username=ledger_bootstrap --encoding=UTF8 --locale=C --auth=scram-sha-256 --pwfile=C:\temp\ledgercore\admin.password
pg_ctl -D C:\temp\ledgercore\data -l C:\temp\ledgercore\server.log -o "-h 127.0.0.1 -p 55432" -w start
# Create roles and the test DB (see scripts/bootstrap-db.sh for SQL).
# Set the three LEDGER_TEST_* URLs to the isolated test database, then:
$env:LEDGER_REQUIRE_INTEGRATION='1'
go test -race -count=1 ./tests
# When finished:
pg_ctl -D C:\temp\ledgercore\data -m fast -w stop
```

## Container verification boundary

All Compose profiles pass `docker compose --profile tools --profile test config
--quiet`; both shell scripts pass `bash -n`; all required Make targets parse.
Actual image building, clean Compose startup and container shutdown are **NOT
RUN** locally because the installed Docker engine cannot start. CI contains image
building and all mandatory checks; a hosted CI run is not claimed.

Container image tags were verified against Docker Hub's official registry API,
then pinned by multi-platform digest in the Dockerfile/Compose file. GitHub
Actions SHA pins were verified against the official repositories' tag refs.

- `golang:1.26.2-bookworm`
- `debian:12.13-slim`
- `postgres:17.9-bookworm`
- `actions/checkout` v4.2.2
- `actions/setup-go` v5.5.0
- Staticcheck 2026.1 (`honnef.co/go/tools` v0.7.0), verified from its
  [official release](https://github.com/dominikh/go-tools/releases/tag/2026.1).

These exact versions make the performed experiment reproducible; selecting
current security patch releases is a separate maintenance task. No `latest` image
tags or floating Action major-version tags are used.
