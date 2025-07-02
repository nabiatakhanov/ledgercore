# Reproducible demo

From a clean checkout with Docker Engine and Compose v2 running:

```sh
docker compose up --build -d
make demo
make reconcile
```

`make up` is the equivalent startup target. First startup initializes separate bootstrap, schema-owner migrator, app and fixture roles, applies embedded migrations, then starts the API after the migration service succeeds. Compose provides localhost-only development credentials by default so the one-command quick start needs no file editing. `.env.example` lists overrides; use URL-safe credential characters in these connection URLs or percent-encode them. Changes to database bootstrap passwords affect a fresh volume only. No credentials are baked into the image.

The explicit administrative fixture command is:

```sh
docker compose --profile tools run --rm ledgerctl fixture --allow-test-funds
```

It creates two deterministic demo wallet UUIDs, then funds each with100000 kopecks from the protected system balancing account. The fixture role must be supplied separately; the API has no such credential. Stable funding keys include the fixture version and account. Repeating the command replays funding. Changing an amount under an existing key conflicts instead of adding new funds.

`make demo` performs that fixture, sends an HTTP2500-kopeck transfer and a full reversal using stable keys, sends16 concurrent HTTP requests with one key for a100-kopeck transfer, prints both balances and runs independent reconciliation. Expected final demo balances are99900 and100100 kopecks. Repeating the entire demo returns those same balances and replays the same three operations; initial funds do not double. The system account is-200000 kopecks. Public `POST /v1/accounts` always creates a separate zero-balance client.

## Native execution

Provision PostgreSQL17.9, a dedicated empty database and the roles in `scripts/bootstrap-db.sh`. On Unix, supply the bootstrap environment shown in that script to `sh scripts/bootstrap-db.sh`. Set these environment variables without writing secrets into source:

- `MIGRATION_DATABASE_URL`: schema-owner login, command `go run ./cmd/ledgerctl migrate`.
- `DATABASE_URL`: `ledger_app` login, command `go run ./cmd/api`.
- `FIXTURE_DATABASE_URL`: `ledger_fixture` login for explicit fixture/demo/bench commands.
- `API_TOKEN`: shared token of at least16 bytes; `API_URL` defaults to `http://127.0.0.1:8080`.

Then use `go run ./cmd/ledgerctl demo` and `go run ./cmd/ledgerctl reconcile`. This native path was exercised in the implementation environment; exact findings are in `environment-notes.md` and `acceptance.md`. Windows GCC requires an ASCII temporary path for race builds on this machine; set task-local `GOTMPDIR`, `TEMP`, `TMP` and add the MSYS2 compiler to PATH.

## Failure and concurrency experiments

`make test-integration` provisions its own tmpfs PostgreSQL in the Compose test profile. Fault triggers, administrator projection corruption and a killed subprocess only run there. Native equivalent: point all three `LEDGER_TEST_DATABASE_URL`, `LEDGER_TEST_FIXTURE_URL`, `LEDGER_TEST_ADMIN_URL` at the same separate database whose name contains`test`, set `LEDGER_REQUIRE_INTEGRATION=1`, then run `make test-integration-local`. The harness verifies actual login roles and refuses to run destructive cases against a non-test database. There is no production fault-injection switch.

The precommit-crash test installs a test-only trigger, holds an advisory-lock barrier on the source account, starts a subprocess executing the real writer, observes its wait in `pg_locks`, kills that process, unlocks and verifies unchanged balances/no journal/no idempotency outcome. It does not rely on an arbitrary sleep to decide when to kill. Another test injects SQLSTATE40001 exactly once and checks whole-transaction retry; a context deadline while a row is locked must return promptly and leave no partial state.

For lost HTTP responses after commit, repeat the exact normalized request with the original key. Do not invent a replacement key. A503 may mean timeout, contention or unknown commit outcome. Final insufficient-funds responses persist and do not become successes after later funding; a new intentional operation requires a new key.

Stop with `make down`; data remains in the named volume. `make destroy-data` explicitly deletes the volume and all development data. It is never part of normal startup or shutdown.
