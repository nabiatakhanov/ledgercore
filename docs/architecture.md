# Architecture

LedgerCore is a single Go HTTP service plus an administrative CLI. PostgreSQL is the only state store. It is an educational RUB ledger using test funds. It has no external payment network or user-level authorization.

`internal/ledger` owns integer money, the pure balance transition, validation and canonical fingerprints. `internal/store` owns explicit SQL and short transactions. `internal/httpapi` owns transport/authentication/limits. `internal/reconcile` independently audits a consistent snapshot. `internal/observability` exposes bounded-label Prometheus metrics. `migrations` embeds checksummed SQL; only the owner role applies it.

## Write path

1. Validate body, key, UUIDs, money and metadata before beginning work. Normalize amount leading zeros, UUID case/outer whitespace and currency case/outer whitespace. Metadata strings remain exact; map ordering is canonical JSON.
2. Begin READ COMMITTED. INSERT an idempotency reservation under the primary key `(kind,key)` with ON CONFLICT DO NOTHING. PostgreSQL waits on an uncommitted conflicting key. On conflict, a subsequent statement sees the committed final response. A different fingerprint returns409. A rollback leaves no reservation.
3. For reversals, read the immutable original. Lock both affected account rows by ascending UUID with ORDER BY/FOR UPDATE. All writer kinds use this order. Resolve business rules while holding these locks; check reverse uniqueness again after acquiring them.
4. Check funds and both int64 bounds; increment account-local history sequences; insert one header, two postings, both projection changes and the final idempotency response. A business refusal writes only its final idempotency response.
5. Deferred database constraints validate the journal and idempotency reservation at commit. Return success only after commit. SQLSTATE40001/40P01 retry the entire transaction at most three attempts with short context-aware backoff. Business outcomes never retry internally. Lock/statement/deadline failures return503 and are safe to repeat with the original key.

There are no network calls while rows are locked. Reversals append an opposite operation, never modify the original. Funding uses the same writer but a separate fixture login with an explicit CLI opt-in. The API login cannot commit funding even if Go checks are bypassed.

## Read path and history

A balance GET is one committed statement snapshot. A later history request can include later activity; these independent endpoints do not claim a joint snapshot. Journal transaction headers/postings become visible atomically and are immutable, so the two queries in transaction GET cannot observe a partially committed operation.

History orders by an account-local posting sequence descending, not wall clock time or a global preallocated sequence. The writer increments it under the same account lock as the balance. The first page captures the account's current committed upper sequence. Subsequent opaque base64url cursors include account ID, upper bound and exclusive last sequence. Each query uses `sequence <= upper AND sequence < before`. This prevents late commits entering an already paged prefix. Cursors are validated and bound to their account; they are navigation data, not authorization. New pagination starts see new entries. There is no expiration/cleanup of idempotency keys in this version; storage grows with operations and final business refusals.

## Database boundary

The migrator owns tables/functions; runtime logins are not owners and cannot create schema objects. Journal UPDATE/DELETE/TRUNCATE are not granted. Mutation triggers add an additional guard. A posting may only attach to a header created in the same database transaction (`xid8`), closing the append-to-old-header hole. Deferred triggers on both headers and postings catch missing, wrong-sign, unbalanced and empty operations and validate full reversal linkage. Runtime INSERT accounts is column-restricted to `(id,name)` so clients start at zero.

The writer enforces synchronous projections, lock order, checked arithmetic and business outcomes. The database enforces journal shape, references, client nonnegativity, unique reversals and immutable outcomes. An app role can directly alter the projection columns it needs; reconciliation detects that misuse. It is not protection against an owner/superuser, credential theft, or arbitrary SQL executed with fixture privileges.

## Audit and operations

Reconciliation is an independent set of NUMERIC aggregate queries inside REPEATABLE READ READ ONLY. Every comparison uses one MVCC snapshot, including while transfers commit. It checks projection/sequence equality, journal shapes, missing entries, account kinds, negative clients, invalid or duplicate reversals, orphan postings and incomplete idempotency reservations. Up to100 issue examples are printed, with a full issue count, and CLI exits nonzero. It never repairs.

API timeouts: body32KiB, request5s, DB statement4s and lock2s, headers2s, read5s, write7s, idle30s. Pool default16 connections (bounded configuration1..128), connect5s, idle5min/lifetime30min. Shutdown cancels background work and drains both listeners within10s. Reconciliation is periodically observed on the service listener's separate metrics port; it uses the app's DB role. Metrics include operations, latency, retries, key conflicts and elapsed account lock acquisition (including query round trip), audit outcomes, Go/process resources when supported. No IDs or user text are labels.

Structured logs contain request IDs, HTTP method, timing and sanitized failure categories, never raw SQL errors, token/key/header bodies or metadata. Each request gets a fresh server request ID; replay preserves operation result/status but has a new request ID. Metrics default to loopback, configurable for a private container network. Health endpoints are unauthenticated; all `/v1` endpoints require the static environment bearer token. TLS/network isolation and secret management belong to the deployment boundary.
