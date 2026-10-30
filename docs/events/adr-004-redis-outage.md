# ADR 004: fail closed on unavailable write admission

Status: accepted. Date: 2026-10-30.

## Context

Two Gateways must share one authenticated client's write budget. Independent counters multiply that budget. Admission outages must differ from exhaustion, while Redis must not become a money/idempotency authority.

## Decision

One Lua script atomically performs INCR, first-request PEXPIRE and PTTL for a versioned client/write key. Identity is SHA-256 of the authenticated static token; the raw bearer is not a key or log field. Default is 60 requests per 1 minute from first request. Later arrivals, including refusals, never extend TTL.

Authenticated `POST /v1/*`, including idempotent retries, consume budget. Exhaustion returns `429 rate_limited` with ceiling-rounded TTL in Retry-After. Redis errors return `503 rate_limiter_unavailable` before Ledger invocation. Reads bypass this write limiter. Configuration and connection/concurrency bounds are validated.

## Consequences

Redis outage makes writes unavailable even if Ledger is healthy. Financial commit rules remain unchanged. Adjacent windows permit boundary bursts, and lost Redis state resets budgets. AOF everysec does not make admission an absolute guarantee. Balances, journal and durable operation deduplication stay in PostgreSQL.
