# ADR 001: events describe committed operations

Status: accepted. Date: 2026-10-30. Scope: LedgerCore event extension; original financial ADRs remain unchanged.

## Context

The existing Store guarantees balanced, immutable journal operations and atomic idempotency outcomes. Gateway/Activity must not become authorities for funds. Kafka outage must not leave a half-transfer or keep account locks open during broker I/O.

## Decision

Keep original financial scenarios. Successful new ordinary transfers/reversals insert their envelope in the same PostgreSQL transaction as journal, balances and outcome. Deferred constraints require the outbox record. Publisher sends only after source commit and its later claim commit. Redis admission occurs before invoking Ledger, outside SQL.

Success means financial SQL commit, independently of Kafka/Activity. Refusal, rollback and idempotent replay create no new logical event. Migration 002 starts the stream; pre-existing history and fixture funding are excluded. Historical backfill is separate work.

## Consequences

Commands do not wait for a network publish under account locks. Activity is eventually consistent and cannot validate funds. Kafka failure accumulates outbox until resources are exhausted; PostgreSQL storage failure rejects the whole operation. `occurred_at` precedes commit and is not an exact commit-to-visible clock. Unknown response outcomes are resolved with the same key or known operation ID.
