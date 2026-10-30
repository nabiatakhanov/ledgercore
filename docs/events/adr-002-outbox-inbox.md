# ADR 002: transactional outbox and durable inbox

Status: accepted. Date: 2026-10-30.

## Context

PostgreSQL and Kafka do not share a transaction. A process can stop after Kafka acknowledges and before outbox is marked published, or after Activity commits SQL and before offset commit. Producer idempotence cannot remove those windows.

## Decision

Deliver at-least-once. Outbox has immutable `event_id = journal_transaction_id` and payload. Publisher claims bounded batches with a short `SKIP LOCKED` transaction, committing a fresh lease-owner UUID before sending. Updates require both event ID and current owner, fencing delayed owners. Delivery results match record identity rather than callback completion position.

Activity stores unique inbox ID/fingerprint, entries and NUMERIC summaries in one independent transaction. Identical replay is a no-op; conflicting content under one ID is classified and sent to DLQ. Source offset commits only after SQL commit or acknowledged DLQ delivery. A failed record never advances its partition prefix past itself.

## Consequences

Restarts and multiple publishers/consumers cause repeated deliveries without repeated projection effects. Distributed exactly-once is not claimed. Persistent consumer errors need repair/restart. Outbox and inbox IDs have no automatic cleanup; monitor storage. Kafka retention limits source replay. DLQ duplicates retain a stable source topic/partition/offset identity.
