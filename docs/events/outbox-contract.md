# Atomic event boundary

New successful ordinary transfers and full reversals append exactly one immutable
outbox event in the same SQL transaction as journal postings, both account
projections and the final idempotency outcome. Business refusal, rollback and
idempotent replay append no event. A deferred trigger requires an outbox record
for every new transfer/reversal header. Funding is intentionally excluded.

Migration002 marks the event boundary. Existing journal rows are unchanged and
have no automatic backfill. An idempotent replay of an old operation also does
not invent a new event. Upgrade requires creating the publisher login before
migrating and deploying the new writer with migration002; an old writer without
outbox support will fail its commit rather than leave a new operation untracked.

The JSON contract is `internal/events.Event`, schema_version1. event_id equals
journal_transaction_id, both the operation's stable UUID. Types are
`transfer.created` and `transfer.reversed`; reversal_of identifies the original.
Amounts are positive canonical decimal strings in kopecks, currencyRUB; no float
money or sensitive metadata is present. Timestamps describe journal creation,
normalized toUTC; they precede the actual SQL commit by the transaction's final
work. Unknown optional fields are accepted; incompatible schema/type/input is a
permanent, classified event error. Full envelopes are at most1MiB on input.

Kafka topic `ledger.operations.v1` uses journal_transaction_id as key. Partition
ordering does not establish a global account order. Activity therefore applies
commutative totals and independent reversed-operation entries, even when a
reversal arrives before the original event. Activity is an eventual history and
statistics projection; financial balances and funds decisions remain in Ledger.

`ledger_app` and `ledger_fixture` can append/read outbox but cannot change
delivery state. `ledger_publisher` can read outbox and update only delivery-state
columns, without account or journal write privileges. Event payloads are also
protected by a mutation trigger. A DBA remains outside this protection boundary.

The publisher's lease is committed before network I/O. A fresh UUID fences every
claimed batch; mark-published/retry updates match both event_id and lease_owner.
An expired previous owner cannot overwrite a renewed batch. Acknowledgment loss
or process death after Kafka acceptance may replay the same event. Kafka producer
idempotence does not remove this application-level crash window; delivery is
at-least-once and the Activity inbox performs durable deduplication.

Unpublished events are retained through outages. Backoff is bounded, while the
durable backlog can grow until storage capacity is exhausted. Monitor count,
oldest age and disk space. A PostgreSQL storage failure rejects/rolls back the
entire financial transaction; the API does not acknowledge a failed commit.
