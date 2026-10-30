# Activity projection and replay

Activity owns a distinct PostgreSQL database and login. A single short SQL
transaction inserts inbox/event_id, two activity entries and numeric incoming/
outgoing totals and counts. The inbox has a stable fingerprint; an identical
event is a no-op, while a conflicting payload under one ID is a permanent event
error. Account summary rows are inserted and locked in UUID order. Histories use
account-local sequences with a first-page upper bound and exclusive cursor, so
concurrent commits do not enter an already paged prefix. Empty history is a valid
empty projection, not evidence that a financial account does not exist.

There is no original-event FK for reversal activity: reversal events may precede
their originals across partitions. Each operation contributes its own incoming
and outgoing statistics. Totals and counts are PostgreSQL NUMERIC decimal
strings; they are not authoritative wallet balances. `Audit` independently
recomputes expected entry shapes and aggregates in REPEATABLE READ READ ONLY.

Kafka auto-commit is disabled. Partition ownership is held while processing a
poll; records within each partition execute sequentially, with bounded parallel
partition handlers. Offset commit follows successful SQL commit (or acknowledged
DLQ delivery). Any persistent DB/fetch/offset/DLQ error stops the consumer without
advancing past a failed record; restart resumes the uncommitted group prefix.
An explicit cancellation stops normally and leaves unfinished commits replayable.

Malformed, unsupported or conflicting events enter a separate DLQ before the
source offset is committed. Its source ID is topic/partition/offset. Payload is
base64 encoded with a bounded prefix, original length/hash and truncation flag;
no raw event content is logged. A failed DLQ publish is not successful processing.
Duplicate DLQ deliveries are possible. Broker/topic message limits are2MiB for
the envelope, while accepted financial event input is bounded to1MiB.

New group IDs replay retained Kafka ranges into the same inbox without doubling
entries or counters. Inbox is not automatically deleted. Kafka retention limits
the available source range; this version does not promise recovery of expired
unarchived events. Dev Kafka has3 partitions, replication factor1 and minISR1:
acks=all covers this one broker, with no node/disk redundancy.

Integration coverage includes real PostgreSQL totals beyond int64 and snapshot
corruption detection, Kafka group/rebalance, replay, poison messages, actual
rejected DLQ writes, DB failure retaining offsets and a killed subprocess after
SQL commit before offset commit. Executed commands and raw final suite results
are consolidated in the [extension acceptance report](acceptance.md).
