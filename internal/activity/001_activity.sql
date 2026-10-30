-- This migration belongs to the Activity database only.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE TABLE activity_inbox (
    event_id uuid PRIMARY KEY,
    schema_version integer NOT NULL CHECK (schema_version=1),
    type text NOT NULL CHECK (type IN ('transfer.created','transfer.reversed')),
    journal_transaction_id uuid NOT NULL UNIQUE,
    from_account_id uuid NOT NULL,
    to_account_id uuid NOT NULL,
    amount_minor bigint NOT NULL CHECK (amount_minor>0),
    currency text NOT NULL CHECK (currency='RUB'),
    occurred_at timestamptz NOT NULL,
    reversal_of uuid,
    fingerprint text NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    projected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK(event_id=journal_transaction_id),
    CHECK(from_account_id<>to_account_id),
    CHECK((type='transfer.reversed')=(reversal_of IS NOT NULL))
);
CREATE TABLE activity_summaries (
    account_id uuid PRIMARY KEY,
    incoming_minor numeric NOT NULL DEFAULT 0 CHECK(incoming_minor>=0 AND incoming_minor=trunc(incoming_minor)),
    outgoing_minor numeric NOT NULL DEFAULT 0 CHECK(outgoing_minor>=0 AND outgoing_minor=trunc(outgoing_minor)),
    incoming_count numeric NOT NULL DEFAULT 0 CHECK(incoming_count>=0 AND incoming_count=trunc(incoming_count)),
    outgoing_count numeric NOT NULL DEFAULT 0 CHECK(outgoing_count>=0 AND outgoing_count=trunc(outgoing_count)),
    entry_sequence bigint NOT NULL DEFAULT 0 CHECK(entry_sequence>=0)
);
CREATE TABLE activity_entries (
    event_id uuid NOT NULL REFERENCES activity_inbox(event_id),
    account_id uuid NOT NULL,
    direction text NOT NULL CHECK(direction IN ('incoming','outgoing')),
    amount_minor bigint NOT NULL CHECK(amount_minor>0),
    account_sequence bigint NOT NULL CHECK(account_sequence>0),
    PRIMARY KEY(event_id,account_id),
    UNIQUE(account_id,account_sequence)
);
GRANT USAGE ON SCHEMA public TO activity_app;
GRANT SELECT ON activity_inbox,activity_summaries,activity_entries TO activity_app;
GRANT INSERT ON activity_inbox,activity_entries TO activity_app;
GRANT INSERT(account_id) ON activity_summaries TO activity_app;
GRANT UPDATE(incoming_minor,outgoing_minor,incoming_count,outgoing_count,entry_sequence) ON activity_summaries TO activity_app;
