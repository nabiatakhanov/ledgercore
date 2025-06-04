-- Run as the schema owner, after creating ledger_app and ledger_fixture.
-- Runtime login roles must not own tables or have membership in the owner role.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE TABLE accounts (
    id uuid PRIMARY KEY,
    name text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 128),
    kind text NOT NULL DEFAULT 'client' CHECK (kind IN ('client', 'system')),
    currency text NOT NULL DEFAULT 'RUB' CHECK (currency = 'RUB'),
    balance_minor bigint NOT NULL DEFAULT 0,
    entry_sequence bigint NOT NULL DEFAULT 0 CHECK (entry_sequence >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (kind = 'system' OR balance_minor >= 0),
    CHECK (kind <> 'system' OR id = '00000000-0000-0000-0000-000000000001')
);

INSERT INTO accounts (id, name, kind)
VALUES ('00000000-0000-0000-0000-000000000001', 'Test funding balancing account', 'system');

CREATE TABLE journal_transactions (
    id uuid PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('transfer', 'funding', 'reversal')),
    from_account_id uuid NOT NULL REFERENCES accounts(id),
    to_account_id uuid NOT NULL REFERENCES accounts(id),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency = 'RUB'),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (
        jsonb_typeof(metadata) = 'object' AND octet_length(metadata::text) <= 4096),
    reversal_of uuid UNIQUE REFERENCES journal_transactions(id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    creation_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    CHECK (from_account_id <> to_account_id),
    CHECK ((kind = 'reversal') = (reversal_of IS NOT NULL))
);

CREATE TABLE postings (
    transaction_id uuid NOT NULL REFERENCES journal_transactions(id),
    account_id uuid NOT NULL REFERENCES accounts(id),
    delta_minor bigint NOT NULL CHECK (delta_minor <> 0),
    account_sequence bigint NOT NULL CHECK (account_sequence > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (transaction_id, account_id),
    UNIQUE (account_id, account_sequence)
);
-- The UNIQUE index above supports reverse B-tree scans for bounded keyset history.
CREATE INDEX journal_transactions_from_idx ON journal_transactions(from_account_id);
CREATE INDEX journal_transactions_to_idx ON journal_transactions(to_account_id);

CREATE TABLE idempotency_records (
    kind text NOT NULL CHECK (kind IN ('transfer', 'funding', 'reversal')),
    key text NOT NULL CHECK (octet_length(key) BETWEEN 1 AND 128),
    fingerprint text NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    status integer CHECK (status BETWEEN 200 AND 499),
    response jsonb CHECK (jsonb_typeof(response) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (kind, key),
    CHECK ((status IS NULL) = (response IS NULL))
);

CREATE FUNCTION reject_journal_mutation() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    RAISE EXCEPTION 'committed journal is append only' USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER journal_immutable BEFORE UPDATE OR DELETE ON journal_transactions
    FOR EACH ROW EXECUTE FUNCTION reject_journal_mutation();
CREATE TRIGGER postings_immutable BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION reject_journal_mutation();

CREATE FUNCTION prepare_journal_header() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    NEW.creation_xid := pg_current_xact_id();
    IF NEW.kind = 'funding' AND NOT pg_has_role(session_user, 'ledger_fixture', 'MEMBER') THEN
        RAISE EXCEPTION 'funding requires fixture role' USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER journal_header_guard BEFORE INSERT ON journal_transactions
    FOR EACH ROW EXECUTE FUNCTION prepare_journal_header();

CREATE FUNCTION guard_posting_append() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE header_xid xid8;
BEGIN
    SELECT creation_xid INTO header_xid FROM journal_transactions WHERE id = NEW.transaction_id;
    IF header_xid IS NOT NULL AND header_xid <> pg_current_xact_id() THEN
        RAISE EXCEPTION 'cannot append to previously committed journal transaction' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER posting_append_guard BEFORE INSERT ON postings
    FOR EACH ROW EXECUTE FUNCTION guard_posting_append();

-- Triggered by both tables: a header with no postings must fail at COMMIT too.
CREATE FUNCTION check_journal_transaction() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE
    target uuid;
    header journal_transactions%ROWTYPE;
    original journal_transactions%ROWTYPE;
    n bigint;
    total numeric;
    expected bigint;
    source_kind text;
    destination_kind text;
BEGIN
    IF TG_TABLE_NAME = 'journal_transactions' THEN target := NEW.id;
    ELSE target := NEW.transaction_id; END IF;
    SELECT * INTO STRICT header FROM journal_transactions WHERE id = target;
    SELECT count(*), COALESCE(sum(delta_minor::numeric), 0),
        count(*) FILTER (WHERE
            (account_id = header.from_account_id AND delta_minor = -header.amount_minor)
            OR (account_id = header.to_account_id AND delta_minor = header.amount_minor))
    INTO n, total, expected FROM postings WHERE transaction_id = target;
    IF n <> 2 OR total <> 0 OR expected <> 2 THEN
        RAISE EXCEPTION 'journal transaction needs two exact balanced postings' USING ERRCODE = '23514';
    END IF;
    SELECT kind INTO STRICT source_kind FROM accounts WHERE id = header.from_account_id;
    SELECT kind INTO STRICT destination_kind FROM accounts WHERE id = header.to_account_id;
    IF header.kind = 'funding' THEN
        IF source_kind <> 'system' OR destination_kind <> 'client' THEN
            RAISE EXCEPTION 'funding must debit system and credit client' USING ERRCODE = '23514';
        END IF;
    ELSIF source_kind <> 'client' OR destination_kind <> 'client' THEN
        RAISE EXCEPTION 'transfers and reversals require client accounts' USING ERRCODE = '23514';
    END IF;
    IF header.kind = 'reversal' THEN
        SELECT * INTO STRICT original FROM journal_transactions WHERE id = header.reversal_of;
        IF original.kind <> 'transfer'
           OR header.from_account_id <> original.to_account_id
           OR header.to_account_id <> original.from_account_id
           OR header.amount_minor <> original.amount_minor
           OR header.currency <> original.currency THEN
            RAISE EXCEPTION 'invalid full reversal' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER journal_balanced AFTER INSERT ON journal_transactions
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_journal_transaction();
CREATE CONSTRAINT TRIGGER postings_balanced AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_journal_transaction();

CREATE FUNCTION guard_idempotency_update() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    IF OLD.status IS NOT NULL THEN
        RAISE EXCEPTION 'final idempotency outcome is immutable' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER idempotency_immutable BEFORE UPDATE ON idempotency_records
    FOR EACH ROW EXECUTE FUNCTION guard_idempotency_update();

CREATE FUNCTION check_idempotency_complete() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE final_status integer;
BEGIN
    SELECT status INTO final_status FROM idempotency_records WHERE kind = NEW.kind AND key = NEW.key;
    IF final_status IS NULL THEN
        RAISE EXCEPTION 'idempotency reservation must finish in same transaction' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER idempotency_complete AFTER INSERT OR UPDATE ON idempotency_records
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_idempotency_complete();

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO ledger_app, ledger_fixture;
GRANT SELECT ON accounts, journal_transactions, postings, idempotency_records TO ledger_app, ledger_fixture;
GRANT INSERT (id, name) ON accounts TO ledger_app, ledger_fixture;
GRANT UPDATE (balance_minor, entry_sequence) ON accounts TO ledger_app, ledger_fixture;
GRANT INSERT ON journal_transactions, postings, idempotency_records TO ledger_app, ledger_fixture;
GRANT UPDATE (status, response) ON idempotency_records TO ledger_app, ledger_fixture;
-- Trigger invocation does not require runtime EXECUTE grants on trigger functions.


