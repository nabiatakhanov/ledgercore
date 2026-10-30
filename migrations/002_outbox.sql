-- New transfer/reversal events start at this migration; there is no backfill.
CREATE TABLE outbox_events (
    event_id uuid PRIMARY KEY,
    journal_transaction_id uuid NOT NULL UNIQUE REFERENCES journal_transactions(id),
    event_type text NOT NULL CHECK (event_type IN ('transfer.created','transfer.reversed')),
    schema_version integer NOT NULL CHECK (schema_version=1),
    occurred_at timestamptz NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object' AND octet_length(payload::text)<=16384),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    lease_owner uuid,
    lease_until timestamptz,
    attempts bigint NOT NULL DEFAULT 0 CHECK (attempts>=0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error_code text CHECK (last_error_code IS NULL OR last_error_code IN ('publish_failed')),
    CHECK (event_id=journal_transaction_id),
    CHECK ((lease_owner IS NULL)=(lease_until IS NULL))
);
CREATE INDEX outbox_pending_idx ON outbox_events(next_attempt_at,created_at,event_id) WHERE published_at IS NULL;

CREATE FUNCTION validate_outbox_event() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE header journal_transactions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT header FROM journal_transactions WHERE id=NEW.journal_transaction_id;
    IF header.creation_xid<>pg_current_xact_id() OR header.kind NOT IN ('transfer','reversal')
       OR NEW.occurred_at<>header.created_at
       OR NEW.event_type<>(CASE header.kind WHEN 'transfer' THEN 'transfer.created' ELSE 'transfer.reversed' END)
       OR NEW.payload->>'event_id' IS DISTINCT FROM NEW.event_id::text
       OR NEW.payload->>'journal_transaction_id' IS DISTINCT FROM header.id::text
       OR NEW.payload->>'type' IS DISTINCT FROM NEW.event_type
       OR NEW.payload->'schema_version' IS DISTINCT FROM '1'::jsonb
       OR NEW.payload->>'from_account_id' IS DISTINCT FROM header.from_account_id::text
       OR NEW.payload->>'to_account_id' IS DISTINCT FROM header.to_account_id::text
       OR NEW.payload->>'amount_minor' IS DISTINCT FROM header.amount_minor::text
       OR NEW.payload->>'currency' IS DISTINCT FROM header.currency
       OR (NEW.payload->>'occurred_at')::timestamptz IS DISTINCT FROM header.created_at
       OR NEW.payload->>'reversal_of' IS DISTINCT FROM header.reversal_of::text THEN
        RAISE EXCEPTION 'outbox must exactly describe a new transfer or reversal' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_validate BEFORE INSERT ON outbox_events FOR EACH ROW EXECUTE FUNCTION validate_outbox_event();

CREATE FUNCTION require_transaction_outbox() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    IF NEW.kind IN ('transfer','reversal') AND NOT EXISTS(SELECT 1 FROM outbox_events WHERE journal_transaction_id=NEW.id) THEN
        RAISE EXCEPTION 'new transfer or reversal requires its atomic outbox event' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER journal_requires_outbox AFTER INSERT ON journal_transactions
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_transaction_outbox();

CREATE FUNCTION protect_outbox_payload() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    IF (NEW.event_id,NEW.journal_transaction_id,NEW.event_type,NEW.schema_version,NEW.occurred_at,NEW.payload,NEW.created_at)
       IS DISTINCT FROM (OLD.event_id,OLD.journal_transaction_id,OLD.event_type,OLD.schema_version,OLD.occurred_at,OLD.payload,OLD.created_at) THEN
        RAISE EXCEPTION 'logical event is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.published_at IS NOT NULL THEN
        RAISE EXCEPTION 'published delivery state is final' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_payload_immutable BEFORE UPDATE ON outbox_events FOR EACH ROW EXECUTE FUNCTION protect_outbox_payload();

REVOKE ALL ON outbox_events FROM PUBLIC;
REVOKE ALL ON FUNCTION validate_outbox_event(),require_transaction_outbox(),protect_outbox_payload() FROM PUBLIC;
GRANT SELECT,INSERT ON outbox_events TO ledger_app,ledger_fixture;
GRANT USAGE ON SCHEMA public TO ledger_publisher;
GRANT SELECT ON outbox_events TO ledger_publisher;
GRANT UPDATE(published_at,lease_owner,lease_until,attempts,next_attempt_at,last_error_code) ON outbox_events TO ledger_publisher;
