-- A durable full-XID watermark lets an independent audit find missing events
-- without relying on wall-clock order. Existing v002 events remain audited by
-- their immutable outbox IDs; there is still no implicit pre-v002 backfill.
CREATE TABLE event_audit_boundary (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    writer_xid xid8 NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO event_audit_boundary(singleton,writer_xid) VALUES(true,pg_current_xact_id());
REVOKE ALL ON event_audit_boundary FROM PUBLIC;
GRANT SELECT ON event_audit_boundary TO ledger_app,ledger_fixture;
