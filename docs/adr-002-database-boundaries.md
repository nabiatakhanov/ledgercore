# ADR 002: enforce journal shape at commit, audit the projection independently

Status: accepted.

The schema owner applies embedded, checksummed migrations. The runtime login
`ledger_app` owns no table and receives SELECT, narrowly scoped account writes,
and append privileges. `ledger_fixture` has the same data access plus permission
to create funding transactions. They must never be members of the owner role.
The public schema is not writable by runtime roles. Trigger function name
resolution uses a fixed search path with the temporary schema last.

A deferred constraint trigger fires for both transaction headers and postings.
At commit it requires two exact opposite postings, supported account kinds, and
valid reversal linkage. A header without postings therefore fails too. This is a
cross-row invariant that an ordinary CHECK constraint cannot establish. Unique
`reversal_of` prevents two successful full reversals of the same transfer.

Column grants prevent runtime modification of journal headers/postings, and an
immutable trigger rejects attempts even if UPDATE permission is accidentally
granted later. A posting append guard binds a new header to its creating xid8;
runtime users cannot add postings to a previously committed transaction. The
idempotency reservation has a separate deferred completion check and its final
outcome is immutable.

The algorithm is responsible for locking both accounts, checking spendable
funds, checked balance arithmetic, and keeping the balance/sequence projection
in step with the journal. A runtime connection with arbitrary SQL can corrupt
the projection through its necessary account UPDATE privilege. Reconciliation
detects that, using NUMERIC sums and a single repeatable-read read-only snapshot.
It does not reuse the write path's validation functions. Schema owners can
disable triggers or alter constraints; this does not protect against a malicious
database administrator.

Consequences: direct SQL fixture writers must insert headers and postings in
one transaction. Migrations run with owner credentials, never API credentials.
Reconciliation reports corruption and never repairs it automatically. Cascading
deletes and automatic journal retention are deliberately absent.
