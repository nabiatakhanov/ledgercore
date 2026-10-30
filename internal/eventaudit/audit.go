// Package eventaudit is an administrative cross-service audit, not a service
// dependency: it uses separate read-only connections without a distributed tx.
package eventaudit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"ledgercore/internal/activity"
	"ledgercore/internal/events"
	"ledgercore/internal/ledger"
)

type Report struct {
	OK              bool            `json:"ok"`
	LedgerEvents    int64           `json:"ledger_events"`
	ProjectedEvents int64           `json:"projected_events"`
	PendingEvents   int64           `json:"pending_events"`
	Activity        activity.Report `json:"activity"`
	Issues          []string        `json:"issues"`
}

// Run requires a quiescent writer when requireCaughtUp is true. Activity's own
// audit is snapshot-consistent; no global snapshot is claimed across databases.
// Source payloads are immutable, so reading them after the activity snapshot is
// safe for verifying every projected ID and amount. Missing events are reported
// as lag, rather than mistaken for a wrong authoritative money balance.
func Run(ctx context.Context, ledgerDB, activityDB *pgxpool.Pool, requireCaughtUp bool) (Report, error) {
	r := Report{Issues: []string{}}
	var err error
	r.Activity, err = activity.Audit(ctx, activityDB)
	if err != nil {
		return r, err
	}
	projection, err := activityDB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return r, err
	}
	defer func() { _ = projection.Rollback(ctx) }()
	rows, err := projection.Query(ctx, `SELECT event_id::text,schema_version,type,journal_transaction_id::text,occurred_at,from_account_id::text,to_account_id::text,amount_minor::text,currency,COALESCE(reversal_of::text,'') FROM activity_inbox`)
	if err != nil {
		return r, err
	}
	projected := map[string]events.Event{}
	for rows.Next() {
		var e events.Event
		if err = rows.Scan(&e.EventID, &e.SchemaVersion, &e.Type, &e.JournalTransactionID, &e.OccurredAt, &e.FromAccountID, &e.ToAccountID, &e.AmountMinor, &e.Currency, &e.ReversalOf); err != nil {
			rows.Close()
			return r, err
		}
		projected[e.EventID] = e
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	if err = projection.Commit(ctx); err != nil {
		return r, err
	}
	r.ProjectedEvents = int64(len(projected))
	source, err := ledgerDB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return r, err
	}
	defer func() { _ = source.Rollback(ctx) }()
	rows, err = source.Query(ctx, `SELECT o.event_id::text,o.payload,COALESCE(h.id::text,''),COALESCE(h.kind,''),COALESCE(h.from_account_id::text,''),COALESCE(h.to_account_id::text,''),COALESCE(h.amount_minor::text,''),COALESCE(h.currency,''),h.reversal_of::text,COALESCE(h.created_at,o.occurred_at) FROM outbox_events o LEFT JOIN journal_transactions h ON h.id=o.journal_transaction_id`)
	if err != nil {
		return r, err
	}
	found := map[string]bool{}
	for rows.Next() {
		var id string
		var raw []byte
		var header ledger.Transaction
		if err = rows.Scan(&id, &raw, &header.ID, &header.Kind, &header.FromAccountID, &header.ToAccountID, &header.AmountMinor, &header.Currency, &header.ReversalOf, &header.CreatedAt); err != nil {
			rows.Close()
			return r, err
		}
		r.LedgerEvents++
		found[id] = true
		expected, err := events.Decode(raw)
		if err != nil {
			r.Issues = append(r.Issues, "invalid source event "+id)
			continue
		}
		fromJournal, journalErr := events.FromTransaction(header)
		if journalErr != nil || fromJournal != expected {
			r.Issues = append(r.Issues, "outbox differs from authoritative journal "+id)
		}
		actual, exists := projected[id]
		if !exists {
			r.PendingEvents++
			continue
		}
		actual.OccurredAt = actual.OccurredAt.UTC()
		a, _ := json.Marshal(actual)
		b, _ := json.Marshal(expected)
		if string(a) != string(b) {
			r.Issues = append(r.Issues, "event payload mismatch "+id)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	rows, err = source.Query(ctx, `SELECT h.id::text FROM journal_transactions h CROSS JOIN event_audit_boundary b WHERE h.kind IN ('transfer','reversal') AND h.creation_xid>=b.writer_xid AND NOT EXISTS(SELECT 1 FROM outbox_events o WHERE o.journal_transaction_id=h.id)`)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return r, err
		}
		r.Issues = append(r.Issues, "new journal operation has no outbox event "+id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	if err = source.Commit(ctx); err != nil {
		return r, err
	}
	for id := range projected {
		if !found[id] {
			r.Issues = append(r.Issues, "projection has no source event "+id)
		}
	}
	if requireCaughtUp && r.PendingEvents > 0 {
		r.Issues = append(r.Issues, fmt.Sprintf("projection still has %d pending events", r.PendingEvents))
	}
	r.OK = r.Activity.OK && len(r.Issues) == 0
	return r, nil
}
