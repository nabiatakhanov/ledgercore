package tests

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"ledgercore/internal/events"
	"ledgercore/internal/ledger"
)

func TestAtomicOutboxFinancialOutcomes(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 100), h.account(t, 0)
	key := ledger.NewID()
	req := request(a, b, 10)
	req.Metadata = map[string]string{"private": "must-not-be-in-event"}
	o, err := h.writer.Transfer(h.ctx, key, req)
	if err != nil || o.Error != nil {
		t.Fatalf("transfer %+v %v", o, err)
	}
	r, err := h.writer.Transfer(h.ctx, key, req)
	if err != nil || !r.Replayed || r.Transaction.ID != o.Transaction.ID {
		t.Fatal("replay", r, err)
	}
	denied := h.transfer(t, ledger.NewID(), a, b, 1000)
	expectCode(t, denied, "insufficient_funds")
	reversed, err := h.writer.Reverse(h.ctx, ledger.NewID(), o.Transaction.ID)
	if err != nil || reversed.Error != nil {
		t.Fatal(reversed, err)
	}
	var count int
	if err = h.app.QueryRow(h.ctx, `SELECT count(*) FROM outbox_events WHERE (payload->>'from_account_id')=$1 OR (payload->>'to_account_id')=$1`, a).Scan(&count); err != nil || count != 2 {
		t.Fatalf("logicalevents=%d err=%v", count, err)
	}
	for _, tx := range []*ledger.Transaction{o.Transaction, reversed.Transaction} {
		var raw []byte
		if err = h.app.QueryRow(h.ctx, `SELECT payload FROM outbox_events WHERE journal_transaction_id=$1`, tx.ID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		e, err := events.Decode(raw)
		if err != nil || e.EventID != tx.ID || e.AmountMinor != "10" || !e.OccurredAt.Equal(tx.CreatedAt) {
			t.Fatal(e, err)
		}
		if strings.Contains(string(raw), "must-not-be-in-event") {
			t.Fatal("metadata leaked")
		}
		if tx.Kind == "reversal" && e.ReversalOf != o.Transaction.ID {
			t.Fatal("missing original")
		}
	}
	_, err = h.app.Exec(h.ctx, `UPDATE outbox_events SET payload='{}' WHERE event_id=$1`, o.Transaction.ID)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
		t.Fatal("app can edit event", err)
	}
	h.clean(t)
}

func TestBalancedHeaderWithoutOutboxCannotCommit(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 10), h.account(t, 0)
	id := ledger.NewID()
	tx, err := h.app.Begin(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(h.ctx, `INSERT INTO journal_transactions(id,kind,from_account_id,to_account_id,amount_minor,currency) VALUES($1,'transfer',$2,$3,1,'RUB')`, id, a, b)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(h.ctx, `INSERT INTO postings(transaction_id,account_id,delta_minor,account_sequence) VALUES($1,$2,-1,10000),($1,$3,1,10000)`, id, a, b)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit(h.ctx)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "23514" {
		t.Fatal("emptyoutboxcommitted", err)
	}
	var count int
	if err = h.app.QueryRow(h.ctx, `SELECT count(*) FROM journal_transactions WHERE id=$1`, id).Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	h.clean(t)
}

func TestInvalidOutboxPayloadRollsBack(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 10), h.account(t, 0)
	id := ledger.NewID()
	tx, err := h.app.Begin(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var transfer ledger.Transaction
	transfer.ID = id
	transfer.Kind = "transfer"
	transfer.FromAccountID = a
	transfer.ToAccountID = b
	transfer.AmountMinor = "1"
	transfer.Currency = "RUB"
	if err = tx.QueryRow(h.ctx, `INSERT INTO journal_transactions(id,kind,from_account_id,to_account_id,amount_minor,currency) VALUES($1,'transfer',$2,$3,1,'RUB') RETURNING created_at`, id, a, b).Scan(&transfer.CreatedAt); err != nil {
		t.Fatal(err)
	}
	e, err := events.FromTransaction(transfer)
	if err != nil {
		t.Fatal(err)
	}
	e.AmountMinor = "2"
	payload, _ := json.Marshal(e)
	_, err = tx.Exec(h.ctx, `INSERT INTO outbox_events(event_id,journal_transaction_id,event_type,schema_version,occurred_at,payload) VALUES($1,$1,'transfer.created',1,$2,$3)`, id, e.OccurredAt, payload)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "23514" {
		t.Fatal("mismatchedpayloadaccepted", err)
	}
	_ = tx.Rollback(h.ctx)
	h.clean(t)
}
