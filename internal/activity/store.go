// Package activity owns the independent, rebuildable event projection database.
package activity

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"ledgercore/internal/events"
	"ledgercore/internal/ledger"
)

var ErrEventConflict = &events.InvalidEvent{Code: "conflicting_event"}
var ErrSequenceExhausted = errors.New("activity account sequence exhausted")

type Store struct{ Pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{Pool: pool} }

type Entry struct {
	EventID              string    `json:"event_id"`
	JournalTransactionID string    `json:"journal_transaction_id"`
	Type                 string    `json:"type"`
	Direction            string    `json:"direction"`
	AmountMinor          string    `json:"amount_minor"`
	Currency             string    `json:"currency"`
	OccurredAt           time.Time `json:"occurred_at"`
	ProjectedAt          time.Time `json:"projected_at"`
	ReversalOf           *string   `json:"reversal_of,omitempty"`
	Sequence             int64     `json:"sequence,string"`
}
type Summary struct {
	AccountID     string `json:"account_id"`
	IncomingMinor string `json:"incoming_minor"`
	OutgoingMinor string `json:"outgoing_minor"`
	IncomingCount string `json:"incoming_count"`
	OutgoingCount string `json:"outgoing_count"`
}
type Page struct {
	Entries    []Entry `json:"entries"`
	Summary    Summary `json:"summary"`
	NextCursor string  `json:"next_cursor,omitempty"`
}

// Apply atomically registers the inbox ID, inserts two activity entries and
// updates commutative numeric summaries. true means a previously applied replay.
// Reversals need no original-event foreign key: cross-partition delivery can
// expose the reversal first and still converges to the same totals.
func (s *Store) Apply(ctx context.Context, e events.Event) (bool, error) {
	if err := e.Validate(); err != nil {
		return false, err
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return false, err
	}
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(payload))
	var reversal any
	if e.ReversalOf != "" {
		reversal = e.ReversalOf
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	result, err := tx.Exec(ctx, `INSERT INTO activity_inbox(event_id,schema_version,type,journal_transaction_id,from_account_id,to_account_id,amount_minor,currency,occurred_at,reversal_of,fingerprint) VALUES($1,$2,$3,$4,$5,$6,$7::bigint,$8,$9,$10,$11) ON CONFLICT(event_id) DO NOTHING`, e.EventID, e.SchemaVersion, e.Type, e.JournalTransactionID, e.FromAccountID, e.ToAccountID, e.AmountMinor, e.Currency, e.OccurredAt, reversal, fingerprint)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() == 0 {
		var saved string
		if err = tx.QueryRow(ctx, `SELECT fingerprint FROM activity_inbox WHERE event_id=$1`, e.EventID).Scan(&saved); err != nil {
			return false, err
		}
		if saved != fingerprint {
			return false, ErrEventConflict
		}
		return true, nil
	}
	ids := []string{e.FromAccountID, e.ToAccountID}
	if ids[0] > ids[1] {
		ids[0], ids[1] = ids[1], ids[0]
	}
	// Insert and lock in the same global account order, including first events.
	for _, id := range ids {
		if _, err = tx.Exec(ctx, `INSERT INTO activity_summaries(account_id) VALUES($1) ON CONFLICT DO NOTHING`, id); err != nil {
			return false, err
		}
	}
	rows, err := tx.Query(ctx, `SELECT account_id::text,entry_sequence FROM activity_summaries WHERE account_id=$1 OR account_id=$2 ORDER BY account_id FOR UPDATE`, ids[0], ids[1])
	if err != nil {
		return false, err
	}
	sequences := map[string]int64{}
	for rows.Next() {
		var id string
		var seq int64
		if err = rows.Scan(&id, &seq); err != nil {
			rows.Close()
			return false, err
		}
		if seq == math.MaxInt64 {
			rows.Close()
			return false, ErrSequenceExhausted
		}
		sequences[id] = seq + 1
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		direction := "incoming"
		if id == e.FromAccountID {
			direction = "outgoing"
		}
		if _, err = tx.Exec(ctx, `UPDATE activity_summaries SET incoming_minor=incoming_minor+CASE WHEN $2='incoming' THEN $3::numeric ELSE 0 END,outgoing_minor=outgoing_minor+CASE WHEN $2='outgoing' THEN $3::numeric ELSE 0 END,incoming_count=incoming_count+CASE WHEN $2='incoming' THEN 1 ELSE 0 END,outgoing_count=outgoing_count+CASE WHEN $2='outgoing' THEN 1 ELSE 0 END,entry_sequence=$4 WHERE account_id=$1`, id, direction, e.AmountMinor, sequences[id]); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO activity_entries(event_id,account_id,direction,amount_minor,account_sequence) VALUES($1,$2,$3,$4::bigint,$5)`, e.EventID, id, direction, e.AmountMinor, sequences[id]); err != nil {
			return false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("activity commit outcome unknown; replay same event: %w", err)
	}
	return false, nil
}

type cursor struct {
	Account string `json:"a"`
	Upper   int64  `json:"u"`
	Before  int64  `json:"b"`
}

func parseCursor(raw, id string) (cursor, error) {
	c := cursor{Account: id}
	if raw == "" {
		return c, nil
	}
	if len(raw) > 256 {
		return c, ledger.Invalid("invalid activity cursor")
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return c, ledger.Invalid("invalid activity cursor")
	}
	if err = json.Unmarshal(b, &c); err != nil || c.Account != id || c.Upper <= 0 || c.Before <= 0 || c.Before > c.Upper {
		return c, ledger.Invalid("invalid activity cursor")
	}
	return c, nil
}

// GetAccountActivity is a snapshot read of a statistical projection, never a
// spendable balance. Empty activity does not prove that a ledger account exists.
func (s *Store) GetAccountActivity(ctx context.Context, id, raw string, limit int) (Page, error) {
	page := Page{Entries: []Entry{}}
	id, err := ledger.NormalizeID(id)
	if err != nil {
		return page, err
	}
	if limit < 1 || limit > 100 {
		return page, ledger.Invalid("limit must be 1..100")
	}
	c, err := parseCursor(raw, id)
	if err != nil {
		return page, err
	}
	page.Summary = Summary{AccountID: id, IncomingMinor: "0", OutgoingMinor: "0", IncomingCount: "0", OutgoingCount: "0"}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return page, err
	}
	defer rollback(tx)
	var upper int64
	err = tx.QueryRow(ctx, `SELECT incoming_minor::text,outgoing_minor::text,incoming_count::text,outgoing_count::text,entry_sequence FROM activity_summaries WHERE account_id=$1`, id).Scan(&page.Summary.IncomingMinor, &page.Summary.OutgoingMinor, &page.Summary.IncomingCount, &page.Summary.OutgoingCount, &upper)
	if err != nil && err != pgx.ErrNoRows {
		return page, err
	}
	if raw == "" {
		c.Upper = upper
	}
	rows, err := tx.Query(ctx, `SELECT i.event_id::text,i.journal_transaction_id::text,i.type,p.direction,p.amount_minor::text,i.currency,i.occurred_at,i.projected_at,i.reversal_of::text,p.account_sequence FROM activity_entries p JOIN activity_inbox i ON i.event_id=p.event_id WHERE p.account_id=$1 AND p.account_sequence<=$2 AND ($3::bigint=0 OR p.account_sequence<$3) ORDER BY p.account_sequence DESC LIMIT $4`, id, c.Upper, c.Before, limit+1)
	if err != nil {
		return page, err
	}
	for rows.Next() {
		var e Entry
		if err = rows.Scan(&e.EventID, &e.JournalTransactionID, &e.Type, &e.Direction, &e.AmountMinor, &e.Currency, &e.OccurredAt, &e.ProjectedAt, &e.ReversalOf, &e.Sequence); err != nil {
			rows.Close()
			return page, err
		}
		page.Entries = append(page.Entries, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return page, err
	}
	if len(page.Entries) > limit {
		page.Entries = page.Entries[:limit]
		c.Before = page.Entries[limit-1].Sequence
		b, _ := json.Marshal(c)
		page.NextCursor = base64.RawURLEncoding.EncodeToString(b)
	}
	if err = tx.Commit(ctx); err != nil {
		return page, err
	}
	return page, nil
}
