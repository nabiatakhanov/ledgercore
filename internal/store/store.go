// Package store implements the ledger's transactional PostgreSQL writer.
package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"ledgercore/internal/ledger"
)

type Observer interface {
	Operation(kind, outcome string, seconds float64)
	Retry()
	Conflict()
	LockWait(seconds float64)
}
type Store struct {
	Pool     *pgxpool.Pool
	observer Observer
}

func New(pool *pgxpool.Pool, observer Observer) *Store { return &Store{Pool: pool, observer: observer} }

func (s *Store) CreateAccount(ctx context.Context, name string) (ledger.Account, error) {
	if err := ledger.ValidateName(name); err != nil {
		return ledger.Account{}, err
	}
	return scanAccount(s.Pool.QueryRow(ctx, `INSERT INTO accounts(id,name) VALUES($1,$2) RETURNING id::text,name,kind,currency,balance_minor,created_at`, ledger.NewID(), name))
}
func scanAccount(row pgx.Row) (ledger.Account, error) {
	var a ledger.Account
	err := row.Scan(&a.ID, &a.Name, &a.Kind, &a.Currency, &a.BalanceMinor, &a.CreatedAt)
	return a, notFound(err)
}
func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Problem("not_found", "resource not found", 404)
	}
	return err
}
func (s *Store) Account(ctx context.Context, id string) (ledger.Account, error) {
	id, err := ledger.NormalizeID(id)
	if err != nil {
		return ledger.Account{}, err
	}
	return scanAccount(s.Pool.QueryRow(ctx, `SELECT id::text,name,kind,currency,balance_minor,created_at FROM accounts WHERE id=$1`, id))
}

func (s *Store) Transfer(ctx context.Context, key string, req ledger.TransferRequest) (ledger.Outcome, error) {
	r, n, err := req.Normalize()
	if err != nil {
		return ledger.Outcome{}, err
	}
	return s.execute(ctx, "transfer", key, "", r, n)
}
func (s *Store) Funding(ctx context.Context, key, to string, amount ledger.Money) (ledger.Outcome, error) {
	r, n, err := (ledger.TransferRequest{FromAccountID: ledger.SystemAccountID, ToAccountID: to, AmountMinor: amount.String(), Currency: "RUB"}).Normalize()
	if err != nil {
		return ledger.Outcome{}, err
	}
	return s.execute(ctx, "funding", key, "", r, n)
}
func (s *Store) Reverse(ctx context.Context, key, id string) (ledger.Outcome, error) {
	id, err := ledger.NormalizeID(id)
	if err != nil {
		return ledger.Outcome{}, err
	}
	return s.execute(ctx, "reversal", key, id, ledger.TransferRequest{}, 0)
}
func retryable(err error) bool {
	var pgerr *pgconn.PgError
	return errors.As(err, &pgerr) && (pgerr.Code == "40001" || pgerr.Code == "40P01")
}

func (s *Store) execute(ctx context.Context, kind, key, original string, req ledger.TransferRequest, n ledger.Money) (out ledger.Outcome, err error) {
	if err = ledger.ValidateKey(key); err != nil {
		return out, err
	}
	start := time.Now()
	defer func() {
		if s.observer != nil {
			label := "success"
			if err != nil {
				label = "infrastructure_error"
			} else if out.Replayed {
				label = "replayed"
			} else if out.Error != nil {
				label = out.Error.Code
			}
			s.observer.Operation(kind, label, time.Since(start).Seconds())
		}
	}()
	fingerprint := ledger.Fingerprint(kind, original, req)
	for attempt := 0; attempt < 3; attempt++ {
		out, err = s.attempt(ctx, kind, key, original, fingerprint, req, n)
		if err == nil || !retryable(err) || attempt == 2 {
			return out, err
		}
		if s.observer != nil {
			s.observer.Retry()
		}
		timer := time.NewTimer(time.Duration(10*(1<<attempt)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ledger.Outcome{}, ctx.Err()
		case <-timer.C:
		}
	}
	return out, err
}

func (s *Store) attempt(ctx context.Context, kind, key, original, fingerprint string, req ledger.TransferRequest, n ledger.Money) (ledger.Outcome, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ledger.Outcome{}, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	insert, err := tx.Exec(ctx, `INSERT INTO idempotency_records(kind,key,fingerprint) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, kind, key, fingerprint)
	if err != nil {
		return ledger.Outcome{}, err
	}
	if insert.RowsAffected() == 0 {
		var saved string
		var status int
		var body []byte
		err = tx.QueryRow(ctx, `SELECT fingerprint,status,response FROM idempotency_records WHERE kind=$1 AND key=$2`, kind, key).Scan(&saved, &status, &body)
		if err != nil {
			return ledger.Outcome{}, err
		}
		if saved != fingerprint {
			if s.observer != nil {
				s.observer.Conflict()
			}
			return ledger.Outcome{Status: 409, Error: ledger.Problem("idempotency_conflict", "key already used with different parameters", 409)}, nil
		}
		var o ledger.Outcome
		if err = json.Unmarshal(body, &o); err != nil {
			return o, fmt.Errorf("decode saved result: %w", err)
		}
		o.Status = status
		o.Replayed = true
		if o.Error != nil {
			o.Error.Status = status
		}
		return o, nil
	}
	o, err := s.write(ctx, tx, kind, original, req, n)
	if err != nil {
		return ledger.Outcome{}, err
	}
	b, err := json.Marshal(o)
	if err != nil {
		return ledger.Outcome{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE idempotency_records SET status=$3,response=$4 WHERE kind=$1 AND key=$2`, kind, key, o.Status, b); err != nil {
		return ledger.Outcome{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ledger.Outcome{}, fmt.Errorf("commit outcome unknown; retry same idempotency key: %w", err)
	}
	return o, nil
}

func rejected(code, message string, status int) ledger.Outcome {
	return ledger.Outcome{Status: status, Error: ledger.Problem(code, message, status)}
}
func business(err error) (ledger.Outcome, error) {
	var p *ledger.Error
	if errors.As(err, &p) {
		return ledger.Outcome{Status: p.Status, Error: p}, nil
	}
	return ledger.Outcome{}, err
}

type lockedAccount struct {
	id, kind     string
	balance, seq int64
}

func (s *Store) write(ctx context.Context, tx pgx.Tx, kind, original string, req ledger.TransferRequest, n ledger.Money) (ledger.Outcome, error) {
	if kind == "reversal" {
		var origKind string
		err := tx.QueryRow(ctx, `SELECT kind,to_account_id::text,from_account_id::text,amount_minor::text,currency FROM journal_transactions WHERE id=$1`, original).Scan(&origKind, &req.FromAccountID, &req.ToAccountID, &req.AmountMinor, &req.Currency)
		if errors.Is(err, pgx.ErrNoRows) {
			return rejected("not_found", "transfer not found", 404), nil
		}
		if err != nil {
			return ledger.Outcome{}, err
		}
		if origKind != "transfer" {
			return rejected("not_reversible", "only ordinary transfers can be reversed", 409), nil
		}
		parsed, err := ledger.ParseMoney(req.AmountMinor)
		if err != nil {
			return ledger.Outcome{}, err
		}
		n = parsed
	}
	start := time.Now()
	rows, err := tx.Query(ctx, `SELECT id::text,kind,balance_minor,entry_sequence FROM accounts WHERE id=$1 OR id=$2 ORDER BY id FOR UPDATE`, req.FromAccountID, req.ToAccountID)
	if err != nil {
		return ledger.Outcome{}, err
	}
	locked := make(map[string]lockedAccount, 2)
	for rows.Next() {
		var a lockedAccount
		if err = rows.Scan(&a.id, &a.kind, &a.balance, &a.seq); err != nil {
			rows.Close()
			return ledger.Outcome{}, err
		}
		locked[a.id] = a
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ledger.Outcome{}, err
	}
	if s.observer != nil {
		s.observer.LockWait(time.Since(start).Seconds())
	}
	if len(locked) != 2 {
		return rejected("not_found", "account not found", 404), nil
	}
	from, to := locked[req.FromAccountID], locked[req.ToAccountID]
	if kind == "funding" {
		if from.kind != "system" || to.kind != "client" {
			return rejected("protected_account", "invalid fixture accounts", 403), nil
		}
	} else if from.kind != "client" || to.kind != "client" {
		return rejected("protected_account", "public operations require client accounts", 403), nil
	}
	if kind == "reversal" {
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM journal_transactions WHERE reversal_of=$1)`, original).Scan(&exists); err != nil {
			return ledger.Outcome{}, err
		}
		if exists {
			return rejected("already_reversed", "transfer already reversed", 409), nil
		}
	}
	newFrom, newTo, err := ledger.Move(from.balance, to.balance, n, kind == "funding")
	if err != nil {
		return business(err)
	}
	if from.seq == math.MaxInt64 || to.seq == math.MaxInt64 {
		return rejected("sequence_overflow", "account history exhausted", 409), nil
	}
	var metadata any = req.Metadata
	if len(req.Metadata) == 0 {
		metadata = map[string]string{}
	}
	id := ledger.NewID()
	var created time.Time
	var reversal any
	if original != "" {
		reversal = original
	}
	err = tx.QueryRow(ctx, `INSERT INTO journal_transactions(id,kind,from_account_id,to_account_id,amount_minor,currency,metadata,reversal_of) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING created_at`, id, kind, from.id, to.id, int64(n), req.Currency, metadata, reversal).Scan(&created)
	if err != nil {
		return ledger.Outcome{}, err
	}
	// Account locks are already held in one global order. Projection and journal commit together.
	if _, err = tx.Exec(ctx, `UPDATE accounts SET balance_minor=CASE id WHEN $1::uuid THEN $3::bigint ELSE $4::bigint END,entry_sequence=entry_sequence+1 WHERE id=$1 OR id=$2`, from.id, to.id, newFrom, newTo); err != nil {
		return ledger.Outcome{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO postings(transaction_id,account_id,delta_minor,account_sequence,created_at) VALUES($1,$2,$4,$6,$8),($1,$3,$5,$7,$8)`, id, from.id, to.id, -int64(n), int64(n), from.seq+1, to.seq+1, created); err != nil {
		return ledger.Outcome{}, err
	}
	t := ledger.Transaction{ID: id, Kind: kind, TransferRequest: req, CreatedAt: created, Postings: []ledger.Posting{{TransactionID: id, AccountID: from.id, DeltaMinor: -int64(n), Sequence: from.seq + 1, CreatedAt: created}, {TransactionID: id, AccountID: to.id, DeltaMinor: int64(n), Sequence: to.seq + 1, CreatedAt: created}}}
	if original != "" {
		t.ReversalOf = &original
	}
	return ledger.Outcome{Status: 201, Transaction: &t}, nil
}

func (s *Store) Transaction(ctx context.Context, id string) (ledger.Transaction, error) {
	id, err := ledger.NormalizeID(id)
	if err != nil {
		return ledger.Transaction{}, err
	}
	var t ledger.Transaction
	var meta []byte
	err = s.Pool.QueryRow(ctx, `SELECT id::text,kind,from_account_id::text,to_account_id::text,amount_minor::text,currency,metadata,reversal_of::text,created_at FROM journal_transactions WHERE id=$1`, id).Scan(&t.ID, &t.Kind, &t.FromAccountID, &t.ToAccountID, &t.AmountMinor, &t.Currency, &meta, &t.ReversalOf, &t.CreatedAt)
	if err != nil {
		return t, notFound(err)
	}
	if err = json.Unmarshal(meta, &t.Metadata); err != nil {
		return t, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT transaction_id::text,account_id::text,delta_minor,account_sequence,created_at FROM postings WHERE transaction_id=$1 ORDER BY delta_minor`, id)
	if err != nil {
		return t, err
	}
	defer rows.Close()
	t.Postings = []ledger.Posting{}
	for rows.Next() {
		var p ledger.Posting
		if err = rows.Scan(&p.TransactionID, &p.AccountID, &p.DeltaMinor, &p.Sequence, &p.CreatedAt); err != nil {
			return t, err
		}
		t.Postings = append(t.Postings, p)
	}
	return t, rows.Err()
}

type cursor struct {
	Account string `json:"a"`
	Upper   int64  `json:"u"`
	Before  int64  `json:"b"`
}

func decodeCursor(raw, id string) (cursor, error) {
	var c cursor
	if len(raw) > 256 {
		return c, ledger.Invalid("invalid cursor")
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return c, ledger.Invalid("invalid cursor")
	}
	if err = json.Unmarshal(b, &c); err != nil || c.Account != id || c.Upper <= 0 || c.Before <= 0 || c.Before > c.Upper {
		return c, ledger.Invalid("invalid cursor")
	}
	return c, nil
}
func (s *Store) Entries(ctx context.Context, id, raw string, limit int) (ledger.Entries, error) {
	page := ledger.Entries{Entries: []ledger.Posting{}}
	id, err := ledger.NormalizeID(id)
	if err != nil {
		return page, err
	}
	if limit < 1 || limit > 100 {
		return page, ledger.Invalid("limit must be 1..100")
	}
	c := cursor{Account: id}
	if raw != "" {
		c, err = decodeCursor(raw, id)
		if err != nil {
			return page, err
		}
	} else {
		err = s.Pool.QueryRow(ctx, `SELECT entry_sequence FROM accounts WHERE id=$1`, id).Scan(&c.Upper)
		if err != nil {
			return page, notFound(err)
		}
	}
	rows, err := s.Pool.Query(ctx, `SELECT transaction_id::text,account_id::text,delta_minor,account_sequence,created_at FROM postings WHERE account_id=$1 AND account_sequence<=$2 AND ($3::bigint=0 OR account_sequence<$3) ORDER BY account_sequence DESC LIMIT $4`, id, c.Upper, c.Before, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var p ledger.Posting
		if err = rows.Scan(&p.TransactionID, &p.AccountID, &p.DeltaMinor, &p.Sequence, &p.CreatedAt); err != nil {
			return page, err
		}
		page.Entries = append(page.Entries, p)
	}
	if err = rows.Err(); err != nil {
		return page, err
	}
	if len(page.Entries) > limit {
		page.Entries = page.Entries[:limit]
		c.Before = page.Entries[limit-1].Sequence
		b, _ := json.Marshal(c)
		page.NextCursor = base64.RawURLEncoding.EncodeToString(b)
	}
	return page, nil
}
