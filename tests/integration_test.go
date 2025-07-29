package tests

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"ledgercore/internal/ledger"
	"ledgercore/internal/reconcile"
	"ledgercore/internal/store"
	"ledgercore/migrations"
)

type harness struct {
	ctx                 context.Context
	app, fixture, admin *pgxpool.Pool
	writer, funder      *store.Store
}

func database(t *testing.T) *harness {
	t.Helper()
	if os.Getenv("LEDGER_TEST_DATABASE_URL") == "" {
		if os.Getenv("LEDGER_REQUIRE_INTEGRATION") == "1" {
			t.Fatal("LEDGER_TEST_DATABASE_URL required")
		}
		t.Skip("NOT RUN: set LEDGER_TEST_DATABASE_URL, LEDGER_TEST_FIXTURE_URL and LEDGER_TEST_ADMIN_URL to an isolated PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	open := func(env string) *pgxpool.Pool {
		dsn := os.Getenv(env)
		if dsn == "" {
			t.Fatalf("%s required with integration database", env)
		}
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.MaxConns = 40
		cfg.ConnConfig.ConnectTimeout = 5 * time.Second
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = "15000"
		cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "20000"
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		var name string
		if err = p.QueryRow(ctx, `SELECT current_database()`).Scan(&name); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(name, "test") {
			t.Fatal("integration database name must contain 'test'; tests include controlled corruption and process death")
		}
		return p
	}
	h := &harness{ctx: ctx, admin: open("LEDGER_TEST_ADMIN_URL"), app: open("LEDGER_TEST_DATABASE_URL"), fixture: open("LEDGER_TEST_FIXTURE_URL")}
	if err := migrations.Apply(ctx, h.admin); err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct {
		pool *pgxpool.Pool
		name string
	}{{h.app, "ledger_app"}, {h.fixture, "ledger_fixture"}} {
		var session string
		if err := pair.pool.QueryRow(ctx, `SELECT session_user`).Scan(&session); err != nil {
			t.Fatal(err)
		}
		if session != pair.name {
			t.Fatalf("real login role required: got %s want %s", session, pair.name)
		}
	}
	h.writer, h.funder = store.New(h.app, nil), store.New(h.fixture, nil)
	return h
}

func (h *harness) account(t *testing.T, amount int64) string {
	t.Helper()
	a, err := h.writer.CreateAccount(h.ctx, "integration wallet")
	if err != nil {
		t.Fatal(err)
	}
	if a.BalanceMinor != 0 || a.Kind != "client" {
		t.Fatalf("new wallet %+v", a)
	}
	if amount > 0 {
		o, err := h.funder.Funding(h.ctx, ledger.NewID(), a.ID, ledger.Money(amount))
		if err != nil || o.Error != nil {
			t.Fatalf("funding: %+v %v", o, err)
		}
	}
	return a.ID
}
func request(from, to string, n int64) ledger.TransferRequest {
	return ledger.TransferRequest{FromAccountID: from, ToAccountID: to, AmountMinor: strconv.FormatInt(n, 10), Currency: "RUB"}
}
func (h *harness) transfer(t *testing.T, key, from, to string, n int64) ledger.Outcome {
	t.Helper()
	o, err := h.writer.Transfer(h.ctx, key, request(from, to, n))
	if err != nil {
		t.Fatal(err)
	}
	return o
}
func (h *harness) balance(t *testing.T, id string) int64 {
	t.Helper()
	a, err := h.writer.Account(h.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return a.BalanceMinor
}
func (h *harness) clean(t *testing.T) {
	t.Helper()
	r, err := reconcile.Run(h.ctx, h.app)
	if err != nil || !r.OK {
		t.Fatalf("reconcile: %+v %v", r, err)
	}
}
func expectCode(t *testing.T, o ledger.Outcome, code string) {
	t.Helper()
	if o.Error == nil || o.Error.Code != code {
		t.Fatalf("want %s: %+v", code, o)
	}
}

func TestTransferLifecycle(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 1000), h.account(t, 0)
	key := ledger.NewID()
	o := h.transfer(t, key, a, b, 250)
	if o.Status != 201 || o.Transaction == nil || len(o.Transaction.Postings) != 2 {
		t.Fatalf("transfer: %+v", o)
	}
	if h.balance(t, a) != 750 || h.balance(t, b) != 250 {
		t.Fatal("incorrect transfer balances")
	}
	// Discarding the first outcome models a lost response after the commit.
	replayed := h.transfer(t, key, a, b, 250)
	if !replayed.Replayed || replayed.Transaction.ID != o.Transaction.ID {
		t.Fatalf("response-loss retry: %+v", replayed)
	}
	read, err := h.writer.Transaction(h.ctx, o.Transaction.ID)
	if err != nil || len(read.Postings) != 2 {
		t.Fatalf("transaction read: %+v %v", read, err)
	}
	r, err := h.writer.Reverse(h.ctx, ledger.NewID(), o.Transaction.ID)
	if err != nil || r.Error != nil || r.Transaction.ReversalOf == nil || *r.Transaction.ReversalOf != o.Transaction.ID {
		t.Fatalf("reversal: %+v %v", r, err)
	}
	if h.balance(t, a) != 1000 || h.balance(t, b) != 0 {
		t.Fatal("incorrect reverse balances")
	}
	r, err = h.writer.Reverse(h.ctx, ledger.NewID(), o.Transaction.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectCode(t, r, "already_reversed")
	h.clean(t)
	if err = migrations.Apply(h.ctx, h.admin); err != nil {
		t.Fatalf("idempotent migration: %v", err)
	}
}

func TestConcurrentDebits(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 100), h.account(t, 0)
	const clients = 32
	start := make(chan struct{})
	results := make(chan ledger.Outcome, clients)
	errorsCh := make(chan error, clients)
	for i := 0; i < clients; i++ {
		go func() {
			<-start
			o, err := h.writer.Transfer(h.ctx, ledger.NewID(), request(a, b, 10))
			results <- o
			errorsCh <- err
		}()
	}
	close(start)
	success, declined := 0, 0
	for i := 0; i < clients; i++ {
		o, err := <-results, <-errorsCh
		if err != nil {
			t.Fatal(err)
		}
		if o.Error == nil {
			success++
		} else {
			expectCode(t, o, "insufficient_funds")
			declined++
		}
	}
	if success != 10 || declined != 22 || h.balance(t, a) != 0 || h.balance(t, b) != 100 {
		t.Fatalf("success=%d declined=%d", success, declined)
	}
	h.clean(t)
}

func TestConcurrentIdempotencyAndNormalization(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 1000), h.account(t, 0)
	key := ledger.NewID()
	const clients = 24
	start := make(chan struct{})
	type response struct {
		o   ledger.Outcome
		err error
	}
	results := make(chan response, clients)
	for i := 0; i < clients; i++ {
		go func(i int) {
			<-start
			r := request(a, b, 100)
			r.Metadata = map[string]string{"purpose": "test"}
			if i%2 == 0 {
				r.FromAccountID = " " + strings.ToUpper(a) + " "
				r.Currency = " rub "
				r.AmountMinor = "000100"
			}
			o, err := h.writer.Transfer(h.ctx, key, r)
			results <- response{o, err}
		}(i)
	}
	close(start)
	id := ""
	for i := 0; i < clients; i++ {
		r := <-results
		if r.err != nil || r.o.Error != nil || r.o.Transaction == nil {
			t.Fatalf("parallel key: %+v %v", r.o, r.err)
		}
		if id == "" {
			id = r.o.Transaction.ID
		}
		if id != r.o.Transaction.ID {
			t.Fatal("duplicate posted operation")
		}
	}
	if h.balance(t, a) != 900 || h.balance(t, b) != 100 {
		t.Fatal("duplicate movement")
	}
	r := request(a, b, 100)
	r.Metadata = map[string]string{"purpose": "changed"}
	o, err := h.writer.Transfer(h.ctx, key, r)
	if err != nil {
		t.Fatal(err)
	}
	expectCode(t, o, "idempotency_conflict")
	var count int
	if err = h.app.QueryRow(h.ctx, `SELECT count(*) FROM journal_transactions WHERE id=$1`, id).Scan(&count); err != nil || count != 1 {
		t.Fatalf("journal count %d: %v", count, err)
	}
	h.clean(t)
}

func TestStickyInsufficientFundsAndReversalRules(t *testing.T) {
	h := database(t)
	a, b, c := h.account(t, 0), h.account(t, 0), h.account(t, 0)
	key := ledger.NewID()
	o := h.transfer(t, key, a, b, 50)
	expectCode(t, o, "insufficient_funds")
	if h.balance(t, a) != 0 || h.balance(t, b) != 0 {
		t.Fatal("denial changed balances")
	}
	fund, err := h.funder.Funding(h.ctx, ledger.NewID(), a, 100)
	if err != nil || fund.Error != nil {
		t.Fatal(err)
	}
	o = h.transfer(t, key, a, b, 50)
	expectCode(t, o, "insufficient_funds")
	if !o.Replayed || h.balance(t, a) != 100 {
		t.Fatal("saved denial became success")
	}
	posted := h.transfer(t, ledger.NewID(), a, b, 50)
	spent := h.transfer(t, ledger.NewID(), b, c, 50)
	if posted.Error != nil || spent.Error != nil {
		t.Fatal("setup failed")
	}
	reverseKey := ledger.NewID()
	o, err = h.writer.Reverse(h.ctx, reverseKey, posted.Transaction.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectCode(t, o, "insufficient_funds")
	if h.balance(t, a) != 50 || h.balance(t, b) != 0 || h.balance(t, c) != 50 {
		t.Fatal("failed reversal changed balances")
	}
	if h.transfer(t, ledger.NewID(), c, b, 50).Error != nil {
		t.Fatal("restore funds")
	}
	o, err = h.writer.Reverse(h.ctx, reverseKey, posted.Transaction.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectCode(t, o, "insufficient_funds")
	o, err = h.writer.Reverse(h.ctx, ledger.NewID(), posted.Transaction.ID)
	if err != nil || o.Error != nil {
		t.Fatalf("new reversal: %+v %v", o, err)
	}
	second := o.Transaction.ID
	o, err = h.writer.Reverse(h.ctx, ledger.NewID(), second)
	if err != nil {
		t.Fatal(err)
	}
	expectCode(t, o, "not_reversible")
	o, err = h.writer.Reverse(h.ctx, ledger.NewID(), fund.Transaction.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectCode(t, o, "not_reversible")
	o = h.transfer(t, ledger.NewID(), ledger.SystemAccountID, a, 1)
	expectCode(t, o, "protected_account")
	h.clean(t)
}

func TestOpposingTransfersAndConcurrentReversals(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 10000), h.account(t, 10000)
	const clients = 32
	start := make(chan struct{})
	type result struct {
		o   ledger.Outcome
		err error
	}
	results := make(chan result, clients)
	for i := 0; i < clients; i++ {
		go func(i int) {
			<-start
			x, y := a, b
			if i%2 == 0 {
				x, y = b, a
			}
			o, err := h.writer.Transfer(h.ctx, ledger.NewID(), request(x, y, 10))
			if err == nil && o.Error == nil {
				o, err = h.writer.Reverse(h.ctx, ledger.NewID(), o.Transaction.ID)
			}
			results <- result{o, err}
		}(i)
	}
	close(start)
	for i := 0; i < clients; i++ {
		r := <-results
		if r.err != nil || r.o.Error != nil {
			t.Fatalf("opposing operation: %+v %v", r.o, r.err)
		}
	}
	if h.balance(t, a) != 10000 || h.balance(t, b) != 10000 {
		t.Fatal("opposing/reversal lost balance")
	}
	one := h.transfer(t, ledger.NewID(), a, b, 10)
	start = make(chan struct{})
	for i := 0; i < clients; i++ {
		go func() {
			<-start
			o, err := h.writer.Reverse(h.ctx, ledger.NewID(), one.Transaction.ID)
			results <- result{o, err}
		}()
	}
	close(start)
	success := 0
	for i := 0; i < clients; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.o.Error == nil {
			success++
		} else {
			expectCode(t, r.o, "already_reversed")
		}
	}
	if success != 1 {
		t.Fatalf("reversals=%d", success)
	}
	h.clean(t)
}

func TestCommitConstraintsAndJournalPrivileges(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 100), h.account(t, 0)
	for _, kind := range []string{"empty", "unbalanced", "wrong_direction", "too_many"} {
		t.Run(kind, func(t *testing.T) {
			tx, err := h.app.Begin(h.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			id := ledger.NewID()
			_, err = tx.Exec(h.ctx, `INSERT INTO journal_transactions(id,kind,from_account_id,to_account_id,amount_minor,currency) VALUES($1,'transfer',$2,$3,1,'RUB')`, id, a, b)
			if err != nil {
				t.Fatal(err)
			}
			if kind != "empty" {
				x, y := int64(-1), int64(1)
				if kind == "unbalanced" {
					y = 2
				}
				if kind == "wrong_direction" {
					x, y = 1, -1
				}
				_, err = tx.Exec(h.ctx, `INSERT INTO postings(transaction_id,account_id,delta_minor,account_sequence) VALUES($1,$2,$4,10000),($1,$3,$5,10000)`, id, a, b, x, y)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "too_many" {
					c := h.account(t, 0)
					_, err = tx.Exec(h.ctx, `INSERT INTO postings(transaction_id,account_id,delta_minor,account_sequence) VALUES($1,$2,1,10000)`, id, c)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			err = tx.Commit(h.ctx)
			var pgerr *pgconn.PgError
			if !errors.As(err, &pgerr) || pgerr.Code != "23514" {
				t.Fatalf("commit accepted %s or wrong error: %v", kind, err)
			}
		})
	}
	o := h.transfer(t, ledger.NewID(), a, b, 1)
	if o.Error != nil {
		t.Fatal(o.Error)
	}
	for _, sql := range []string{
		`UPDATE journal_transactions SET amount_minor=2 WHERE id=$1`,
		`DELETE FROM journal_transactions WHERE id=$1`,
		`UPDATE postings SET delta_minor=2 WHERE transaction_id=$1`,
		`DELETE FROM postings WHERE transaction_id=$1`,
	} {
		_, err := h.app.Exec(h.ctx, sql, o.Transaction.ID)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
			t.Fatalf("runtime journal mutation not denied: %v", err)
		}
	}
	_, err := h.app.Exec(h.ctx, `TRUNCATE postings`)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
		t.Fatalf("truncate not denied: %v", err)
	}
	_, err = h.app.Exec(h.ctx, `INSERT INTO postings(transaction_id,account_id,delta_minor,account_sequence) VALUES($1,$2,1,10001)`, o.Transaction.ID, h.account(t, 0))
	if !errors.As(err, &pgerr) || pgerr.Code != "55000" {
		t.Fatalf("committed posting append allowed: %v", err)
	}
	_, err = h.app.Exec(h.ctx, `INSERT INTO accounts(id,name,kind,balance_minor) VALUES($1,'bad','system',1)`, ledger.NewID())
	if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
		t.Fatalf("protected account creation allowed: %v", err)
	}
	_, err = h.writer.Funding(h.ctx, ledger.NewID(), a, 1)
	if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
		t.Fatalf("public DB role funding allowed: %v", err)
	}
	tx, err := h.app.Begin(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(h.ctx, `INSERT INTO idempotency_records(kind,key,fingerprint) VALUES('transfer',$1,$2)`, ledger.NewID(), strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit(h.ctx)
	if !errors.As(err, &pgerr) || pgerr.Code != "23514" {
		t.Fatalf("unfinished reservation committed: %v", err)
	}
	h.clean(t)
}

func TestOverflowRejectionAndIndependentCorruptionDetection(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 1), h.account(t, 0)
	// Administrative corruption is confined to this isolated test DB and restored.
	_, err := h.admin.Exec(h.ctx, `UPDATE accounts SET balance_minor=$2 WHERE id=$1`, b, int64(math.MaxInt64))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = h.admin.Exec(context.Background(), `UPDATE accounts SET balance_minor=0 WHERE id=$1`, b)
	}()
	o := h.transfer(t, ledger.NewID(), a, b, 1)
	expectCode(t, o, "balance_overflow")
	if h.balance(t, a) != 1 || h.balance(t, b) != math.MaxInt64 {
		t.Fatal("overflow partially changed balances")
	}
	r, err := reconcile.Run(h.ctx, h.app)
	if err != nil || r.OK {
		t.Fatalf("corruption not detected: %+v %v", r, err)
	}
	found := false
	for _, issue := range r.Issues {
		if issue.Kind == "balance_mismatch" && issue.ID == b {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing corruption evidence: %+v", r)
	}
	_, err = h.admin.Exec(h.ctx, `UPDATE accounts SET balance_minor=0 WHERE id=$1`, b)
	if err != nil {
		t.Fatal(err)
	}
	h.clean(t)
}

func TestSeededSequentialModel(t *testing.T) {
	h := database(t)
	const seed int64 = 20250508
	rng := rand.New(rand.NewSource(seed))
	const count = 6
	ids := make([]string, count)
	balances := make([]int64, count)
	for i := range ids {
		ids[i] = h.account(t, 1000)
		balances[i] = 1000
	}
	for step := 0; step < 200; step++ {
		from := rng.Intn(count)
		to := rng.Intn(count - 1)
		if to >= from {
			to++
		}
		amount := int64(rng.Intn(500) + 1)
		o := h.transfer(t, fmt.Sprintf("model-%d-%s-%d", seed, ids[0], step), ids[from], ids[to], amount)
		if balances[from] < amount {
			expectCode(t, o, "insufficient_funds")
		} else {
			if o.Error != nil {
				t.Fatalf("seed=%d step=%d: %v", seed, step, o.Error)
			}
			balances[from] -= amount
			balances[to] += amount
		}
	}
	for i, id := range ids {
		if got := h.balance(t, id); got != balances[i] {
			t.Fatalf("seed=%d account%d balance%d want%d", seed, i, got, balances[i])
		}
	}
	h.clean(t)
}

func TestSnapshotReconcileDuringWrites(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 10000), h.account(t, 10000)
	start := make(chan struct{})
	done := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		go func(i int) {
			<-start
			for j := 0; j < 30; j++ {
				from, to := a, b
				if (i+j)%2 == 0 {
					from, to = b, a
				}
				o, err := h.writer.Transfer(h.ctx, ledger.NewID(), request(from, to, 1))
				if err != nil {
					done <- err
					return
				}
				if o.Error != nil {
					done <- o.Error
					return
				}
			}
			done <- nil
		}(worker)
	}
	close(start)
	for i := 0; i < 8; i++ {
		h.clean(t)
	}
	for i := 0; i < 4; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	h.clean(t)
}

func TestStableHistoryPagination(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 1000), h.account(t, 0)
	for i := 0; i < 9; i++ {
		if h.transfer(t, ledger.NewID(), a, b, 1).Error != nil {
			t.Fatal("setup")
		}
	}
	page, err := h.writer.Entries(h.ctx, b, "", 3)
	if err != nil || len(page.Entries) != 3 || page.NextCursor == "" {
		t.Fatalf("first page %+v %v", page, err)
	}
	seen := map[int64]bool{}
	last := int64(math.MaxInt64)
	for _, p := range page.Entries {
		if p.Sequence >= last {
			t.Fatal("order")
		}
		last = p.Sequence
		seen[p.Sequence] = true
	}
	if h.transfer(t, ledger.NewID(), a, b, 1).Error != nil {
		t.Fatal("concurrent append")
	}
	for page.NextCursor != "" {
		page, err = h.writer.Entries(h.ctx, b, page.NextCursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range page.Entries {
			if seen[p.Sequence] || p.Sequence >= last {
				t.Fatalf("duplicate or out of order sequence %d", p.Sequence)
			}
			seen[p.Sequence] = true
			last = p.Sequence
		}
	}
	if len(seen) != 9 || seen[10] {
		t.Fatalf("snapshot bound violated: %v", seen)
	}
	h.clean(t)
}

// Helper runs only in the explicitly spawned test process, never production API.
func TestStoreCrashHelper(t *testing.T) {
	if os.Getenv("LEDGER_CRASH_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(os.Getenv("LEDGER_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = "ledger-crash-helper"
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	_, err = store.New(p, nil).Transfer(ctx, os.Getenv("LEDGER_CRASH_KEY"), request(os.Getenv("LEDGER_CRASH_FROM"), os.Getenv("LEDGER_CRASH_TO"), 1))
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal("parent must kill helper before commit")
}

func TestProcessDeathBeforeCommit(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 10), h.account(t, 0)
	key := ledger.NewID()
	// Parent holds a session advisory lock; the test-only trigger waits for it
	// after the header/projection changes but before postings and commit.
	const lock int64 = 734255099
	conn, err := h.admin.Acquire(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err = conn.Exec(h.ctx, `SELECT pg_advisory_lock($1)`, lock); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lock) }()
	sql := fmt.Sprintf(`CREATE FUNCTION test_crash_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.account_id='%s'::uuid THEN PERFORM pg_advisory_xact_lock(%d); END IF; RETURN NEW; END $$; CREATE TRIGGER test_crash_pause BEFORE INSERT ON postings FOR EACH ROW EXECUTE FUNCTION test_crash_pause();`, a, lock)
	if _, err = h.admin.Exec(h.ctx, sql); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lock)
		_, _ = h.admin.Exec(context.Background(), `DROP TRIGGER IF EXISTS test_crash_pause ON postings; DROP FUNCTION IF EXISTS test_crash_pause()`)
	}()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(h.ctx, binary, "-test.run=^TestStoreCrashHelper$", "-test.timeout=40s")
	cmd.Env = append(os.Environ(), "LEDGER_CRASH_HELPER=1", "LEDGER_CRASH_KEY="+key, "LEDGER_CRASH_FROM="+a, "LEDGER_CRASH_TO="+b)
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	waitCtx, cancel := context.WithTimeout(h.ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err = h.admin.QueryRow(waitCtx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=0 AND objid=$1::oid AND objsubid=1)`, lock).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("helper never reached pre-commit barrier")
		case <-ticker.C:
		}
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("helper exited successfully instead of being killed")
	}
	if _, err = conn.Exec(h.ctx, `SELECT pg_advisory_unlock($1)`, lock); err != nil {
		t.Fatal(err)
	}
	if h.balance(t, a) != 10 || h.balance(t, b) != 0 {
		t.Fatal("pre-commit process death changed balance")
	}
	var reservations int
	if err = h.app.QueryRow(h.ctx, `SELECT count(*) FROM idempotency_records WHERE key=$1`, key).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("crash reservation %d %v", reservations, err)
	}
	h.clean(t)
}

type retryObserver struct{ attempts atomic.Int64 }

func (*retryObserver) Operation(string, string, float64) {}
func (o *retryObserver) Retry()                          { o.attempts.Add(1) }
func (*retryObserver) Conflict()                         {}
func (*retryObserver) LockWait(float64)                  {}

func TestBoundedTransactionRetryAndDeadline(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 10), h.account(t, 0)
	// A sequence is deliberately nontransactional, making exactly one injected
	// serialization failure survive the rollback without a timing assumption.
	sql := fmt.Sprintf(`CREATE SEQUENCE test_retry_seq; GRANT USAGE ON SEQUENCE test_retry_seq TO ledger_app; CREATE FUNCTION test_retry_once() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.from_account_id='%s'::uuid AND nextval('test_retry_seq')=1 THEN RAISE EXCEPTION 'test serialization failure' USING ERRCODE='40001'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_retry_once BEFORE INSERT ON journal_transactions FOR EACH ROW EXECUTE FUNCTION test_retry_once();`, a)
	if _, err := h.admin.Exec(h.ctx, sql); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = h.admin.Exec(context.Background(), `DROP TRIGGER IF EXISTS test_retry_once ON journal_transactions; DROP FUNCTION IF EXISTS test_retry_once(); DROP SEQUENCE IF EXISTS test_retry_seq`)
	}()
	observer := &retryObserver{}
	w := store.New(h.app, observer)
	key := ledger.NewID()
	o, err := w.Transfer(h.ctx, key, request(a, b, 1))
	if err != nil || o.Error != nil || observer.attempts.Load() != 1 {
		t.Fatalf("retry: %+v %v count%d", o, err, observer.attempts.Load())
	}
	if h.balance(t, a) != 9 || h.balance(t, b) != 1 {
		t.Fatal("retry duplicated a transfer")
	}
	// An actual row lock makes the deadline test observe a DB wait instead of
	// relying on an arbitrary sleep to race a transaction.
	tx, err := h.admin.Begin(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err = tx.Exec(h.ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, a); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(h.ctx, 100*time.Millisecond)
	_, err = h.writer.Transfer(deadline, ledger.NewID(), request(a, b, 1))
	cancel()
	if err == nil {
		t.Fatal("row lock ignored context deadline")
	}
	if err = tx.Rollback(h.ctx); err != nil {
		t.Fatal(err)
	}
	if h.balance(t, a) != 9 || h.balance(t, b) != 1 {
		t.Fatal("deadline left partial change")
	}
	h.clean(t)
}

func TestConcurrentStickyRefusal(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 0), h.account(t, 0)
	key := ledger.NewID()
	start := make(chan struct{})
	type result struct {
		o   ledger.Outcome
		err error
	}
	results := make(chan result, 32)
	for range 32 {
		go func() { <-start; o, err := h.writer.Transfer(h.ctx, key, request(a, b, 1)); results <- result{o, err} }()
	}
	close(start)
	replayed := 0
	for range 32 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		expectCode(t, r.o, "insufficient_funds")
		if r.o.Replayed {
			replayed++
		}
	}
	if replayed != 31 {
		t.Fatalf("want31replays got%d", replayed)
	}
	var count int
	if err := h.app.QueryRow(h.ctx, `SELECT count(*) FROM idempotency_records WHERE kind='transfer' AND key=$1`, key).Scan(&count); err != nil || count != 1 {
		t.Fatalf("persisted outcomes=%d err=%v", count, err)
	}
	if h.balance(t, a) != 0 || h.balance(t, b) != 0 {
		t.Fatal("refusal changed projection")
	}
	o, err := h.funder.Funding(h.ctx, ledger.NewID(), a, 10)
	if err != nil || o.Error != nil {
		t.Fatalf("fund %+v %v", o, err)
	}
	o = h.transfer(t, key, a, b, 1)
	expectCode(t, o, "insufficient_funds")
	if !o.Replayed || h.balance(t, a) != 10 || h.balance(t, b) != 0 {
		t.Fatal("concurrent refusal changed after funding")
	}
	h.clean(t)
}

func TestRetryExhaustionDoesNotPersist(t *testing.T) {
	h := database(t)
	a, b := h.account(t, 10), h.account(t, 0)
	sql := fmt.Sprintf(`CREATE SEQUENCE test_exhaust_seq; GRANT USAGE ON SEQUENCE test_exhaust_seq TO ledger_app; CREATE FUNCTION test_retry_exhaust() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.from_account_id='%s'::uuid THEN PERFORM nextval('test_exhaust_seq'); RAISE EXCEPTION 'test retry exhaustion' USING ERRCODE='40001'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_retry_exhaust BEFORE INSERT ON journal_transactions FOR EACH ROW EXECUTE FUNCTION test_retry_exhaust();`, a)
	if _, err := h.admin.Exec(h.ctx, sql); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		_, _ = h.admin.Exec(context.Background(), `DROP TRIGGER IF EXISTS test_retry_exhaust ON journal_transactions; DROP FUNCTION IF EXISTS test_retry_exhaust(); DROP SEQUENCE IF EXISTS test_exhaust_seq`)
	}
	defer cleanup()
	observer := &retryObserver{}
	w := store.New(h.app, observer)
	key := ledger.NewID()
	_, err := w.Transfer(h.ctx, key, request(a, b, 1))
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "40001" || observer.attempts.Load() != 2 {
		t.Fatalf("exhaustion err=%v retries=%d", err, observer.attempts.Load())
	}
	var attempts, reservations, headers int64
	if err = h.admin.QueryRow(h.ctx, `SELECT last_value FROM test_exhaust_seq`).Scan(&attempts); err != nil || attempts != 3 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
	if err = h.app.QueryRow(h.ctx, `SELECT count(*) FROM idempotency_records WHERE kind='transfer' AND key=$1`, key).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("reservation=%d err=%v", reservations, err)
	}
	if err = h.app.QueryRow(h.ctx, `SELECT count(*) FROM journal_transactions WHERE from_account_id=$1`, a).Scan(&headers); err != nil || headers != 0 {
		t.Fatalf("journal leak=%d err=%v", headers, err)
	}
	if h.balance(t, a) != 10 || h.balance(t, b) != 0 {
		t.Fatal("retry exhaustion changed balances")
	}
	cleanup()
	o := h.transfer(t, key, a, b, 1)
	if o.Error != nil || o.Replayed {
		t.Fatalf("infrastructure failure cached: %+v", o)
	}
	h.clean(t)
}
