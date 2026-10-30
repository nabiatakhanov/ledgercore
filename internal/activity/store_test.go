package activity

import (
	"context"
	"errors"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"ledgercore/internal/events"
	"ledgercore/internal/ledger"
)

func TestActivityCursorValidation(t *testing.T) {
	id := ledger.NewID()
	for _, raw := range []string{"x", strings.Repeat("x", 257), "eyJhIjoiYmFkIiwidSI6MSwiYiI6MX0"} {
		if _, err := parseCursor(raw, id); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	if _, err := parseCursor("", id); err != nil {
		t.Fatal(err)
	}
}
func TestConsumerConfigurationLimits(t *testing.T) {
	for _, c := range []ConsumerConfig{{}, {Brokers: []string{"localhost:9092"}, Topic: "x", DLQTopic: "x", Group: "x", MaxPartitions: 1, RecordTimeout: time.Second}, {Brokers: []string{"localhost:9092"}, Topic: "x", DLQTopic: "d", Group: "x", MaxPartitions: 65, RecordTimeout: time.Second}} {
		if _, err := NewConsumer(c, nil, nil); err == nil {
			t.Fatal("accepted invalid config")
		}
	}
}

type testDB struct {
	ctx        context.Context
	app, admin *pgxpool.Pool
	s          *Store
}

func database(t *testing.T) *testDB {
	t.Helper()
	if os.Getenv("ACTIVITY_TEST_DATABASE_URL") == "" {
		if os.Getenv("LEDGER_REQUIRE_EVENTS_INTEGRATION") == "1" {
			t.Fatal("ACTIVITY_TEST_DATABASE_URL required")
		}
		t.Skip("NOT RUN: isolated Activity PostgreSQL URLs not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	open := func(key string) *pgxpool.Pool {
		dsn := os.Getenv(key)
		if dsn == "" {
			t.Fatalf("%s required", key)
		}
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.MaxConns = 32
		cfg.ConnConfig.ConnectTimeout = 3 * time.Second
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
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
			t.Fatal("Activity test database must contain 'test'")
		}
		return p
	}
	d := &testDB{ctx: ctx, admin: open("ACTIVITY_TEST_MIGRATION_URL"), app: open("ACTIVITY_TEST_DATABASE_URL")}
	if err := Migrate(ctx, d.admin); err != nil {
		t.Fatal(err)
	}
	d.s = New(d.app)
	var login string
	if err := d.app.QueryRow(ctx, `SELECT session_user`).Scan(&login); err != nil || login != "activity_app" {
		t.Fatalf("real activity_app login required; got%s error%v", login, err)
	}
	return d
}
func event(a, b, amount string) events.Event {
	id := ledger.NewID()
	return events.Event{EventID: id, SchemaVersion: 1, Type: events.TransferCreated, JournalTransactionID: id, FromAccountID: a, ToAccountID: b, AmountMinor: amount, Currency: "RUB", OccurredAt: time.Now().UTC().Truncate(time.Microsecond)}
}
func (d *testDB) apply(t *testing.T, e events.Event) {
	t.Helper()
	duplicate, err := d.s.Apply(d.ctx, e)
	if err != nil || duplicate {
		t.Fatalf("apply duplicate=%v error=%v", duplicate, err)
	}
}
func (d *testDB) audit(t *testing.T) {
	t.Helper()
	r, err := Audit(d.ctx, d.app)
	if err != nil || !r.OK {
		t.Fatalf("audit %+v %v", r, err)
	}
}

func TestActivityAtomicReplayAndUnboundedTotals(t *testing.T) {
	d := database(t)
	a, b := ledger.NewID(), ledger.NewID()
	e := event(a, b, "9223372036854775807")
	const n = 24
	start := make(chan struct{})
	type result struct {
		dup bool
		err error
	}
	results := make(chan result, n)
	for i := 0; i < n; i++ {
		go func() { <-start; dup, err := d.s.Apply(d.ctx, e); results <- result{dup, err} }()
	}
	close(start)
	fresh := 0
	for i := 0; i < n; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if !r.dup {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("fresh effects %d", fresh)
	}
	d.apply(t, event(a, b, "9223372036854775807"))
	p, err := d.s.GetAccountActivity(d.ctx, a, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	expected := new(big.Int).Mul(big.NewInt(9223372036854775807), big.NewInt(2)).String()
	if len(p.Entries) != 2 || p.Summary.OutgoingMinor != expected || p.Summary.OutgoingCount != "2" {
		t.Fatalf("numeric summary %+v", p)
	}
	e.AmountMinor = "1"
	_, err = d.s.Apply(d.ctx, e)
	var invalid *events.InvalidEvent
	if !errors.As(err, &invalid) || invalid.Code != "conflicting_event" {
		t.Fatalf("conflicting event %v", err)
	}
	for _, sql := range []string{`UPDATE activity_inbox SET amount_minor=1 WHERE event_id=$1`, `DELETE FROM activity_inbox WHERE event_id=$1`, `DELETE FROM activity_entries WHERE event_id=$1`} {
		_, err = d.app.Exec(d.ctx, sql, e.EventID)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
			t.Fatalf("runtime erased replay protection: %v", err)
		}
	}
	d.audit(t)
}

func TestActivityLateReversalAndStablePagination(t *testing.T) {
	d := database(t)
	a, b := ledger.NewID(), ledger.NewID()
	original := event(a, b, "10")
	reversal := event(b, a, "10")
	reversal.Type = events.TransferReversed
	reversal.ReversalOf = original.EventID
	d.apply(t, reversal)
	d.apply(t, original)
	for i := 0; i < 7; i++ {
		d.apply(t, event(a, b, "1"))
	}
	p, err := d.s.GetAccountActivity(d.ctx, a, "", 3)
	if err != nil || p.NextCursor == "" {
		t.Fatalf("page %+v %v", p, err)
	}
	if p.Summary.IncomingMinor != "10" || p.Summary.OutgoingMinor != "17" {
		t.Fatalf("late reversal totals %+v", p.Summary)
	}
	seen := map[int64]bool{}
	last := int64(10)
	for _, e := range p.Entries {
		if e.Sequence >= last {
			t.Fatal("order")
		}
		last = e.Sequence
		seen[e.Sequence] = true
	}
	d.apply(t, event(a, b, "1"))
	for p.NextCursor != "" {
		p, err = d.s.GetAccountActivity(d.ctx, a, p.NextCursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range p.Entries {
			if seen[e.Sequence] || e.Sequence >= last {
				t.Fatal("duplicate or unstable page")
			}
			seen[e.Sequence] = true
			last = e.Sequence
		}
	}
	if len(seen) != 9 || seen[10] {
		t.Fatalf("pagination bound %v", seen)
	}
	empty, err := d.s.GetAccountActivity(d.ctx, ledger.NewID(), "", 10)
	if err != nil || len(empty.Entries) != 0 || empty.Summary.IncomingMinor != "0" {
		t.Fatalf("empty projection %+v %v", empty, err)
	}
	d.audit(t)
}

func TestActivitySnapshotAuditUnderLoadAndCorruption(t *testing.T) {
	d := database(t)
	a, b := ledger.NewID(), ledger.NewID()
	start := make(chan struct{})
	done := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		go func() {
			<-start
			for j := 0; j < 20; j++ {
				_, err := d.s.Apply(d.ctx, event(a, b, "3"))
				if err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	close(start)
	for i := 0; i < 8; i++ {
		d.audit(t)
	}
	for i := 0; i < 4; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	d.audit(t)
	if _, err := d.admin.Exec(d.ctx, `UPDATE activity_summaries SET outgoing_minor=outgoing_minor+1 WHERE account_id=$1`, a); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = d.admin.Exec(context.Background(), `UPDATE activity_summaries SET outgoing_minor=outgoing_minor-1 WHERE account_id=$1`, a)
	}()
	r, err := Audit(d.ctx, d.app)
	if err != nil || r.OK {
		t.Fatalf("corruption undetected %+v %v", r, err)
	}
}
