package rpc

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	activityv1 "ledgercore/gen/activity/v1"
	ledgerv1 "ledgercore/gen/ledger/v1"
	"ledgercore/internal/activity"
	"ledgercore/internal/httpapi"
	"ledgercore/internal/ledger"
)

const testToken = "internal-rpc-test-secret-0123456789"

type application struct {
	mu                  sync.Mutex
	saved               map[string]ledger.Outcome
	calls, accountCalls int
	loseReply, sticky   bool
	requestID           string
	blocked             chan struct{}
}

func (a *application) Transfer(ctx context.Context, key string, r ledger.TransferRequest) (ledger.Outcome, error) {
	if a.blocked != nil {
		close(a.blocked)
		<-ctx.Done()
		return ledger.Outcome{}, ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.requestID = RequestID(ctx)
	if saved, ok := a.saved[key]; ok {
		saved.Replayed = true
		return saved, nil
	}
	if a.saved == nil {
		a.saved = map[string]ledger.Outcome{}
	}
	o := ledger.Outcome{Status: 201, Transaction: &ledger.Transaction{ID: ledger.NewID(), Kind: "transfer", TransferRequest: r, CreatedAt: time.Now(), Postings: []ledger.Posting{}}}
	if a.sticky {
		o = ledger.Outcome{Status: 409, Error: ledger.Problem("insufficient_funds", "insufficient funds", 409)}
	}
	a.saved[key] = o
	if a.loseReply {
		return ledger.Outcome{}, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("test lost committed reply")}
	}
	return o, nil
}
func (a *application) Reverse(ctx context.Context, key, id string) (ledger.Outcome, error) {
	return a.Transfer(ctx, key, ledger.TransferRequest{FromAccountID: id, ToAccountID: ledger.SystemAccountID, AmountMinor: "1", Currency: "RUB"})
}
func (a *application) CreateAccount(ctx context.Context, name string) (ledger.Account, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.accountCalls++
	a.requestID = RequestID(ctx)
	if a.loseReply {
		return ledger.Account{}, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("lost create reply")}
	}
	return ledger.Account{ID: ledger.NewID(), Name: name, Kind: "client", Currency: "RUB", CreatedAt: time.Now()}, nil
}
func (a *application) Account(context.Context, string) (ledger.Account, error) {
	return ledger.Account{ID: ledger.NewID(), BalanceMinor: 9223372036854775807, CreatedAt: time.Now()}, nil
}
func (a *application) Transaction(context.Context, string) (ledger.Transaction, error) {
	return ledger.Transaction{}, ledger.Problem("not_found", "resource not found", 404)
}
func (a *application) Entries(context.Context, string, string, int) (ledger.Entries, error) {
	return ledger.Entries{Entries: []ledger.Posting{}}, nil
}

type activityReader struct{}

func (activityReader) GetAccountActivity(_ context.Context, id, _ string, _ int) (activity.Page, error) {
	return activity.Page{Entries: []activity.Entry{{EventID: ledger.NewID(), JournalTransactionID: ledger.NewID(), Type: "transfer.posted", Direction: "incoming", AmountMinor: "9223372036854775807", Currency: "RUB", OccurredAt: time.Now(), ProjectedAt: time.Now(), Sequence: 1}}, Summary: activity.Summary{AccountID: id, IncomingMinor: "18446744073709551614", OutgoingMinor: "0", IncomingCount: "2", OutgoingCount: "0"}}, nil
}
func start(t *testing.T, app *application) (*grpc.ClientConn, string, *bytes.Buffer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	s := grpc.NewServer(ServerOptions(testToken, slog.New(slog.NewTextHandler(&logs, nil)), nil)...)
	ledgerv1.RegisterLedgerServiceServer(s, NewLedgerServer(app))
	activityv1.RegisterActivityServiceServer(s, NewActivityServer(activityReader{}))
	RegisterHealth(s, func(context.Context) error { return nil })
	go func() { _ = s.Serve(listener) }()
	t.Cleanup(s.Stop)
	cc, err := Dial(listener.Addr().String(), testToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc, listener.Addr().String(), &logs
}
func request() ledger.TransferRequest {
	return ledger.TransferRequest{FromAccountID: ledger.NewID(), ToAccountID: ledger.NewID(), AmountMinor: "9223372036854775807", Currency: "RUB", Metadata: map[string]string{"private": "sensitive-payload"}}
}
func TestRealTCPRetryReusesCommittedKey(t *testing.T) {
	app := &application{loseReply: true}
	cc, _, logs := start(t, app)
	client := NewLedgerClient(cc)
	req := request()
	key := "secret-idempotency-key"
	o, err := client.Transfer(context.Background(), key, req)
	if err != nil || o.Error != nil || !o.Replayed || o.Transaction.AmountMinor != req.AmountMinor {
		t.Fatalf("%+v %v", o, err)
	}
	app.mu.Lock()
	calls, count := app.calls, len(app.saved)
	app.mu.Unlock()
	if calls != 2 || count != 1 {
		t.Fatal(calls, count)
	}
	if strings.Contains(logs.String(), testToken) || strings.Contains(logs.String(), key) || strings.Contains(logs.String(), "sensitive-payload") {
		t.Fatal("secret in RPC log")
	}
}
func TestBusinessStatusAndReplayOverTCP(t *testing.T) {
	app := &application{sticky: true}
	cc, _, _ := start(t, app)
	client := NewLedgerClient(cc)
	r := request()
	for i := 0; i < 2; i++ {
		o, err := client.Transfer(context.Background(), "same-key", r)
		if err != nil || o.Status != 409 || o.Error.Code != "insufficient_funds" || o.Replayed != (i == 1) {
			t.Fatalf("%+v %v", o, err)
		}
	}
	app.mu.Lock()
	calls := app.calls
	app.mu.Unlock()
	if calls != 2 {
		t.Fatal("business failure retried")
	}
}
func TestUnkeyedCreateIsNotRetried(t *testing.T) {
	app := &application{loseReply: true}
	cc, _, _ := start(t, app)
	_, err := NewLedgerClient(cc).CreateAccount(context.Background(), "wallet")
	app.mu.Lock()
	calls := app.accountCalls
	app.mu.Unlock()
	if err == nil || calls != 1 {
		t.Fatal(err, calls)
	}
}
func TestAuthMessageLimitsAndActivityOverTCP(t *testing.T) {
	cc, address, _ := start(t, &application{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_, err = ledgerv1.NewLedgerServiceClient(raw).GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: ledger.NewID()})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	if err = CheckHealth(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	_, err = ledgerv1.NewLedgerServiceClient(cc).CreateAccount(ctx, &ledgerv1.CreateAccountRequest{Name: strings.Repeat("a", MaxMessageBytes+1)})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	page, err := NewActivityClient(cc).GetAccountActivity(context.Background(), ledger.NewID(), "", 50)
	if err != nil || len(page.Entries) != 1 || page.Entries[0].AmountMinor != "9223372036854775807" || page.Summary.IncomingMinor != "18446744073709551614" {
		t.Fatal(page, err)
	}
}
func TestDeadlineCancelsServerWork(t *testing.T) {
	app := &application{blocked: make(chan struct{})}
	cc, _, _ := start(t, app)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := NewLedgerClient(cc).Transfer(ctx, "deadline-key", request()); result <- err }()
	select {
	case <-app.blocked:
	case <-time.After(time.Second):
		t.Fatal("server never started")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled RPC succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("RPC ignored cancellation")
	}
}
func TestRequestIDFromExistingHTTPAdapter(t *testing.T) {
	app := &application{}
	cc, _, _ := start(t, app)
	handler := httpapi.New(NewLedgerClient(cc), testToken, func(context.Context) error { return nil }, nil)
	r := httptest.NewRequest("POST", "/v1/accounts", strings.NewReader(`{"name":"wallet"}`))
	r.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	app.mu.Lock()
	id := app.requestID
	app.mu.Unlock()
	if w.Code != 201 || id != w.Header().Get("X-Request-ID") {
		t.Fatal(w.Code, id, w.Header())
	}
}
func TestSanitizedStatusClassification(t *testing.T) {
	for _, row := range []struct {
		err  error
		want codes.Code
	}{{context.DeadlineExceeded, codes.DeadlineExceeded}, {context.Canceled, codes.Canceled}, {&pgconn.PgError{Code: "23514", Message: "private SQL"}, codes.Internal}, {&pgconn.PgError{Code: "57P01", Message: "private SQL"}, codes.Unavailable}, {ledger.Invalid("bad request"), codes.InvalidArgument}} {
		err := ToStatus(row.err, false)
		if status.Code(err) != row.want || strings.Contains(err.Error(), "private SQL") {
			t.Fatal(err, row.want)
		}
	}
}
