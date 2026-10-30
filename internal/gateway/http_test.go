package gateway

import (
	"context"
	"errors"
	"ledgercore/internal/activity"
	"ledgercore/internal/ledger"
	"ledgercore/internal/ratelimit"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const externalToken = "external-test-token-1234567890"

type budget struct {
	calls atomic.Int64
	limit int64
	err   error
}

func (b *budget) Allow(context.Context, string) (ratelimit.Decision, error) {
	n := b.calls.Add(1)
	return ratelimit.Decision{Allowed: n <= b.limit, Count: n, RemainingTTL: 1501 * time.Millisecond}, b.err
}

type app struct{}

func (app) CreateAccount(context.Context, string) (ledger.Account, error) {
	return ledger.Account{ID: ledger.NewID()}, nil
}
func (app) Account(context.Context, string) (ledger.Account, error) {
	return ledger.Account{ID: ledger.NewID()}, nil
}
func (app) Transfer(context.Context, string, ledger.TransferRequest) (ledger.Outcome, error) {
	return ledger.Outcome{Status: 201, Transaction: &ledger.Transaction{ID: ledger.NewID()}}, nil
}
func (app) Reverse(context.Context, string, string) (ledger.Outcome, error) {
	return ledger.Outcome{Status: 201, Transaction: &ledger.Transaction{ID: ledger.NewID()}}, nil
}
func (app) Transaction(context.Context, string) (ledger.Transaction, error) {
	return ledger.Transaction{ID: ledger.NewID()}, nil
}
func (app) Entries(context.Context, string, string, int) (ledger.Entries, error) {
	return ledger.Entries{Entries: []ledger.Posting{}}, nil
}
func (app) GetAccountActivity(_ context.Context, id, _ string, _ int) (activity.Page, error) {
	return activity.Page{Entries: []activity.Entry{{AmountMinor: "9223372036854775807"}}, Summary: activity.Summary{AccountID: id, IncomingMinor: "18446744073709551614", OutgoingMinor: "0", IncomingCount: "2", OutgoingCount: "0"}}, nil
}
func invoke(t *testing.T, h http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("Idempotency-Key", "same-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestAuthenticateBeforeRedisAndSharedWriteGroup(t *testing.T) {
	b := &budget{limit: 2}
	handler := New(app{}, app{}, b, externalToken, func(context.Context) error { return nil }, nil, nil)
	if w := invoke(t, handler, "POST", "/v1/accounts", `{"name":"wallet"}`, "wrong"); w.Code != 401 {
		t.Fatal(w.Code)
	}
	if b.calls.Load() != 0 {
		t.Fatal("unauthenticated request reached Redis")
	}
	if w := invoke(t, handler, "POST", "/v1/accounts", `{"name":"wallet"}`, externalToken); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	body := `{"from_account_id":"` + ledger.NewID() + `","to_account_id":"` + ledger.NewID() + `","amount_minor":"1","currency":"RUB"}`
	if w := invoke(t, handler, "POST", "/v1/transfers", body, externalToken); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	w := invoke(t, handler, "POST", "/v1/transfers/"+ledger.NewID()+"/reversal", "", externalToken)
	if w.Code != 429 || w.Header().Get("Retry-After") != "2" || !strings.Contains(w.Body.String(), "rate_limited") {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	if w := invoke(t, handler, "GET", "/v1/accounts/"+ledger.NewID(), "", externalToken); w.Code != 200 || b.calls.Load() != 3 {
		t.Fatal("read consumed write budget", w.Code, b.calls.Load())
	}
}
func TestRedisOutageIs503Not429(t *testing.T) {
	b := &budget{limit: 10, err: errors.New("network outage")}
	handler := New(app{}, app{}, b, externalToken, func(context.Context) error { return nil }, nil, nil)
	w := invoke(t, handler, "POST", "/v1/accounts", `{"name":"wallet"}`, externalToken)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "rate_limiter_unavailable") || w.Header().Get("Retry-After") != "1" {
		t.Fatal(w.Code, w.Body.String(), w.Header())
	}
}
func TestActivityMoneyAndEventualSemantics(t *testing.T) {
	b := &budget{limit: 1}
	handler := New(app{}, app{}, b, externalToken, func(context.Context) error { return nil }, nil, nil)
	path := "/v1/accounts/" + ledger.NewID() + "/activity"
	w := invoke(t, handler, "GET", path, "", externalToken)
	if w.Code != 200 || w.Header().Get("X-Read-Model") != "eventually-consistent" || !strings.Contains(w.Body.String(), `"amount_minor":"9223372036854775807"`) || !strings.Contains(w.Body.String(), `"incoming_minor":"18446744073709551614"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, query := range []string{"?limit=0", "?limit=101", "?limit=1&limit=2", "?cursor=a&cursor=b"} {
		if w = invoke(t, handler, "GET", path+query, "", externalToken); w.Code != 400 {
			t.Fatal(query, w.Code)
		}
	}
	if b.calls.Load() != 0 {
		t.Fatal("activity reads consumed write quota")
	}
}
