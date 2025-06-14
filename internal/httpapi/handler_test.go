package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ledgercore/internal/ledger"
)

const token = "unit-test-token-0123456789"
const firstID = "10000000-0000-0000-0000-000000000001"
const secondID = "10000000-0000-0000-0000-000000000002"

type fakeService struct {
	called  int
	request ledger.TransferRequest
	outcome ledger.Outcome
	err     error
	limit   int
}

func (f *fakeService) CreateAccount(_ context.Context, name string) (ledger.Account, error) {
	f.called++
	return ledger.Account{ID: firstID, Name: name, Kind: "client", Currency: "RUB"}, f.err
}
func (f *fakeService) Account(context.Context, string) (ledger.Account, error) {
	f.called++
	return ledger.Account{ID: firstID, BalanceMinor: 9223372036854775807}, f.err
}
func (f *fakeService) Transfer(_ context.Context, _ string, r ledger.TransferRequest) (ledger.Outcome, error) {
	f.called++
	f.request = r
	return f.outcome, f.err
}
func (f *fakeService) Reverse(context.Context, string, string) (ledger.Outcome, error) {
	f.called++
	return f.outcome, f.err
}
func (f *fakeService) Transaction(context.Context, string) (ledger.Transaction, error) {
	f.called++
	return ledger.Transaction{ID: firstID, Postings: []ledger.Posting{}}, f.err
}
func (f *fakeService) Entries(_ context.Context, _ string, _ string, limit int) (ledger.Entries, error) {
	f.called++
	f.limit = limit
	return ledger.Entries{Entries: []ledger.Posting{}}, f.err
}

func call(t *testing.T, h http.Handler, method, path, body string, authorized bool, key string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if authorized {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func newHandler(f *fakeService) http.Handler {
	return New(f, token, func(context.Context) error { return nil }, nil)
}

func TestAuthenticationAndHealth(t *testing.T) {
	f := &fakeService{}
	h := newHandler(f)
	for _, path := range []string{"/v1/accounts", "/v1/accounts/" + firstID, "/v1/transfers", "/v1/transfers/" + firstID + "/reversal"} {
		w := call(t, h, "GET", path, "", false, "")
		if w.Code != 401 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		assertProblem(t, w, "unauthorized")
	}
	if f.called != 0 {
		t.Fatal("unauthenticated service call")
	}
	for _, path := range []string{"/livez", "/readyz"} {
		if w := call(t, h, "GET", path, "", false, ""); w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	h = New(f, token, func(context.Context) error { return errors.New("down") }, nil)
	if w := call(t, h, "GET", "/readyz", "", false, ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
}
func TestTransferCanonicalizationAndReplay(t *testing.T) {
	f := &fakeService{outcome: ledger.Outcome{Status: 201, Transaction: &ledger.Transaction{ID: firstID}, Replayed: true}}
	h := newHandler(f)
	body := `{"from_account_id":" 10000000-0000-0000-0000-000000000001 ","to_account_id":"10000000-0000-0000-0000-000000000002","amount_minor":"00042","currency":" rub ","metadata":{"note":"private"}}`
	w := call(t, h, "POST", "/v1/transfers", body, true, "same-key")
	if w.Code != 201 || f.called != 1 || f.request.AmountMinor != "42" || f.request.Currency != "RUB" || f.request.FromAccountID != firstID {
		t.Fatal(w.Code, w.Body.String(), f.request)
	}
	if w.Header().Get("Idempotency-Replayed") != "true" || w.Header().Get("Location") != "/v1/transfers/"+firstID {
		t.Fatal(w.Header())
	}
	if w.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing request id")
	}
	f.outcome = ledger.Outcome{Status: 409, Error: ledger.Problem("insufficient_funds", "insufficient funds", 409), Replayed: true}
	w = call(t, h, "POST", "/v1/transfers", body, true, "same-key")
	if w.Code != 409 {
		t.Fatal(w.Code)
	}
	assertProblem(t, w, "insufficient_funds")
}
func TestStrictBodyAndLimits(t *testing.T) {
	f := &fakeService{}
	h := newHandler(f)
	for _, body := range []string{`null`, `{}`, `{"name":""}`, `{"name":"x","balance_minor":"100"}`, `{"name":"x"} {}`, `{"name":1}`, `[]`, "{\"name\":\"\xff\"}"} {
		w := call(t, h, "POST", "/v1/accounts", body, true, "")
		if w.Code != 400 {
			t.Fatalf("body %q: %d", body, w.Code)
		}
	}
	w := call(t, h, "POST", "/v1/accounts", `{"name":"`+strings.Repeat("x", MaxBodyBytes)+`"}`, true, "")
	if w.Code != 413 {
		t.Fatal(w.Code)
	}
	if f.called != 0 {
		t.Fatal("invalid body reached service")
	}
	w = call(t, h, "POST", "/v1/accounts", `{"name":"wallet"}`, true, "")
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, body := range []string{`{"from_account_id":"` + firstID + `","to_account_id":"` + secondID + `","amount_minor":1,"currency":"RUB"}`, `{"from_account_id":"` + firstID + `","to_account_id":"` + secondID + `","amount_minor":"1","currency":"RUB","extra":true}`} {
		w = call(t, h, "POST", "/v1/transfers", body, true, "key")
		if w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
	for _, key := range []string{"", "with spaces"} {
		if w = call(t, h, "POST", "/v1/transfers", `{}`, true, key); w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
	w = call(t, h, "POST", "/v1/transfers/"+firstID+"/reversal", `{"amount_minor":"1"}`, true, "key")
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}
func TestReadPaginationAndStringMoney(t *testing.T) {
	f := &fakeService{}
	h := newHandler(f)
	w := call(t, h, "GET", "/v1/accounts/"+firstID, "", true, "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"balance_minor":"9223372036854775807"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, query := range []string{"?limit=0", "?limit=101", "?limit=wat", "?limit=1&limit=2", "?cursor=a&cursor=b"} {
		w = call(t, h, "GET", "/v1/accounts/"+firstID+"/entries"+query, "", true, "")
		if w.Code != 400 {
			t.Fatal(query, w.Code)
		}
	}
	w = call(t, h, "GET", "/v1/accounts/"+firstID+"/entries", "", true, "")
	if w.Code != 200 || f.limit != 50 {
		t.Fatal(w.Code, f.limit)
	}
	w = call(t, h, "GET", "/v1/accounts/"+firstID+"/entries?limit=100", "", true, "")
	if w.Code != 200 || f.limit != 100 {
		t.Fatal(w.Code, f.limit)
	}
	w = call(t, h, "GET", "/v1/accounts/invalid", "", true, "")
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	w = call(t, h, "DELETE", "/v1/accounts/"+firstID, "", true, "")
	if w.Code != 405 || w.Header().Get("Allow") != "GET" {
		t.Fatal(w.Code)
	}
}
func TestInfrastructureErrorsAndLogsDoNotLeak(t *testing.T) {
	var logs bytes.Buffer
	f := &fakeService{err: errors.New("password=secret-token metadata=private idempotency=private-key")}
	h := New(f, token, func(context.Context) error { return nil }, slog.New(slog.NewJSONHandler(&logs, nil)))
	w := call(t, h, "GET", "/v1/accounts/"+firstID, "", true, "")
	if w.Code != 503 || w.Header().Get("Retry-After") != "1" {
		t.Fatal(w.Code, w.Header())
	}
	assertProblem(t, w, "temporarily_unavailable")
	for _, secret := range []string{"secret-token", "private", "password", token} {
		if strings.Contains(logs.String(), secret) || strings.Contains(w.Body.String(), secret) {
			t.Fatal("secret leak")
		}
	}
	if !strings.Contains(logs.String(), w.Header().Get("X-Request-ID")) {
		t.Fatal("missing correlation")
	}
}
func assertProblem(t *testing.T, w *httptest.ResponseRecorder, code string) {
	t.Helper()
	var body struct {
		Error     ledger.Error `json:"error"`
		RequestID string       `json:"request_id"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != code || body.RequestID != w.Header().Get("X-Request-ID") {
		t.Fatal(body, w.Header())
	}
}
func TestMediaTypeAndDuplicateHeaders(t *testing.T) {
	h := newHandler(&fakeService{})
	r := httptest.NewRequest("POST", "/v1/accounts", strings.NewReader(`{"name":"a"}`))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 415 {
		t.Fatal(w.Code)
	}
	r = httptest.NewRequest("POST", "/v1/transfers", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Add("Idempotency-Key", "a")
	r.Header.Add("Idempotency-Key", "b")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	r = httptest.NewRequest("GET", "/v1/accounts/"+firstID, nil)
	r.Header.Add("Authorization", "Bearer "+token)
	r.Header.Add("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
}
func TestEmptyReversalBody(t *testing.T) {
	f := &fakeService{outcome: ledger.Outcome{Status: 201, Transaction: &ledger.Transaction{ID: secondID}}}
	h := newHandler(f)
	for _, body := range []string{"", "{}"} {
		w := call(t, h, "POST", "/v1/transfers/"+firstID+"/reversal", body, true, "reversal")
		if w.Code != 201 {
			b, _ := io.ReadAll(w.Body)
			t.Fatal(w.Code, string(b))
		}
	}
}
