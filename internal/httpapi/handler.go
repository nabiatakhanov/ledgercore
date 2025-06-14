// Package httpapi serves the trusted internal ledger API.
package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"ledgercore/internal/ledger"
)

const MaxBodyBytes = 32 << 10

type Service interface {
	CreateAccount(context.Context, string) (ledger.Account, error)
	Account(context.Context, string) (ledger.Account, error)
	Transfer(context.Context, string, ledger.TransferRequest) (ledger.Outcome, error)
	Reverse(context.Context, string, string) (ledger.Outcome, error)
	Transaction(context.Context, string) (ledger.Transaction, error)
	Entries(context.Context, string, string, int) (ledger.Entries, error)
}

type handler struct {
	service Service
	token   [32]byte
	ready   func(context.Context) error
	log     *slog.Logger
}

type requestIDKey struct{}

// New requires a configured nonempty bearer token. Production validates its
// length before starting listeners. Readiness must check the application's DB.
func New(service Service, token string, ready func(context.Context) error, log *slog.Logger) http.Handler {
	if token == "" {
		panic("empty API token")
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	h := &handler{service: service, token: sha256.Sum256([]byte(token)), ready: ready, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", h.livez)
	mux.HandleFunc("/readyz", h.readyz)
	mux.HandleFunc("/v1/accounts", h.auth(h.accounts))
	mux.HandleFunc("/v1/accounts/{id}", h.auth(h.account))
	mux.HandleFunc("/v1/accounts/{id}/entries", h.auth(h.entries))
	mux.HandleFunc("/v1/transfers", h.auth(h.transfers))
	mux.HandleFunc("/v1/transfers/{id}", h.auth(h.transaction))
	mux.HandleFunc("/v1/transfers/{id}/reversal", h.auth(h.reversal))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		h.problem(w, r, ledger.Problem("not_found", "route not found", http.StatusNotFound))
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := ledger.NewID()
		ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), requestIDKey{}, id), 5*time.Second)
		defer cancel()
		r = r.WithContext(ctx)
		w.Header().Set("X-Request-ID", id)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		started := time.Now()
		defer func() {
			if recover() != nil {
				h.log.Error("request panic", "request_id", id)
				h.problem(w, r, ledger.Problem("internal_error", "internal server error", 500))
			}
			h.log.Info("request completed", "request_id", id, "method", r.Method, "duration_ms", time.Since(started).Milliseconds())
		}()
		mux.ServeHTTP(w, r)
	})
}

func (h *handler) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		var candidate string
		if len(values) == 1 && strings.HasPrefix(values[0], "Bearer ") {
			candidate = strings.TrimPrefix(values[0], "Bearer ")
		}
		hash := sha256.Sum256([]byte(candidate))
		if candidate == "" || subtle.ConstantTimeCompare(hash[:], h.token[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			h.problem(w, r, ledger.Problem("unauthorized", "valid bearer token required", 401))
			return
		}
		next(w, r)
	}
}

func (h *handler) method(w http.ResponseWriter, r *http.Request, expected string) bool {
	if r.Method == expected {
		return true
	}
	w.Header().Set("Allow", expected)
	h.problem(w, r, ledger.Problem("method_not_allowed", "method not allowed", 405))
	return false
}
func (h *handler) livez(w http.ResponseWriter, r *http.Request) {
	if h.method(w, r, http.MethodGet) {
		h.json(w, 200, map[string]string{"status": "ok"})
	}
}
func (h *handler) readyz(w http.ResponseWriter, r *http.Request) {
	if !h.method(w, r, http.MethodGet) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if h.ready == nil || h.ready(ctx) != nil {
		h.problem(w, r, ledger.Problem("not_ready", "database unavailable", 503))
		return
	}
	h.json(w, 200, map[string]string{"status": "ready"})
}
func (h *handler) accounts(w http.ResponseWriter, r *http.Request) {
	if !h.method(w, r, http.MethodPost) {
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if !h.decode(w, r, &body, false) {
		return
	}
	if err := ledger.ValidateName(body.Name); err != nil {
		h.failure(w, r, err)
		return
	}
	account, err := h.service.CreateAccount(r.Context(), body.Name)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/accounts/"+account.ID)
	h.json(w, 201, account)
}
func (h *handler) account(w http.ResponseWriter, r *http.Request) {
	if !h.method(w, r, http.MethodGet) {
		return
	}
	id, err := ledger.NormalizeID(r.PathValue("id"))
	if err != nil {
		h.failure(w, r, err)
		return
	}
	account, err := h.service.Account(r.Context(), id)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.json(w, 200, account)
}
func (h *handler) entries(w http.ResponseWriter, r *http.Request) {
	if !h.method(w, r, http.MethodGet) {
		return
	}
	id, err := ledger.NormalizeID(r.PathValue("id"))
	if err != nil {
		h.failure(w, r, err)
		return
	}
	limit := 50
	if values, ok := r.URL.Query()["limit"]; ok {
		if len(values) != 1 {
			h.failure(w, r, ledger.Invalid("limit must appear once"))
			return
		}
		limit, err = strconv.Atoi(values[0])
		if err != nil || limit < 1 || limit > 100 {
			h.failure(w, r, ledger.Invalid("limit must be between 1 and 100"))
			return
		}
	}
	cursors := r.URL.Query()["cursor"]
	if len(cursors) > 1 || len(r.URL.Query().Get("cursor")) > 512 {
		h.failure(w, r, ledger.Invalid("invalid cursor"))
		return
	}
	page, err := h.service.Entries(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.json(w, 200, page)
}
func (h *handler) transfers(w http.ResponseWriter, r *http.Request) {
	if !h.method(w, r, http.MethodPost) {
		return
	}
	key, ok := h.key(w, r)
	if !ok {
		return
	}
	var req ledger.TransferRequest
	if !h.decode(w, r, &req, false) {
		return
	}
	req, _, err := req.Normalize()
	if err != nil {
		h.failure(w, r, err)
		return
	}
	out, err := h.service.Transfer(r.Context(), key, req)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.outcome(w, r, out)
}
func (h *handler) transaction(w http.ResponseWriter, r *http.Request) {
	if !h.method(w, r, http.MethodGet) {
		return
	}
	id, err := ledger.NormalizeID(r.PathValue("id"))
	if err != nil {
		h.failure(w, r, err)
		return
	}
	tx, err := h.service.Transaction(r.Context(), id)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.json(w, 200, tx)
}
func (h *handler) reversal(w http.ResponseWriter, r *http.Request) {
	if !h.method(w, r, http.MethodPost) {
		return
	}
	key, ok := h.key(w, r)
	if !ok {
		return
	}
	id, err := ledger.NormalizeID(r.PathValue("id"))
	if err != nil {
		h.failure(w, r, err)
		return
	}
	var body struct{}
	if !h.decode(w, r, &body, true) {
		return
	}
	out, err := h.service.Reverse(r.Context(), key, id)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.outcome(w, r, out)
}
func (h *handler) key(w http.ResponseWriter, r *http.Request) (string, bool) {
	values := r.Header.Values("Idempotency-Key")
	if len(values) != 1 {
		h.failure(w, r, ledger.Invalid("one Idempotency-Key header is required"))
		return "", false
	}
	if err := ledger.ValidateKey(values[0]); err != nil {
		h.failure(w, r, err)
		return "", false
	}
	return values[0], true
}
func (h *handler) decode(w http.ResponseWriter, r *http.Request, target any, optional bool) bool {
	if contentType := r.Header.Get("Content-Type"); contentType != "" && !strings.EqualFold(strings.TrimSpace(strings.Split(contentType, ";")[0]), "application/json") {
		h.problem(w, r, ledger.Problem("unsupported_media_type", "Content-Type must be application/json", 415))
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var limit *http.MaxBytesError
		if errors.As(err, &limit) {
			h.problem(w, r, ledger.Problem("body_too_large", "request body exceeds 32768 bytes", 413))
		} else {
			h.failure(w, r, ledger.Invalid("could not read JSON body"))
		}
		return false
	}
	if optional && len(bytes.TrimSpace(body)) == 0 {
		return true
	}
	if !utf8.Valid(body) || bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
		h.failure(w, r, ledger.Invalid("body must be a UTF-8 JSON object"))
		return false
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err = d.Decode(target); err != nil {
		h.failure(w, r, ledger.Invalid("invalid JSON body or unknown fields"))
		return false
	}
	if err = d.Decode(new(any)); err != io.EOF {
		h.failure(w, r, ledger.Invalid("body must contain exactly one JSON object"))
		return false
	}
	return true
}
func (h *handler) outcome(w http.ResponseWriter, r *http.Request, out ledger.Outcome) {
	w.Header().Set("Idempotency-Replayed", strconv.FormatBool(out.Replayed))
	if out.Error != nil {
		p := *out.Error
		p.Status = out.Status
		h.problem(w, r, &p)
		return
	}
	if out.Transaction == nil || out.Status < 200 || out.Status >= 300 {
		h.failure(w, r, errors.New("invalid service outcome"))
		return
	}
	w.Header().Set("Location", "/v1/transfers/"+out.Transaction.ID)
	h.json(w, out.Status, out)
}
func (h *handler) failure(w http.ResponseWriter, r *http.Request, err error) {
	var p *ledger.Error
	if errors.As(err, &p) {
		h.problem(w, r, p)
		return
	}
	// Do not log raw database errors: they may embed parameter or metadata values.
	h.log.Error("request infrastructure failure", "request_id", r.Context().Value(requestIDKey{}))
	h.problem(w, r, ledger.Problem("temporarily_unavailable", "operation outcome may be unknown; retry with the same idempotency key", 503))
}
func (h *handler) problem(w http.ResponseWriter, r *http.Request, p *ledger.Error) {
	status := p.Status
	if status < 400 || status > 599 {
		status = 500
	}
	if status == 503 {
		w.Header().Set("Retry-After", "1")
	}
	h.json(w, status, struct {
		Error     *ledger.Error `json:"error"`
		RequestID string        `json:"request_id"`
	}{p, r.Context().Value(requestIDKey{}).(string)})
}
func (h *handler) json(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
