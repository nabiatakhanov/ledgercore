// Package gateway composes existing HTTP contracts with gRPC clients and Redis.
package gateway

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"ledgercore/internal/activity"
	"ledgercore/internal/httpapi"
	"ledgercore/internal/ledger"
	"ledgercore/internal/ratelimit"
	"ledgercore/internal/rpc"
)

type Budget interface {
	Allow(context.Context, string) (ratelimit.Decision, error)
}
type ActivityReader interface {
	GetAccountActivity(context.Context, string, string, int) (activity.Page, error)
}
type Metrics struct {
	http    *prometheus.CounterVec
	latency *prometheus.HistogramVec
	rate    *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_http_requests_total", Help: "Gateway requests by fixed route and status class."}, []string{"route", "status"}), prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_http_duration_seconds", Help: "Gateway end-to-end HTTP latency.", Buckets: prometheus.DefBuckets}, []string{"route"}), prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_rate_admissions_total", Help: "Redis admission decisions: allowed, limited, error."}, []string{"outcome"})}
	reg.MustRegister(m.http, m.latency, m.rate)
	return m
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
		w.ResponseWriter.WriteHeader(code)
	}
}
func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func routeLabel(r *http.Request) string {
	p := r.URL.Path
	switch {
	case p == "/livez":
		return "livez"
	case p == "/readyz":
		return "readyz"
	case strings.HasSuffix(p, "/activity") && strings.HasPrefix(p, "/v1/accounts/"):
		return "activity"
	case strings.HasPrefix(p, "/v1/accounts"):
		return "accounts"
	case strings.HasPrefix(p, "/v1/transfers"):
		return "transfers"
	default:
		return "other"
	}
}
func New(service httpapi.Service, reader ActivityReader, budget Budget, token string, ready func(context.Context) error, log *slog.Logger, metrics *Metrics) http.Handler {
	base := httpapi.New(service, token, ready, log)
	expected := sha256.Sum256([]byte(token))
	clientID := ratelimit.ClientID(token)
	authenticated := func(r *http.Request) bool {
		values := r.Header.Values("Authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
			return false
		}
		actual := sha256.Sum256([]byte(strings.TrimPrefix(values[0], "Bearer ")))
		return subtle.ConstantTimeCompare(expected[:], actual[:]) == 1
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusWriter{ResponseWriter: w}
		defer func() {
			if metrics != nil {
				code := wrapped.status
				if code == 0 {
					code = 200
				}
				metrics.http.WithLabelValues(routeLabel(r), strconv.Itoa(code/100)+"xx").Inc()
				metrics.latency.WithLabelValues(routeLabel(r)).Observe(time.Since(start).Seconds())
			}
		}()
		w = wrapped
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/") {
			if !authenticated(r) {
				writeError(w, 401, "unauthorized", "valid bearer token required", 0)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 300*time.Millisecond)
			decision, err := budget.Allow(ctx, clientID)
			cancel()
			if err != nil {
				if metrics != nil {
					metrics.rate.WithLabelValues("error").Inc()
				}
				writeError(w, 503, "rate_limiter_unavailable", "write admission unavailable; retry with the same idempotency key", 1)
				return
			}
			if !decision.Allowed {
				if metrics != nil {
					metrics.rate.WithLabelValues("limited").Inc()
				}
				writeError(w, 429, "rate_limited", "write request budget exhausted", ratelimit.RetryAfter(decision.RemainingTTL))
				return
			}
			if metrics != nil {
				metrics.rate.WithLabelValues("allowed").Inc()
			}
		}
		if strings.HasPrefix(r.URL.Path, "/v1/accounts/") && strings.HasSuffix(r.URL.Path, "/activity") {
			if !authenticated(r) {
				writeError(w, 401, "unauthorized", "valid bearer token required", 0)
				return
			}
			if r.Method != http.MethodGet {
				w.Header().Set("Allow", "GET")
				writeError(w, 405, "method_not_allowed", "method not allowed", 0)
				return
			}
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/accounts/"), "/activity")
			id, err := ledger.NormalizeID(id)
			if err != nil {
				writeError(w, 400, "invalid_request", "invalid account UUID", 0)
				return
			}
			limit := 50
			query := r.URL.Query()
			if raw, ok := query["limit"]; ok {
				if len(raw) != 1 {
					writeError(w, 400, "invalid_request", "one limit is required", 0)
					return
				}
				limit, err = strconv.Atoi(raw[0])
				if err != nil || limit < 1 || limit > 100 {
					writeError(w, 400, "invalid_request", "limit must be1..100", 0)
					return
				}
			}
			if len(query["cursor"]) > 1 || len(query.Get("cursor")) > 256 {
				writeError(w, 400, "invalid_request", "invalid cursor", 0)
				return
			}
			requestID := ledger.NewID()
			ctx, cancel := context.WithTimeout(rpc.WithRequestID(r.Context(), requestID), rpc.CallTimeout)
			defer cancel()
			page, err := reader.GetAccountActivity(ctx, id, query.Get("cursor"), limit)
			if err != nil {
				if p, ok := err.(*ledger.Error); ok {
					writeErrorID(w, p.Status, p.Code, p.Message, 0, requestID)
				} else {
					writeErrorID(w, 503, "temporarily_unavailable", "activity projection unavailable", 1, requestID)
				}
				return
			}
			w.Header().Set("X-Request-ID", requestID)
			w.Header().Set("X-Read-Model", "eventually-consistent")
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			_ = json.NewEncoder(w).Encode(page)
			return
		}
		base.ServeHTTP(w, r)
	})
}
func writeError(w http.ResponseWriter, status int, code, message string, retry int64) {
	writeErrorID(w, status, code, message, retry, ledger.NewID())
}
func writeErrorID(w http.ResponseWriter, status int, code, message string, retry int64, id string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Request-ID", id)
	w.Header().Set("Cache-Control", "no-store")
	if status == 401 {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	if retry > 0 {
		w.Header().Set("Retry-After", strconv.FormatInt(retry, 10))
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error     *ledger.Error `json:"error"`
		RequestID string        `json:"request_id"`
	}{ledger.Problem(code, message, status), id})
}
