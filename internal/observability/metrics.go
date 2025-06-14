// Package observability provides bounded-label Prometheus ledger metrics.
package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry         *prometheus.Registry
	operations       *prometheus.CounterVec
	latency          *prometheus.HistogramVec
	retries          prometheus.Counter
	conflicts        prometheus.Counter
	lockWait         prometheus.Histogram
	reconciliations  *prometheus.CounterVec
	reconcileLatency prometheus.Histogram
}

func New() *Metrics {
	m := &Metrics{
		Registry:         prometheus.NewRegistry(),
		operations:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ledger_operations_total", Help: "Ledger operation attempts by kind and final outcome."}, []string{"kind", "outcome"}),
		latency:          prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "ledger_operation_duration_seconds", Help: "Ledger operation duration including retries and lock waits.", Buckets: prometheus.DefBuckets}, []string{"kind"}),
		retries:          prometheus.NewCounter(prometheus.CounterOpts{Name: "ledger_transaction_retries_total", Help: "Whole transaction retries for retryable SQLSTATEs."}),
		conflicts:        prometheus.NewCounter(prometheus.CounterOpts{Name: "ledger_idempotency_conflicts_total", Help: "Reused keys with different normalized parameters."}),
		lockWait:         prometheus.NewHistogram(prometheus.HistogramOpts{Name: "ledger_account_lock_wait_seconds", Help: "Elapsed time acquiring account row locks (includes DB round trip).", Buckets: prometheus.DefBuckets}),
		reconciliations:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ledger_reconciliations_total", Help: "Snapshot reconciliations by result."}, []string{"result"}),
		reconcileLatency: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "ledger_reconciliation_duration_seconds", Help: "Snapshot reconciliation duration.", Buckets: prometheus.DefBuckets}),
	}
	m.Registry.MustRegister(m.operations, m.latency, m.retries, m.conflicts, m.lockWait, m.reconciliations, m.reconcileLatency, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
func (m *Metrics) Operation(kind, outcome string, seconds float64) {
	switch kind {
	case "transfer", "reversal", "funding":
	default:
		kind = "other"
	}
	switch outcome {
	case "success", "business_error", "infrastructure_error", "replayed":
	case "not_found", "protected_account", "already_reversed", "insufficient_funds", "balance_overflow", "sequence_overflow", "idempotency_conflict", "not_reversible":
		outcome = "business_error"
	default:
		outcome = "other"
	}
	m.operations.WithLabelValues(kind, outcome).Inc()
	m.latency.WithLabelValues(kind).Observe(seconds)
}
func (m *Metrics) Retry()                   { m.retries.Inc() }
func (m *Metrics) Conflict()                { m.conflicts.Inc() }
func (m *Metrics) LockWait(seconds float64) { m.lockWait.Observe(seconds) }
func (m *Metrics) Reconciliation(ok bool, seconds float64) {
	result := "failed"
	if ok {
		result = "ok"
	}
	m.reconciliations.WithLabelValues(result).Inc()
	m.reconcileLatency.Observe(seconds)
}
