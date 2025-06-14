package observability

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsUseBoundedLabels(t *testing.T) {
	m := New()
	m.Operation("transfer", "success", 0.01)
	m.Operation("an-account-id", "secret-error-text", 0.01)
	m.Retry()
	m.Conflict()
	m.LockWait(0.005)
	m.Reconciliation(true, 0.2)
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	for _, want := range []string{`ledger_operations_total{kind="transfer",outcome="success"} 1`, `ledger_operations_total{kind="other",outcome="other"} 1`, `ledger_transaction_retries_total 1`, `ledger_idempotency_conflicts_total 1`, `ledger_reconciliations_total{result="ok"} 1`} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatal("missing", want)
		}
	}
	if strings.Contains(w.Body.String(), "an-account-id") || strings.Contains(w.Body.String(), "secret-error-text") {
		t.Fatal("unbounded label")
	}
}
