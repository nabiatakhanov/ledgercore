package store

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"ledgercore/internal/ledger"
	"testing"
)

func TestRetryClassification(t *testing.T) {
	for _, code := range []string{"40001", "40P01", "23505", "57014", "08006"} {
		err := fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: code})
		want := code == "40001" || code == "40P01"
		if retryable(err) != want {
			t.Fatal(code)
		}
	}
	if retryable(ledger.Invalid("x")) {
		t.Fatal("business retry")
	}
}
func TestCursor(t *testing.T) {
	id := ledger.NewID()
	b, _ := json.Marshal(cursor{Account: id, Upper: 42, Before: 20})
	raw := base64.RawURLEncoding.EncodeToString(b)
	if _, err := decodeCursor(raw, id); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCursor(raw, ledger.NewID()); err == nil {
		t.Fatal("cross-account cursor")
	}
	if _, err := decodeCursor("!!!", id); err == nil {
		t.Fatal("invalid cursor")
	}
}
