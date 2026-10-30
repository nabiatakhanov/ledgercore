package events

import (
	"encoding/json"
	"ledgercore/internal/ledger"
	"strings"
	"testing"
	"time"
)

func TestContract(t *testing.T) {
	tx := ledger.Transaction{ID: ledger.NewID(), Kind: "transfer", TransferRequest: ledger.TransferRequest{FromAccountID: ledger.NewID(), ToAccountID: ledger.NewID(), AmountMinor: "9223372036854775807", Currency: "RUB", Metadata: map[string]string{"secret": "omit"}}, CreatedAt: time.Now().UTC()}
	e, err := FromTransaction(tx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "secret") {
		t.Fatal("metadata leaked")
	}
	decoded, err := Decode(append(b[:len(b)-1], []byte(`,"optional_future_field":true}`)...))
	if err != nil || decoded.AmountMinor != tx.AmountMinor {
		t.Fatal(decoded, err)
	}
	for _, s := range []string{`{}`, `null`, `{"schema_version":99}`, `{"amount_minor":1}`, `broken`} {
		if _, err := Decode([]byte(s)); err == nil {
			t.Fatal("accepted", s)
		}
	}
	e.AmountMinor = "01"
	if e.Validate() == nil {
		t.Fatal("noncanonical amount")
	}
	e.AmountMinor = "1"
	e.Type = TransferReversed
	e.ReversalOf = ledger.NewID()
	if e.Validate() != nil {
		t.Fatal("late reversal must not require original projection")
	}
}
