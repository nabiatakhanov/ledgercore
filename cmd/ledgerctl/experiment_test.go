package main

import (
	"encoding/json"
	"ledgercore/internal/ledger"
	"testing"
)

func TestBenchmarkClassification(t *testing.T) {
	r := ledger.TransferRequest{FromAccountID: ledger.NewID(), ToAccountID: ledger.NewID(), AmountMinor: "1", Currency: "RUB"}
	tx := ledger.Transaction{ID: ledger.NewID(), Kind: "transfer", TransferRequest: r, Postings: make([]ledger.Posting, 2)}
	b, _ := json.Marshal(ledger.Outcome{Transaction: &tx})
	if classifyResponse(201, b, r) != 201 {
		t.Fatal("valid result")
	}
	for _, x := range []struct {
		status int
		body   string
	}{{401, `{"error":{"code":"unauthorized"}}`}, {404, `{"error":{"code":"not_found"}}`}, {201, `{}`}, {503, `{"error":{"code":"temporarily_unavailable"}}`}, {409, `{"error":{"code":"idempotency_conflict"}}`}} {
		if classifyResponse(x.status, []byte(x.body), r) != 0 {
			t.Fatal(x)
		}
	}
	if classifyResponse(409, []byte(`{"error":{"code":"insufficient_funds"}}`), r) != 409 {
		t.Fatal("business refusal")
	}
	tx.AmountMinor = "2"
	b, _ = json.Marshal(ledger.Outcome{Transaction: &tx})
	if classifyResponse(201, b, r) != 0 {
		t.Fatal("wrong transfer accepted")
	}
}
