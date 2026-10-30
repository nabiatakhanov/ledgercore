// Package events defines the versioned, precision-safe Kafka operation contract.
package events

import (
	"encoding/json"
	"fmt"
	"time"

	"ledgercore/internal/ledger"
)

const SchemaVersion = 1
const TransferCreated = "transfer.created"
const TransferReversed = "transfer.reversed"
const MaxBytes = 1 << 20

// Event intentionally excludes idempotency keys, tokens and client metadata.
// Every new ordinary transfer or reversal has exactly one logical event. Funding
// and operations committed before migration 002 have no implicit activity event.
type Event struct {
	EventID              string    `json:"event_id"`
	SchemaVersion        int       `json:"schema_version"`
	Type                 string    `json:"type"`
	JournalTransactionID string    `json:"journal_transaction_id"`
	OccurredAt           time.Time `json:"occurred_at"`
	FromAccountID        string    `json:"from_account_id"`
	ToAccountID          string    `json:"to_account_id"`
	AmountMinor          string    `json:"amount_minor"`
	Currency             string    `json:"currency"`
	ReversalOf           string    `json:"reversal_of,omitempty"`
}

// InvalidEvent classifies permanent input errors for DLQ. Infrastructure errors
// must not be converted into this type or silently advance a consumer offset.
type InvalidEvent struct{ Code string }

func (e *InvalidEvent) Error() string { return e.Code }

func FromTransaction(t ledger.Transaction) (Event, error) {
	e := Event{EventID: t.ID, SchemaVersion: SchemaVersion, JournalTransactionID: t.ID, OccurredAt: t.CreatedAt.UTC(), FromAccountID: t.FromAccountID, ToAccountID: t.ToAccountID, AmountMinor: t.AmountMinor, Currency: t.Currency}
	switch t.Kind {
	case "transfer":
		e.Type = TransferCreated
	case "reversal":
		e.Type = TransferReversed
		if t.ReversalOf != nil {
			e.ReversalOf = *t.ReversalOf
		}
	default:
		return e, fmt.Errorf("operation %s has no activity event", t.Kind)
	}
	return e, e.Validate()
}

func (e Event) Validate() error {
	if e.SchemaVersion != SchemaVersion {
		return &InvalidEvent{Code: "unsupported_schema"}
	}
	for _, id := range []string{e.EventID, e.JournalTransactionID, e.FromAccountID, e.ToAccountID} {
		canonical, err := ledger.NormalizeID(id)
		if err != nil || canonical != id {
			return &InvalidEvent{Code: "invalid_event"}
		}
	}
	if e.EventID != e.JournalTransactionID {
		return &InvalidEvent{Code: "invalid_event"}
	}
	if e.FromAccountID == e.ToAccountID || e.Currency != "RUB" || e.OccurredAt.IsZero() {
		return &InvalidEvent{Code: "invalid_event"}
	}
	amount, err := ledger.ParseMoney(e.AmountMinor)
	if err != nil || amount.String() != e.AmountMinor {
		return &InvalidEvent{Code: "invalid_event"}
	}
	switch e.Type {
	case TransferCreated:
		if e.ReversalOf != "" {
			return &InvalidEvent{Code: "invalid_event"}
		}
	case TransferReversed:
		canonical, err := ledger.NormalizeID(e.ReversalOf)
		if err != nil || canonical != e.ReversalOf || e.ReversalOf == e.JournalTransactionID {
			return &InvalidEvent{Code: "invalid_event"}
		}
	default:
		return &InvalidEvent{Code: "unsupported_type"}
	}
	return nil
}

func Decode(data []byte) (Event, error) {
	var e Event
	if len(data) == 0 || len(data) > MaxBytes {
		return e, &InvalidEvent{Code: "malformed"}
	}
	// Unknown optional fields are additive-compatible. Money remains a string,
	// and missing required fields fail semantic validation below.
	if err := json.Unmarshal(data, &e); err != nil {
		return e, &InvalidEvent{Code: "malformed"}
	}
	e.OccurredAt = e.OccurredAt.UTC()
	return e, e.Validate()
}
