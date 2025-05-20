// Package ledger defines the checked monetary model and canonical requests.
package ledger

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const SystemAccountID = "00000000-0000-0000-0000-000000000001"

// Money is an integral number of minor currency units. Operations require > 0.
type Money int64

func ParseMoney(s string) (Money, error) {
	if len(s) == 0 || len(s) > 64 {
		return 0, Invalid("amount_minor must be a positive decimal int64 string")
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, Invalid("amount_minor must contain decimal digits")
		}
	}
	n, err := strconv.ParseInt(strings.TrimLeft(s, "0"), 10, 64)
	if err != nil || n <= 0 {
		return 0, Invalid("amount_minor is outside positive int64 range")
	}
	return Money(n), nil
}

func Add(balance int64, delta int64) (int64, error) {
	if (delta > 0 && balance > math.MaxInt64-delta) || (delta < 0 && balance < math.MinInt64-delta) {
		return 0, Problem("balance_overflow", "balance exceeds int64 range", 409)
	}
	return balance + delta, nil
}

// Move is the complete pure state transition, shared by the writer and model tests.
func Move(from, to int64, amount Money, allowNegative bool) (int64, int64, error) {
	if amount <= 0 {
		return 0, 0, Invalid("amount must be positive")
	}
	if !allowNegative && from < int64(amount) {
		return 0, 0, Problem("insufficient_funds", "insufficient funds", 409)
	}
	a, err := Add(from, -int64(amount))
	if err != nil {
		return 0, 0, err
	}
	b, err := Add(to, int64(amount))
	if err != nil {
		return 0, 0, err
	}
	return a, b, nil
}

func NormalizeID(s string) (string, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) != 36 {
		return "", Invalid("invalid UUID")
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return "", Invalid("invalid UUID")
			}
			continue
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return "", Invalid("invalid UUID")
		}
	}
	return s, nil
}

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	s := hex.EncodeToString(b[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"-"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }
func Problem(code, message string, status int) *Error {
	return &Error{Code: code, Message: message, Status: status}
}
func Invalid(message string) *Error { return Problem("invalid_request", message, 400) }

type TransferRequest struct {
	FromAccountID string            `json:"from_account_id"`
	ToAccountID   string            `json:"to_account_id"`
	AmountMinor   string            `json:"amount_minor"`
	Currency      string            `json:"currency"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

func (r TransferRequest) Normalize() (TransferRequest, Money, error) {
	var err error
	r.FromAccountID, err = NormalizeID(r.FromAccountID)
	if err != nil {
		return r, 0, err
	}
	r.ToAccountID, err = NormalizeID(r.ToAccountID)
	if err != nil {
		return r, 0, err
	}
	if r.FromAccountID == r.ToAccountID {
		return r, 0, Invalid("accounts must differ")
	}
	n, err := ParseMoney(r.AmountMinor)
	if err != nil {
		return r, 0, err
	}
	r.AmountMinor = strconv.FormatInt(int64(n), 10)
	r.Currency = strings.ToUpper(strings.TrimSpace(r.Currency))
	if r.Currency != "RUB" {
		return r, 0, Invalid("only RUB is supported")
	}
	if err = ValidateMetadata(r.Metadata); err != nil {
		return r, 0, err
	}
	if len(r.Metadata) == 0 {
		r.Metadata = nil
	} else {
		m := make(map[string]string, len(r.Metadata))
		for k, v := range r.Metadata {
			m[k] = v
		}
		r.Metadata = m
	}
	return r, n, nil
}

func ValidateName(name string) error {
	if !utf8.ValidString(name) || len(name) == 0 || len(name) > 128 || strings.TrimSpace(name) == "" || strings.ContainsRune(name, 0) {
		return Invalid("name must contain 1..128 UTF-8 bytes")
	}
	return nil
}
func ValidateMetadata(m map[string]string) error {
	if len(m) > 16 {
		return Invalid("metadata has more than 16 fields")
	}
	for k, v := range m {
		if !utf8.ValidString(k) || !utf8.ValidString(v) || len(k) == 0 || len(k) > 64 || len(v) > 256 || strings.ContainsRune(k, 0) || strings.ContainsRune(v, 0) {
			return Invalid("invalid metadata field length or encoding")
		}
	}
	b, _ := json.Marshal(m)
	if len(b) > 2048 {
		return Invalid("metadata exceeds 2048 encoded bytes")
	}
	return nil
}
func ValidateKey(key string) error {
	if len(key) == 0 || len(key) > 128 {
		return Invalid("Idempotency-Key must contain 1..128 visible ASCII characters")
	}
	for _, c := range key {
		if c < 33 || c > 126 {
			return Invalid("invalid Idempotency-Key")
		}
	}
	return nil
}
func Fingerprint(kind, original string, r TransferRequest) string {
	b, _ := json.Marshal(struct {
		Kind     string
		Original string
		Request  TransferRequest
	}{kind, original, r})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type Account struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Kind         string    `json:"kind"`
	Currency     string    `json:"currency"`
	BalanceMinor int64     `json:"balance_minor,string"`
	CreatedAt    time.Time `json:"created_at"`
}
type Posting struct {
	TransactionID string    `json:"transaction_id"`
	AccountID     string    `json:"account_id"`
	DeltaMinor    int64     `json:"delta_minor,string"`
	Sequence      int64     `json:"sequence,string"`
	CreatedAt     time.Time `json:"created_at"`
}
type Transaction struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	TransferRequest
	ReversalOf *string   `json:"reversal_of,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	Postings   []Posting `json:"postings"`
}
type Outcome struct {
	Status      int          `json:"-"`
	Transaction *Transaction `json:"transaction,omitempty"`
	Error       *Error       `json:"error,omitempty"`
	Replayed    bool         `json:"-"`
}
type Entries struct {
	Entries    []Posting `json:"entries"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

func (o Outcome) BusinessError() error {
	if o.Error != nil {
		return o.Error
	}
	return nil
}
func (m Money) String() string { return fmt.Sprint(int64(m)) }
