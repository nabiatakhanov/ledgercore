package rpc

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	ledgerv1 "ledgercore/gen/ledger/v1"
	"ledgercore/internal/httpapi"
	"ledgercore/internal/ledger"
	"strconv"
)

type LedgerApplication = httpapi.Service
type LedgerServer struct {
	ledgerv1.UnimplementedLedgerServiceServer
	app LedgerApplication
}

func NewLedgerServer(app LedgerApplication) *LedgerServer { return &LedgerServer{app: app} }
func (s *LedgerServer) CreateTransfer(ctx context.Context, r *ledgerv1.CreateTransferRequest) (*ledgerv1.OperationResponse, error) {
	o, err := s.app.Transfer(ctx, r.IdempotencyKey, ledger.TransferRequest{FromAccountID: r.FromAccountId, ToAccountID: r.ToAccountId, AmountMinor: strconv.FormatInt(r.AmountMinor, 10), Currency: r.Currency, Metadata: r.Metadata})
	return operation(o, err)
}
func (s *LedgerServer) ReverseTransfer(ctx context.Context, r *ledgerv1.ReverseTransferRequest) (*ledgerv1.OperationResponse, error) {
	o, err := s.app.Reverse(ctx, r.IdempotencyKey, r.Id)
	return operation(o, err)
}
func operation(o ledger.Outcome, err error) (*ledgerv1.OperationResponse, error) {
	if err != nil {
		return nil, ToStatus(err, false)
	}
	if o.Error != nil {
		return nil, ToStatus(o.Error, o.Replayed)
	}
	t, err := transactionProto(o.Transaction)
	if err != nil {
		return nil, ToStatus(err, false)
	}
	return &ledgerv1.OperationResponse{Transaction: t, Replayed: o.Replayed}, nil
}
func (s *LedgerServer) GetTransfer(ctx context.Context, r *ledgerv1.GetTransferRequest) (*ledgerv1.GetTransferResponse, error) {
	t, err := s.app.Transaction(ctx, r.Id)
	if err != nil {
		return nil, ToStatus(err, false)
	}
	v, err := transactionProto(&t)
	if err != nil {
		return nil, ToStatus(err, false)
	}
	return &ledgerv1.GetTransferResponse{Transaction: v}, nil
}
func (s *LedgerServer) CreateAccount(ctx context.Context, r *ledgerv1.CreateAccountRequest) (*ledgerv1.Account, error) {
	a, err := s.app.CreateAccount(ctx, r.Name)
	if err != nil {
		return nil, ToStatus(err, false)
	}
	return accountProto(a), nil
}
func (s *LedgerServer) GetAccount(ctx context.Context, r *ledgerv1.GetAccountRequest) (*ledgerv1.Account, error) {
	a, err := s.app.Account(ctx, r.Id)
	if err != nil {
		return nil, ToStatus(err, false)
	}
	return accountProto(a), nil
}
func (s *LedgerServer) GetAccountEntries(ctx context.Context, r *ledgerv1.GetAccountEntriesRequest) (*ledgerv1.GetAccountEntriesResponse, error) {
	page, err := s.app.Entries(ctx, r.AccountId, r.Cursor, int(r.Limit))
	if err != nil {
		return nil, ToStatus(err, false)
	}
	out := &ledgerv1.GetAccountEntriesResponse{NextCursor: page.NextCursor}
	for _, p := range page.Entries {
		out.Entries = append(out.Entries, postingProto(p))
	}
	return out, nil
}
func transactionProto(t *ledger.Transaction) (*ledgerv1.Transaction, error) {
	if t == nil {
		return nil, errors.New("missing application transaction")
	}
	n, err := ledger.ParseMoney(t.AmountMinor)
	if err != nil {
		return nil, errors.New("invalid application transaction amount")
	}
	r := &ledgerv1.Transaction{Id: t.ID, Kind: t.Kind, FromAccountId: t.FromAccountID, ToAccountId: t.ToAccountID, AmountMinor: int64(n), Currency: t.Currency, Metadata: t.Metadata, CreatedAt: timestamppb.New(t.CreatedAt)}
	if t.ReversalOf != nil {
		r.ReversalOf = *t.ReversalOf
	}
	for _, p := range t.Postings {
		r.Postings = append(r.Postings, postingProto(p))
	}
	return r, nil
}
func postingProto(p ledger.Posting) *ledgerv1.Posting {
	return &ledgerv1.Posting{TransactionId: p.TransactionID, AccountId: p.AccountID, DeltaMinor: p.DeltaMinor, Sequence: p.Sequence, CreatedAt: timestamppb.New(p.CreatedAt)}
}
func accountProto(a ledger.Account) *ledgerv1.Account {
	return &ledgerv1.Account{Id: a.ID, Name: a.Name, Kind: a.Kind, Currency: a.Currency, BalanceMinor: a.BalanceMinor, CreatedAt: timestamppb.New(a.CreatedAt)}
}
func transactionDomain(t *ledgerv1.Transaction) *ledger.Transaction {
	if t == nil {
		return nil
	}
	r := &ledger.Transaction{ID: t.Id, Kind: t.Kind, TransferRequest: ledger.TransferRequest{FromAccountID: t.FromAccountId, ToAccountID: t.ToAccountId, AmountMinor: strconv.FormatInt(t.AmountMinor, 10), Currency: t.Currency, Metadata: t.Metadata}, CreatedAt: t.CreatedAt.AsTime(), Postings: []ledger.Posting{}}
	if t.ReversalOf != "" {
		r.ReversalOf = &t.ReversalOf
	}
	for _, p := range t.Postings {
		r.Postings = append(r.Postings, postingDomain(p))
	}
	return r
}
func postingDomain(p *ledgerv1.Posting) ledger.Posting {
	return ledger.Posting{TransactionID: p.TransactionId, AccountID: p.AccountId, DeltaMinor: p.DeltaMinor, Sequence: p.Sequence, CreatedAt: p.CreatedAt.AsTime()}
}
func accountDomain(a *ledgerv1.Account) ledger.Account {
	return ledger.Account{ID: a.Id, Name: a.Name, Kind: a.Kind, Currency: a.Currency, BalanceMinor: a.BalanceMinor, CreatedAt: a.CreatedAt.AsTime()}
}

type LedgerClient struct{ client ledgerv1.LedgerServiceClient }

func NewLedgerClient(cc grpc.ClientConnInterface) *LedgerClient {
	return &LedgerClient{ledgerv1.NewLedgerServiceClient(cc)}
}
func (c *LedgerClient) Transfer(ctx context.Context, key string, r ledger.TransferRequest) (ledger.Outcome, error) {
	r, n, err := r.Normalize()
	if err != nil {
		return ledger.Outcome{}, err
	}
	if err = ledger.ValidateKey(key); err != nil {
		return ledger.Outcome{}, err
	}
	req := &ledgerv1.CreateTransferRequest{FromAccountId: r.FromAccountID, ToAccountId: r.ToAccountID, AmountMinor: int64(n), Currency: r.Currency, Metadata: r.Metadata, IdempotencyKey: key}
	v, err := call(ctx, true, func(ctx context.Context) (*ledgerv1.OperationResponse, error) {
		return c.client.CreateTransfer(ctx, req)
	})
	return operationDomain(v, err)
}
func (c *LedgerClient) Reverse(ctx context.Context, key, id string) (ledger.Outcome, error) {
	id, err := ledger.NormalizeID(id)
	if err != nil {
		return ledger.Outcome{}, err
	}
	if err = ledger.ValidateKey(key); err != nil {
		return ledger.Outcome{}, err
	}
	req := &ledgerv1.ReverseTransferRequest{Id: id, IdempotencyKey: key}
	v, err := call(ctx, true, func(ctx context.Context) (*ledgerv1.OperationResponse, error) {
		return c.client.ReverseTransfer(ctx, req)
	})
	return operationDomain(v, err)
}
func operationDomain(v *ledgerv1.OperationResponse, err error) (ledger.Outcome, error) {
	if err != nil {
		if p, replayed, ok := domainError(err); ok {
			return ledger.Outcome{Status: p.Status, Error: p, Replayed: replayed}, nil
		}
		return ledger.Outcome{}, clientError(err)
	}
	if v == nil || v.Transaction == nil || v.Transaction.AmountMinor <= 0 {
		return ledger.Outcome{}, clientError(nil)
	}
	return ledger.Outcome{Status: 201, Transaction: transactionDomain(v.Transaction), Replayed: v.Replayed}, nil
}
func (c *LedgerClient) Transaction(ctx context.Context, id string) (ledger.Transaction, error) {
	v, err := call(ctx, true, func(ctx context.Context) (*ledgerv1.GetTransferResponse, error) {
		return c.client.GetTransfer(ctx, &ledgerv1.GetTransferRequest{Id: id})
	})
	if err != nil {
		return ledger.Transaction{}, clientError(err)
	}
	t := transactionDomain(v.Transaction)
	if t == nil {
		return ledger.Transaction{}, clientError(nil)
	}
	return *t, nil
}

// Account creation has no operation key; never retry its application call.
func (c *LedgerClient) CreateAccount(ctx context.Context, name string) (ledger.Account, error) {
	v, err := call(ctx, false, func(ctx context.Context) (*ledgerv1.Account, error) {
		return c.client.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{Name: name})
	})
	if err != nil {
		return ledger.Account{}, clientError(err)
	}
	return accountDomain(v), nil
}
func (c *LedgerClient) Account(ctx context.Context, id string) (ledger.Account, error) {
	v, err := call(ctx, true, func(ctx context.Context) (*ledgerv1.Account, error) {
		return c.client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: id})
	})
	if err != nil {
		return ledger.Account{}, clientError(err)
	}
	return accountDomain(v), nil
}
func (c *LedgerClient) Entries(ctx context.Context, id, cursor string, limit int) (ledger.Entries, error) {
	v, err := call(ctx, true, func(ctx context.Context) (*ledgerv1.GetAccountEntriesResponse, error) {
		return c.client.GetAccountEntries(ctx, &ledgerv1.GetAccountEntriesRequest{AccountId: id, Cursor: cursor, Limit: int32(limit)})
	})
	if err != nil {
		return ledger.Entries{}, clientError(err)
	}
	r := ledger.Entries{Entries: []ledger.Posting{}, NextCursor: v.NextCursor}
	for _, p := range v.Entries {
		r.Entries = append(r.Entries, postingDomain(p))
	}
	return r, nil
}
