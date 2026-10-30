package rpc

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	activityv1 "ledgercore/gen/activity/v1"
	"ledgercore/internal/activity"
	"ledgercore/internal/ledger"
	"strconv"
)

type ActivityApplication interface {
	GetAccountActivity(context.Context, string, string, int) (activity.Page, error)
}
type ActivityServer struct {
	activityv1.UnimplementedActivityServiceServer
	app ActivityApplication
}

func NewActivityServer(app ActivityApplication) *ActivityServer { return &ActivityServer{app: app} }
func (s *ActivityServer) GetAccountActivity(ctx context.Context, r *activityv1.GetAccountActivityRequest) (*activityv1.GetAccountActivityResponse, error) {
	page, err := s.app.GetAccountActivity(ctx, r.AccountId, r.Cursor, int(r.Limit))
	if err != nil {
		return nil, ToStatus(err, false)
	}
	v := &activityv1.GetAccountActivityResponse{NextCursor: page.NextCursor, Summary: &activityv1.Summary{AccountId: page.Summary.AccountID, IncomingMinor: page.Summary.IncomingMinor, OutgoingMinor: page.Summary.OutgoingMinor, IncomingCount: page.Summary.IncomingCount, OutgoingCount: page.Summary.OutgoingCount}}
	for _, e := range page.Entries {
		n, err := ledger.ParseMoney(e.AmountMinor)
		if err != nil {
			return nil, ToStatus(err, false)
		}
		p := &activityv1.Entry{EventId: e.EventID, JournalTransactionId: e.JournalTransactionID, Type: e.Type, Direction: e.Direction, AmountMinor: int64(n), Currency: e.Currency, OccurredAt: timestamppb.New(e.OccurredAt), ProjectedAt: timestamppb.New(e.ProjectedAt), Sequence: e.Sequence}
		if e.ReversalOf != nil {
			p.ReversalOf = *e.ReversalOf
		}
		v.Entries = append(v.Entries, p)
	}
	return v, nil
}

type ActivityClient struct {
	client activityv1.ActivityServiceClient
}

func NewActivityClient(cc grpc.ClientConnInterface) *ActivityClient {
	return &ActivityClient{activityv1.NewActivityServiceClient(cc)}
}
func (c *ActivityClient) GetAccountActivity(ctx context.Context, id, cursor string, limit int) (activity.Page, error) {
	v, err := call(ctx, true, func(ctx context.Context) (*activityv1.GetAccountActivityResponse, error) {
		return c.client.GetAccountActivity(ctx, &activityv1.GetAccountActivityRequest{AccountId: id, Cursor: cursor, Limit: int32(limit)})
	})
	if err != nil {
		return activity.Page{}, clientError(err)
	}
	r := activity.Page{Entries: []activity.Entry{}, NextCursor: v.NextCursor}
	if s := v.Summary; s != nil {
		r.Summary = activity.Summary{AccountID: s.AccountId, IncomingMinor: s.IncomingMinor, OutgoingMinor: s.OutgoingMinor, IncomingCount: s.IncomingCount, OutgoingCount: s.OutgoingCount}
	}
	for _, e := range v.Entries {
		p := activity.Entry{EventID: e.EventId, JournalTransactionID: e.JournalTransactionId, Type: e.Type, Direction: e.Direction, AmountMinor: strconv.FormatInt(e.AmountMinor, 10), Currency: e.Currency, OccurredAt: e.OccurredAt.AsTime(), ProjectedAt: e.ProjectedAt.AsTime(), Sequence: e.Sequence}
		if e.ReversalOf != "" {
			p.ReversalOf = &e.ReversalOf
		}
		r.Entries = append(r.Entries, p)
	}
	return r, nil
}
