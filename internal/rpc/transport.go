// Package rpc adapts application use cases to private versioned gRPC contracts.
package rpc

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"ledgercore/internal/httpapi"
	"ledgercore/internal/ledger"
)

const MaxMessageBytes = 1 << 20
const CallTimeout = 4 * time.Second

type requestIDKey struct{}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}
func RequestID(ctx context.Context) string {
	if id, _ := ctx.Value(requestIDKey{}).(string); id != "" {
		return id
	}
	return httpapi.RequestID(ctx)
}

type Metrics struct {
	calls    *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	inFlight *prometheus.GaugeVec
}

func NewMetrics(r prometheus.Registerer) *Metrics {
	m := &Metrics{calls: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ledger_grpc_calls_total", Help: "gRPC calls by fixed method, side and status."}, []string{"side", "method", "code"}), latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "ledger_grpc_duration_seconds", Help: "gRPC latency including server application or client attempts.", Buckets: prometheus.DefBuckets}, []string{"side", "method"}), inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "ledger_grpc_in_flight", Help: "Current admitted application/server calls and outbound client attempts."}, []string{"side"})}
	r.MustRegister(m.calls, m.latency, m.inFlight)
	return m
}
func methodLabel(method string) string {
	switch method {
	case "/ledger.v1.LedgerService/CreateTransfer", "/ledger.v1.LedgerService/GetTransfer", "/ledger.v1.LedgerService/ReverseTransfer", "/ledger.v1.LedgerService/CreateAccount", "/ledger.v1.LedgerService/GetAccount", "/ledger.v1.LedgerService/GetAccountEntries", "/activity.v1.ActivityService/GetAccountActivity", "/grpc.health.v1.Health/Check":
		return method
	default:
		return "other"
	}
}
func (m *Metrics) observe(side, method string, start time.Time, err error) {
	if m == nil {
		return
	}
	method = methodLabel(method)
	m.calls.WithLabelValues(side, method, status.Code(err).String()).Inc()
	m.latency.WithLabelValues(side, method).Observe(time.Since(start).Seconds())
}
func ServerOptions(token string, log *slog.Logger, m *Metrics) []grpc.ServerOption {
	if len(token) < 16 {
		panic("internal token must contain at least16 bytes")
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	expected := sha256.Sum256([]byte(token))
	// One service-wide admission budget, in addition to each connection's stream
	// cap and the DB pool, prevents unbounded application goroutines waiting on SQL.
	slots := make(chan struct{}, 64)
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (response any, err error) {
		start := time.Now()
		id := ""
		md, _ := metadata.FromIncomingContext(ctx)
		if values := md.Get("x-request-id"); len(values) == 1 {
			id, _ = ledger.NormalizeID(values[0])
		}
		if id == "" {
			id = ledger.NewID()
		}
		ctx = WithRequestID(ctx, id)
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-request-id", id))
		defer func() {
			if recover() != nil {
				err = status.Error(codes.Internal, "internal service error")
			}
			m.observe("server", info.FullMethod, start, err)
			log.Info("gRPC completed", "request_id", id, "method", methodLabel(info.FullMethod), "code", status.Code(err).String(), "duration_ms", time.Since(start).Milliseconds())
		}()
		if info.FullMethod != "/grpc.health.v1.Health/Check" {
			values := md.Get("authorization")
			candidate := ""
			if len(values) == 1 && strings.HasPrefix(values[0], "Bearer ") {
				candidate = strings.TrimPrefix(values[0], "Bearer ")
			}
			actual := sha256.Sum256([]byte(candidate))
			if candidate == "" || subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
				return nil, status.Error(codes.Unauthenticated, "internal authentication required")
			}
		}
		ctx, cancel := context.WithTimeout(ctx, CallTimeout)
		defer cancel()
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			return nil, status.Error(codes.ResourceExhausted, "service concurrency budget exhausted")
		}
		if m != nil {
			m.inFlight.WithLabelValues("server").Inc()
			defer m.inFlight.WithLabelValues("server").Dec()
		}
		return next(ctx, req)
	}
	return []grpc.ServerOption{grpc.UnaryInterceptor(interceptor), grpc.ConnectionTimeout(5 * time.Second), grpc.MaxHeaderListSize(16 << 10), grpc.MaxConcurrentStreams(64), grpc.MaxRecvMsgSize(MaxMessageBytes), grpc.MaxSendMsgSize(MaxMessageBytes)}
}
func Dial(target, token string, m *Metrics) (*grpc.ClientConn, error) {
	if target == "" || len(token) < 16 {
		return nil, errors.New("gRPC target and internal token required")
	}
	intercept := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, next grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		if m != nil {
			m.inFlight.WithLabelValues("client").Inc()
			defer m.inFlight.WithLabelValues("client").Dec()
		}
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		md.Set("authorization", "Bearer "+token)
		id := RequestID(ctx)
		if id == "" {
			id = ledger.NewID()
		}
		md.Set("x-request-id", id)
		ctx = metadata.NewOutgoingContext(ctx, md)
		err := next(ctx, method, req, reply, cc, opts...)
		m.observe("client", method, start, err)
		return err
	}
	return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithUnaryInterceptor(intercept), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageBytes), grpc.MaxCallSendMsgSize(MaxMessageBytes)))
}

// Only known application failures cross the trust boundary; raw SQL never does.
var domainStatuses = map[string]int{"invalid_request": 400, "not_found": 404, "protected_account": 403, "insufficient_funds": 409, "balance_overflow": 409, "sequence_overflow": 409, "already_reversed": 409, "not_reversible": 409, "idempotency_conflict": 409}

func ToStatus(err error, replayed bool) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "deadline exceeded; retry same operation key")
	}
	var p *ledger.Error
	if errors.As(err, &p) && domainStatuses[p.Code] != 0 && domainStatuses[p.Code] == p.Status {
		code := codes.FailedPrecondition
		switch p.Status {
		case 400:
			code = codes.InvalidArgument
		case 403:
			code = codes.PermissionDenied
		case 404:
			code = codes.NotFound
		}
		if p.Code == "idempotency_conflict" || p.Code == "already_reversed" {
			code = codes.AlreadyExists
		}
		s := status.New(code, p.Message)
		with, e := s.WithDetails(&errdetails.ErrorInfo{Reason: strings.ToUpper(p.Code), Domain: "ledgercore", Metadata: map[string]string{"domain_code": p.Code, "http_status": strconv.Itoa(p.Status), "replayed": strconv.FormatBool(replayed)}})
		if e == nil {
			return with.Err()
		}
	}
	var network net.Error
	var postgres *pgconn.PgError
	if errors.As(err, &network) || errors.As(err, &postgres) && (strings.HasPrefix(postgres.Code, "08") || strings.HasPrefix(postgres.Code, "53") || strings.HasPrefix(postgres.Code, "57") || postgres.Code == "55P03") {
		return status.Error(codes.Unavailable, "service temporarily unavailable; retry same operation key")
	}
	return status.Error(codes.Internal, "internal service error; retry same operation key to establish outcome")
}
func domainError(err error) (*ledger.Error, bool, bool) {
	s, ok := status.FromError(err)
	if !ok || s == nil {
		return nil, false, false
	}
	for _, d := range s.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.Domain == "ledgercore" {
			code := info.Metadata["domain_code"]
			httpStatus := domainStatuses[code]
			if httpStatus != 0 && strconv.Itoa(httpStatus) == info.Metadata["http_status"] {
				return ledger.Problem(code, s.Message(), httpStatus), info.Metadata["replayed"] == "true", true
			}
		}
	}
	return nil, false, false
}
func clientError(err error) error {
	if p, _, ok := domainError(err); ok {
		return p
	}
	return errors.New("remote service unavailable; retry same operation key")
}

// Retry only Unavailable, at most once, under one deadline and one request/key.
func call[T any](ctx context.Context, retry bool, invoke func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	id := RequestID(ctx)
	if id == "" {
		id = ledger.NewID()
	}
	ctx = WithRequestID(ctx, id)
	v, err := invoke(ctx)
	if retry && status.Code(err) == codes.Unavailable && ctx.Err() == nil {
		timer := time.NewTimer(20 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return v, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
		return invoke(ctx)
	}
	return v, err
}

type healthServer struct {
	healthpb.UnimplementedHealthServer
	ready func(context.Context) error
}

func (h *healthServer) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	s := healthpb.HealthCheckResponse_SERVING
	if h.ready != nil && h.ready(ctx) != nil {
		s = healthpb.HealthCheckResponse_NOT_SERVING
	}
	return &healthpb.HealthCheckResponse{Status: s}, nil
}
func RegisterHealth(s *grpc.Server, ready func(context.Context) error) {
	healthpb.RegisterHealthServer(s, &healthServer{ready: ready})
}
func CheckHealth(ctx context.Context, cc grpc.ClientConnInterface) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if r.Status != healthpb.HealthCheckResponse_SERVING {
		return errors.New("service not serving")
	}
	return nil
}
func GracefulStop(ctx context.Context, s *grpc.Server) error {
	done := make(chan struct{})
	go func() { s.GracefulStop(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.Stop()
		return ctx.Err()
	}
}
func CloseWithin(ctx context.Context, closeFn func()) error {
	done := make(chan struct{})
	go func() { closeFn(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
