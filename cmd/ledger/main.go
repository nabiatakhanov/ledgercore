// ledger serves the original financial application over private gRPC.
package main

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	ledgerv1 "ledgercore/gen/ledger/v1"
	"ledgercore/internal/observability"
	"ledgercore/internal/reconcile"
	"ledgercore/internal/rpc"
	"ledgercore/internal/store"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("ledger stopped", "error", err.Error())
		os.Exit(1)
	}
}
func run(log *slog.Logger) (result error) {
	token := os.Getenv("INTERNAL_RPC_TOKEN")
	if len(token) < 16 {
		return errors.New("INTERNAL_RPC_TOKEN at least16bytes is required")
	}
	if len(os.Args) > 1 && os.Args[1] == "health" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cc, err := rpc.Dial(env("LEDGER_HEALTH_TARGET", "127.0.0.1:50051"), token, nil)
		if err != nil {
			return err
		}
		defer cc.Close()
		return rpc.CheckHealth(ctx, cc)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("invalid DATABASE_URL")
	}
	cfg.MaxConns = 16
	if raw := os.Getenv("DB_MAX_CONNS"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 128 {
			return errors.New("DB_MAX_CONNS must be1..128")
		}
		cfg.MaxConns = int32(n)
	}
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "4000"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return errors.New("database pool unavailable")
	}
	var deadline time.Time
	defer func() {
		if deadline.IsZero() {
			deadline = time.Now().Add(5 * time.Second)
		}
		cleanup, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		result = errors.Join(result, rpc.CloseWithin(cleanup, pool.Close))
	}()
	startup, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = pool.Ping(startup)
	cancel()
	if err != nil {
		return errors.New("ledger database unavailable")
	}
	metrics := observability.New()
	rpcMetrics := rpc.NewMetrics(metrics.Registry)
	metrics.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "ledger_db_connections_in_use", Help: "Borrowed ledger PostgreSQL connections."}, func() float64 { return float64(pool.Stat().AcquiredConns()) }), prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "ledger_db_connections_total", Help: "Current ledger PostgreSQL connections."}, func() float64 { return float64(pool.Stat().TotalConns()) }))
	s := grpc.NewServer(rpc.ServerOptions(token, log, rpcMetrics)...)
	ledgerv1.RegisterLedgerServiceServer(s, rpc.NewLedgerServer(store.New(pool, metrics)))
	rpc.RegisterHealth(s, pool.Ping)
	listener, err := net.Listen("tcp", env("GRPC_ADDR", ":50051"))
	if err != nil {
		return errors.New("ledger gRPC listen failed")
	}
	defer listener.Close()
	metricMux := http.NewServeMux()
	metricMux.Handle("/metrics", metrics.Handler())
	metricServer := &http.Server{Addr: env("METRICS_ADDR", "127.0.0.1:9091"), Handler: metricMux, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	metricListener, err := net.Listen("tcp", metricServer.Addr)
	if err != nil {
		return errors.New("ledger metrics listen failed")
	}
	defer metricListener.Close()
	results := make(chan error, 2)
	go func() { results <- s.Serve(listener) }()
	go func() { results <- metricServer.Serve(metricListener) }()
	auditCtx, cancelAudit := context.WithCancel(ctx)
	auditDone := auditLoop(auditCtx, pool, metrics, log)
	defer cancelAudit()
	log.Info("ledger gRPC started")
	select {
	case <-ctx.Done():
	case err = <-results:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			result = errors.New("ledger listener failed")
		}
	}
	cancelAudit()
	deadline = time.Now().Add(10 * time.Second)
	shutdown, cancelShutdown := context.WithDeadline(context.Background(), deadline)
	defer cancelShutdown()
	drains := make(chan error, 2)
	go func() { drains <- rpc.GracefulStop(shutdown, s) }()
	go func() {
		e := metricServer.Shutdown(shutdown)
		if e != nil {
			_ = metricServer.Close()
		}
		drains <- e
	}()
	for range 2 {
		result = errors.Join(result, <-drains)
	}
	select {
	case <-auditDone:
	case <-shutdown.Done():
		result = errors.Join(result, shutdown.Err())
	}
	return result
}
func auditLoop(ctx context.Context, pool *pgxpool.Pool, m *observability.Metrics, log *slog.Logger) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			start := time.Now()
			audit, cancel := context.WithTimeout(ctx, 30*time.Second)
			r, err := reconcile.Run(audit, pool)
			cancel()
			if ctx.Err() != nil {
				return
			}
			m.Reconciliation(err == nil && r.OK, time.Since(start).Seconds())
			if err != nil || !r.OK {
				log.Error("ledger snapshot audit failed")
			}
			timer.Reset(time.Minute)
		}
	}()
	return done
}
func env(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}
