package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"ledgercore/internal/httpapi"
	"ledgercore/internal/observability"
	"ledgercore/internal/reconcile"
	"ledgercore/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("API stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dsn, token := os.Getenv("DATABASE_URL"), os.Getenv("API_TOKEN")
	if dsn == "" || len(token) < 16 {
		return errors.New("DATABASE_URL and API_TOKEN (at least 16 bytes) are required")
	}
	auditInterval, err := time.ParseDuration(env("RECONCILE_INTERVAL", "60s"))
	if err != nil || auditInterval < 10*time.Second || auditInterval > 24*time.Hour {
		return errors.New("RECONCILE_INTERVAL must be a duration between 10s and 24h")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("invalid DATABASE_URL")
	}
	config.MaxConns = 16
	if value := os.Getenv("DB_MAX_CONNS"); value != "" {
		n, e := strconv.Atoi(value)
		if e != nil || n < 1 || n > 128 {
			return errors.New("DB_MAX_CONNS must be between 1 and 128")
		}
		config.MaxConns = int32(n)
	}
	config.MinConns = 0
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	config.ConnConfig.RuntimeParams["statement_timeout"] = "4000"
	config.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	config.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return errors.New("could not create database pool")
	}
	defer pool.Close()
	startup, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = pool.Ping(startup); err != nil {
		return errors.New("database unavailable during startup")
	}
	metrics := observability.New()
	service := store.New(pool, metrics)
	api := server(env("HTTP_ADDR", ":8080"), httpapi.New(service, token, pool.Ping, log))
	metricMux := http.NewServeMux()
	metricMux.Handle("/metrics", metrics.Handler())
	metricServer := server(env("METRICS_ADDR", "127.0.0.1:9090"), metricMux)
	apiListener, err := net.Listen("tcp", api.Addr)
	if err != nil {
		return fmt.Errorf("listen HTTP: %w", err)
	}
	defer apiListener.Close()
	metricListener, err := net.Listen("tcp", metricServer.Addr)
	if err != nil {
		return fmt.Errorf("listen metrics: %w", err)
	}
	defer metricListener.Close()
	auditCtx, cancelAudit := context.WithCancel(ctx)
	auditDone := startAudit(auditCtx, pool, metrics, log, auditInterval)
	defer cancelAudit()
	results := make(chan error, 2)
	go func() { results <- api.Serve(apiListener) }()
	go func() { results <- metricServer.Serve(metricListener) }()
	log.Info("API started", "http_addr", api.Addr, "metrics_addr", metricServer.Addr)
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-results:
	}
	cancelAudit()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	// Both listeners drain concurrently under the same bounded shutdown budget.
	shutdownResults := make(chan error, 2)
	go func() { shutdownResults <- api.Shutdown(shutdown) }()
	go func() { shutdownResults <- metricServer.Shutdown(shutdown) }()
	for range 2 {
		if e := <-shutdownResults; e != nil {
			api.Close()
			metricServer.Close()
			if serveErr == nil {
				serveErr = e
			}
		}
	}
	select {
	case <-auditDone:
	case <-shutdown.Done():
		if serveErr == nil {
			serveErr = shutdown.Err()
		}
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	log.Info("API stopped gracefully")
	return nil
}

// startAudit produces reconciliation metrics in the long-running service, using
// exactly the same independent snapshot audit as ledgerctl. It never repairs.
func startAudit(ctx context.Context, pool *pgxpool.Pool, metrics *observability.Metrics, log *slog.Logger, interval time.Duration) <-chan struct{} {
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
			started := time.Now()
			auditCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			report, err := reconcile.Run(auditCtx, pool)
			cancel()
			if ctx.Err() != nil {
				return
			}
			metrics.Reconciliation(err == nil && report.OK, time.Since(started).Seconds())
			if err != nil {
				log.Error("snapshot reconciliation query failed")
			} else if !report.OK {
				log.Error("snapshot reconciliation failed", "issues", report.IssueCount)
			} else {
				log.Info("snapshot reconciliation passed", "accounts", report.Accounts, "transactions", report.Transactions)
			}
			timer.Reset(interval)
		}
	}()
	return done
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func server(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 7 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
}
