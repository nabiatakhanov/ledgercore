package main

import (
	"context"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"ledgercore/internal/gateway"
	"ledgercore/internal/ratelimit"
	"ledgercore/internal/rpc"
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
		log.Error("gateway stopped", "error", err.Error())
		os.Exit(1)
	}
}
func run(log *slog.Logger) (result error) {
	token, internal := os.Getenv("API_TOKEN"), os.Getenv("INTERNAL_RPC_TOKEN")
	if len(token) < 16 || len(internal) < 16 {
		return errors.New("API_TOKEN and INTERNAL_RPC_TOKEN at least16bytes required")
	}
	limit, err := strconv.ParseInt(env("RATE_LIMIT", "60"), 10, 64)
	if err != nil {
		return errors.New("invalid RATE_LIMIT")
	}
	window, err := time.ParseDuration(env("RATE_WINDOW", "1m"))
	if err != nil {
		return errors.New("invalid RATE_WINDOW")
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	rm := rpc.NewMetrics(reg)
	httpMetrics := gateway.NewMetrics(reg)
	ledgerConn, err := rpc.Dial(env("LEDGER_GRPC_TARGET", "ledger:50051"), internal, rm)
	if err != nil {
		return err
	}
	activityConn, err := rpc.Dial(env("ACTIVITY_GRPC_TARGET", "activity:50052"), internal, rm)
	if err != nil {
		_ = ledgerConn.Close()
		return err
	}
	cache := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "redis:6379"), Password: os.Getenv("REDIS_PASSWORD"), DialTimeout: 500 * time.Millisecond, ReadTimeout: 300 * time.Millisecond, WriteTimeout: 300 * time.Millisecond, PoolTimeout: 300 * time.Millisecond, PoolSize: 16, MaxActiveConns: 16, MaxConcurrentDials: 4, PipelinePoolSize: -1, MaxRetries: -1, ContextTimeoutEnabled: true})
	budget, err := ratelimit.New(cache, limit, window)
	var deadline time.Time
	defer func() {
		if deadline.IsZero() {
			deadline = time.Now().Add(5 * time.Second)
		}
		cleanup, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		result = errors.Join(result, rpc.CloseWithin(cleanup, func() { _ = ledgerConn.Close(); _ = activityConn.Close(); _ = cache.Close() }))
	}()
	if err != nil {
		return err
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "gateway_redis_connections_total", Help: "Current Redis client connections."}, func() float64 { return float64(cache.PoolStats().TotalConns) }))
	ready := func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if e := rpc.CheckHealth(ctx, ledgerConn); e != nil {
			return errors.New("ledger unavailable")
		}
		if e := cache.Ping(ctx).Err(); e != nil {
			return errors.New("Redis unavailable")
		}
		return nil
	}
	handler := gateway.New(rpc.NewLedgerClient(ledgerConn), rpc.NewActivityClient(activityConn), budget, token, ready, log, httpMetrics)
	api := server(env("HTTP_ADDR", ":8080"), handler)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metrics := server(env("METRICS_ADDR", "127.0.0.1:9090"), mux)
	a, err := net.Listen("tcp", api.Addr)
	if err != nil {
		return errors.New("Gateway HTTP listen failed")
	}
	defer a.Close()
	b, err := net.Listen("tcp", metrics.Addr)
	if err != nil {
		return errors.New("Gateway metrics listen failed")
	}
	defer b.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	results := make(chan error, 2)
	go func() { results <- api.Serve(a) }()
	go func() { results <- metrics.Serve(b) }()
	log.Info("gateway started")
	select {
	case <-ctx.Done():
	case err = <-results:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			result = errors.New("Gateway listener failed")
		}
	}
	deadline = time.Now().Add(10 * time.Second)
	shutdown, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	done := make(chan error, 2)
	for _, s := range []*http.Server{api, metrics} {
		go func() {
			e := s.Shutdown(shutdown)
			if e != nil {
				_ = s.Close()
			}
			done <- e
		}()
	}
	for range 2 {
		result = errors.Join(result, <-done)
	}
	return result
}
func env(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}
func server(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 7 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
}
