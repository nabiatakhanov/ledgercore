// activity consumes committed ledger events and serves an eventual read model.
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
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	pb "ledgercore/gen/activity/v1"
	"ledgercore/internal/activity"
	"ledgercore/internal/rpc"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, log, os.Args[1:]); err != nil {
		log.Error("activity stopped", "category", "component_failure")
		os.Exit(1)
	}
}
func env(key, fallback string) string {
	if x := os.Getenv(key); x != "" {
		return x
	}
	return fallback
}
func pool(ctx context.Context, key string) (*pgxpool.Pool, error) {
	dsn := os.Getenv(key)
	if dsn == "" {
		return nil, fmt.Errorf("%s is required", key)
	}
	c, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid activity database configuration")
	}
	maxConns, err := strconv.Atoi(env("ACTIVITY_DB_MAX_CONNS", "16"))
	if err != nil || maxConns < 1 || maxConns > 128 {
		return nil, errors.New("ACTIVITY_DB_MAX_CONNS must be 1..128")
	}
	c.MaxConns = int32(maxConns)
	c.MinConns = 1
	c.MaxConnIdleTime = 5 * time.Minute
	c.MaxConnLifetime = 30 * time.Minute
	c.ConnConfig.ConnectTimeout = 3 * time.Second
	c.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	c.ConnConfig.RuntimeParams["lock_timeout"] = "3000"
	c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "10000"
	p, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, errors.New("activity database pool failed")
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, errors.New("activity database unavailable")
	}
	return p, nil
}
func run(ctx context.Context, log *slog.Logger, args []string) error {
	if len(args) > 0 && (len(args) != 1 || args[0] != "migrate") {
		return errors.New("usage: activity [migrate]")
	}
	if len(args) > 0 && args[0] == "migrate" {
		deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		p, err := pool(deadline, "ACTIVITY_MIGRATION_DATABASE_URL")
		if err != nil {
			return err
		}
		defer p.Close()
		return activity.Migrate(deadline, p)
	}
	token := os.Getenv("INTERNAL_RPC_TOKEN")
	if len(token) < 16 {
		return errors.New("INTERNAL_RPC_TOKEN requires at least16 bytes")
	}
	start, cancel := context.WithTimeout(ctx, 5*time.Second)
	p, err := pool(start, "ACTIVITY_DATABASE_URL")
	cancel()
	if err != nil {
		return err
	}
	defer func() {
		shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = rpc.CloseWithin(shutdown, p.Close)
	}()
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	parallel, err := strconv.Atoi(env("ACTIVITY_PARTITION_CONCURRENCY", "4"))
	if err != nil {
		return errors.New("invalid partition concurrency")
	}
	c, err := activity.NewConsumer(activity.ConsumerConfig{Brokers: strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ","), Topic: env("KAFKA_TOPIC", "ledger.operations.v1"), DLQTopic: env("KAFKA_DLQ_TOPIC", "ledger.operations.dlq.v1"), Group: env("ACTIVITY_GROUP_ID", "activity-v1"), MaxPartitions: parallel, RecordTimeout: 15 * time.Second}, p, registry)
	if err != nil {
		return err
	}
	defer func() {
		shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = rpc.CloseWithin(shutdown, c.Close)
	}()
	check := func(checkCtx context.Context) error {
		if err := p.Ping(checkCtx); err != nil {
			return errors.New("activity database unavailable")
		}
		if err := c.Kafka.Ping(checkCtx); err != nil {
			return errors.New("activity Kafka unavailable")
		}
		return nil
	}
	s := grpc.NewServer(rpc.ServerOptions(token, log, rpc.NewMetrics(registry))...)
	pb.RegisterActivityServiceServer(s, rpc.NewActivityServer(activity.New(p)))
	rpc.RegisterHealth(s, check)
	listener, err := net.Listen("tcp", env("ACTIVITY_GRPC_ADDR", ":50052"))
	if err != nil {
		return err
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Timeout: 2 * time.Second}))
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ready, done := context.WithTimeout(r.Context(), time.Second)
		defer done()
		if check(ready) != nil {
			http.Error(w, "not ready", 503)
			return
		}
		w.WriteHeader(200)
	})
	metrics := &http.Server{Addr: env("ACTIVITY_METRICS_ADDR", "127.0.0.1:9093"), Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second}
	workCtx, done := context.WithCancel(ctx)
	defer done()
	errs := make(chan error, 3)
	go func() { errs <- s.Serve(listener) }()
	go func() { errs <- metrics.ListenAndServe() }()
	go func() { errs <- c.Run(workCtx) }()
	log.Info("activity started", "grpc_addr", listener.Addr().String())
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	done()
	shutdown, finish := context.WithTimeout(context.Background(), 10*time.Second)
	defer finish()
	_ = metrics.Shutdown(shutdown)
	_ = rpc.GracefulStop(shutdown, s)
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
