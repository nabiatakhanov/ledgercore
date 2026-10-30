// publisher owns durable outbox delivery; it never receives money-write rights.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"ledgercore/internal/outbox"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, log); err != nil {
		log.Error("publisher stopped", "category", "component_failure")
		os.Exit(1)
	}
}
func value(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}
func run(parent context.Context, log *slog.Logger) (result error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	dsn := os.Getenv("PUBLISHER_DATABASE_URL")
	if dsn == "" {
		return errors.New("PUBLISHER_DATABASE_URL is required")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("invalid publisher database configuration")
	}
	config.MaxConns = 4
	config.MinConns = 0
	config.ConnConfig.ConnectTimeout = 3 * time.Second
	config.ConnConfig.RuntimeParams["statement_timeout"] = "4000"
	config.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	config.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	var shutdownDeadline time.Time
	defer func() {
		if shutdownDeadline.IsZero() {
			shutdownDeadline = time.Now().Add(5 * time.Second)
		}
		c, done := context.WithDeadline(context.Background(), shutdownDeadline)
		defer done()
		closed := make(chan struct{})
		go func() { pool.Close(); close(closed) }()
		select {
		case <-closed:
		case <-c.Done():
			result = errors.Join(result, c.Err())
		}
	}()
	startup, done := context.WithTimeout(ctx, 5*time.Second)
	err = pool.Ping(startup)
	done()
	if err != nil {
		return errors.New("publisher database unavailable")
	}
	producer, err := outbox.NewProducer(strings.Split(value("KAFKA_BROKERS", "kafka:9092"), ","))
	if err != nil {
		return err
	}
	producerClosed := make(chan struct{})
	closeProducer := sync.OnceFunc(func() { producer.Close(); close(producerClosed) })
	defer func() {
		if shutdownDeadline.IsZero() {
			shutdownDeadline = time.Now().Add(5 * time.Second)
		}
		cleanup, done := context.WithDeadline(context.Background(), shutdownDeadline)
		defer done()
		go closeProducer()
		select {
		case <-producerClosed:
		case <-cleanup.Done():
			result = errors.Join(result, cleanup.Err())
		}
	}()
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := outbox.NewMetrics(registry)
	p, err := outbox.New(pool, producer, outbox.DefaultConfig(value("KAFKA_TOPIC", "ledger.operations.v1")), metrics)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		c, done := context.WithTimeout(r.Context(), time.Second)
		defer done()
		if pool.Ping(c) != nil || producer.Ping(c) != nil {
			http.Error(w, "dependency unavailable", 503)
			return
		}
		w.WriteHeader(200)
	})
	server := &http.Server{Addr: value("METRICS_ADDR", "127.0.0.1:9092"), Handler: mux, ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	results := make(chan error, 2)
	go func() { results <- server.Serve(listener) }()
	go func() { results <- p.Run(ctx) }()
	log.Info("publisher started", "topic", p.Config.Topic)
	select {
	case <-ctx.Done():
	case result = <-results:
	}
	stopping := parent.Err() != nil
	if stopping {
		result = nil
	}
	cancel()
	go closeProducer()
	shutdownDeadline = time.Now().Add(10 * time.Second)
	shutdown, done := context.WithDeadline(context.Background(), shutdownDeadline)
	defer done()
	if e := server.Shutdown(shutdown); e != nil {
		_ = server.Close()
		result = errors.Join(result, e)
	}
	// Canceling the producer aborts pending network work before closing clients.
	select {
	case e := <-results:
		if e != nil && !errors.Is(e, http.ErrServerClosed) && !stopping {
			result = errors.Join(result, e)
		}
	case <-shutdown.Done():
		result = errors.Join(result, shutdown.Err())
	}
	if errors.Is(result, http.ErrServerClosed) {
		return nil
	}
	return result
}
