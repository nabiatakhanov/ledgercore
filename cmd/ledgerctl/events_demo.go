package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"ledgercore/internal/activity"
	"ledgercore/internal/eventaudit"
	"ledgercore/internal/ledger"
	"ledgercore/internal/ratelimit"
	"ledgercore/internal/reconcile"
)

func eventsDemo(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("events-demo", flag.ContinueOnError)
	phase := f.String("phase", "normal", "normal, enqueue, verify, status, replay, limiter, redis-outage")
	runID := f.String("run-id", "repeatable-v1", "identity for a controlled demonstration batch")
	count := f.Int("count", 20, "operations/requests in the demonstration phase")
	timeout := f.Duration("timeout", 2*time.Minute, "overall deadline for this demonstration phase")
	if err := f.Parse(args); err != nil {
		return err
	}
	if len(*runID) == 0 || len(*runID) > 64 || ledger.ValidateKey(*runID) != nil || *count < 1 || *count > 10000 {
		return errors.New("invalid demonstration parameters")
	}
	if *timeout < time.Second || *timeout > 10*time.Minute {
		return errors.New("phase timeout must be1s..10m")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	app, err := open(ctx, "DATABASE_URL")
	if err != nil {
		return err
	}
	defer app.Close()
	read, err := open(ctx, "ACTIVITY_DATABASE_URL")
	if err != nil {
		return err
	}
	defer read.Close()
	switch *phase {
	case "status":
		return eventStatus(ctx, app, read)
	case "verify":
		started := time.Now()
		if err = waitAllEvents(ctx, app, read); err != nil {
			return err
		}
		financial, err := reconcile.Run(ctx, app)
		if err != nil {
			return err
		}
		r, err := eventaudit.Run(ctx, app, read, true)
		if err != nil {
			return err
		}
		if err = output(map[string]any{"ledger": financial, "projection": r, "catch_up_seconds": time.Since(started).Seconds()}); err != nil {
			return err
		}
		if !r.OK || !financial.OK {
			return errors.New("projection audit failed")
		}
		return nil
	case "replay":
		return replay(ctx, read)
	case "normal", "enqueue", "limiter", "redis-outage":
	default:
		return errors.New("unknown events-demo phase")
	}
	fixtureDB, err := open(ctx, "FIXTURE_DATABASE_URL")
	if err != nil {
		return err
	}
	defer fixtureDB.Close()
	ids, err := fixture(ctx, fixtureDB, "events-demo-v1", 1000000)
	if err != nil {
		return err
	}
	c, err := newClient()
	if err != nil {
		return err
	}
	if *phase == "limiter" {
		return limiterProbe(ctx, c, ids, *runID, *count)
	}
	if *phase == "redis-outage" {
		key := "events/redis-outage/" + *runID
		req := ledger.TransferRequest{FromAccountID: ids[0], ToAccountID: ids[1], AmountMinor: "1", Currency: "RUB"}
		status, body, err := c.call(ctx, "POST", "/v1/transfers", key, req)
		if err != nil {
			return err
		}
		var result ledger.Outcome
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		var reservations int
		if err = app.QueryRow(ctx, `SELECT count(*) FROM idempotency_records WHERE kind='transfer' AND key=$1`, key).Scan(&reservations); err != nil {
			return err
		}
		if result.Error == nil || result.Error.Code != "rate_limiter_unavailable" || status != 503 || reservations != 0 {
			return errors.New("redis outage was not fail-closed")
		}
		return output(map[string]any{"http_status": status, "code": result.Error.Code, "idempotency_records_for_key": reservations})
	}
	if *phase == "enqueue" {
		for i := range *count {
			req := ledger.TransferRequest{FromAccountID: ids[0], ToAccountID: ids[1], AmountMinor: "1", Currency: "RUB", Metadata: map[string]string{"demo_run": *runID, "phase": "outage"}}
			if _, err = transferHTTP(ctx, c, fmt.Sprintf("events/%s/%d", *runID, i), req); err != nil {
				return err
			}
		}
		return eventStatus(ctx, app, read)
	}
	req := ledger.TransferRequest{FromAccountID: ids[0], ToAccountID: ids[1], AmountMinor: "2500", Currency: "RUB"}
	tx, err := transferHTTP(ctx, c, "events-demo-v1/normal", req)
	if err != nil {
		return err
	}
	status, body, err := c.call(ctx, "POST", "/v1/transfers/"+tx.ID+"/reversal", "events-demo-v1/reversal", nil)
	if err != nil {
		return err
	}
	if status != 201 {
		return fmt.Errorf("reversal status%d", status)
	}
	var reversal ledger.Outcome
	if err = json.Unmarshal(body, &reversal); err != nil || reversal.Transaction == nil {
		return errors.New("invalid reversal response")
	}
	// A replay resolves the same committed operation through both gateway and
	// gRPC without generating a replacement key or a second logical event.
	again, err := transferHTTP(ctx, c, "events-demo-v1/normal", req)
	if err != nil || again.ID != tx.ID {
		return errors.New("same-key replay changed operation")
	}
	if err = waitIDs(ctx, read, []string{tx.ID, reversal.Transaction.ID}); err != nil {
		return err
	}
	status, body, err = c.call(ctx, "GET", "/v1/accounts/"+ids[0]+"/activity?limit=10", "", nil)
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("activity status%d", status)
	}
	var page activity.Page
	if err = json.Unmarshal(body, &page); err != nil {
		return err
	}
	financial, err := reconcile.Run(ctx, app)
	if err != nil {
		return err
	}
	report, err := eventaudit.Run(ctx, app, read, true)
	if err != nil {
		return err
	}
	if err = output(map[string]any{"transfer_id": tx.ID, "reversal_id": reversal.Transaction.ID, "replayed_id": again.ID, "activity": page, "ledger": financial, "projection": report}); err != nil {
		return err
	}
	if !report.OK || !financial.OK {
		return errors.New("projection not reconciled")
	}
	return nil
}

func transferHTTP(ctx context.Context, c *client, key string, req ledger.TransferRequest) (ledger.Transaction, error) {
	status, b, err := c.call(ctx, "POST", "/v1/transfers", key, req)
	if err != nil {
		return ledger.Transaction{}, err
	}
	var o ledger.Outcome
	if json.Unmarshal(b, &o) != nil {
		return ledger.Transaction{}, errors.New("invalid HTTP response")
	}
	if status != 201 || o.Transaction == nil {
		return ledger.Transaction{}, fmt.Errorf("transfer status%d", status)
	}
	return *o.Transaction, nil
}
func waitIDs(ctx context.Context, db *pgxpool.Pool, ids []string) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var n int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM activity_inbox WHERE event_id::text=ANY($1::text[])`, ids).Scan(&n); err != nil {
			return err
		}
		if n == len(ids) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("activity did not catch up: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
func waitAllEvents(ctx context.Context, ledgerDB, activityDB *pgxpool.Pool) error {
	rows, err := ledgerDB.Query(ctx, `SELECT event_id::text FROM outbox_events`)
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	return waitIDs(ctx, activityDB, ids)
}
func eventStatus(ctx context.Context, l, a *pgxpool.Pool) error {
	var backlog, total, projected int64
	var oldest float64
	if err := l.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE published_at IS NULL),COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(created_at) FILTER(WHERE published_at IS NULL)),0)::double precision FROM outbox_events`).Scan(&total, &backlog, &oldest); err != nil {
		return err
	}
	if err := a.QueryRow(ctx, `SELECT count(*) FROM activity_inbox`).Scan(&projected); err != nil {
		return err
	}
	return output(map[string]any{"outbox_events": total, "outbox_backlog": backlog, "oldest_backlog_seconds": oldest, "projected_events": projected})
}

func replay(ctx context.Context, db *pgxpool.Pool) error {
	before, err := activity.Audit(ctx, db)
	if err != nil {
		return err
	}
	topic := envValue("KAFKA_TOPIC", "ledger.operations.v1")
	group := "ledger-replay-" + ledger.NewID()
	c, err := activity.NewConsumer(activity.ConsumerConfig{Brokers: strings.Split(envValue("KAFKA_BROKERS", "kafka:9092"), ","), Topic: topic, DLQTopic: envValue("KAFKA_DLQ_TOPIC", "ledger.operations.dlq.v1"), Group: group, MaxPartitions: 3, RecordTimeout: 15 * time.Second}, db, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	admin := kadm.NewClient(c.Kafka)
	end, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	start := time.Now()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var runErr error
	for {
		offsets, e := admin.FetchOffsets(ctx, group)
		caught := e == nil
		for partition, last := range end[topic] {
			if last.Err != nil {
				return last.Err
			}
			offset, ok := offsets.Lookup(topic, partition)
			if last.Offset > 0 && (!ok || offset.Err != nil || offset.At < last.Offset) {
				caught = false
			}
		}
		if caught {
			break
		}
		select {
		case runErr = <-done:
			cancel()
			return fmt.Errorf("replay stopped: %w", runErr)
		case <-ctx.Done():
			cancel()
			return ctx.Err()
		case <-ticker.C:
		}
	}
	cancel()
	select {
	case runErr = <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if runErr != nil {
		return runErr
	}
	after, err := activity.Audit(ctx, db)
	if err != nil {
		return err
	}
	same := before.OK && after.OK && before.Events == after.Events && before.Entries == after.Entries && before.Accounts == after.Accounts
	if err = output(map[string]any{"group_id": group, "duration_seconds": time.Since(start).Seconds(), "before": before, "after": after, "counts_unchanged": same}); err != nil {
		return err
	}
	if !same {
		return errors.New("replay changed projection counts")
	}
	return nil
}

func limiterProbe(ctx context.Context, c *client, ids []string, runID string, n int) error {
	cache := redis.NewClient(&redis.Options{Addr: envValue("REDIS_ADDR", "redis:6379"), Password: os.Getenv("REDIS_PASSWORD"), DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, ContextTimeoutEnabled: true})
	defer cache.Close()
	key := "ledgercore:rate:v1:write:" + ratelimit.ClientID(c.token)
	// Observe the current window expiring; never reset shared limiter state.
	for {
		ttl, err := cache.PTTL(ctx, key).Result()
		if err != nil {
			return err
		}
		if ttl < 0 {
			break
		}
		timer := time.NewTimer(ttl + time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	urls := strings.Split(envValue("GATEWAY_URLS", c.base), ",")
	if len(urls) < 2 {
		return errors.New("GATEWAY_URLS must contain two gateway addresses")
	}
	clients := make([]*client, len(urls))
	for i, url := range urls {
		clients[i] = &client{base: strings.TrimRight(url, "/"), token: c.token, http: c.http}
	}
	statuses := make([]int, n)
	errorsByIndex := make([]error, n)
	start := make(chan struct{})
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range min(n, 64) {
		wg.Go(func() {
			<-start
			for i := range jobs {
				req := ledger.TransferRequest{FromAccountID: ids[0], ToAccountID: ids[1], AmountMinor: "1", Currency: "RUB"}
				statuses[i], _, errorsByIndex[i] = clients[i%len(clients)].call(ctx, "POST", "/v1/transfers", "events/limiter/"+runID, req)
			}
		})
	}
	close(start)
	for i := range n {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	allowed, limited, infra := 0, 0, 0
	for i, status := range statuses {
		if errorsByIndex[i] != nil {
			infra++
			continue
		}
		switch status {
		case 201:
			allowed++
		case 429:
			limited++
		default:
			infra++
		}
	}
	expected, err := strconv.Atoi(envValue("RATE_LIMIT", "60"))
	if err != nil {
		return err
	}
	want := min(expected, n)
	if err = output(map[string]any{"gateways": len(urls), "requests": n, "configured_limit": expected, "allowed": allowed, "rate_limited": limited, "infrastructure_errors": infra}); err != nil {
		return err
	}
	if allowed != want || limited != n-want || infra != 0 {
		return errors.New("shared rate budget differs from configured limit")
	}
	return nil
}
func envValue(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}
