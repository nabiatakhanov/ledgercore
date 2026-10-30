package eventtests

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	activityv1 "ledgercore/gen/activity/v1"
	ledgerv1 "ledgercore/gen/ledger/v1"
	"ledgercore/internal/activity"
	"ledgercore/internal/eventaudit"
	"ledgercore/internal/gateway"
	"ledgercore/internal/httpapi"
	"ledgercore/internal/ledger"
	"ledgercore/internal/outbox"
	"ledgercore/internal/ratelimit"
	"ledgercore/internal/reconcile"
	"ledgercore/internal/rpc"
	"ledgercore/internal/store"
	"ledgercore/migrations"
)

type environment struct {
	ctx                                                     context.Context
	app, owner, fixture, publisher, activity, activityOwner *pgxpool.Pool
	s                                                       *store.Store
}

func setup(t *testing.T) *environment {
	t.Helper()
	if os.Getenv("EVENTS_E2E_TEST_DATABASE_URL") == "" {
		if os.Getenv("LEDGER_REQUIRE_EVENTS_INTEGRATION") == "1" {
			t.Fatal("EVENTS_E2E_TEST_DATABASE_URL required")
		}
		t.Skip("NOT RUN: separate end-to-end databases not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	open := func(key string) *pgxpool.Pool {
		dsn := os.Getenv(key)
		if dsn == "" {
			t.Fatalf("%s required", key)
		}
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.MaxConns = 8
		cfg.ConnConfig.ConnectTimeout = 3 * time.Second
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
		cfg.ConnConfig.RuntimeParams["lock_timeout"] = "5000"
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		var name string
		if err = p.QueryRow(ctx, `SELECT current_database()`).Scan(&name); err != nil || !strings.Contains(name, "e2e_test") {
			t.Fatalf("isolated e2e_test DB required: %v", err)
		}
		return p
	}
	e := &environment{ctx: ctx, app: open("EVENTS_E2E_TEST_DATABASE_URL"), owner: open("EVENTS_E2E_TEST_MIGRATION_URL"), fixture: open("EVENTS_E2E_TEST_FIXTURE_URL"), publisher: open("EVENTS_E2E_TEST_PUBLISHER_URL"), activity: open("EVENTS_E2E_TEST_ACTIVITY_DATABASE_URL"), activityOwner: open("EVENTS_E2E_TEST_ACTIVITY_MIGRATION_URL")}
	if err := migrations.Apply(ctx, e.owner); err != nil {
		t.Fatal(err)
	}
	if err := activity.Migrate(ctx, e.activityOwner); err != nil {
		t.Fatal(err)
	}
	e.s = store.New(e.app, nil)
	return e
}
func (e *environment) wallet(t *testing.T, amount int64) string {
	t.Helper()
	a, err := e.s.CreateAccount(e.ctx, "event integration wallet")
	if err != nil {
		t.Fatal(err)
	}
	if amount > 0 {
		o, err := store.New(e.fixture, nil).Funding(e.ctx, ledger.NewID(), a.ID, ledger.Money(amount))
		if err != nil || o.Error != nil {
			t.Fatalf("funding %+v %v", o, err)
		}
	}
	return a.ID
}
func request(a, b string, n int64) ledger.TransferRequest {
	return ledger.TransferRequest{FromAccountID: a, ToAccountID: b, AmountMinor: fmt.Sprint(n), Currency: "RUB"}
}
func wait(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("bounded condition deadline", ctx.Err())
		case <-ticker.C:
		}
	}
}

func serveRPC(t *testing.T, app httpapi.Service, projection *activity.Store, token string) (*grpc.ClientConn, *grpc.ClientConn) {
	t.Helper()
	start := func(register func(*grpc.Server)) *grpc.ClientConn {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		server := grpc.NewServer(rpc.ServerOptions(token, nil, nil)...)
		register(server)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = rpc.GracefulStop(ctx, server)
			_ = listener.Close()
		})
		conn, err := rpc.Dial(listener.Addr().String(), token, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	return start(func(s *grpc.Server) { ledgerv1.RegisterLedgerServiceServer(s, rpc.NewLedgerServer(app)) }), start(func(s *grpc.Server) { activityv1.RegisterActivityServiceServer(s, rpc.NewActivityServer(projection)) })
}
func gateways(t *testing.T, l, a *grpc.ClientConn, redisClient *redis.Client, token string, limit int64, window time.Duration) [2]*httptest.Server {
	t.Helper()
	budget, err := ratelimit.New(redisClient, limit, window)
	if err != nil {
		t.Fatal(err)
	}
	var result [2]*httptest.Server
	for i := range result {
		result[i] = httptest.NewServer(gateway.New(rpc.NewLedgerClient(l), rpc.NewActivityClient(a), budget, token, nil, nil, nil))
		t.Cleanup(result[i].Close)
	}
	return result
}
func post(t *testing.T, url, token, key string, body any) (int, ledger.Outcome, http.Header) {
	t.Helper()
	b, _ := json.Marshal(body)
	r, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Idempotency-Key", key)
	r.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 6 * time.Second}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var out ledger.Outcome
	if err = json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, out, response.Header
}
func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		if os.Getenv("LEDGER_REQUIRE_EVENTS_INTEGRATION") == "1" {
			t.Fatal("REDIS_ADDR required")
		}
		t.Skip("NOT RUN: Redis test address missing")
	}
	c := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return c
}

type afterCommitBarrier struct {
	*store.Store
	key       string
	committed chan ledger.Outcome
}

func (a *afterCommitBarrier) Transfer(ctx context.Context, key string, r ledger.TransferRequest) (ledger.Outcome, error) {
	o, err := a.Store.Transfer(ctx, key, r)
	if key == a.key && err == nil && !o.Replayed {
		a.committed <- o
		<-ctx.Done()
	}
	return o, err
}

func TestGatewayGRPCCommitOutcomeAndSharedRedisBudget(t *testing.T) {
	e := setup(t)
	redisConn := redisClient(t)
	a, b := e.wallet(t, 10000), e.wallet(t, 0)
	lostKey := ledger.NewID()
	app := &afterCommitBarrier{Store: e.s, key: lostKey, committed: make(chan ledger.Outcome, 1)}
	internalToken := "test-internal-" + ledger.NewID()
	lconn, aconn := serveRPC(t, app, activity.New(e.activity), internalToken)
	token := "test-http-" + ledger.NewID()
	g := gateways(t, lconn, aconn, redisConn, token, 1000, time.Second)
	key := ledger.NewID()
	code, one, _ := post(t, g[0].URL+"/v1/transfers", token, key, request(a, b, 100))
	if code != 201 || one.Transaction == nil {
		t.Fatalf("gateway transfer %+v status%d", one, code)
	}
	code, two, _ := post(t, g[1].URL+"/v1/transfers", token, key, request(a, b, 100))
	if code != 201 || two.Transaction == nil || two.Transaction.ID != one.Transaction.ID {
		t.Fatalf("cross-Gateway replay %+v status%d", two, code)
	}
	var count int
	if err := e.app.QueryRow(e.ctx, `SELECT count(*) FROM outbox_events WHERE journal_transaction_id=$1`, one.Transaction.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("logical outbox count%d %v", count, err)
	}
	code, denied, _ := post(t, g[0].URL+"/v1/transfers", token, ledger.NewID(), request(b, a, 999999))
	if code != 409 || denied.Error == nil || denied.Error.Code != "insufficient_funds" {
		t.Fatalf("business refusal %+v status%d", denied, code)
	}
	var actual int
	if err := e.app.QueryRow(e.ctx, `SELECT count(*) FROM outbox_events o JOIN journal_transactions t ON t.id=o.journal_transaction_id WHERE t.from_account_id=$1 AND t.to_account_id=$2`, b, a).Scan(&actual); err != nil || actual != 0 {
		t.Fatalf("refusal created event%d %v", actual, err)
	}
	deadline, cancel := context.WithTimeout(e.ctx, 500*time.Millisecond)
	defer cancel()
	rpcDone := make(chan error, 1)
	go func() {
		_, err := ledgerv1.NewLedgerServiceClient(lconn).CreateTransfer(deadline, &ledgerv1.CreateTransferRequest{IdempotencyKey: lostKey, FromAccountId: a, ToAccountId: b, AmountMinor: 7, Currency: "RUB"})
		rpcDone <- err
	}()
	var committed ledger.Outcome
	select {
	case committed = <-app.committed:
	case <-e.ctx.Done():
		t.Fatal("no post-commit barrier")
	}
	if err := <-rpcDone; status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected actual gRPC deadline, got%v", err)
	}
	code, replay, _ := post(t, g[1].URL+"/v1/transfers", token, lostKey, request(a, b, 7))
	if code != 201 || replay.Transaction == nil || replay.Transaction.ID != committed.Transaction.ID {
		t.Fatalf("deadline retry %+v status%d", replay, code)
	}
	// Both distinct HTTP servers share one Lua-scripted Redis admission window.
	budgetToken := "budget-" + ledger.NewID()
	limited := gateways(t, lconn, aconn, redisConn, budgetToken, 5, 2*time.Second)
	sameKey := ledger.NewID()
	start := make(chan struct{})
	codesCh := make(chan int, 16)
	for i := 0; i < 16; i++ {
		go func(i int) {
			<-start
			body, _ := json.Marshal(request(a, b, 1))
			r, _ := http.NewRequestWithContext(e.ctx, "POST", limited[i%2].URL+"/v1/transfers", bytes.NewReader(body))
			r.Header.Set("Authorization", "Bearer "+budgetToken)
			r.Header.Set("Idempotency-Key", sameKey)
			r.Header.Set("Content-Type", "application/json")
			response, err := (&http.Client{Timeout: 6 * time.Second}).Do(r)
			if err != nil {
				codesCh <- 0
				return
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			codesCh <- response.StatusCode
		}(i)
	}
	close(start)
	allowed, throttled := 0, 0
	for i := 0; i < 16; i++ {
		switch c := <-codesCh; c {
		case 201:
			allowed++
		case 429:
			throttled++
		default:
			t.Fatalf("unexpected admission status%d", c)
		}
	}
	if allowed != 5 || throttled != 11 {
		t.Fatalf("shared admission allowed%d throttled%d", allowed, throttled)
	}
	redisKey := "ledgercore:rate:v1:write:" + ratelimit.ClientID(budgetToken)
	wait(t, e.ctx, func() bool { return redisConn.Exists(e.ctx, redisKey).Val() == 0 })
	code, _, _ = post(t, limited[0].URL+"/v1/transfers", budgetToken, sameKey, request(a, b, 1))
	if code != 201 {
		t.Fatalf("TTL did not reopen shared budget:%d", code)
	}
	if os.Getenv("LEDGER_ALLOW_CONTAINER_FAULTS") == "1" {
		container(t, e.ctx, "stop", testContainer("redis"))
		defer restoreContainer(testContainer("redis"))
		blockedKey := ledger.NewID()
		code, refused, _ := post(t, g[0].URL+"/v1/transfers", token, blockedKey, request(a, b, 1))
		if code != 503 || refused.Error == nil || refused.Error.Code != "rate_limiter_unavailable" {
			t.Fatalf("actual Redis outage %+v status%d", refused, code)
		}
		var reserved int
		if err := e.app.QueryRow(e.ctx, `SELECT count(*) FROM idempotency_records WHERE kind='transfer' AND key=$1`, blockedKey).Scan(&reserved); err != nil || reserved != 0 {
			t.Fatalf("Redis failure reached ledger %d %v", reserved, err)
		}
		container(t, e.ctx, "start", testContainer("redis"))
		wait(t, e.ctx, func() bool { return redisConn.Ping(e.ctx).Err() == nil })
		code, _, _ = post(t, g[1].URL+"/v1/transfers", token, blockedKey, request(a, b, 1))
		if code != 201 {
			t.Fatalf("Redis recovery status%d", code)
		}
	} else {
		t.Log("NOT RUN: actual Redis container outage; set LEDGER_ALLOW_CONTAINER_FAULTS=1 in isolated infrastructure")
	}
	r, err := reconcile.Run(e.ctx, e.app)
	if err != nil || !r.OK {
		t.Fatalf("financial audit %+v %v", r, err)
	}
}

func kafka(t *testing.T, e *environment) (*kgo.Client, activity.ConsumerConfig) {
	t.Helper()
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		if os.Getenv("LEDGER_REQUIRE_EVENTS_INTEGRATION") == "1" {
			t.Fatal("KAFKA_TEST_BROKERS required")
		}
		t.Skip("NOT RUN: Kafka test brokers missing")
	}
	producer, err := outbox.NewProducer(strings.Split(brokers, ","))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	c := activity.ConsumerConfig{Brokers: strings.Split(brokers, ","), Topic: "events-e2e-" + ledger.NewID(), DLQTopic: "events-e2e-dlq-" + ledger.NewID(), Group: "events-e2e-group-" + ledger.NewID(), MaxPartitions: 3, RecordTimeout: 5 * time.Second}
	admin := kadm.NewClient(producer)
	for _, topic := range []string{c.Topic, c.DLQTopic} {
		results, err := admin.CreateTopics(e.ctx, 3, 1, nil, topic)
		if err != nil {
			t.Fatal(err)
		}
		for _, result := range results {
			if result.Err != nil {
				t.Fatal(result.Err)
			}
		}
	}
	return producer, c
}
func makePublisher(t *testing.T, e *environment, k *kgo.Client, topic string) *outbox.Publisher {
	t.Helper()
	p, err := outbox.New(e.publisher, k, outbox.DefaultConfig(topic), nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func assertAudits(t *testing.T, e *environment) {
	t.Helper()
	r, err := reconcile.Run(e.ctx, e.app)
	if err != nil || !r.OK {
		t.Fatalf("ledger audit %+v %v", r, err)
	}
	projection, err := eventaudit.Run(e.ctx, e.app, e.activity, true)
	if err != nil || !projection.OK {
		t.Fatalf("cross-service audit %+v %v", projection, err)
	}
}

func TestPublisherRecoveryFencingAndActivityConvergence(t *testing.T) {
	e := setup(t)
	producer, cfg := kafka(t, e)
	p := makePublisher(t, e, producer, cfg.Topic)
	if os.Getenv("LEDGER_ALLOW_CONTAINER_FAULTS") == "1" {
		container(t, e.ctx, "stop", testContainer("kafka"))
		defer restoreContainer(testContainer("kafka"))
		a, b := e.wallet(t, 100), e.wallet(t, 0)
		accepted, err := e.s.Transfer(e.ctx, ledger.NewID(), request(a, b, 1))
		if err != nil || accepted.Error != nil {
			t.Fatalf("Kafka outage blocked SQL commit %+v %v", accepted, err)
		}
		p.Config.SendTimeout = time.Second
		if _, err = p.RunOnce(e.ctx); err != nil {
			t.Fatal(err)
		}
		var pending bool
		var failure *string
		if err = e.app.QueryRow(e.ctx, `SELECT published_at IS NULL,last_error_code FROM outbox_events WHERE event_id=$1`, accepted.Transaction.ID).Scan(&pending, &failure); err != nil || !pending || failure == nil || *failure != "publish_failed" {
			t.Fatalf("outage lost pending event pending%v failure%v error%v", pending, failure, err)
		}
		container(t, e.ctx, "start", testContainer("kafka"))
		wait(t, e.ctx, func() bool {
			check, cancel := context.WithTimeout(e.ctx, time.Second)
			defer cancel()
			return producer.Ping(check) == nil
		})
		p.Config.SendTimeout = 10 * time.Second
	} else {
		t.Log("NOT RUN: actual Kafka container outage; set LEDGER_ALLOW_CONTAINER_FAULTS=1 in isolated infrastructure")
	}
	consumer, err := activity.NewConsumer(cfg, e.activity, nil)
	if err != nil {
		t.Fatal(err)
	}
	consumeCtx, stop := context.WithCancel(e.ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(consumeCtx) }()
	t.Cleanup(func() { stop(); consumer.Close() })
	// Drain any events created by the preceding real Gateway test.
	for {
		n, err := p.RunOnce(e.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	a, b := e.wallet(t, 1000), e.wallet(t, 0)
	o, err := e.s.Transfer(e.ctx, ledger.NewID(), request(a, b, 20))
	if err != nil || o.Error != nil {
		t.Fatalf("setup %+v %v", o, err)
	}
	claims, err := p.ClaimBatch(e.ctx)
	if err != nil || len(claims) != 1 {
		t.Fatalf("first claim%d %v", len(claims), err)
	}
	first := claims[0]
	// Kafka acknowledges, then a test-only administrative lease expiry permits
	// a second worker to recover the same immutable logical event.
	if err = p.Deliver(e.ctx, claims)[0]; err != nil {
		t.Fatal(err)
	}
	if _, err = e.owner.Exec(e.ctx, `UPDATE outbox_events SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1`, first.EventID); err != nil {
		t.Fatal(err)
	}
	secondPublisher := makePublisher(t, e, producer, cfg.Topic)
	secondClaims, err := secondPublisher.ClaimBatch(e.ctx)
	if err != nil || len(secondClaims) != 1 || secondClaims[0].Owner == first.Owner {
		t.Fatalf("renewed claim %+v %v", secondClaims, err)
	}
	if acknowledged, err := p.Ack(e.ctx, first, nil); err != nil || acknowledged {
		t.Fatalf("stale owner overwrote new lease:%v %v", acknowledged, err)
	}
	if err = secondPublisher.Deliver(e.ctx, secondClaims)[0]; err != nil {
		t.Fatal(err)
	}
	if acknowledged, err := secondPublisher.Ack(e.ctx, secondClaims[0], nil); err != nil || !acknowledged {
		t.Fatalf("new owner ack:%v %v", acknowledged, err)
	}
	// Two live workers claim a disjoint bounded batch under SKIP LOCKED.
	for i := 0; i < 2; i++ {
		created, err := e.s.Transfer(e.ctx, ledger.NewID(), request(a, b, 1))
		if err != nil || created.Error != nil {
			t.Fatalf("parallel publisher setup %+v %v", created, err)
		}
	}
	p.Config.BatchSize = 1
	secondPublisher.Config.BatchSize = 1
	type claimed struct {
		publisher *outbox.Publisher
		claims    []outbox.Claim
		err       error
	}
	workers := make(chan claimed, 2)
	start := make(chan struct{})
	for _, publisher := range []*outbox.Publisher{p, secondPublisher} {
		go func(publisher *outbox.Publisher) {
			<-start
			claims, err := publisher.ClaimBatch(e.ctx)
			workers <- claimed{publisher, claims, err}
		}(publisher)
	}
	close(start)
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		work := <-workers
		if work.err != nil || len(work.claims) != 1 || seen[work.claims[0].EventID] {
			t.Fatalf("overlapping claims %+v %v", work.claims, work.err)
		}
		seen[work.claims[0].EventID] = true
		if err := work.publisher.Deliver(e.ctx, work.claims)[0]; err != nil {
			t.Fatal(err)
		}
		if ok, err := work.publisher.Ack(e.ctx, work.claims[0], nil); err != nil || !ok {
			t.Fatalf("concurrent publisher Ack%v %v", ok, err)
		}
	}
	// Exercise the real publisher process window after Kafka ack and before SQL Ack.
	crashTransfer, err := e.s.Transfer(e.ctx, ledger.NewID(), request(a, b, 3))
	if err != nil || crashTransfer.Error != nil {
		t.Fatal(err)
	}
	crashPublisher(t, e, cfg.Topic)
	if _, err = e.owner.Exec(e.ctx, `UPDATE outbox_events SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1`, crashTransfer.Transaction.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := p.RunOnce(e.ctx); err != nil || n != 1 {
		t.Fatalf("recover crashed publisher%d %v", n, err)
	}
	reversed, err := e.s.Reverse(e.ctx, ledger.NewID(), o.Transaction.ID)
	if err != nil || reversed.Error != nil {
		t.Fatalf("reverse %+v %v", reversed, err)
	}
	if _, err = p.RunOnce(e.ctx); err != nil {
		t.Fatal(err)
	}
	wait(t, e.ctx, func() bool { r, err := eventaudit.Run(e.ctx, e.app, e.activity, true); return err == nil && r.OK })
	assertAudits(t, e)
	// Prove the cross-service audit uses the immutable financial journal as its
	// reference, rather than accepting a matching-but-corrupt event envelope.
	var originalPayload []byte
	if err = e.owner.QueryRow(e.ctx, `SELECT payload FROM outbox_events WHERE event_id=$1`, o.Transaction.ID).Scan(&originalPayload); err != nil {
		t.Fatal(err)
	}
	var modified map[string]json.RawMessage
	if err = json.Unmarshal(originalPayload, &modified); err != nil {
		t.Fatal(err)
	}
	modified["amount_minor"] = json.RawMessage(`"999"`)
	corruptPayload, _ := json.Marshal(modified)
	if err = replacePayload(e.ctx, e.owner, o.Transaction.ID, corruptPayload); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = replacePayload(cleanup, e.owner, o.Transaction.ID, originalPayload)
	}()
	corruption, err := eventaudit.Run(e.ctx, e.app, e.activity, true)
	if err != nil || corruption.OK || len(corruption.Issues) == 0 {
		t.Fatalf("source envelope drift undetected %+v %v", corruption, err)
	}
	journalMismatch := false
	for _, issue := range corruption.Issues {
		if strings.Contains(issue, "authoritative journal") {
			journalMismatch = true
		}
	}
	if !journalMismatch {
		t.Fatalf("audit compared events without checking the financial journal: %+v", corruption)
	}
	if err = replacePayload(e.ctx, e.owner, o.Transaction.ID, originalPayload); err != nil {
		t.Fatal(err)
	}
	assertAudits(t, e)
	stop()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}
func replacePayload(ctx context.Context, pool *pgxpool.Pool, id string, payload []byte) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, `ALTER TABLE outbox_events DISABLE TRIGGER outbox_payload_immutable`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE outbox_events SET payload=$2 WHERE event_id=$1`, id, payload); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `ALTER TABLE outbox_events ENABLE TRIGGER outbox_payload_immutable`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func container(t *testing.T, ctx context.Context, action, name string) {
	t.Helper()
	if (name != testContainer("kafka") && name != testContainer("redis")) || strings.HasPrefix(name, "-") || (action != "stop" && action != "start") {
		t.Fatal("container fault target not allowlisted")
	}
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(deadline, "docker", action, name).CombinedOutput(); err != nil {
		t.Fatalf("isolated container %s failed: %v %s", action, err, output)
	}
}
func testContainer(component string) string {
	key := "LEDGER_TEST_KAFKA_CONTAINER"
	fallback := "ledger-events-infra-kafka"
	if component == "redis" {
		key = "LEDGER_TEST_REDIS_CONTAINER"
		fallback = "ledger-events-infra-redis"
	}
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func restoreContainer(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = exec.CommandContext(ctx, "docker", "start", name).Run()
}

func TestPublisherCrashHelper(t *testing.T) {
	if os.Getenv("EVENTS_PUBLISHER_CRASH_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	e := setup(t)
	k, err := outbox.NewProducer(strings.Split(os.Getenv("KAFKA_TEST_BROKERS"), ","))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	p := makePublisher(t, e, k, os.Getenv("EVENTS_PUBLISHER_CRASH_TOPIC"))
	claims, err := p.ClaimBatch(e.ctx)
	if err != nil || len(claims) != 1 {
		t.Fatalf("helper claims%d %v", len(claims), err)
	}
	if err = p.Deliver(e.ctx, claims)[0]; err != nil {
		t.Fatal(err)
	}
	fmt.Println("PUBLISHER_KAFKA_ACKNOWLEDGED")
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	t.Fatal("parent must kill before Ack")
}
func crashPublisher(t *testing.T, e *environment, topic string) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(e.ctx, binary, "-test.run=^TestPublisherCrashHelper$", "-test.timeout=40s")
	cmd.Env = append(os.Environ(), "EVENTS_PUBLISHER_CRASH_HELPER=1", "EVENTS_PUBLISHER_CRASH_TOPIC="+topic)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "PUBLISHER_KAFKA_ACKNOWLEDGED" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("publisher helper never acknowledged Kafka")
		}
	case <-e.ctx.Done():
		t.Fatal("publisher barrier deadline")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("publisher exited without process death")
	}
}
