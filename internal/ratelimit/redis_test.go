package ratelimit

import (
	"context"
	"github.com/redis/go-redis/v9"
	"ledgercore/internal/ledger"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryAfter(t *testing.T) {
	for _, row := range []struct {
		ttl  time.Duration
		want int64
	}{{0, 1}, {time.Millisecond, 1}, {time.Second, 1}, {time.Second + time.Millisecond, 2}} {
		if got := RetryAfter(row.ttl); got != row.want {
			t.Fatal(got, row.want)
		}
	}
	if ClientID("token") == "token" || len(ClientID("token")) != 64 || ClientID("token") != ClientID("token") {
		t.Fatal("unstable client identity")
	}
}
func TestSharedRealRedisBudgetAndTTL(t *testing.T) {
	addr := os.Getenv("LEDGER_TEST_REDIS_ADDR")
	if addr == "" {
		addr = os.Getenv("EVENTS_TEST_REDIS_ADDR")
	}
	if addr == "" {
		if os.Getenv("LEDGER_REQUIRE_EVENTS_INTEGRATION") == "1" {
			t.Fatal("LEDGER_TEST_REDIS_ADDR required")
		}
		t.Skip("NOT RUN: real Redis requires LEDGER_TEST_REDIS_ADDR")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true})
	b := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true})
	defer a.Close()
	defer b.Close()
	const count = 64
	const limit = 13
	window := 2 * time.Second
	first, err := New(a, limit, window)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := New(b, limit, window)
	id := ClientID("test-" + ledger.NewID())
	key := "ledgercore:rate:v1:write:" + id
	defer a.Del(context.Background(), key)
	start := make(chan struct{})
	results := make(chan error, count)
	var accepted atomic.Int64
	for i := range count {
		go func() {
			<-start
			l := first
			if i%2 != 0 {
				l = second
			}
			d, e := l.Allow(ctx, id)
			if e == nil && d.Allowed {
				accepted.Add(1)
			}
			results <- e
		}()
	}
	close(start)
	for range count {
		if e := <-results; e != nil {
			t.Fatal(e)
		}
	}
	if accepted.Load() != limit {
		t.Fatal("shared budget", accepted.Load())
	}
	ttl, err := a.PTTL(ctx, key).Result()
	if err != nil || ttl <= 0 || ttl > window {
		t.Fatal(ttl, err)
	}
	if _, err = second.Allow(ctx, id); err != nil {
		t.Fatal(err)
	}
	after, _ := a.PTTL(ctx, key).Result()
	if after > ttl {
		t.Fatal("subsequent request extended window")
	}
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
	for {
		exists, err := a.Exists(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if exists == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-poll.C:
		}
	}
	d, err := first.Allow(ctx, id)
	if err != nil || !d.Allowed || d.Count != 1 {
		t.Fatal(d, err)
	}
}
