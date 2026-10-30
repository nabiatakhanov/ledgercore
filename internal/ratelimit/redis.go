// Package ratelimit implements a shared fixed-window-from-first-request budget.
package ratelimit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/redis/go-redis/v9"
	"time"
)

// The atomic script sets expiry only on the first request. Later arrivals,
// including refused requests, consume count but never extend the window.
const windowScript = `local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then return redis.error_reply('rate window missing TTL') end
return {count, ttl}`

type Limiter struct {
	client redis.Scripter
	limit  int64
	window time.Duration
	script *redis.Script
}
type Decision struct {
	Allowed      bool
	Count        int64
	RemainingTTL time.Duration
}

func New(client redis.Scripter, limit int64, window time.Duration) (*Limiter, error) {
	if client == nil || limit < 1 || limit > 1000000 || window < 10*time.Millisecond || window > 24*time.Hour {
		return nil, errors.New("rate limit must be1..1000000 and window10ms..24h")
	}
	return &Limiter{client, limit, window, redis.NewScript(windowScript)}, nil
}
func ClientID(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}
func (l *Limiter) Allow(ctx context.Context, clientID string) (Decision, error) {
	if len(clientID) != 64 {
		return Decision{}, errors.New("invalid client identity")
	}
	for _, c := range clientID {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return Decision{}, errors.New("invalid client identity")
		}
	}
	value, err := l.script.Run(ctx, l.client, []string{"ledgercore:rate:v1:write:" + clientID}, l.window.Milliseconds()).Slice()
	if err != nil {
		return Decision{}, errors.New("rate admission unavailable")
	}
	if len(value) != 2 {
		return Decision{}, errors.New("invalid rate admission reply")
	}
	count, ok := value[0].(int64)
	ttl, ttlOK := value[1].(int64)
	if !ok || !ttlOK || count < 1 || ttl < 0 {
		return Decision{}, errors.New("invalid rate admission reply")
	}
	return Decision{Allowed: count <= l.limit, Count: count, RemainingTTL: time.Duration(ttl) * time.Millisecond}, nil
}
func RetryAfter(ttl time.Duration) int64 {
	seconds := int64((ttl + time.Second - 1) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}
