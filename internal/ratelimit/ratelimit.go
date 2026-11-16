// Package ratelimit is a token bucket per API key, kept in Redis so every gateway replica
// shares it. One Lua script does refill + take atomically.
//
// Two buckets per key: requests per minute, and tokens per minute. A request is charged its
// estimated tokens (prompt + max_tokens) up front; what it didn't use is refunded when it ends.
package ratelimit

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"time"

	"github.com/Epinnn14/infergate/internal/redis"
)

// KEYS[1] bucket; ARGV capacity, refill per ms, now ms, cost (negative = refund)
// returns {allowed 0|1, tokens left (floor), ms until enough tokens}
const script = `
local cap = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if tokens == nil then tokens = cap; ts = now end
tokens = math.min(cap, tokens + (now - ts) * rate)
local allowed = 0
local wait = 0
if cost <= tokens then
  tokens = tokens - cost
  allowed = 1
else
  wait = math.ceil((cost - tokens) / rate)
end
if tokens > cap then tokens = cap end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(cap / rate) + 1000)
return {allowed, math.floor(tokens), wait}
`

var sha = func() string { h := sha1.Sum([]byte(script)); return hex.EncodeToString(h[:]) }()

// Limiter checks buckets in Redis.
type Limiter struct {
	rc  *redis.Client
	now func() time.Time
}

func New(rc *redis.Client) *Limiter { return &Limiter{rc: rc, now: time.Now} }

// Result of a check.
type Result struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
}

// Take charges cost against a bucket of capacity perMinute that refills evenly over a minute.
// A perMinute of 0 means unlimited.
func (l *Limiter) Take(ctx context.Context, bucket string, perMinute int64, cost float64) (Result, error) {
	if perMinute <= 0 {
		return Result{Allowed: true, Remaining: -1}, nil
	}
	rate := float64(perMinute) / 60000.0
	args := []any{"ig:rl:" + bucket, perMinute, rate, l.now().UnixMilli(), cost}
	v, err := l.rc.Do(ctx, append([]any{"EVALSHA", sha, 1}, args...)...)
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		v, err = l.rc.Do(ctx, append([]any{"EVAL", script, 1}, args...)...)
	}
	if err != nil {
		return Result{}, err
	}
	arr, _ := v.([]any)
	if len(arr) != 3 {
		return Result{}, redis.Error("ratelimit: bad script reply")
	}
	allowed, _ := arr[0].(int64)
	left, _ := arr[1].(int64)
	wait, _ := arr[2].(int64)
	return Result{Allowed: allowed == 1, Remaining: left, RetryAfter: time.Duration(wait) * time.Millisecond}, nil
}

// Refund gives unused tokens back.
func (l *Limiter) Refund(ctx context.Context, bucket string, perMinute int64, amount float64) {
	if perMinute <= 0 || amount <= 0 {
		return
	}
	_, _ = l.Take(ctx, bucket, perMinute, -amount)
}
