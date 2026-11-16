package redis_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Epinnn14/infergate/internal/redis"
	"github.com/Epinnn14/infergate/internal/redistest"
)

func TestCommands(t *testing.T) {
	rc, _ := redistest.Start(t)
	ctx := context.Background()
	if s, err := redis.String(rc.Do(ctx, "SET", "k", "v")); err != nil || s != "OK" {
		t.Fatal(s, err)
	}
	if s, err := redis.String(rc.Do(ctx, "GET", "k")); err != nil || s != "v" {
		t.Fatal(s, err)
	}
	if _, err := rc.Do(ctx, "GET", "missing"); !errors.Is(err, redis.Nil) {
		t.Fatalf("want Nil, got %v", err)
	}
	if n, err := redis.Int(rc.Do(ctx, "INCRBY", "n", 5)); err != nil || n != 5 {
		t.Fatal(n, err)
	}
	var re redis.Error
	if _, err := rc.Do(ctx, "INCR", "k"); !errors.As(err, &re) {
		t.Fatalf("want server error, got %v", err)
	}
	// the connection is still usable after an error reply
	if _, err := rc.Do(ctx, "PING"); err != nil {
		t.Fatal(err)
	}
	m, err := redis.StringMap(rc.Do(ctx, "HSET", "h", "a", "1", "b", "2"))
	_ = m
	if err == nil {
		t.Fatal("HSET returns an int, not a map")
	}
	if m, err := redis.StringMap(rc.Do(ctx, "HGETALL", "h")); err != nil || m["a"] != "1" || m["b"] != "2" {
		t.Fatal(m, err)
	}
}

func TestStreamsAndBlocking(t *testing.T) {
	rc, _ := redistest.Start(t)
	ctx := context.Background()
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = rc.Do(ctx, "XADD", "s", "*", "type", "token", "text", "hello world")
	}()
	start := time.Now()
	res, err := redis.Streams(rc.Do(ctx, "XREAD", "BLOCK", 2000, "STREAMS", "s", "0"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res["s"]) != 1 || res["s"][0].Fields["text"] != "hello world" {
		t.Fatalf("got %+v", res)
	}
	if time.Since(start) > time.Second {
		t.Fatal("did not wake up on XADD")
	}
	// a timeout is Nil, not an error
	if _, err := rc.Do(ctx, "XREAD", "BLOCK", 50, "STREAMS", "s", "$"); !errors.Is(err, redis.Nil) {
		t.Fatalf("want Nil on timeout, got %v", err)
	}
}

func TestContextCancelsBlockingRead(t *testing.T) {
	rc, _ := redistest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := rc.Do(ctx, "XREAD", "BLOCK", 10000, "STREAMS", "nothing", "$")
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("cancel didn't interrupt the read: %v after %v", err, time.Since(start))
	}
	// pool still works afterwards
	if _, err := rc.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentUse(t *testing.T) {
	rc, _ := redistest.Start(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if _, err := rc.Do(ctx, "INCR", "c"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if n, _ := redis.Int(rc.Do(ctx, "GET", "c")); n != 5000 {
		t.Fatalf("lost increments: %d", n)
	}
}
