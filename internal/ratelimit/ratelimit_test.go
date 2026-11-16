package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/Epinnn14/infergate/internal/redistest"
)

func TestBucket(t *testing.T) {
	rc, _ := redistest.Start(t)
	l := New(rc)
	now := time.Unix(1_700_000_000, 0)
	l.now = func() time.Time { return now }
	ctx := context.Background()
	// 60 per minute = 1 per second, burst of 60
	for i := 0; i < 60; i++ {
		r, err := l.Take(ctx, "k:req", 60, 1)
		if err != nil || !r.Allowed {
			t.Fatalf("request %d: %+v %v", i, r, err)
		}
	}
	r, _ := l.Take(ctx, "k:req", 60, 1)
	if r.Allowed || r.RetryAfter <= 0 || r.RetryAfter > time.Second+10*time.Millisecond {
		t.Fatalf("61st should wait about a second: %+v", r)
	}
	now = now.Add(2 * time.Second)
	if r, _ := l.Take(ctx, "k:req", 60, 1); !r.Allowed {
		t.Fatalf("refilled after 2s: %+v", r)
	}
	// refunds go back, capped at capacity
	l.Refund(ctx, "k:req", 60, 1000)
	if r, _ := l.Take(ctx, "k:req", 60, 0); r.Remaining != 60 {
		t.Fatalf("refund should cap at 60: %+v", r)
	}
	// 0 = unlimited
	if r, _ := l.Take(ctx, "other", 0, 1e9); !r.Allowed {
		t.Fatal("unlimited")
	}
}
