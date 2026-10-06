package ratelimit_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Alok1764/GoGuard-Distributed-Rate-Limiter/ratelimit"
)

func TestTokenBucketBurstThenOneTokenAtATime(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustToken(t, 1, time.Second, 3, clk)

	for i := 0; i < 3; i++ {
		d := mustAllow(t, lim, "alice")
		if !d.Allowed {
			t.Fatalf("burst request %d denied", i+1)
		}
		if d.Remaining != 2-i {
			t.Fatalf("request %d: remaining = %d, want %d", i+1, d.Remaining, 2-i)
		}
		if d.Limit != 3 {
			t.Fatalf("limit = %d, want 3", d.Limit)
		}
	}

	denied := mustAllow(t, lim, "alice")
	if denied.Allowed {
		t.Fatal("4th request allowed with an empty bucket")
	}
	if denied.RetryAfter != time.Second {
		t.Fatalf("retry-after = %s, want 1s", denied.RetryAfter)
	}

	clk.Advance(time.Second)
	if d := mustAllow(t, lim, "alice"); !d.Allowed {
		t.Fatal("the refilled token was refused")
	} else if d.Remaining != 0 {
		t.Fatalf("remaining = %d, want 0", d.Remaining)
	}
	if d := mustAllow(t, lim, "alice"); d.Allowed {
		t.Fatal("the whole burst came back after one second")
	}
}

func TestTokenBucketCapsRefillAtBurst(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustToken(t, 1, time.Second, 3, clk)
	for range 3 {
		mustAllow(t, lim, "alice")
	}

	clk.Advance(10 * time.Second)
	for i := range 3 {
		if d := mustAllow(t, lim, "alice"); !d.Allowed {
			t.Fatalf("capped refill request %d denied", i+1)
		}
	}
	if d := mustAllow(t, lim, "alice"); d.Allowed {
		t.Fatal("idle time refilled past the burst")
	}
}

func TestTokenBucketKeepsThePartialToken(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustToken(t, 10, time.Second, 5, clk)
	for range 5 {
		mustAllow(t, lim, "alice")
	}

	// 250ms at 10 tokens per second is 2 tokens and 50ms toward the next.
	clk.Advance(250 * time.Millisecond)
	for i := range 2 {
		if d := mustAllow(t, lim, "alice"); !d.Allowed {
			t.Fatalf("partial refill request %d denied", i+1)
		}
	}
	denied := mustAllow(t, lim, "alice")
	if denied.Allowed {
		t.Fatal("a third token was minted from 250ms")
	}
	if denied.RetryAfter != 50*time.Millisecond {
		t.Fatalf("retry-after = %s, want 50ms", denied.RetryAfter)
	}
}

func TestTokenBucketEvictDropsOnlyAFullBucket(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustToken(t, 1, time.Second, 2, clk)

	mustAllow(t, lim, "alice")
	mustAllow(t, lim, "alice")
	mustAllow(t, lim, "bob")
	if n := lim.Evict(); n != 0 {
		t.Fatalf("evicted %d keys that were not full", n)
	}

	clk.Advance(time.Second)
	if n := lim.Evict(); n != 1 {
		t.Fatalf("evicted %d, want 1", n)
	}
	if lim.Len() != 1 {
		t.Fatalf("len = %d, want 1", lim.Len())
	}
}

func TestTokenBucketRejectsBadConfig(t *testing.T) {
	if _, err := ratelimit.NewTokenBucket(0, time.Second, 1, nil); !errors.Is(err, ratelimit.ErrInvalidLimit) {
		t.Fatalf("rate: got %v", err)
	}
	if _, err := ratelimit.NewTokenBucket(1, time.Second, 0, nil); !errors.Is(err, ratelimit.ErrInvalidLimit) {
		t.Fatalf("burst: got %v", err)
	}
	if _, err := ratelimit.NewTokenBucket(1, 0, 1, nil); !errors.Is(err, ratelimit.ErrInvalidWindow) {
		t.Fatalf("per: got %v", err)
	}
	if _, err := ratelimit.NewTokenBucket(math.MaxInt64, time.Hour, 1, nil); !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("overflow: got %v", err)
	}

	lim := mustToken(t, 1, time.Second, 1, nil)
	if _, err := lim.Allow(context.Background(), ""); !errors.Is(err, ratelimit.ErrEmptyKey) {
		t.Fatalf("key: got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lim.Allow(ctx, "alice"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx: got %v", err)
	}
}

func TestTokenBucketConcurrentExactBurst(t *testing.T) {
	const burst = 100
	clk := newFakeClock(time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC))
	lim := mustToken(t, 1, time.Minute, burst, clk)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	const workers = 64
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range 20 {
				d, err := lim.Allow(context.Background(), "same-key")
				if err != nil {
					t.Errorf("allow: %v", err)
					return
				}
				if d.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != burst {
		t.Fatalf("allowed = %d, want %d", got, burst)
	}
}

func TestTokenBucketKeysDoNotShareALock(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	release := make(chan struct{})
	entered := make(chan struct{})
	clk := &blockingClock{now: start, release: release, entered: entered}
	lim := mustToken(t, 1, time.Hour, 1, clk)
	defer close(release)

	go func() {
		_, _ = lim.Allow(context.Background(), "alice")
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("alice did not reach the clock")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		d, err := lim.Allow(context.Background(), "bob")
		if err != nil {
			t.Errorf("bob: %v", err)
			return
		}
		if !d.Allowed {
			t.Error("bob denied while alice was blocked")
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bob waited on alice's lock")
	}
}

func mustToken(t *testing.T, rate int, per time.Duration, burst int, clock ratelimit.Clock) *ratelimit.TokenBucket {
	t.Helper()
	lim, err := ratelimit.NewTokenBucket(rate, per, burst, clock)
	if err != nil {
		t.Fatalf("new token bucket: %v", err)
	}
	t.Cleanup(lim.Close)
	return lim
}
