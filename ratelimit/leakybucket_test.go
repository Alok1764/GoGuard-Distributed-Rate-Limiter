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

func TestLeakyBucketFillsThenLeaksOneAtATime(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustLeak(t, 1, time.Second, 3, clk)

	for i := 0; i < 3; i++ {
		d := mustAllow(t, lim, "alice")
		if !d.Allowed {
			t.Fatalf("fill request %d denied", i+1)
		}
		if d.Remaining != 2-i {
			t.Fatalf("request %d: remaining = %d, want %d", i+1, d.Remaining, 2-i)
		}
	}

	denied := mustAllow(t, lim, "alice")
	if denied.Allowed {
		t.Fatal("4th request allowed on a full meter")
	}
	if denied.RetryAfter != time.Second {
		t.Fatalf("retry-after = %s, want 1s", denied.RetryAfter)
	}

	clk.Advance(time.Second)
	if d := mustAllow(t, lim, "alice"); !d.Allowed {
		t.Fatal("the leaked unit was not replaced")
	} else if d.Remaining != 0 {
		t.Fatalf("remaining = %d, want 0", d.Remaining)
	}
	if d := mustAllow(t, lim, "alice"); d.Allowed {
		t.Fatal("the whole capacity came back after one second")
	}
}

func TestLeakyBucketIdleEmptiesAndRestoresTheBurst(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustLeak(t, 1, time.Second, 3, clk)
	for range 3 {
		mustAllow(t, lim, "alice")
	}

	clk.Advance(10 * time.Second)
	for i := range 3 {
		if d := mustAllow(t, lim, "alice"); !d.Allowed {
			t.Fatalf("request %d denied after the bucket drained", i+1)
		}
	}
	if d := mustAllow(t, lim, "alice"); d.Allowed {
		t.Fatal("capacity grew while the bucket was idle")
	}
}

func TestLeakyBucketKeepsThePartialLeak(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustLeak(t, 10, time.Second, 5, clk)
	for range 5 {
		mustAllow(t, lim, "alice")
	}

	// 250ms at 10 units per second leaks 2 units and 50ms toward the next.
	// Level 5 becomes 3, so two calls fit and the third waits 50ms.
	clk.Advance(250 * time.Millisecond)
	for i := range 2 {
		if d := mustAllow(t, lim, "alice"); !d.Allowed {
			t.Fatalf("partial leak request %d denied", i+1)
		}
	}
	denied := mustAllow(t, lim, "alice")
	if denied.Allowed {
		t.Fatal("a third call was accepted before the next unit leaked")
	}
	if denied.RetryAfter != 50*time.Millisecond {
		t.Fatalf("retry-after = %s, want 50ms", denied.RetryAfter)
	}
}

func TestLeakyBucketMatchesTokenBucket(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	meter := mustLeak(t, 10, time.Second, 5, clk)
	tokens := mustToken(t, 10, time.Second, 5, clk)

	steps := []time.Duration{
		0,
		0, 0, 0, 0, 0,
		250 * time.Millisecond,
		0, 0, 0,
		50 * time.Millisecond,
		0,
		10 * time.Second,
		0, 0, 0, 0, 0, 0,
	}
	for i, step := range steps {
		if step > 0 {
			clk.Advance(step)
		}
		leak := mustAllow(t, meter, "alice")
		token := mustAllow(t, tokens, "alice")
		if leak.Allowed != token.Allowed || leak.RetryAfter != token.RetryAfter || leak.Remaining != token.Remaining {
			t.Fatalf("step %d: meter allowed=%v remaining=%d retry=%s, token allowed=%v remaining=%d retry=%s",
				i, leak.Allowed, leak.Remaining, leak.RetryAfter, token.Allowed, token.Remaining, token.RetryAfter)
		}
	}
}

func TestLeakyBucketEvictDropsOnlyAnEmptyMeter(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustLeak(t, 1, time.Second, 2, clk)

	mustAllow(t, lim, "alice")
	mustAllow(t, lim, "alice")
	mustAllow(t, lim, "bob")
	if n := lim.Evict(); n != 0 {
		t.Fatalf("evicted %d keys that still hold water", n)
	}

	clk.Advance(time.Second)
	if n := lim.Evict(); n != 1 {
		t.Fatalf("evicted %d, want 1", n)
	}
	if lim.Len() != 1 {
		t.Fatalf("len = %d, want 1", lim.Len())
	}
}

func TestLeakyBucketRejectsBadConfig(t *testing.T) {
	if _, err := ratelimit.NewLeakyBucket(0, time.Second, 1, nil); !errors.Is(err, ratelimit.ErrInvalidLimit) {
		t.Fatalf("rate: got %v", err)
	}
	if _, err := ratelimit.NewLeakyBucket(1, time.Second, 0, nil); !errors.Is(err, ratelimit.ErrInvalidLimit) {
		t.Fatalf("capacity: got %v", err)
	}
	if _, err := ratelimit.NewLeakyBucket(1, 0, 1, nil); !errors.Is(err, ratelimit.ErrInvalidWindow) {
		t.Fatalf("per: got %v", err)
	}
	if _, err := ratelimit.NewLeakyBucket(math.MaxInt64, time.Hour, 1, nil); !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("overflow: got %v", err)
	}

	lim := mustLeak(t, 1, time.Second, 1, nil)
	if _, err := lim.Allow(context.Background(), ""); !errors.Is(err, ratelimit.ErrEmptyKey) {
		t.Fatalf("key: got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lim.Allow(ctx, "alice"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx: got %v", err)
	}
}

func TestLeakyBucketConcurrentExactCapacity(t *testing.T) {
	const capacity = 100
	clk := newFakeClock(time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC))
	lim := mustLeak(t, 1, time.Minute, capacity, clk)

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

	if got := allowed.Load(); got != capacity {
		t.Fatalf("allowed = %d, want %d", got, capacity)
	}
}

func TestLeakyBucketKeysDoNotShareALock(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	release := make(chan struct{})
	entered := make(chan struct{})
	clk := &blockingClock{now: start, release: release, entered: entered}
	lim := mustLeak(t, 1, time.Hour, 1, clk)
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

func mustLeak(t *testing.T, rate int, per time.Duration, capacity int, clock ratelimit.Clock) *ratelimit.LeakyBucket {
	t.Helper()
	lim, err := ratelimit.NewLeakyBucket(rate, per, capacity, clock)
	if err != nil {
		t.Fatalf("new leaky bucket: %v", err)
	}
	t.Cleanup(lim.Close)
	return lim
}
