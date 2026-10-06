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

func TestSlidingCounterFreshKeyStopsAtLimit(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustCounter(t, 3, time.Second, clk)

	for i := 0; i < 3; i++ {
		d := mustAllow(t, lim, "alice")
		if !d.Allowed {
			t.Fatalf("request %d denied", i+1)
		}
		if d.Remaining != 2-i {
			t.Fatalf("request %d: remaining = %d, want %d", i+1, d.Remaining, 2-i)
		}
		if !d.ResetAt.Equal(start.Add(time.Second)) {
			t.Fatalf("reset at %s", d.ResetAt)
		}
	}

	denied := mustAllow(t, lim, "alice")
	if denied.Allowed {
		t.Fatal("4th request allowed")
	}
	// The next bucket still weights this full count at its first instant,
	// so the slot opens one nanosecond after the boundary.
	if denied.RetryAfter != time.Second+time.Nanosecond {
		t.Fatalf("retry-after = %s, want 1s+1ns", denied.RetryAfter)
	}
}

func TestSlidingCounterHalfOfPreviousBucketStillCounts(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustCounter(t, 10, 10*time.Second, clk)

	for range 10 {
		if d := mustAllow(t, lim, "alice"); !d.Allowed {
			t.Fatal("filling the first bucket failed")
		}
	}

	// Halfway through the next bucket the previous 10 still contribute 5.
	clk.Advance(15 * time.Second)
	for i := 0; i < 5; i++ {
		d := mustAllow(t, lim, "alice")
		if !d.Allowed {
			t.Fatalf("halfway allow %d denied", i+1)
		}
		if i == 0 && d.Remaining != 4 {
			t.Fatalf("remaining = %d, want 4", d.Remaining)
		}
	}
	denied := mustAllow(t, lim, "alice")
	if denied.Allowed {
		t.Fatal("a full new quota opened at the next bucket")
	}
	if denied.RetryAfter != time.Nanosecond {
		t.Fatalf("retry-after = %s, want 1ns", denied.RetryAfter)
	}
}

func TestSlidingCounterGapClearsPreviousCount(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustCounter(t, 10, 10*time.Second, clk)
	for range 10 {
		mustAllow(t, lim, "alice")
	}

	// Two buckets later the previous bucket, relative to now, is empty.
	clk.Advance(20 * time.Second)
	for i := range 10 {
		if d := mustAllow(t, lim, "alice"); !d.Allowed {
			t.Fatalf("request %d denied after a two-bucket gap", i+1)
		}
	}
}

func TestSlidingCounterDisagreesWithLogOnAClusteredPreviousBucket(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	window := 10 * time.Second
	clk := newFakeClock(start)
	counter := mustCounter(t, 10, window, clk)
	log := mustLog(t, 10, window, clk)

	for range 10 {
		mustAllow(t, counter, "alice")
		mustAllow(t, log, "alice")
	}

	// Age of every hit is exactly the window. The log drops them.
	// The counter still weights the whole previous bucket.
	clk.Advance(window)
	if d := mustAllow(t, log, "alice"); !d.Allowed {
		t.Fatal("log refused a hit that had aged out")
	}
	if d := mustAllow(t, counter, "alice"); d.Allowed {
		t.Fatal("counter ignored the previous bucket at the boundary")
	}
}

func TestSlidingCounterOverAdmitsAgainstTheLog(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	window := 10 * time.Second
	clk := newFakeClock(start.Add(window - time.Nanosecond))
	counter := mustCounter(t, 10, window, clk)
	log := mustLog(t, 10, window, clk)

	for range 10 {
		mustAllow(t, counter, "alice")
		mustAllow(t, log, "alice")
	}

	// Nine seconds into the next bucket those hits are still inside the log.
	// The counter weights only the last tenth of the previous bucket.
	clk.Advance(time.Nanosecond + 9*time.Second)
	if d := mustAllow(t, log, "alice"); d.Allowed {
		t.Fatal("log allowed a hit that is still inside the window")
	}
	if d := mustAllow(t, counter, "alice"); !d.Allowed {
		t.Fatal("counter refused a call its weight still had room for")
	}
}

func TestSlidingCounterEvictKeepsABucketThatStillOverlaps(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustCounter(t, 1, 10*time.Second, clk)
	mustAllow(t, lim, "alice")
	mustAllow(t, lim, "bob")
	if n := lim.Evict(); n != 0 {
		t.Fatalf("evicted %d live keys", n)
	}

	clk.Advance(10 * time.Second)
	if n := lim.Evict(); n != 0 {
		t.Fatalf("evicted a key whose previous bucket still overlaps: %d", n)
	}

	clk.Advance(10 * time.Second)
	mustAllow(t, lim, "alice")
	if n := lim.Evict(); n != 1 {
		t.Fatalf("evicted %d, want 1", n)
	}
	if lim.Len() != 1 {
		t.Fatalf("len = %d, want 1", lim.Len())
	}
}

func TestSlidingCounterRejectsBadConfig(t *testing.T) {
	if _, err := ratelimit.NewSlidingWindowCounter(0, time.Second, nil); !errors.Is(err, ratelimit.ErrInvalidLimit) {
		t.Fatalf("limit: got %v", err)
	}
	if _, err := ratelimit.NewSlidingWindowCounter(1, 0, nil); !errors.Is(err, ratelimit.ErrInvalidWindow) {
		t.Fatalf("window: got %v", err)
	}
	if _, err := ratelimit.NewSlidingWindowCounter(math.MaxInt64, time.Hour, nil); !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("overflow: got %v", err)
	}

	lim := mustCounter(t, 1, time.Second, nil)
	if _, err := lim.Allow(context.Background(), ""); !errors.Is(err, ratelimit.ErrEmptyKey) {
		t.Fatalf("key: got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lim.Allow(ctx, "alice"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx: got %v", err)
	}
}

func TestSlidingCounterConcurrentExactLimit(t *testing.T) {
	const limit = 100
	lim := mustCounter(t, limit, time.Minute, ratelimit.SystemClock())

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

	if got := allowed.Load(); got != limit {
		t.Fatalf("allowed = %d, want %d", got, limit)
	}
}

func TestSlidingCounterKeysDoNotShareALock(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	release := make(chan struct{})
	entered := make(chan struct{})
	clk := &blockingClock{now: start, release: release, entered: entered}
	lim := mustCounter(t, 1, time.Hour, clk)
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

func mustCounter(t *testing.T, limit int, window time.Duration, clock ratelimit.Clock) *ratelimit.SlidingWindowCounter {
	t.Helper()
	lim, err := ratelimit.NewSlidingWindowCounter(limit, window, clock)
	if err != nil {
		t.Fatalf("new sliding counter: %v", err)
	}
	t.Cleanup(lim.Close)
	return lim
}
