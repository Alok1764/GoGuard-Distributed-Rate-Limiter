package ratelimit_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Alok1764/GoGuard-Distributed-Rate-Limiter/ratelimit"
)

func TestSlidingLogAllowsUntilLimitThenRejects(t *testing.T) {
	start := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustLog(t, 3, time.Second, clk)

	for i := 0; i < 3; i++ {
		d := mustAllow(t, lim, "alice")
		if !d.Allowed {
			t.Fatalf("request %d denied", i+1)
		}
		if d.Remaining != 2-i {
			t.Fatalf("request %d: remaining = %d, want %d", i+1, d.Remaining, 2-i)
		}
		if !d.ResetAt.Equal(start.Add(time.Second)) {
			t.Fatalf("reset at %s, want %s", d.ResetAt, start.Add(time.Second))
		}
	}

	denied := mustAllow(t, lim, "alice")
	if denied.Allowed {
		t.Fatal("4th request allowed")
	}
	if denied.RetryAfter != time.Second {
		t.Fatalf("retry-after = %s, want 1s", denied.RetryAfter)
	}
}

func TestSlidingLogDoesNotBurstAtAFixedBoundary(t *testing.T) {
	// Three allows spread over the first 200ms. A fixed window of 1s would
	// open a fresh limit at the next whole second. The log must still count
	// the hits that are younger than the window, and free them one at a time.
	start := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustLog(t, 3, time.Second, clk)

	mustAllow(t, lim, "alice")
	clk.Advance(100 * time.Millisecond)
	mustAllow(t, lim, "alice")
	clk.Advance(100 * time.Millisecond)
	mustAllow(t, lim, "alice")

	// t = 200ms. All three hits are inside the window. The fixed-window
	// boundary is still 800ms away, and the log is already closed.
	if d := mustAllow(t, lim, "alice"); d.Allowed {
		t.Fatal("allowed a 4th hit inside the window")
	} else if d.RetryAfter != 800*time.Millisecond {
		t.Fatalf("retry-after = %s, want 800ms", d.RetryAfter)
	}

	// t = 1s. The hit at t=0 has age == window and drops. One slot opens.
	// The hits at 100ms and 200ms are still inside.
	clk.Advance(800 * time.Millisecond)
	if d := mustAllow(t, lim, "alice"); !d.Allowed {
		t.Fatal("denied after the oldest hit aged out")
	} else if d.Remaining != 0 {
		t.Fatalf("remaining = %d, want 0", d.Remaining)
	}
	if d := mustAllow(t, lim, "alice"); d.Allowed {
		t.Fatal("a second hit was allowed before the next timestamp aged out")
	}
}

func TestSlidingLogKeysAreIndependent(t *testing.T) {
	clk := newFakeClock(time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC))
	lim := mustLog(t, 1, time.Minute, clk)

	if d := mustAllow(t, lim, "alice"); !d.Allowed {
		t.Fatal("alice denied")
	}
	if d := mustAllow(t, lim, "alice"); d.Allowed {
		t.Fatal("alice allowed twice")
	}
	if d := mustAllow(t, lim, "bob"); !d.Allowed {
		t.Fatal("bob shared alice's log")
	}
}

func TestSlidingLogEvictDropsExpiredKeysOnly(t *testing.T) {
	start := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustLog(t, 1, time.Second, clk)

	mustAllow(t, lim, "alice")
	mustAllow(t, lim, "bob")
	if n := lim.Evict(); n != 0 {
		t.Fatalf("evicted %d live keys", n)
	}

	clk.Advance(time.Second)
	mustAllow(t, lim, "alice")
	if n := lim.Evict(); n != 1 {
		t.Fatalf("evicted %d, want 1", n)
	}
	if lim.Len() != 1 {
		t.Fatalf("len = %d, want 1", lim.Len())
	}
}

func TestSlidingLogRejectsBadConfig(t *testing.T) {
	if _, err := ratelimit.NewSlidingWindowLog(0, time.Second, nil); !errors.Is(err, ratelimit.ErrInvalidLimit) {
		t.Fatalf("limit: got %v", err)
	}
	if _, err := ratelimit.NewSlidingWindowLog(1, 0, nil); !errors.Is(err, ratelimit.ErrInvalidWindow) {
		t.Fatalf("window: got %v", err)
	}

	lim := mustLog(t, 1, time.Second, nil)
	if _, err := lim.Allow(context.Background(), ""); !errors.Is(err, ratelimit.ErrEmptyKey) {
		t.Fatalf("key: got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lim.Allow(ctx, "alice"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx: got %v", err)
	}
}

func TestSlidingLogConcurrentExactLimit(t *testing.T) {
	const limit = 100
	lim := mustLog(t, limit, time.Minute, ratelimit.SystemClock())

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

func TestSlidingLogKeysDoNotShareALock(t *testing.T) {
	start := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	release := make(chan struct{})
	entered := make(chan struct{})
	clk := &blockingClock{now: start, release: release, entered: entered}
	lim := mustLog(t, 1, time.Hour, clk)
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

func mustLog(t *testing.T, limit int, window time.Duration, clock ratelimit.Clock) *ratelimit.SlidingWindowLog {
	t.Helper()
	lim, err := ratelimit.NewSlidingWindowLog(limit, window, clock)
	if err != nil {
		t.Fatalf("new sliding log: %v", err)
	}
	t.Cleanup(lim.Close)
	return lim
}
