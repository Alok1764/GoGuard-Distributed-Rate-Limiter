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

// fakeClock is mutable test time. Its mutex exists because Allow reads Now
// while a test may Advance from another goroutine; the race detector checks this too.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{now: t}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func TestFixedWindowAllowsUntilLimitThenRejects(t *testing.T) {
	start := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	lim := mustFixed(t, 3, time.Second, clk)

	for i := 0; i < 3; i++ {
		d := mustAllow(t, lim, "alice")
		if !d.Allowed {
			t.Fatalf("request %d: got denied", i+1)
		}
		if d.Remaining != 2-i {
			t.Fatalf("request %d: remaining = %d, want %d", i+1, d.Remaining, 2-i)
		}
		if d.RetryAfter != 0 {
			t.Fatalf("request %d: retry-after = %s, want 0", i+1, d.RetryAfter)
		}
		if !d.ResetAt.Equal(start.Add(time.Second)) {
			t.Fatalf("reset at %s, want %s", d.ResetAt, start.Add(time.Second))
		}
	}

	denied := mustAllow(t, lim, "alice")
	if denied.Allowed {
		t.Fatal("4th request allowed")
	}
	if denied.Remaining != 0 {
		t.Fatalf("remaining = %d, want 0", denied.Remaining)
	}
	if denied.RetryAfter != time.Second {
		t.Fatalf("retry-after = %s, want 1s", denied.RetryAfter)
	}
}

func TestFixedWindowBoundaryBurst(t *testing.T) {
	// Limit 3 per second. Three requests land 1ms before the bucket rolls,
	// three more land on the new bucket. All six are allowed, 1ms apart.
	// That is the fixed-window hole: the limit holds inside a bucket, not
	// across a bucket edge.
	start := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	clk := newFakeClock(start.Add(time.Second - time.Millisecond))
	lim := mustFixed(t, 3, time.Second, clk)

	for i := 0; i < 3; i++ {
		if d := mustAllow(t, lim, "alice"); !d.Allowed {
			t.Fatalf("pre-boundary request %d denied", i+1)
		}
	}
	if d := mustAllow(t, lim, "alice"); d.Allowed {
		t.Fatal("4th pre-boundary request allowed")
	} else if d.RetryAfter != time.Millisecond {
		t.Fatalf("retry-after = %s, want 1ms", d.RetryAfter)
	}

	clk.Advance(time.Millisecond)

	for i := 0; i < 3; i++ {
		if d := mustAllow(t, lim, "alice"); !d.Allowed {
			t.Fatalf("post-boundary request %d denied", i+1)
		}
	}
}

func TestFixedWindowKeysAreIndependent(t *testing.T) {
	clk := newFakeClock(time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC))
	lim := mustFixed(t, 1, time.Minute, clk)

	if d := mustAllow(t, lim, "alice"); !d.Allowed {
		t.Fatal("alice denied")
	}
	if d := mustAllow(t, lim, "alice"); d.Allowed {
		t.Fatal("alice allowed twice")
	}
	if d := mustAllow(t, lim, "bob"); !d.Allowed {
		t.Fatal("bob shared alice's counter")
	}
}

func TestFixedWindowRejectsBadConfig(t *testing.T) {
	if _, err := ratelimit.NewFixedWindow(0, time.Second, nil); !errors.Is(err, ratelimit.ErrInvalidLimit) {
		t.Fatalf("limit: got %v", err)
	}
	if _, err := ratelimit.NewFixedWindow(1, 0, nil); !errors.Is(err, ratelimit.ErrInvalidWindow) {
		t.Fatalf("window: got %v", err)
	}

	lim := mustFixed(t, 1, time.Second, nil)
	if _, err := lim.Allow(context.Background(), ""); !errors.Is(err, ratelimit.ErrEmptyKey) {
		t.Fatalf("key: got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lim.Allow(ctx, "alice"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx: got %v", err)
	}
}

func TestFixedWindowConcurrentExactLimit(t *testing.T) {
	const limit = 100
	lim := mustFixed(t, limit, time.Minute, ratelimit.SystemClock())

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

func mustFixed(t *testing.T, limit int, window time.Duration, clock ratelimit.Clock) *ratelimit.FixedWindow {
	t.Helper()
	lim, err := ratelimit.NewFixedWindow(limit, window, clock)
	if err != nil {
		t.Fatalf("new fixed window: %v", err)
	}
	return lim
}

func mustAllow(t *testing.T, lim ratelimit.Limiter, key string) ratelimit.Decision {
	t.Helper()
	d, err := lim.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("allow %q: %v", key, err)
	}
	return d
}
