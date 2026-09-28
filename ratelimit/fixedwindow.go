package ratelimit

import (
	"context"
	"sync"
	"time"
)

// FixedWindow counts requests inside aligned buckets of length window.
//
// The bucket is floor(now / window), not "window starts at the first request".
// Aligned buckets are the same idea a Redis key will use later
// (one counter per key per bucket id), so the in-memory and distributed
// algorithms stay comparable.
//
// mu guards keys. One mutex for the whole map keeps the check-and-increment
// atomic across goroutines. Unrelated keys share this lock; that is correct,
// and it becomes the bottleneck once many keys are hot. Splitting the lock
// is a later step, taken when a benchmark shows contention.
type FixedWindow struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	clock  Clock
	keys   map[string]*fixedCounter
}

type fixedCounter struct {
	start time.Time
	count int
}

// NewFixedWindow returns a limiter that allows limit events per key per window.
// A nil clock means the wall clock.
func NewFixedWindow(limit int, window time.Duration, clock Clock) (*FixedWindow, error) {
	if limit < 1 {
		return nil, ErrInvalidLimit
	}
	if window <= 0 {
		return nil, ErrInvalidWindow
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &FixedWindow{
		limit:  limit,
		window: window,
		clock:  clock,
		keys:   make(map[string]*fixedCounter),
	}, nil
}

// Allow reports whether key may proceed.
// Only allowed calls consume a count. A rejected call leaves the counter
// unchanged so a burst of 429s cannot push the window further closed.
func (f *FixedWindow) Allow(ctx context.Context, key string) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	if key == "" {
		return Decision{}, ErrEmptyKey
	}

	// The timestamp selects the bucket, so it is sampled under the same lock
	// as the counter. Reading it beforehand would let a stalled goroutine
	// apply an old bucket and wipe a newer count.
	f.mu.Lock()
	defer f.mu.Unlock()

	now := f.clock.Now()
	start := now.Truncate(f.window)
	resetAt := start.Add(f.window)

	st := f.keys[key]
	if st == nil || !st.start.Equal(start) {
		st = &fixedCounter{start: start}
		f.keys[key] = st
	}

	if st.count >= f.limit {
		retry := resetAt.Sub(now)
		if retry < 0 {
			retry = 0
		}
		return Decision{
			Allowed:    false,
			Limit:      f.limit,
			Remaining:  0,
			RetryAfter: retry,
			ResetAt:    resetAt,
		}, nil
	}

	st.count++
	return Decision{
		Allowed:   true,
		Limit:     f.limit,
		Remaining: f.limit - st.count,
		ResetAt:   resetAt,
	}, nil
}
