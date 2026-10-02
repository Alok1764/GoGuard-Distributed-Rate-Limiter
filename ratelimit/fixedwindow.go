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
// Two locks, always acquired in this order: mu, then the counter's mu.
// mu only protects the map of pointers. The counter lock protects that key's
// bucket and count. Unrelated keys do not wait on each other. Taking the
// locks in the other order deadlocks with Evict, which walks the map under mu
// and then locks each counter.
type FixedWindow struct {
	mu       sync.Mutex
	limit    int
	window   time.Duration
	clock    Clock
	keys     map[string]*fixedCounter
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

type fixedCounter struct {
	mu    sync.Mutex
	start time.Time
	count int
}

// NewFixedWindow returns a limiter that allows limit events per key per window.
// A nil clock means the wall clock.
//
// A background sweep drops keys whose bucket is no longer current. Call Close
// to stop it. Without Close, the goroutine lives until the process exits.
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
	f := &FixedWindow{
		limit:   limit,
		window:  window,
		clock:   clock,
		keys:    make(map[string]*fixedCounter),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go f.sweep()
	return f, nil
}

// Close stops the background sweep. It is safe to call more than once.
// Close waits until the sweep goroutine has exited.
func (f *FixedWindow) Close() {
	f.stopOnce.Do(func() { close(f.stop) })
	<-f.stopped
}

// Len reports how many keys currently have a counter.
func (f *FixedWindow) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.keys)
}

// Evict deletes counters that belong to an older bucket.
// A key that is inside the current bucket stays, including one that has used
// its whole limit. Returns how many keys were removed.
func (f *FixedWindow) Evict() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	removed := 0
	for key, st := range f.keys {
		st.mu.Lock()
		// Sample time while this counter is locked. An earlier sample could
		// observe a stale bucket and delete a counter Allow has already rolled
		// into the new window.
		current := f.clock.Now().Truncate(f.window)
		if !st.start.Equal(current) {
			delete(f.keys, key)
			removed++
		}
		st.mu.Unlock()
	}
	return removed
}

func (f *FixedWindow) sweep() {
	defer close(f.stopped)
	tick := time.NewTicker(f.window)
	defer tick.Stop()
	for {
		select {
		case <-f.stop:
			return
		case <-tick.C:
			f.Evict()
		}
	}
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

	// Lock the counter before releasing the map. Otherwise Evict can remove
	// the pointer in between, and this call would increment a counter the
	// map no longer holds. The next call would then start again at zero.
	f.mu.Lock()
	st := f.keys[key]
	if st == nil {
		st = &fixedCounter{}
		f.keys[key] = st
	}
	st.mu.Lock()
	f.mu.Unlock()
	defer st.mu.Unlock()

	now := f.clock.Now()
	start := now.Truncate(f.window)
	resetAt := start.Add(f.window)

	if !st.start.Equal(start) {
		st.start = start
		st.count = 0
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
