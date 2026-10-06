package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"
)

// SlidingWindowCounter estimates the trailing window from two bucket totals.
//
// prev is the count from the previous aligned bucket. curr is the count in
// the bucket that contains now. The estimate is
//
//	prev*(window-elapsed)/window + curr
//
// which is the share of the previous bucket that still overlaps the trailing
// window, plus every call in the current bucket. The even-spread assumption
// is the error: calls clustered at one end of the previous bucket are counted
// too heavily or too lightly. The log is the exact version of the same idea.
//
// Lock order matches the other limiters: the map lock, then the key lock,
// and the key lock is taken before the map lock is released.
type SlidingWindowCounter struct {
	mu       sync.Mutex
	limit    int
	window   time.Duration
	clock    Clock
	keys     map[string]*weightedCounter
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

type weightedCounter struct {
	mu    sync.Mutex
	start time.Time
	prev  int
	curr  int
}

// NewSlidingWindowCounter returns a limiter that allows about limit events
// per key per trailing window. A nil clock means the wall clock.
//
// limit*window must fit in an int64. The allow check multiplies those two
// values, and a wrapping product would admit or refuse the wrong calls.
func NewSlidingWindowCounter(limit int, window time.Duration, clock Clock) (*SlidingWindowCounter, error) {
	if limit < 1 {
		return nil, ErrInvalidLimit
	}
	if window <= 0 {
		return nil, ErrInvalidWindow
	}
	if int64(limit) > math.MaxInt64/int64(window) {
		return nil, ErrOverflow
	}
	if clock == nil {
		clock = systemClock{}
	}
	s := &SlidingWindowCounter{
		limit:   limit,
		window:  window,
		clock:   clock,
		keys:    make(map[string]*weightedCounter),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go s.sweep()
	return s, nil
}

// Close stops the background sweep. It is safe to call more than once.
func (s *SlidingWindowCounter) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.stopped
}

// Len reports how many keys currently have a counter.
func (s *SlidingWindowCounter) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.keys)
}

// Evict deletes keys whose previous and current counts are both zero after
// the buckets are rolled forward to now. A key that still overlaps the
// trailing window stays, even if it has already used its whole limit.
func (s *SlidingWindowCounter) Evict() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for key, st := range s.keys {
		st.mu.Lock()
		// Sample under the key lock. A time read before this lock can belong
		// to an older bucket than the one Allow has already rolled onto.
		s.roll(st, s.clock.Now())
		if st.prev == 0 && st.curr == 0 {
			delete(s.keys, key)
			removed++
		}
		st.mu.Unlock()
	}
	return removed
}

func (s *SlidingWindowCounter) sweep() {
	defer close(s.stopped)
	tick := time.NewTicker(s.window)
	defer tick.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-tick.C:
			s.Evict()
		}
	}
}

// Allow reports whether key may proceed.
// A call is refused when the weighted estimate is already at least limit.
// Only an allowed call increments curr.
func (s *SlidingWindowCounter) Allow(ctx context.Context, key string) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	if key == "" {
		return Decision{}, ErrEmptyKey
	}

	s.mu.Lock()
	st := s.keys[key]
	if st == nil {
		st = &weightedCounter{}
		s.keys[key] = st
	}
	st.mu.Lock()
	s.mu.Unlock()
	defer st.mu.Unlock()

	now := s.clock.Now()
	s.roll(st, now)
	elapsed := now.Sub(st.start)

	if reached(st.prev, st.curr, s.limit, elapsed, s.window) {
		resetAt := s.nextOpen(st.prev, st.curr, st.start, elapsed)
		retry := resetAt.Sub(now)
		if retry < 0 {
			retry = 0
		}
		return Decision{
			Allowed:    false,
			Limit:      s.limit,
			Remaining:  0,
			RetryAfter: retry,
			ResetAt:    resetAt,
		}, nil
	}

	st.curr++
	return Decision{
		Allowed:   true,
		Limit:     s.limit,
		Remaining: remainingAfter(st.prev, st.curr, s.limit, elapsed, s.window),
		ResetAt:   st.start.Add(s.window),
	}, nil
}

// roll moves prev and curr onto the bucket that contains now.
// One bucket forward keeps the old curr as prev. A wider gap clears both,
// because the bucket immediately before now had no calls.
func (s *SlidingWindowCounter) roll(st *weightedCounter, now time.Time) {
	start := now.Truncate(s.window)
	if st.start.Equal(start) {
		return
	}
	if !st.start.IsZero() && start.Equal(st.start.Add(s.window)) {
		st.prev = st.curr
		st.curr = 0
		st.start = start
		return
	}
	st.prev = 0
	st.curr = 0
	st.start = start
}

// reached reports whether prev*(window-elapsed)/window + curr is at least limit.
// The cross-multiplication avoids a floating-point weight. curr >= limit is
// decided before any multiply, so a full current bucket does not depend on prev.
func reached(prev, curr, limit int, elapsed, window time.Duration) bool {
	if curr >= limit {
		return true
	}
	if prev <= 0 {
		return false
	}
	remain := window - elapsed
	if remain < 0 {
		remain = 0
	}
	return int64(prev)*int64(remain) >= int64(limit-curr)*int64(window)
}

// remainingAfter is how many further calls would still pass at this same
// elapsed time, after curr has already been incremented for the call in hand.
func remainingAfter(prev, curr, limit int, elapsed, window time.Duration) int {
	if prev <= 0 {
		if limit > curr {
			return limit - curr
		}
		return 0
	}
	remain := window - elapsed
	if remain < 0 {
		remain = 0
	}
	// The largest curr that is still under the limit satisfies
	// prev*remain < (limit-curr)*window, so
	// curr <= limit - floor(prev*remain/window) - 1.
	used := int((int64(prev)*int64(remain) + int64(window) - 1) / int64(window))
	maxCurr := limit - used
	if maxCurr < curr {
		return 0
	}
	return maxCurr - curr
}

// nextOpen is the first instant the estimate falls strictly below limit.
// Inside this bucket the current count does not decay, so a full curr waits
// for the next bucket. At the first instant of that bucket the old curr is
// prev with weight 1, which is still at the limit, so the slot opens one
// nanosecond later.
func (s *SlidingWindowCounter) nextOpen(prev, curr int, start time.Time, elapsed time.Duration) time.Time {
	if curr < s.limit && prev > 0 {
		// prev*(window-e) < (limit-curr)*window
		// window-e <= floor(((limit-curr)*window - 1) / prev)
		room := int64(s.limit-curr) * int64(s.window)
		maxRemain := (room - 1) / int64(prev)
		need := int64(s.window) - maxRemain
		if need < int64(elapsed) {
			need = int64(elapsed)
		}
		if need < int64(s.window) {
			return start.Add(time.Duration(need))
		}
	}
	boundary := start.Add(s.window)
	if curr < s.limit {
		return boundary
	}
	if curr == s.limit {
		return boundary.Add(time.Nanosecond)
	}
	room := int64(s.limit) * int64(s.window)
	maxRemain := (room - 1) / int64(curr)
	return boundary.Add(time.Duration(int64(s.window) - maxRemain))
}
