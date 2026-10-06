package ratelimit

import (
	"context"
	"sync"
	"time"
)

// SlidingWindowLog keeps the time of each allowed call and counts only those
// still inside the last window.
//
// A hit at time t counts while now.Sub(t) < window. There is no bucket edge,
// so the fixed-window spike (a full limit on each side of a boundary) does not
// happen. The cost is memory: each live key stores up to limit timestamps,
// because a refused call is not stored and the log is pruned on every Allow.
//
// Lock order matches FixedWindow: the map lock, then the key lock. The key
// lock is taken before the map lock is released, so Evict cannot drop the
// pointer out from under an in-flight Allow.
type SlidingWindowLog struct {
	mu       sync.Mutex
	limit    int
	window   time.Duration
	clock    Clock
	keys     map[string]*logCounter
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

type logCounter struct {
	mu   sync.Mutex
	hits []time.Time
}

// NewSlidingWindowLog returns a limiter that allows limit events per key
// over the trailing window. A nil clock means the wall clock.
//
// A background sweep drops keys whose hits have all aged out. Call Close
// to stop it.
func NewSlidingWindowLog(limit int, window time.Duration, clock Clock) (*SlidingWindowLog, error) {
	if limit < 1 {
		return nil, ErrInvalidLimit
	}
	if window <= 0 {
		return nil, ErrInvalidWindow
	}
	if clock == nil {
		clock = systemClock{}
	}
	s := &SlidingWindowLog{
		limit:   limit,
		window:  window,
		clock:   clock,
		keys:    make(map[string]*logCounter),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go s.sweep()
	return s, nil
}

// Close stops the background sweep. It is safe to call more than once.
func (s *SlidingWindowLog) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.stopped
}

// Len reports how many keys currently have a log.
func (s *SlidingWindowLog) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.keys)
}

// Evict deletes keys with no hits inside the current window.
func (s *SlidingWindowLog) Evict() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for key, st := range s.keys {
		st.mu.Lock()
		st.prune(s.clock.Now(), s.window)
		if len(st.hits) == 0 {
			delete(s.keys, key)
			removed++
		}
		st.mu.Unlock()
	}
	return removed
}

func (s *SlidingWindowLog) sweep() {
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
// Only an allowed call appends a timestamp. A refusal leaves the log unchanged.
func (s *SlidingWindowLog) Allow(ctx context.Context, key string) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	if key == "" {
		return Decision{}, ErrEmptyKey
	}

	s.mu.Lock()
	st := s.keys[key]
	if st == nil {
		st = &logCounter{}
		s.keys[key] = st
	}
	st.mu.Lock()
	s.mu.Unlock()
	defer st.mu.Unlock()
 
	now := s.clock.Now()
	st.prune(now, s.window)

	if len(st.hits) >= s.limit {
		// The oldest hit is the first one that will age out, which is the
		// earliest moment this key can be allowed again.
		resetAt := st.hits[0].Add(s.window)
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

	st.hits = append(st.hits, now)
	return Decision{
		Allowed:   true,
		Limit:     s.limit,
		Remaining: s.limit - len(st.hits),
		ResetAt:   st.hits[0].Add(s.window),
	}, nil
}

// prune drops hits whose age is greater than or equal to window.
// Hits are stored oldest-first, so the expired prefix is contiguous.
func (st *logCounter) prune(now time.Time, window time.Duration) {
	cutoff := now.Add(-window)
	n := 0
	for n < len(st.hits) && !st.hits[n].After(cutoff) {
		n++
	}
	if n == 0 {
		return
	}
	live := len(st.hits) - n
	copy(st.hits, st.hits[n:])
	clear(st.hits[live:])
	st.hits = st.hits[:live]
}
