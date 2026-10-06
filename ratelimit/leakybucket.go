package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"
)

// LeakyBucket is the meter form of a leaky bucket.
//
// An allowed call adds one to the water level. Water leaks at rate units per
// per. A call that would pass capacity is refused. A new key starts empty,
// which is the permissive state: the caller may add capacity units before the
// bucket is full.
//
// With the same rate and capacity, this meter allows and denies on the same
// calls as a TokenBucket. The mapping is level = capacity - tokens. The queue
// form, which delays a call instead of only refusing it, is a different API.
//
// Lock order matches the other limiters: the map lock, then the key lock,
// and the key lock is taken before the map lock is released.
type LeakyBucket struct {
	mu       sync.Mutex
	rate     int
	per      time.Duration
	capacity int
	clock    Clock
	keys     map[string]*leakState
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

type leakState struct {
	mu        sync.Mutex
	level     int
	remainder time.Duration
	updated   time.Time
}

// NewLeakyBucket returns a meter that holds at most capacity and leaks rate
// units over each per interval. A nil clock means the wall clock.
//
// rate*per must fit in an int64. The leak multiplies those values, and a
// wrapping product would drop or keep the wrong amount of water.
func NewLeakyBucket(rate int, per time.Duration, capacity int, clock Clock) (*LeakyBucket, error) {
	if rate < 1 || capacity < 1 {
		return nil, ErrInvalidLimit
	}
	if per <= 0 {
		return nil, ErrInvalidWindow
	}
	if int64(rate) > math.MaxInt64/int64(per) {
		return nil, ErrOverflow
	}
	if clock == nil {
		clock = systemClock{}
	}
	b := &LeakyBucket{
		rate:     rate,
		per:      per,
		capacity: capacity,
		clock:    clock,
		keys:     make(map[string]*leakState),
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	go b.sweep()
	return b, nil
}

// Close stops the background sweep. It is safe to call more than once.
func (b *LeakyBucket) Close() {
	b.stopOnce.Do(func() { close(b.stop) })
	<-b.stopped
}

// Len reports how many keys currently have a bucket.
func (b *LeakyBucket) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.keys)
}

// Evict deletes keys whose level is already zero after leaking.
// A new key starts empty, so deleting an empty one changes nothing.
// A key that still holds water stays. Deleting it would open an empty bucket
// and hand the caller the full capacity again.
func (b *LeakyBucket) Evict() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	removed := 0
	for key, st := range b.keys {
		st.mu.Lock()
		b.leak(st, b.clock.Now())
		if st.level == 0 && st.remainder == 0 {
			delete(b.keys, key)
			removed++
		}
		st.mu.Unlock()
	}
	return removed
}

func (b *LeakyBucket) sweep() {
	defer close(b.stopped)
	tick := time.NewTicker(b.per)
	defer tick.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-tick.C:
			b.Evict()
		}
	}
}

// Allow reports whether key may add one unit of water.
func (b *LeakyBucket) Allow(ctx context.Context, key string) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	if key == "" {
		return Decision{}, ErrEmptyKey
	}

	b.mu.Lock()
	st := b.keys[key]
	if st == nil {
		st = &leakState{}
		b.keys[key] = st
	}
	st.mu.Lock()
	b.mu.Unlock()
	defer st.mu.Unlock()

	now := b.clock.Now()
	b.leak(st, now)

	if st.level >= b.capacity {
		retry := b.untilLeak(st.remainder)
		resetAt := now.Add(retry)
		return Decision{
			Allowed:    false,
			Limit:      b.capacity,
			Remaining:  0,
			RetryAfter: retry,
			ResetAt:    resetAt,
		}, nil
	}

	st.level++
	return Decision{
		Allowed:   true,
		Limit:     b.capacity,
		Remaining: b.capacity - st.level,
		ResetAt:   now.Add(b.untilLeak(st.remainder)),
	}, nil
}

// leak subtracts whole units earned since st.updated and keeps the leftover
// time. The level stops at zero. A span too large to multiply empties the
// bucket: the key has been idle longer than any level can remember.
func (b *LeakyBucket) leak(st *leakState, now time.Time) {
	if st.updated.IsZero() {
		st.level = 0
		st.remainder = 0
		st.updated = now
		return
	}
	elapsed := now.Sub(st.updated)
	if elapsed <= 0 {
		return
	}
	st.updated = now
	if st.level == 0 {
		st.remainder = 0
		return
	}

	if st.remainder > math.MaxInt64-elapsed {
		st.level = 0
		st.remainder = 0
		return
	}
	span := st.remainder + elapsed
	if int64(b.rate) != 0 && int64(span) > math.MaxInt64/int64(b.rate) {
		st.level = 0
		st.remainder = 0
		return
	}
	dropped := int64(span) * int64(b.rate) / int64(b.per)
	if dropped == 0 {
		st.remainder = span
		return
	}
	if dropped >= int64(st.level) {
		st.level = 0
		st.remainder = 0
		return
	}
	consumed := time.Duration(dropped * int64(b.per) / int64(b.rate))
	st.remainder = span - consumed
	st.level -= int(dropped)
}

// untilLeak is the wait until (remainder + wait) * rate covers one unit.
func (b *LeakyBucket) untilLeak(remainder time.Duration) time.Duration {
	have := int64(remainder) * int64(b.rate)
	if have >= int64(b.per) {
		return 0
	}
	need := int64(b.per) - have
	return time.Duration((need + int64(b.rate) - 1) / int64(b.rate))
}
