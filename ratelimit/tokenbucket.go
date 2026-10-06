package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"
)

// TokenBucket allows a burst, then a steady refill.
//
// The bucket holds at most burst tokens and gains rate tokens per per.
// One allowed call spends one token. Refill is computed from the elapsed
// time on the next Allow, so there is no ticker goroutine per key.
//
// tokens and remainder move together. remainder is the time already waited
// toward the next whole token. Dropping it would make that token arrive late.
// An atomic add on tokens alone would let two goroutines both refill from the
// same timestamp and mint extra tokens.
//
// Lock order matches the other limiters: the map lock, then the key lock,
// and the key lock is taken before the map lock is released.
type TokenBucket struct {
	mu       sync.Mutex
	rate     int
	per      time.Duration
	burst    int
	clock    Clock
	keys     map[string]*tokenState
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

type tokenState struct {
	mu        sync.Mutex
	tokens    int
	remainder time.Duration
	updated   time.Time
}

// NewTokenBucket returns a limiter that starts each key full, at burst tokens,
// and refills rate tokens over each per interval.
// A nil clock means the wall clock.
//
// rate*per must fit in an int64. The refill multiplies those values, and a
// wrapping product would mint or drop tokens.
func NewTokenBucket(rate int, per time.Duration, burst int, clock Clock) (*TokenBucket, error) {
	if rate < 1 || burst < 1 {
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
	b := &TokenBucket{
		rate:    rate,
		per:     per,
		burst:   burst,
		clock:   clock,
		keys:    make(map[string]*tokenState),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go b.sweep()
	return b, nil
}

// Close stops the background sweep. It is safe to call more than once.
func (b *TokenBucket) Close() {
	b.stopOnce.Do(func() { close(b.stop) })
	<-b.stopped
}

// Len reports how many keys currently have a bucket.
func (b *TokenBucket) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.keys)
}

// Evict deletes keys whose bucket is already full after refill.
// A full key comes back full on the next call, so deleting it changes nothing.
// A key that has spent tokens stays. Deleting it would hand the caller a new
// full burst.
func (b *TokenBucket) Evict() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	removed := 0
	for key, st := range b.keys {
		st.mu.Lock()
		b.refill(st, b.clock.Now())
		if st.tokens == b.burst && st.remainder == 0 {
			delete(b.keys, key)
			removed++
		}
		st.mu.Unlock()
	}
	return removed
}

func (b *TokenBucket) sweep() {
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

// Allow reports whether key may spend one token.
func (b *TokenBucket) Allow(ctx context.Context, key string) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	if key == "" {
		return Decision{}, ErrEmptyKey
	}

	b.mu.Lock()
	st := b.keys[key]
	if st == nil {
		st = &tokenState{}
		b.keys[key] = st
	}
	st.mu.Lock()
	b.mu.Unlock()
	defer st.mu.Unlock()

	now := b.clock.Now()
	b.refill(st, now)

	if st.tokens < 1 {
		retry := b.untilNext(st.remainder)
		resetAt := now.Add(retry)
		return Decision{
			Allowed:    false,
			Limit:      b.burst,
			Remaining:  0,
			RetryAfter: retry,
			ResetAt:    resetAt,
		}, nil
	}

	st.tokens--
	return Decision{
		Allowed:   true,
		Limit:     b.burst,
		Remaining: st.tokens,
		ResetAt:   now.Add(b.untilNext(st.remainder)),
	}, nil
}

// refill adds whole tokens earned since st.updated and keeps the leftover
// time. A span too large to multiply fills the bucket: the key has been idle
// longer than any burst can remember.
func (b *TokenBucket) refill(st *tokenState, now time.Time) {
	if st.updated.IsZero() {
		st.tokens = b.burst
		st.remainder = 0
		st.updated = now
		return
	}
	elapsed := now.Sub(st.updated)
	if elapsed <= 0 {
		return
	}
	st.updated = now

	if st.remainder > math.MaxInt64-elapsed {
		st.tokens = b.burst
		st.remainder = 0
		return
	}
	span := st.remainder + elapsed
	if int64(b.rate) != 0 && int64(span) > math.MaxInt64/int64(b.rate) {
		st.tokens = b.burst
		st.remainder = 0
		return
	}
	product := int64(span) * int64(b.rate)
	added := product / int64(b.per)
	if added >= int64(b.burst-st.tokens) {
		st.tokens = b.burst
		st.remainder = 0
		return
	}
	consumed := time.Duration(added * int64(b.per) / int64(b.rate))
	st.remainder = span - consumed
	st.tokens += int(added)
}

// untilNext is the wait until (remainder + wait) * rate covers one token.
func (b *TokenBucket) untilNext(remainder time.Duration) time.Duration {
	have := int64(remainder) * int64(b.rate)
	if have >= int64(b.per) {
		return 0
	}
	need := int64(b.per) - have
	return time.Duration((need + int64(b.rate) - 1) / int64(b.rate))
}
