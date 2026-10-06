// Package ratelimit decides whether a caller identified by a key may proceed.
//
// Every algorithm implements Limiter. HTTP handlers, tests, and a future Redis
// backend all call Allow, so the policy stays independent of the transport.
package ratelimit

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidLimit is returned when a limiter is built with a non-positive limit.
	ErrInvalidLimit = errors.New("ratelimit: limit must be positive")
	// ErrInvalidWindow is returned when a limiter is built with a non-positive window.
	ErrInvalidWindow = errors.New("ratelimit: window must be positive")
	// ErrEmptyKey is returned when Allow is called without a caller identity.
	ErrEmptyKey = errors.New("ratelimit: key must be non-empty")
	// ErrOverflow is returned when limit and window are too large for an exact
	// integer comparison in the sliding window counter.
	ErrOverflow = errors.New("ratelimit: limit and window overflow an exact comparison")
)

// Decision is the outcome of one Allow call.
// Remaining and RetryAfter are what an HTTP layer turns into rate-limit headers.
type Decision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration // zero when the request is allowed
	ResetAt    time.Time
}

// Limiter is the only API the rest of the program depends on.
// ctx is part of the contract now so a Redis backend can cancel an in-flight
// round trip without changing every caller later.
type Limiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
}

// Clock supplies the instant used to choose a window.
// Production uses the wall clock. Tests pass a clock they can move forward,
// which is the only way boundary cases stay deterministic.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// SystemClock returns the process wall clock.
func SystemClock() Clock { return systemClock{} }
