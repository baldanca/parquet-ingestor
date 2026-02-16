package ingestor

import (
	"context"
	"math/rand"
	"time"
)

// RetryPolicy retries transient failures. Implementations must honor ctx.
//
// A staff+ friendly default is ExponentialBackoffPolicy.
type RetryPolicy interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}

type nopRetry struct{}

func (nopRetry) Do(ctx context.Context, fn func(ctx context.Context) error) error { return fn(ctx) }

// ExponentialBackoffPolicy retries with exponential backoff and jitter.
//
// Note: error classification (retryable vs non-retryable) is intentionally left to the caller.
// Wrap your fn to short-circuit when needed.
type ExponentialBackoffPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	JitterPct   float64 // 0.2 = ±20%
}

func (p ExponentialBackoffPolicy) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	base := p.BaseDelay
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	maxDelay := p.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second
	}
	jitter := p.JitterPct
	if jitter < 0 {
		jitter = 0
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fn(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == maxAttempts {
			break
		}
		delay := base << (attempt - 1)
		if delay > maxDelay {
			delay = maxDelay
		}
		if jitter > 0 {
			// Apply symmetrical jitter.
			j := 1 + (rand.Float64()*2-1)*jitter
			if j < 0 {
				j = 0
			}
			delay = time.Duration(float64(delay) * j)
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return lastErr
}
