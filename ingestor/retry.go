package ingestor

import (
	"context"
	"math/rand"
	"time"
)

type RetryPolicy interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}

type nopRetry struct{}

func (nopRetry) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(ctx)
}

// SimpleRetry is a small, dependency-free retry policy with exponential backoff.
// It retries on ANY error returned by fn (you can wrap fn to filter errors if desired).
type SimpleRetry struct {
	Attempts  int           // total attempts (>=1). Example: 3 means 1 try + 2 retries.
	BaseDelay time.Duration // initial delay, e.g. 50ms
	MaxDelay  time.Duration // cap delay, e.g. 2s
	Jitter    bool          // add +/- 20% jitter
}

func (r SimpleRetry) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	attempts := r.Attempts
	if attempts <= 0 {
		attempts = 1
	}

	// Fast-path: no sleep/timers.
	// Useful for "immediate retries" (and makes benches sane on Windows).
	// Triggered when BOTH BaseDelay and MaxDelay are <= 0 (explicit opt-in).
	if r.BaseDelay <= 0 && r.MaxDelay <= 0 {
		var last error
		for i := 0; i < attempts; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			last = fn(ctx)
			if last == nil {
				return nil
			}
		}
		return last
	}

	base := r.BaseDelay
	if base <= 0 {
		base = 50 * time.Millisecond
	}
	max := r.MaxDelay
	if max <= 0 {
		max = 2 * time.Second
	}
	if max < base {
		max = base
	}

	var last error
	delay := base

	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := fn(ctx); err == nil {
			return nil
		} else {
			last = err
		}

		// no sleep after last attempt
		if i == attempts-1 {
			break
		}

		d := delay
		if r.Jitter {
			// +/- 20%
			j := 0.8 + rand.Float64()*0.4
			d = time.Duration(float64(d) * j)
		}
		if d > max {
			d = max
		}

		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		// exponential backoff
		delay *= 2
		if delay > max {
			delay = max
		}
	}

	return last
}
