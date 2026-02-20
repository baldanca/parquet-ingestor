package ingestor

import (
	"context"
	"math/rand"
	"time"
)

// RetryPolicy wraps an operation with retries.
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

// SimpleRetry retries an operation using exponential backoff.
//
// It retries on any error returned by fn. If you need conditional retries,
// wrap fn and decide which errors to return.
type SimpleRetry struct {
	Attempts  int
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    bool
}

func (r SimpleRetry) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	attempts := r.Attempts
	if attempts <= 0 {
		attempts = 1
	}

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

		if i == attempts-1 {
			break
		}

		d := delay
		if r.Jitter {
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

		delay *= 2
		if delay > max {
			delay = max
		}
	}

	return last
}
