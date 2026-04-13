package ingestor

import (
	"context"
	"math/rand/v2"
	"time"
)

// RetryPolicy wraps an operation with conditional retries. Implementations
// must honour context cancellation and deadlines: when ctx is cancelled, Do
// must return ctx.Err() promptly without starting another attempt.
//
// RetryPolicy is used for both sink writes (SetRetryPolicy) and source
// acknowledgements (SetAckRetryPolicy).
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

// SimpleRetry retries fn up to Attempts times using exponential backoff.
//
// It retries on any non-nil error. To retry only on specific errors, wrap fn
// and return nil for errors you want to swallow.
//
// Delay behaviour:
//   - First failure sleeps BaseDelay (default 50 ms when only MaxDelay is set).
//   - Each subsequent delay doubles, capped at MaxDelay (default 2 s).
//   - When Jitter is true, each sleep is scaled by a random factor in [0.8, 1.2]
//     to spread retries across concurrent callers.
//   - Setting both BaseDelay and MaxDelay to 0 retries immediately with no sleep.
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
