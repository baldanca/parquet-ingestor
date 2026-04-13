package ingestor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNopRetry_CallsOnce(t *testing.T) {
	var calls int32
	r := nopRetry{}
	err := r.Do(context.Background(), func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d want=1", calls)
	}
}

func TestSimpleRetry_SucceedsFirstTry(t *testing.T) {
	var calls int32
	r := SimpleRetry{Attempts: 5, BaseDelay: time.Nanosecond, MaxDelay: time.Nanosecond, Jitter: false}

	err := r.Do(context.Background(), func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d want=1", calls)
	}
}

func TestSimpleRetry_RetriesUntilSuccess(t *testing.T) {
	var calls int32
	wantCalls := int32(3)

	r := SimpleRetry{Attempts: 10, BaseDelay: time.Nanosecond, MaxDelay: time.Nanosecond, Jitter: false}

	err := r.Do(context.Background(), func(ctx context.Context) error {
		c := atomic.AddInt32(&calls, 1)
		if c < wantCalls {
			return errors.New("fail")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != wantCalls {
		t.Fatalf("calls=%d want=%d", calls, wantCalls)
	}
}

func TestSimpleRetry_ReturnsLastError(t *testing.T) {
	var calls int32
	r := SimpleRetry{Attempts: 4, BaseDelay: time.Nanosecond, MaxDelay: time.Nanosecond, Jitter: false}

	sentinel := errors.New("boom")
	err := r.Do(context.Background(), func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	if calls != 4 {
		t.Fatalf("calls=%d want=4", calls)
	}
}

func TestSimpleRetry_RespectsContextCancel(t *testing.T) {
	var calls int32
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := SimpleRetry{Attempts: 10, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, Jitter: false}

	err := r.Do(ctx, func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("calls=%d want=0", calls)
	}
}

// TestNopRetry_CancelledContext verifies that nopRetry returns ctx.Err()
// immediately without invoking fn when the context is already cancelled.
func TestNopRetry_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var called bool
	r := nopRetry{}
	err := r.Do(ctx, func(ctx context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if called {
		t.Fatal("fn must not be called when context is already cancelled")
	}
}

// TestSimpleRetry_NoDelay_ContextCancelDuringLoop verifies that context
// cancellation inside the no-delay loop (BaseDelay=0, MaxDelay=0) is detected
// between attempts.
func TestSimpleRetry_NoDelay_ContextCancelDuringLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var calls int32
	r := SimpleRetry{Attempts: 100, BaseDelay: 0, MaxDelay: 0}

	err := r.Do(ctx, func(ctx context.Context) error {
		c := atomic.AddInt32(&calls, 1)
		if c == 2 {
			cancel()
		}
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// Cancelled after attempt 2, so exactly 2 or 3 calls are valid depending on
	// goroutine scheduling; at minimum 2, at most 3.
	if calls < 2 || calls > 3 {
		t.Fatalf("calls=%d, want 2 or 3", calls)
	}
}

// TestSimpleRetry_BaseDelayZero_MaxDelaySet verifies that when BaseDelay=0 but
// MaxDelay>0 the implementation uses a default 50 ms base (the delay path is
// taken). We only need one attempt + one failure to exercise that branch; the
// test uses nanosecond delays to stay fast.
func TestSimpleRetry_BaseDelayZero_MaxDelaySet(t *testing.T) {
	var calls int32
	r := SimpleRetry{Attempts: 2, BaseDelay: 0, MaxDelay: time.Nanosecond}

	err := r.Do(context.Background(), func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected error after all attempts")
	}
	if calls != 2 {
		t.Fatalf("calls=%d, want 2", calls)
	}
}

// TestSimpleRetry_MaxDelayLessThanBase verifies that when MaxDelay < BaseDelay
// the implementation sets max = base so the cap logic does not panic.
func TestSimpleRetry_MaxDelayLessThanBase(t *testing.T) {
	var calls int32
	r := SimpleRetry{Attempts: 2, BaseDelay: time.Millisecond, MaxDelay: time.Nanosecond}

	err := r.Do(context.Background(), func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected error after all attempts")
	}
	if calls != 2 {
		t.Fatalf("calls=%d, want 2", calls)
	}
}

// TestSimpleRetry_Jitter_DoesNotPanic exercises the Jitter=true code path.
// We use nanosecond delays so the test remains fast.
func TestSimpleRetry_Jitter_DoesNotPanic(t *testing.T) {
	var calls int32
	r := SimpleRetry{Attempts: 3, BaseDelay: time.Nanosecond, MaxDelay: time.Nanosecond, Jitter: true}

	err := r.Do(context.Background(), func(ctx context.Context) error {
		c := atomic.AddInt32(&calls, 1)
		if c == 2 {
			return nil
		}
		return errors.New("fail")
	})
	if err != nil {
		t.Fatalf("expected nil error (success on attempt 2), got %v", err)
	}
}

// TestSimpleRetry_ContextCancelDuringSleep verifies that cancelling the context
// while sleeping between retries returns context.Canceled promptly.
func TestSimpleRetry_ContextCancelDuringSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Use a delay long enough that the goroutine will definitely be inside the
	// timer select when we cancel.
	r := SimpleRetry{Attempts: 5, BaseDelay: 200 * time.Millisecond, MaxDelay: time.Second}

	var calls int32
	done := make(chan error, 1)
	go func() {
		done <- r.Do(ctx, func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return errors.New("fail") // always fail so we sleep between attempts
		})
	}()

	// Give the goroutine time to make the first call and enter the sleep.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Do to return after context cancel")
	}
}

func BenchmarkNopRetry(b *testing.B) {
	r := nopRetry{}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = r.Do(ctx, func(ctx context.Context) error { return nil })
	}
}

func BenchmarkSimpleRetry_SuccessFirstTry(b *testing.B) {
	r := SimpleRetry{Attempts: 5, BaseDelay: time.Nanosecond, MaxDelay: time.Nanosecond, Jitter: false}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = r.Do(ctx, func(ctx context.Context) error { return nil })
	}
}

func BenchmarkSimpleRetry_FailAllAttempts(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in -short; retry benchmarks may sleep depending on configuration")
	}

	r := SimpleRetry{Attempts: 3, BaseDelay: time.Nanosecond, MaxDelay: time.Nanosecond, Jitter: false}
	ctx := context.Background()
	errFail := errors.New("fail")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = r.Do(ctx, func(ctx context.Context) error { return errFail })
	}
}

func BenchmarkSimpleRetry_FailAllAttempts_NoSleep(b *testing.B) {
	r := SimpleRetry{Attempts: 10, BaseDelay: 0, MaxDelay: 0, Jitter: false}
	ctx := context.Background()
	errFail := errors.New("fail")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = r.Do(ctx, func(ctx context.Context) error { return errFail })
	}
}
