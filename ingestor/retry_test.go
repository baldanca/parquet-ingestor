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
