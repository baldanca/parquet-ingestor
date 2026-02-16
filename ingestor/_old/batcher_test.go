package ingestor

import (
	"testing"
	"time"
)

func TestBatcher_FlushByMaxItems(t *testing.T) {
	cfg := DefaultConfig
	cfg.MaxEstimatedInputBytes = 1 << 60
	cfg.MaxItems = 3
	cfg.FlushInterval = time.Second

	b, err := NewBatcher[int](cfg)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(0, 0)
	flush := b.Add(now, 1, &fakeMessage{}, 1)
	if flush {
		t.Fatalf("unexpected flush")
	}
	flush = b.Add(now, 2, &fakeMessage{}, 1)
	if flush {
		t.Fatalf("unexpected flush")
	}
	flush = b.Add(now, 3, &fakeMessage{}, 1)
	if !flush {
		t.Fatalf("expected flush")
	}
}

func TestBatcher_FlushByEstimatedBytes(t *testing.T) {
	cfg := DefaultConfig
	cfg.MaxEstimatedInputBytes = 10
	cfg.MaxItems = 0
	cfg.FlushInterval = time.Second

	b, err := NewBatcher[int](cfg)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(0, 0)
	if b.Add(now, 1, &fakeMessage{}, 6) {
		t.Fatalf("unexpected flush")
	}
	if !b.Add(now, 2, &fakeMessage{}, 4) {
		t.Fatalf("expected flush")
	}
}

func TestBatcher_Deadline(t *testing.T) {
	cfg := DefaultConfig
	cfg.MaxEstimatedInputBytes = 1 << 60
	cfg.FlushInterval = 2 * time.Second

	b, err := NewBatcher[int](cfg)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(100, 0)
	_ = b.Add(now, 1, &fakeMessage{}, 1)
	dl, ok := b.Deadline()
	if !ok {
		t.Fatalf("expected deadline")
	}
	if !dl.Equal(now.Add(2 * time.Second)) {
		t.Fatalf("unexpected deadline: %v", dl)
	}
}
