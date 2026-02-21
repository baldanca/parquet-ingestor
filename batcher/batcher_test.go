package batcher

import (
	"testing"
	"time"

	"github.com/baldanca/parquet-ingestor/source"
)

func testMsg() source.Message {
	var m source.Message
	return m
}

func TestBatcherConfig_validate(t *testing.T) {
	ok := DefaultBatcherConfig
	if err := ok.validate(); err != nil {
		t.Fatalf("expected default config to be valid: %v", err)
	}

	c := ok
	c.MaxEstimatedInputBytes = 0
	if err := c.validate(); err == nil {
		t.Fatalf("expected error when MaxEstimatedInputBytes <= 0")
	}

	c = ok
	c.FlushInterval = 0
	if err := c.validate(); err == nil {
		t.Fatalf("expected error when FlushInterval <= 0")
	}

	c = ok
	c.MaxItems = -1
	if err := c.validate(); err == nil {
		t.Fatalf("expected error when MaxItems < 0")
	}
}

func TestNewBatcher_ReusesBuffersWhenEnabled(t *testing.T) {
	cfg := DefaultBatcherConfig
	cfg.ReuseBuffers = true

	b, err := NewBatcher[int](cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}

	if b.items == nil || b.spareItems == nil {
		t.Fatalf("expected items/spareItems allocated when ReuseBuffers=true")
	}
}

func TestNewBatcher_DoesNotAllocateWhenReuseDisabled(t *testing.T) {
	cfg := DefaultBatcherConfig
	cfg.ReuseBuffers = false

	b, err := NewBatcher[int](cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	if b.items != nil {
		t.Fatalf("expected items nil when ReuseBuffers=false")
	}
}

func TestBatcher_Add_ActivatesAndSetsDeadline(t *testing.T) {
	cfg := DefaultBatcherConfig
	cfg.FlushInterval = 2 * time.Second
	cfg.ReuseBuffers = true

	b, err := NewBatcher[int](cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}

	now := time.Unix(100, 0)
	if b.active {
		t.Fatalf("expected inactive initially")
	}

	flush := b.Add(now, 1, testMsg(), 10)
	if flush {
		t.Fatalf("did not expect flush for small add")
	}
	if !b.active {
		t.Fatalf("expected active after first Add")
	}

	dl, ok := b.Deadline()
	if !ok {
		t.Fatalf("expected deadline ok")
	}
	want := now.Add(cfg.FlushInterval)
	if !dl.Equal(want) {
		t.Fatalf("deadline=%v want=%v", dl, want)
	}
}

func TestBatcher_Add_SizeNegativeIsClamped(t *testing.T) {
	cfg := DefaultBatcherConfig
	cfg.MaxEstimatedInputBytes = 1
	cfg.ReuseBuffers = true

	b, _ := NewBatcher[int](cfg)
	now := time.Unix(0, 0)

	flush := b.Add(now, 1, testMsg(), -999)
	if flush {
		t.Fatalf("should not flush; bytes should clamp to 0")
	}
	if b.bytes != 0 {
		t.Fatalf("bytes=%d want 0", b.bytes)
	}
}

func TestBatcher_Add_FlushByBytes(t *testing.T) {
	cfg := DefaultBatcherConfig
	cfg.MaxEstimatedInputBytes = 100
	cfg.MaxItems = 0
	cfg.ReuseBuffers = true

	b, _ := NewBatcher[int](cfg)
	now := time.Unix(0, 0)

	if b.Add(now, 1, testMsg(), 60) {
		t.Fatalf("should not flush yet")
	}
	if !b.Add(now, 2, testMsg(), 40) {
		t.Fatalf("expected flush by bytes")
	}
}

func TestBatcher_Add_FlushByMaxItems(t *testing.T) {
	cfg := DefaultBatcherConfig
	cfg.MaxEstimatedInputBytes = 1 << 30
	cfg.MaxItems = 3
	cfg.ReuseBuffers = true

	b, _ := NewBatcher[int](cfg)
	now := time.Unix(0, 0)

	if b.Add(now, 1, testMsg(), 1) {
		t.Fatalf("should not flush at 1")
	}
	if b.Add(now, 2, testMsg(), 1) {
		t.Fatalf("should not flush at 2")
	}
	if !b.Add(now, 3, testMsg(), 1) {
		t.Fatalf("expected flush at 3 (MaxItems)")
	}
}

func TestBatcher_ShouldFlushTime(t *testing.T) {
	cfg := DefaultBatcherConfig
	cfg.FlushInterval = 1 * time.Second
	cfg.ReuseBuffers = true

	b, _ := NewBatcher[int](cfg)

	now := time.Unix(100, 0)
	_ = b.Add(now, 1, testMsg(), 1)

	if b.ShouldFlushTime(now) {
		t.Fatalf("should not flush at start time")
	}
	if !b.ShouldFlushTime(now.Add(1 * time.Second)) {
		t.Fatalf("expected flush at deadline")
	}
	if !b.ShouldFlushTime(now.Add(2 * time.Second)) {
		t.Fatalf("expected flush after deadline")
	}
}

func TestBatcher_Flush_ResetsStateAndReturnsBatch(t *testing.T) {
	cfg := DefaultBatcherConfig
	cfg.FlushInterval = 10 * time.Second
	cfg.ReuseBuffers = true

	b, _ := NewBatcher[int](cfg)
	now := time.Unix(0, 0)

	_ = b.Add(now, 10, testMsg(), 7)
	_ = b.Add(now, 20, testMsg(), 3)

	out := b.Flush()

	if len(out.Items) != 2 || out.Items[0] != 10 || out.Items[1] != 20 {
		t.Fatalf("unexpected items: %+v", out.Items)
	}
	if out.Bytes != 10 {
		t.Fatalf("bytes=%d want 10", out.Bytes)
	}

	if b.active {
		t.Fatalf("expected inactive after flush")
	}
	if b.bytes != 0 {
		t.Fatalf("expected bytes reset to 0, got %d", b.bytes)
	}
	if b.deadline != (time.Time{}) {
		t.Fatalf("expected deadline reset")
	}
}

func TestBatcher_Flush_ReusesBuffers(t *testing.T) {
	cfg := DefaultBatcherConfig
	cfg.ReuseBuffers = true
	cfg.MaxEstimatedInputBytes = 5 * 1024 * 1024

	b, _ := NewBatcher[int](cfg)
	now := time.Unix(0, 0)

	for i := 0; i < 1000; i++ {
		_ = b.Add(now, i, testMsg(), 100)
	}

	origItemsPtr := &b.items[:cap(b.items)][0] // safe because cap>0 with reuse enabled

	out := b.Flush()

	_ = b.Add(now, 1, testMsg(), 1)
	if cap(b.items) == 0 {
		t.Fatalf("expected reused capacity after flush")
	}

	if len(out.Items) == 0 {
		t.Fatalf("expected out.Items not empty")
	}

	_ = origItemsPtr
}

func benchMsg() source.Message {
	var m source.Message
	return m
}

func BenchmarkBatcher_Add_ReuseBuffers(b *testing.B) {
	cfg := DefaultBatcherConfig
	cfg.ReuseBuffers = true
	cfg.MaxEstimatedInputBytes = 1 << 60 // never flush by bytes
	cfg.MaxItems = 0

	bt, err := NewBatcher[int](cfg)
	if err != nil {
		b.Fatalf("NewBatcher: %v", err)
	}

	now := time.Unix(0, 0)
	msg := benchMsg()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bt.Add(now, i, msg, 64)
		if len(bt.items) >= 10_000 {
			_ = bt.Flush()
		}
	}
}

func BenchmarkBatcher_Add_NoReuse(b *testing.B) {
	cfg := DefaultBatcherConfig
	cfg.ReuseBuffers = false
	cfg.MaxEstimatedInputBytes = 1 << 60 // never flush by bytes
	cfg.MaxItems = 0

	bt, err := NewBatcher[int](cfg)
	if err != nil {
		b.Fatalf("NewBatcher: %v", err)
	}

	now := time.Unix(0, 0)
	msg := benchMsg()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bt.Add(now, i, msg, 64)
		if len(bt.items) >= 10_000 {
			_ = bt.Flush()
		}
	}
}

func BenchmarkBatcher_Flush_ReuseBuffers(b *testing.B) {
	cfg := DefaultBatcherConfig
	cfg.ReuseBuffers = true
	cfg.MaxEstimatedInputBytes = 1 << 60
	cfg.MaxItems = 0

	bt, err := NewBatcher[int](cfg)
	if err != nil {
		b.Fatalf("NewBatcher: %v", err)
	}

	now := time.Unix(0, 0)
	msg := benchMsg()

	for i := 0; i < 10_000; i++ {
		_ = bt.Add(now, i, msg, 64)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bt.Flush()
		for j := 0; j < 10_000; j++ {
			_ = bt.Add(now, j, msg, 64)
		}
	}
}

func BenchmarkBatcher_ShouldFlushTime(b *testing.B) {
	cfg := DefaultBatcherConfig
	cfg.ReuseBuffers = true
	cfg.FlushInterval = 5 * time.Second

	bt, _ := NewBatcher[int](cfg)
	start := time.Unix(100, 0)
	_ = bt.Add(start, 1, benchMsg(), 1)

	b.ReportAllocs()
	b.ResetTimer()

	tm := start
	for i := 0; i < b.N; i++ {
		_ = bt.ShouldFlushTime(tm)
		tm = tm.Add(1 * time.Millisecond)
	}
}
