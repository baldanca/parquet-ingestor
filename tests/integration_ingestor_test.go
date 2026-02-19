package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/ingestor"
	"github.com/baldanca/parquet-ingestor/sink"
	"github.com/baldanca/parquet-ingestor/source"
)

// Integration test types (mirrors encoder unit test struct tags).
type testItem struct {
	ID    int64   `parquet:"name=id"`
	Name  string  `parquet:"name=name"`
	Value float64 `parquet:"name=value"`
}

type memMsg struct {
	env  source.Envelope
	size int64
	meta source.AckMetadata

	failed atomic.Int32
}

func (m *memMsg) Data() source.Envelope                        { return m.env }
func (m *memMsg) EstimatedSizeBytes() (int64, bool)            { return m.size, true }
func (m *memMsg) Fail(ctx context.Context, reason error) error { m.failed.Store(1); return nil }
func (m *memMsg) AckMeta() (source.AckMetadata, bool)          { return m.meta, true } // fast-path for AckGroup

type memSource struct {
	ch chan source.Message
	// counts both AckBatch and AckBatchMeta
	acked atomic.Int64
}

func newMemSource(buf int) *memSource {
	return &memSource{ch: make(chan source.Message, buf)}
}

func (s *memSource) Receive(ctx context.Context) (source.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m := <-s.ch:
		if m == nil {
			// Nil sentinel => behave like a graceful stop.
			return nil, context.Canceled
		}
		return m, nil
	}
}

func (s *memSource) AckBatch(ctx context.Context, msgs []source.Message) error {
	s.acked.Add(int64(len(msgs)))
	return nil
}

// AckBatchMeta is the fast-path used by source.AckGroup when available.
func (s *memSource) AckBatchMeta(ctx context.Context, metas []source.AckMetadata) error {
	s.acked.Add(int64(len(metas)))
	return nil
}

type memSink struct {
	mu     sync.Mutex
	writes int
	bytes  int64
}

func (s *memSink) Write(ctx context.Context, req sink.WriteRequest) error {
	s.mu.Lock()
	s.writes++
	s.bytes += int64(len(req.Data))
	s.mu.Unlock()
	return nil
}

type countingWriter struct {
	n *int64
}

func (w countingWriter) Write(p []byte) (int, error) {
	atomic.AddInt64(w.n, int64(len(p)))
	return len(p), nil
}

func (s *memSink) WriteStream(ctx context.Context, req sink.StreamWriteRequest) error {
	var n int64
	err := req.Writer.WriteTo(io.MultiWriter(io.Discard, countingWriter{n: &n}))
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.writes++
	s.bytes += n
	s.mu.Unlock()
	return nil
}

// noStreamSink forces fallback path by *not* implementing WriteStream.
type noStreamSink struct {
	mu     sync.Mutex
	writes int
	bytes  int64
}

func (s *noStreamSink) Write(ctx context.Context, req sink.WriteRequest) error {
	s.mu.Lock()
	s.writes++
	s.bytes += int64(len(req.Data))
	s.mu.Unlock()
	return nil
}

type jsonTransformer struct{}

func (t jsonTransformer) Transform(ctx context.Context, env source.Envelope) (testItem, error) {
	var it testItem
	if b, ok := env.Payload.([]byte); ok {
		return it, json.Unmarshal(b, &it)
	}
	switch v := env.Payload.(type) {
	case string:
		return it, json.Unmarshal([]byte(v), &it)
	default:
		return it, errors.New("unsupported payload type")
	}
}

func waitForAcks(t *testing.T, src *memSource, want int64, timeout time.Duration) {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		if src.acked.Load() >= want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timeout waiting for acks: got=%d want=%d", src.acked.Load(), want)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestIntegration_Ingestor_Streaming_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := newMemSource(2048)
	snk := &memSink{}

	enc := encoder.ParquetEncoder[testItem]{Compression: "snappy"}

	// Deterministic: flush by MaxItems.
	const total = 500
	cfg := batcher.BatcherConfig{
		MaxEstimatedInputBytes: 256 * 1024,
		MaxItems:               total,            // flush exactly once when all arrive
		FlushInterval:          10 * time.Second, // irrelevant due to MaxItems
		ReuseBuffers:           true,
	}

	ing, err := ingestor.NewIngestor[testItem](
		cfg,
		src,
		jsonTransformer{},
		enc,
		snk,
		func(ctx context.Context, b batcher.Batch[testItem]) (string, error) { return "test/key.parquet", nil },
	)
	if err != nil {
		t.Fatal(err)
	}

	// Produce messages.
	for i := 0; i < total; i++ {
		it := testItem{ID: int64(i), Name: "x", Value: float64(i)}
		b, _ := json.Marshal(it)
		src.ch <- &memMsg{
			env:  source.Envelope{Payload: b},
			size: int64(len(b)),
			meta: source.AckMetadata{ID: "id", Handle: "rh"},
		}
	}
	// ✅ sentinel to stop Receive() deterministically
	src.ch <- nil

	done := make(chan error, 1)
	go func() {
		// flushWorkers=2, queue=4
		done <- ing.Run(ctx, 2, 4)
	}()

	// Wait for all acks (means: flush happened + commit succeeded).
	waitForAcks(t, src, int64(total), 3*time.Second)

	// Stop the ingestor.
	cancel()

	runErr := <-done
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("expected nil or context.Canceled, got %v", runErr)
	}

	snk.mu.Lock()
	defer snk.mu.Unlock()
	if snk.writes == 0 || snk.bytes == 0 {
		t.Fatalf("expected writes and bytes > 0, got writes=%d bytes=%d", snk.writes, snk.bytes)
	}
}

func TestIntegration_Ingestor_Fallback_EncodeThenWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := newMemSource(2048)
	snk := &noStreamSink{} // forces fallback path

	enc := encoder.ParquetEncoder[testItem]{Compression: ""}

	// Force multiple fallback writes by MaxItems.
	const total = 250
	cfg := batcher.BatcherConfig{
		MaxEstimatedInputBytes: 32 * 1024,
		MaxItems:               100, // flushes at 100, 200; remaining 50 flushed on stop (if needed)
		FlushInterval:          10 * time.Second,
		ReuseBuffers:           true,
	}

	ing, err := ingestor.NewIngestor[testItem](
		cfg,
		src,
		jsonTransformer{},
		enc,
		snk,
		func(ctx context.Context, b batcher.Batch[testItem]) (string, error) { return "test/key.parquet", nil },
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < total; i++ {
		it := testItem{ID: int64(i), Name: "y", Value: float64(i)}
		b, _ := json.Marshal(it)
		src.ch <- &memMsg{
			env:  source.Envelope{Payload: b},
			size: int64(len(b)),
			meta: source.AckMetadata{ID: "id", Handle: "rh"},
		}
	}
	// ✅ sentinel to stop Receive() deterministically
	src.ch <- nil

	done := make(chan error, 1)
	go func() { done <- ing.Run(ctx, 2, 4) }()

	// ✅ Wait until ALL messages are acked (no timing assumptions).
	waitForAcks(t, src, int64(total), 3*time.Second)

	// Stop the ingestor.
	cancel()

	runErr := <-done
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("expected nil or context.Canceled, got %v", runErr)
	}

	if got := src.acked.Load(); got != int64(total) {
		t.Fatalf("expected %d acked, got %d", total, got)
	}

	snk.mu.Lock()
	defer snk.mu.Unlock()
	if snk.writes < 2 || snk.bytes == 0 {
		t.Fatalf("expected >=2 writes and bytes > 0, got writes=%d bytes=%d", snk.writes, snk.bytes)
	}

	// Sanity: parquet magic bytes "PAR1".
	var payload bytes.Buffer
	_ = enc.EncodeTo(context.Background(), []testItem{{ID: 1, Name: "z", Value: 1}}, &payload)
	if b := payload.Bytes(); len(b) < 4 || string(b[:4]) != "PAR1" {
		t.Fatalf("expected parquet magic header PAR1, got %q", b[:4])
	}
}
