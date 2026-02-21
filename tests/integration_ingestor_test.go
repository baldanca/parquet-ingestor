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
	ch    chan source.Message
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
			return nil, context.Canceled
		}
		return m, nil
	}
}

func (s *memSource) AckBatch(ctx context.Context, msgs []source.Message) error {
	s.acked.Add(int64(len(msgs)))
	return nil
}

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
	err := req.Writer.Write(io.MultiWriter(io.Discard, countingWriter{n: &n}))
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.writes++
	s.bytes += n
	s.mu.Unlock()
	return nil
}

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

	for i := 0; i < total; i++ {
		it := testItem{ID: int64(i), Name: "x", Value: float64(i)}
		b, _ := json.Marshal(it)
		src.ch <- &memMsg{
			env:  source.Envelope{Payload: b},
			size: int64(len(b)),
			meta: source.AckMetadata{ID: "id", Handle: "rh"},
		}
	}
	src.ch <- nil

	done := make(chan error, 1)
	go func() {
		done <- ing.Run(ctx, 2, 4)
	}()

	waitForAcks(t, src, int64(total), 3*time.Second)

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
	src.ch <- nil

	done := make(chan error, 1)
	go func() { done <- ing.Run(ctx, 2, 4) }()

	waitForAcks(t, src, int64(total), 3*time.Second)

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

	var payload bytes.Buffer
	_ = enc.EncodeTo(context.Background(), []testItem{{ID: 1, Name: "z", Value: 1}}, &payload)
	if b := payload.Bytes(); len(b) < 4 || string(b[:4]) != "PAR1" {
		t.Fatalf("expected parquet magic header PAR1, got %q", b[:4])
	}
}

type failingMemSink struct {
	failLeft atomic.Int32
	writes   atomic.Int32
}

func (s *failingMemSink) Write(ctx context.Context, req sink.WriteRequest) error {
	s.writes.Add(1)
	if s.failLeft.Load() > 0 {
		s.failLeft.Add(-1)
		return errors.New("temporary sink error")
	}
	return nil
}

// Compatível com o estado atual do repo:
// - não depende de transformer
// - não depende de retry API inexistente
// - ainda valida um contrato importante: "ack só depois de write OK"
func TestIntegration_Ingestor_DoesNotAckIfSinkFails(t *testing.T) {
	src := newMemSource(1024)

	// Faz o sink falhar durante todo o teste
	sk := &failingMemSink{}
	sk.failLeft.Store(1_000_000)

	enc := encoder.NewParquetEncoder[testItem](encoder.ParquetCompressionSnappy)

	bcfg := batcher.BatcherConfig{
		MaxEstimatedInputBytes: 1024,
		FlushInterval:          50 * time.Millisecond,
		MaxItems:               10_000,
		ReuseBuffers:           true,
	}

	keyFunc := func(ctx context.Context, b batcher.Batch[testItem]) (string, error) { return "k", nil }

	ig, err := ingestor.NewIngestor[testItem](bcfg, src, jsonTransformer{}, enc, sk, keyFunc)
	if err != nil {
		t.Fatalf("NewIngestor: %v", err)
	}

	// Push algumas mensagens e sinaliza fim
	for i := 0; i < 10; i++ {
		payload, _ := json.Marshal(testItem{ID: int64(i), Name: "n", Value: 1.0})
		src.ch <- &memMsg{
			env:  source.Envelope{Payload: payload},
			size: int64(len(payload)),
			meta: source.AckMetadata{Handle: "rh"},
		}
	}
	src.ch <- nil

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	_ = ig.Run(ctx, 1, 1)

	// Se o contrato é ack somente após write OK, então com sink falhando:
	if got := src.acked.Load(); got != 0 {
		t.Fatalf("expected 0 acked when sink keeps failing, got %d", got)
	}

	// E o sink precisa ter sido tentado pelo menos 1 vez.
	if got := sk.writes.Load(); got == 0 {
		t.Fatalf("expected sink to be attempted at least once, got %d", got)
	}
}
