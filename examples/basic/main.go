package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/ingestor"
	"github.com/baldanca/parquet-ingestor/sink"
	"github.com/baldanca/parquet-ingestor/source"
)

type item struct {
	ID    int64   `parquet:"name=id"`
	Name  string  `parquet:"name=name"`
	Value float64 `parquet:"name=value"`
}

type memMsg struct {
	env  source.Envelope
	size int64
}

func (m *memMsg) Data() source.Envelope                        { return m.env }
func (m *memMsg) EstimatedSizeBytes() (int64, bool)            { return m.size, true }
func (m *memMsg) Fail(ctx context.Context, reason error) error { return nil }

type memSource struct {
	ch chan source.Message
}

func newMemSource(buf int) *memSource { return &memSource{ch: make(chan source.Message, buf)} }

func (s *memSource) Receive(ctx context.Context) (source.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m := <-s.ch:
		return m, nil
	}
}

func (s *memSource) AckBatch(ctx context.Context, msgs []source.Message) error { return nil }

type jsonTransformer struct{}

func (jsonTransformer) Transform(ctx context.Context, env source.Envelope) (item, error) {
	var out item
	b, ok := env.Payload.([]byte)
	if !ok {
		return out, fmt.Errorf("payload is not []byte")
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return out, err
	}
	return out, nil
}

type memSink struct {
	last sink.WriteRequest
}

func (s *memSink) Write(ctx context.Context, req sink.WriteRequest) error {
	s.last = req
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	src := newMemSource(16)
	sk := &memSink{}

	enc := encoder.NewParquetEncoder[item](encoder.ParquetCompressionSnappy)

	// NEW API: BatcherConfig fields were renamed.
	bcfg := batcher.BatcherConfig{
		MaxEstimatedInputBytes: 5 * 1024 * 1024,
		FlushInterval:          5 * time.Minute,
		MaxItems:               100_000,
		ReuseBuffers:           true,
	}

	keyFunc := func(ctx context.Context, b batcher.Batch[item]) (string, error) {
		return "example/items.parquet", nil
	}

	ig, err := ingestor.NewIngestor[item](bcfg, src, jsonTransformer{}, enc, sk, keyFunc)
	if err != nil {
		panic(err)
	}

	go func() {
		for i := 0; i < 100; i++ {
			payload, _ := json.Marshal(item{ID: int64(i), Name: "x", Value: 42.0})
			src.ch <- &memMsg{env: source.Envelope{Payload: payload}, size: int64(len(payload))}
		}
		// Let the batcher flush by time.
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()

	if err := ig.Run(ctx, 1, 1); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		panic(err)
	}

	fmt.Printf("Wrote %d bytes to %s (%s)\n", len(sk.last.Data), sk.last.Key, sk.last.ContentType)
	fmt.Printf("First 16 bytes: %x\n", bytes.TrimSpace(sk.last.Data)[:16])
}
