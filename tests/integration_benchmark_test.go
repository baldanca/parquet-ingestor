package tests

import (
	"context"
	"encoding/json"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/ingestor"
	"github.com/baldanca/parquet-ingestor/sink"
	"github.com/baldanca/parquet-ingestor/source"
)

type benchSource struct {
	msgs []source.Message
	i    int
	done chan struct{}
}

func (s *benchSource) Receive(ctx context.Context) (source.Message, error) {
	// No blocking while we still have messages: tight loop for determinism.
	if s.i < len(s.msgs) {
		m := s.msgs[s.i]
		s.i++
		if s.i == len(s.msgs) && s.done != nil {
			select {
			case <-s.done:
			default:
				close(s.done)
			}
		}
		return m, nil
	}

	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *benchSource) AckBatch(ctx context.Context, msgs []source.Message) error { return nil }

// Optional fast path — your AckGroup uses it when available.
// Keep it to avoid building []Message path during Commit.
func (s *benchSource) AckBatchMeta(ctx context.Context, metas []source.AckMetadata) error { return nil }

type blackholeSink struct{}

func (s blackholeSink) Write(ctx context.Context, req sink.WriteRequest) error { return nil }
func (s blackholeSink) WriteStream(ctx context.Context, req sink.StreamWriteRequest) error {
	return req.Writer.WriteTo(io.Discard)
}

// NOTE: these types must exist in your tests package already.
// Keeping placeholders here for clarity:
//
// type testItem struct { ID int64; Name string; Value float64 }
// type memMsg struct { env source.Envelope; size int64; meta source.AckMetadata }
// func (m *memMsg) Data() source.Envelope { return m.env }
// func (m *memMsg) EstimatedSizeBytes() (int64, bool) { return m.size, true }
// func (m *memMsg) Fail(ctx context.Context, reason error) error { return nil }
// func (m *memMsg) AckMeta() (source.AckMetadata, bool) { return m.meta, true }
//
// type jsonTransformer struct{}
// func (jsonTransformer) Transform(ctx context.Context, env source.Envelope) (testItem, error) { ... }

func BenchmarkIntegration_Ingestor_Streaming(b *testing.B) {
	const batchItems = 1000

	enc := encoder.ParquetEncoder[testItem]{Compression: "snappy"}

	cfg := batcher.BatcherConfig{
		// keep it reasonably small so you see multiple flushes if you change MaxItems
		MaxEstimatedInputBytes: 256 * 1024,
		MaxItems:               batchItems, // force flush exactly at batchItems
		FlushInterval:          10 * time.Second,
		ReuseBuffers:           true,
	}

	keyFn := func(ctx context.Context, bt batcher.Batch[testItem]) (string, error) {
		return "bench/key.parquet", nil
	}

	// sanity: ensure benchmark doesn't get optimized away in weird ways
	var totalRuns atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Build messages per-iteration to avoid sharing mutable state across runs.
		msgs := make([]source.Message, 0, batchItems)
		for j := 0; j < batchItems; j++ {
			it := testItem{ID: int64(j), Name: "bench", Value: float64(j)}
			by, _ := json.Marshal(it)
			msgs = append(msgs, &memMsg{
				env:  source.Envelope{Payload: by},
				size: int64(len(by)),
				meta: source.AckMetadata{ID: "id", Handle: "rh"},
			})
		}

		ctx, cancel := context.WithCancel(context.Background())
		src := &benchSource{msgs: msgs, done: make(chan struct{})}

		ing, err := ingestor.NewIngestor[testItem](
			cfg,
			src,
			jsonTransformer{},
			enc,
			blackholeSink{},
			keyFn,
		)
		if err != nil {
			b.Fatal(err)
		}

		done := make(chan error, 1)
		go func() {
			// 2 flush workers, queue 4 (ajuste se quiser)
			done <- ing.Run(ctx, 2, 4)
		}()

		// Wait until last message is delivered (flush happens due to MaxItems), then cancel.
		<-src.done
		cancel()

		if err := <-done; err != nil && err != context.Canceled {
			// Run should typically end with context.Canceled after cancel().
			// Any other error is a failure.
			b.Fatalf("ingestor run error: %v", err)
		}

		totalRuns.Add(1)
	}

	_ = totalRuns.Load()
}
