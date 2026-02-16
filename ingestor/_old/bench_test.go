package ingestor

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type benchEvent struct {
	EventID   string `json:"event_id" parquet:"name=event_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	UserID    string `json:"user_id" parquet:"name=user_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Type      string `json:"type" parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8"`
	CreatedAt int64  `json:"created_at" parquet:"name=created_at, type=INT64"`
}

type benchFakeSource struct{}

func (benchFakeSource) Receive(ctx context.Context) (Message, error)       { return nil, ctx.Err() }
func (benchFakeSource) AckBatch(ctx context.Context, msgs []Message) error { return nil }

type benchFakeMsg struct{ payload []byte }

func (m benchFakeMsg) Data() Envelope                               { return Envelope{Payload: m.payload} }
func (m benchFakeMsg) EstimatedSizeBytes() (int64, bool)            { return int64(len(m.payload)), true }
func (m benchFakeMsg) Ack(ctx context.Context) error                { return nil }
func (m benchFakeMsg) Fail(ctx context.Context, reason error) error { return nil }

type benchTransformer struct{}

func (benchTransformer) Transform(ctx context.Context, in Envelope) (benchEvent, error) {
	_ = ctx
	var e benchEvent
	_ = json.Unmarshal(in.Payload.([]byte), &e)
	return e, nil
}

type benchFakeSink struct{}

func (benchFakeSink) Write(ctx context.Context, req WriteRequest) error { return nil }

func BenchmarkParquetEncode_Snappy_5k(b *testing.B) {
	items := make([]benchEvent, 5000)
	for i := range items {
		items[i] = benchEvent{EventID: "e", UserID: "u", Type: "t", CreatedAt: int64(i)}
	}
	enc := ParquetEncoder[benchEvent]{Compression: "snappy"}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := enc.Encode(ctx, items)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark the full flush path (encode + sink + ack) using in-memory fakes.
func BenchmarkIngestorFlush_ParquetSnappy(b *testing.B) {
	enc := ParquetEncoder[benchEvent]{Compression: "snappy"}
	cfg := Config{FlushInterval: 5 * time.Second, MaxItems: 5000, MaxEstimatedInputBytes: 12 << 20, ReuseBuffers: true}
	ing, err := NewIngestor[benchEvent](cfg, benchFakeSource{}, benchTransformer{}, enc, benchFakeSink{}, DefaultKeyFunc(enc))
	if err != nil {
		b.Fatal(err)
	}

	// Prepare a batch worth of messages.
	sample := []byte(`{"event_id":"e","user_id":"u","type":"t","created_at":1}`)
	msgs := make([]Message, 5000)
	for i := range msgs {
		msgs[i] = benchFakeMsg{payload: sample}
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Fill batcher
		ing.batcher = mustNewBatcher[benchEvent](cfg)
		for _, m := range msgs {
			_, _ = ing.processMessage(ctx, m)
		}
		if err := ing.flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func mustNewBatcher[T any](cfg Config) *Batcher[T] {
	b, err := NewBatcher[T](cfg)
	if err != nil {
		panic(err)
	}
	return b
}
