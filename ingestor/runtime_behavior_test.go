package ingestor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/observability"
	"github.com/baldanca/parquet-ingestor/sink"
	"github.com/baldanca/parquet-ingestor/source"
)

type runtimeTestMessage struct{ payload any }

func (m runtimeTestMessage) Data() source.Envelope             { return source.Envelope{Payload: m.payload} }
func (m runtimeTestMessage) EstimatedSizeBytes() (int64, bool) { return 1, true }
func (m runtimeTestMessage) Fail(context.Context, error) error { return nil }

type runtimeSource struct {
	mu       sync.Mutex
	msgs     []source.Message
	received int
}

func (s *runtimeSource) Receive(ctx context.Context) (source.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.received < len(s.msgs) {
		m := s.msgs[s.received]
		s.received++
		return m, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (s *runtimeSource) AckBatch(context.Context, []source.Message) error { return nil }

type passthroughTransformer struct{}

func (passthroughTransformer) Transform(_ context.Context, in source.Envelope) ([]map[string]any, error) {
	return []map[string]any{{"value": in.Payload}}, nil
}

type bytesEncoder struct{}

func (bytesEncoder) Encode(_ context.Context, items []map[string]any) ([]byte, error) {
	return []byte("ok"), nil
}
func (bytesEncoder) ContentType() string   { return "application/octet-stream" }
func (bytesEncoder) FileExtension() string { return ".bin" }

type flakySink struct {
	mu    sync.Mutex
	calls int
}

func (s *flakySink) Write(_ context.Context, req sink.WriteRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 {
		return errors.New("boom")
	}
	return nil
}

func (s *flakySink) ResolvePath(key string) string { return "s3://bucket/" + key }

type captureLogger struct {
	mu     sync.Mutex
	events []string
}

func (l *captureLogger) Debug(msg string, _ ...any) { l.add(msg) }
func (l *captureLogger) Info(msg string, _ ...any)  { l.add(msg) }
func (l *captureLogger) Warn(msg string, _ ...any)  { l.add(msg) }
func (l *captureLogger) Error(msg string, _ ...any) { l.add(msg) }
func (l *captureLogger) add(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, msg)
}
func (l *captureLogger) contains(msg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, event := range l.events {
		if event == msg {
			return true
		}
	}
	return false
}

func TestRunContinuesAfterSinkError(t *testing.T) {
	src := &runtimeSource{msgs: []source.Message{
		runtimeTestMessage{payload: "first"},
		runtimeTestMessage{payload: "second"},
	}}
	sk := &flakySink{}
	enc := bytesEncoder{}
	logger := &captureLogger{}
	metrics := &observability.Registry{}

	ing, err := NewIngestor(
		batcher.BatcherConfig{MaxItems: 1, FlushInterval: time.Hour},
		src,
		passthroughTransformer{},
		enc,
		sk,
		DefaultKeyFunc(enc),
	)
	if err != nil {
		t.Fatalf("NewIngestor() error = %v", err)
	}
	ing.SetLogger(logger)
	ing.SetMetricsRegistry(metrics)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	if err := ing.Run(ctx, 1, 1); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	sk.mu.Lock()
	calls := sk.calls
	sk.mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected 2 sink write attempts, got %d", calls)
	}

	snap := metrics.Snapshot()
	if snap["ingestor_sink_errors_total"] != 1 {
		t.Fatalf("expected one sink error metric, got %v", snap["ingestor_sink_errors_total"])
	}
	if !logger.contains("ingestor.flush.failed") {
		t.Fatalf("expected runtime error log for failed flush")
	}
	if !logger.contains("ingestor.flush.sink_write_succeeded") {
		t.Fatalf("expected success log for second sink write")
	}
}

var _ encoder.Encoder[map[string]any] = bytesEncoder{}
