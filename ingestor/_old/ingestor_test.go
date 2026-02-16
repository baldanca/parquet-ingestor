package ingestor

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeMessage struct {
	estSize int64
	acked   bool
	failed  bool
}

func (m *fakeMessage) Data() Envelope { return Envelope{Payload: map[string]any{"x": 1}} }
func (m *fakeMessage) EstimatedSizeBytes() (int64, bool) {
	if m.estSize <= 0 {
		return 0, false
	}
	return m.estSize, true
}
func (m *fakeMessage) Ack(context.Context) error         { m.acked = true; return nil }
func (m *fakeMessage) Fail(context.Context, error) error { m.failed = true; return nil }

type fakeSource struct {
	ackedBatches int
	ackedMsgs    int
}

func (s *fakeSource) Receive(context.Context) (Message, error) { return nil, errors.New("not used") }
func (s *fakeSource) AckBatch(_ context.Context, msgs []Message) error {
	s.ackedBatches++
	s.ackedMsgs += len(msgs)
	return nil
}

type passthroughTransformer struct{}

func (passthroughTransformer) Transform(_ context.Context, in Envelope) (int, error) {
	_ = in
	return 1, nil
}

type bytesEncoder struct{}

func (bytesEncoder) Encode(_ context.Context, items []int) ([]byte, string, error) {
	_ = items
	return []byte("ok"), "application/octet-stream", nil
}
func (bytesEncoder) FileExtension() string { return ".bin" }

type fakeSink struct {
	writes int
	fail   bool
}

func (s *fakeSink) Write(context.Context, WriteRequest) error {
	s.writes++
	if s.fail {
		return errors.New("sink down")
	}
	return nil
}

func TestIngestor_AckAfterWrite(t *testing.T) {
	src := &fakeSource{}
	sink := &fakeSink{}

	cfg := DefaultConfig
	cfg.MaxItems = 1
	cfg.FlushInterval = time.Second

	ing, err := NewIngestor[int](cfg, src, passthroughTransformer{}, bytesEncoder{}, sink, DefaultKeyFunc[int](bytesEncoder{}))
	if err != nil {
		t.Fatal(err)
	}

	msg := &fakeMessage{estSize: 10}
	flushNow, err := ing.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if !flushNow {
		t.Fatalf("expected flushNow")
	}
	if err := ing.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sink.writes != 1 {
		t.Fatalf("expected 1 write, got %d", sink.writes)
	}
	if src.ackedBatches != 1 || src.ackedMsgs != 1 {
		t.Fatalf("expected ack batch 1/1, got batches=%d msgs=%d", src.ackedBatches, src.ackedMsgs)
	}
}

func TestIngestor_DoesNotAckOnSinkFailure(t *testing.T) {
	src := &fakeSource{}
	sink := &fakeSink{fail: true}

	cfg := DefaultConfig
	cfg.MaxItems = 1
	cfg.FlushInterval = time.Second

	ing, err := NewIngestor[int](cfg, src, passthroughTransformer{}, bytesEncoder{}, sink, DefaultKeyFunc[int](bytesEncoder{}))
	if err != nil {
		t.Fatal(err)
	}

	msg := &fakeMessage{estSize: 10}
	_, _ = ing.processMessage(context.Background(), msg)
	if err := ing.flush(context.Background()); err == nil {
		t.Fatalf("expected error")
	}
	if src.ackedBatches != 0 {
		t.Fatalf("expected no ack on sink failure")
	}
}
