package source

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// --- AckBatch ---

func TestAckBatch_Empty_ReturnsNil(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	if err := src.AckBatch(context.Background(), nil); err != nil {
		t.Fatalf("expected nil for empty msgs, got %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.delCalls != 0 {
		t.Fatalf("expected 0 delete calls, got %d", f.delCalls)
	}
}

func TestAckBatch_SingleBatch_UsesMessageType(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	msgs := []Message{
		&message{src: src, m: &sqstypes.Message{
			MessageId:     aws.String("id-0"),
			ReceiptHandle: aws.String("rh-0"),
		}},
		&message{src: src, m: &sqstypes.Message{
			MessageId:     aws.String("id-1"),
			ReceiptHandle: aws.String("rh-1"),
		}},
	}

	if err := src.AckBatch(context.Background(), msgs); err != nil {
		t.Fatalf("AckBatch: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.delCalls != 1 {
		t.Fatalf("expected 1 delete call, got %d", f.delCalls)
	}
	if f.delBatchSizes[0] != 2 {
		t.Fatalf("expected batch size 2, got %d", f.delBatchSizes[0])
	}
}

// ackableMessage implements the ackable interface without being a *message.
type ackableMessage struct {
	id            string
	receiptHandle string
}

func (a ackableMessage) AckHandle() ackHandle {
	return ackHandle(a)
}

func (a ackableMessage) Data() Envelope                        { return Envelope{} }
func (a ackableMessage) EstimatedSizeBytes() (int64, bool)     { return 0, false }
func (a ackableMessage) Fail(_ context.Context, _ error) error { return nil }
func (a ackableMessage) AckMeta() (AckMetadata, bool) {
	return AckMetadata{ID: a.id, Handle: a.receiptHandle}, true
}

func TestAckBatch_AckableInterface_Works(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	msgs := []Message{
		ackableMessage{id: "id-0", receiptHandle: "rh-0"},
	}

	if err := src.AckBatch(context.Background(), msgs); err != nil {
		t.Fatalf("AckBatch with ackable: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.delCalls != 1 {
		t.Fatalf("expected 1 delete call, got %d", f.delCalls)
	}
}

func TestAckBatch_NilMessageSkipped(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	msgs := []Message{
		nil,
		&message{src: src, m: &sqstypes.Message{
			MessageId:     aws.String("id-1"),
			ReceiptHandle: aws.String("rh-1"),
		}},
	}

	if err := src.AckBatch(context.Background(), msgs); err != nil {
		t.Fatalf("AckBatch: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// Only 1 real message after skipping nil.
	if f.delBatchSizes[0] != 1 {
		t.Fatalf("expected batch size 1 after nil skip, got %d", f.delBatchSizes[0])
	}
}

func TestAckBatch_MultipleBatches_ChunksOf10(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	msgs := make([]Message, 25)
	for i := range msgs {
		msgs[i] = &message{src: src, m: &sqstypes.Message{
			MessageId:     aws.String(fmt.Sprintf("id-%d", i)),
			ReceiptHandle: aws.String(fmt.Sprintf("rh-%d", i)),
		}}
	}

	if err := src.AckBatch(context.Background(), msgs); err != nil {
		t.Fatalf("AckBatch: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.delCalls != 3 {
		t.Fatalf("expected 3 delete calls, got %d", f.delCalls)
	}
	if f.delBatchSizes[0] != 10 || f.delBatchSizes[1] != 10 || f.delBatchSizes[2] != 5 {
		t.Fatalf("unexpected batch sizes: %v", f.delBatchSizes)
	}
}

func TestAckBatch_DeleteError_ReturnsError(t *testing.T) {
	f := newFakeSQSAPI(1)
	f.delErr = errors.New("sqs unavailable")
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	msgs := []Message{
		&message{src: src, m: &sqstypes.Message{
			MessageId:     aws.String("id-0"),
			ReceiptHandle: aws.String("rh-0"),
		}},
	}

	err := src.AckBatch(context.Background(), msgs)
	if err == nil {
		t.Fatal("expected error from DeleteMessageBatch")
	}
}

func TestAckBatch_PartialFailure_ReturnsError(t *testing.T) {
	f := newFakeSQSAPI(1)
	f.delFail = true
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	msgs := []Message{
		&message{src: src, m: &sqstypes.Message{
			MessageId:     aws.String("id-0"),
			ReceiptHandle: aws.String("rh-0"),
		}},
	}

	err := src.AckBatch(context.Background(), msgs)
	if err == nil {
		t.Fatal("expected error for partial failure")
	}
	if !strings.Contains(err.Error(), "partially failed") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// nonAckableMessage does not implement ackable or *message.
type nonAckableMessage struct{}

func (nonAckableMessage) Data() Envelope                        { return Envelope{} }
func (nonAckableMessage) EstimatedSizeBytes() (int64, bool)     { return 0, false }
func (nonAckableMessage) Fail(_ context.Context, _ error) error { return nil }
func (nonAckableMessage) AckMeta() (AckMetadata, bool)          { return AckMetadata{}, false }

func TestAckBatch_NonAckable_ReturnsError(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	msgs := []Message{nonAckableMessage{}}

	err := src.AckBatch(context.Background(), msgs)
	if err == nil {
		t.Fatal("expected error for non-ackable message")
	}
}

// --- ExtendVisibilityBatch ---

func TestExtendVisibilityBatch_Empty_ReturnsNil(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	if err := src.ExtendVisibilityBatch(context.Background(), nil, 30); err != nil {
		t.Fatalf("expected nil for empty metas, got %v", err)
	}
}

func TestExtendVisibilityBatch_NegativeTimeout_ReturnsError(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	metas := []AckMetadata{{ID: "id-0", Handle: "rh-0"}}

	err := src.ExtendVisibilityBatch(context.Background(), metas, -1)
	if err == nil {
		t.Fatal("expected error for negative timeout")
	}
}

func TestExtendVisibilityBatch_Normal_CallsAPI(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	metas := []AckMetadata{
		{ID: "id-0", Handle: "rh-0"},
		{ID: "id-1", Handle: "rh-1"},
	}

	if err := src.ExtendVisibilityBatch(context.Background(), metas, 30); err != nil {
		t.Fatalf("ExtendVisibilityBatch: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.visBatchCalls != 1 {
		t.Fatalf("expected 1 visibility batch call, got %d", f.visBatchCalls)
	}
}

func TestExtendVisibilityBatch_MultipleBatches_ChunksOf10(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	metas := make([]AckMetadata, 25)
	for i := range metas {
		metas[i] = AckMetadata{ID: fmt.Sprintf("id-%d", i), Handle: fmt.Sprintf("rh-%d", i)}
	}

	if err := src.ExtendVisibilityBatch(context.Background(), metas, 60); err != nil {
		t.Fatalf("ExtendVisibilityBatch: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.visBatchCalls != 3 {
		t.Fatalf("expected 3 batch calls, got %d", f.visBatchCalls)
	}
}

func TestExtendVisibilityBatch_APIError_ReturnsError(t *testing.T) {
	f := newFakeSQSAPI(1)
	f.visBatchErr = errors.New("sqs unavailable")
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	metas := []AckMetadata{{ID: "id-0", Handle: "rh-0"}}

	err := src.ExtendVisibilityBatch(context.Background(), metas, 30)
	if err == nil {
		t.Fatal("expected error from ChangeMessageVisibilityBatch")
	}
}

func TestExtendVisibilityBatch_EmptyHandle_Skipped(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	// All handles empty → all skipped → no API call.
	metas := []AckMetadata{
		{ID: "id-0", Handle: ""},
		{ID: "id-1", Handle: ""},
	}

	if err := src.ExtendVisibilityBatch(context.Background(), metas, 30); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.visBatchCalls != 0 {
		t.Fatalf("expected 0 API calls when all handles are empty, got %d", f.visBatchCalls)
	}
}

// fakeSQSWithVisFailure returns a partial failure response from ChangeMessageVisibilityBatch.
type fakeSQSWithVisFailure struct {
	fakeSQSNoCapture
}

func (f *fakeSQSWithVisFailure) ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	f.mu.Lock()
	f.visBatchCalls++
	f.mu.Unlock()
	return &sqs.ChangeMessageVisibilityBatchOutput{
		Failed: []sqstypes.BatchResultErrorEntry{
			{Id: aws.String("0"), Code: aws.String("InternalError"), Message: aws.String("boom")},
		},
	}, nil
}

func TestExtendVisibilityBatch_PartialFailure_ReturnsError(t *testing.T) {
	api := &fakeSQSWithVisFailure{}
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), api, "q", cfg)

	metas := []AckMetadata{{ID: "id-0", Handle: "rh-0"}}

	err := src.ExtendVisibilityBatch(context.Background(), metas, 30)
	if err == nil {
		t.Fatal("expected error for partial failure")
	}
	if !strings.Contains(err.Error(), "partially failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- NewSourceSQS ---

// TestNewSourceSQS_UsesDefaultConfig verifies the trivial constructor returns a
// non-nil, functional SourceSQS backed by the default config.
func TestNewSourceSQS_UsesDefaultConfig(t *testing.T) {
	f := newFakeSQSAPI(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := NewSourceSQS(ctx, testClientWrapper{api: f}, "https://sqs.us-east-1.amazonaws.com/123/test")
	if src == nil {
		t.Fatal("expected non-nil SourceSQS")
	}
	defer src.Close()

	// Pollers field must be set from DefaultSourceSQSConfig.
	if src.cfg.Pollers != DefaultSourceSQSConfig.Pollers {
		t.Fatalf("Pollers = %d, want %d", src.cfg.Pollers, DefaultSourceSQSConfig.Pollers)
	}
}
