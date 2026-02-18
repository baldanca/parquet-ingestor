package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type fakeSQSAPI struct {
	recvCh  chan *sqs.ReceiveMessageOutput
	recvErr error

	mu sync.Mutex

	delErr     error
	delFail    bool
	delCalls   int
	delBatches [][]string

	visErr    error
	visCalls  int
	lastVisRH string
	lastVisTO int32
}

// fakeSQSNoCapture is a lighter fake used for benchmarks.
// It avoids recording/copying batch entries so benchmark numbers reflect
// SourceSQS costs rather than test-fixture overhead.
type fakeSQSNoCapture struct {
	mu sync.Mutex

	delErr   error
	delFail  bool
	delCalls int

	out    sqs.DeleteMessageBatchOutput
	failed [1]sqstypes.BatchResultErrorEntry
}

func (f *fakeSQSNoCapture) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	// Not used by ack benchmarks.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return &sqs.ReceiveMessageOutput{}, nil
	}
}

func (f *fakeSQSNoCapture) DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	f.mu.Lock()
	f.delCalls++
	delErr := f.delErr
	delFail := f.delFail
	f.mu.Unlock()

	if delErr != nil {
		return nil, delErr
	}

	// Reuse output across calls to keep benchmarks focused on SourceSQS.
	f.out.Failed = f.out.Failed[:0]
	if delFail && len(in.Entries) > 0 {
		f.failed[0] = sqstypes.BatchResultErrorEntry{
			Id:      in.Entries[0].Id,
			Code:    aws.String("InternalError"),
			Message: aws.String("boom"),
		}
		f.out.Failed = f.failed[:1]
	}
	return &f.out, nil
}

func (f *fakeSQSNoCapture) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	// Not used by ack benchmarks.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return &sqs.ChangeMessageVisibilityOutput{}, nil
	}
}

func newFakeSQSAPI(buf int) *fakeSQSAPI {
	if buf <= 0 {
		buf = 1
	}
	return &fakeSQSAPI{recvCh: make(chan *sqs.ReceiveMessageOutput, buf)}
}

func (f *fakeSQSAPI) pushReceive(out *sqs.ReceiveMessageOutput) {
	f.recvCh <- out
}

func (f *fakeSQSAPI) pushEmptyReceive() {
	f.recvCh <- &sqs.ReceiveMessageOutput{}
}

func (f *fakeSQSAPI) pushReceiveJSON(jsonStr string) error {
	var out sqs.ReceiveMessageOutput
	if err := json.Unmarshal([]byte(jsonStr), &out); err != nil {
		return err
	}
	f.pushReceive(&out)
	return nil
}

func (f *fakeSQSAPI) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	select {
	case out := <-f.recvCh:
		if out == nil {
			return &sqs.ReceiveMessageOutput{}, nil
		}
		return out, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeSQSAPI) DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.delCalls++

	ids := make([]string, 0, len(in.Entries))
	for _, e := range in.Entries {
		ids = append(ids, aws.ToString(e.Id))
	}
	f.delBatches = append(f.delBatches, append([]string(nil), ids...))

	if f.delErr != nil {
		return nil, f.delErr
	}

	out := &sqs.DeleteMessageBatchOutput{}
	if f.delFail && len(in.Entries) > 0 {
		out.Failed = []sqstypes.BatchResultErrorEntry{
			{
				Id:      in.Entries[0].Id,
				Code:    aws.String("InternalError"),
				Message: aws.String("boom"),
			},
		}
	}
	return out, nil
}

func (f *fakeSQSAPI) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visCalls++
	f.lastVisRH = aws.ToString(in.ReceiptHandle)
	f.lastVisTO = in.VisibilityTimeout
	if f.visErr != nil {
		return nil, f.visErr
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

type testClientWrapper struct{ api sqsAPI }

func (w testClientWrapper) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return w.api.ReceiveMessage(ctx, in, optFns...)
}
func (w testClientWrapper) DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	return w.api.DeleteMessageBatch(ctx, in, optFns...)
}
func (w testClientWrapper) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return w.api.ChangeMessageVisibility(ctx, in, optFns...)
}

func newTestSourceNoPollers(ctx context.Context, api sqsAPI, queueURL string, cfg SourceSQSConfig) (*SourceSQS, context.Context) {
	cfg.validate()
	ctx2, cancel := context.WithCancel(ctx)
	s := &SourceSQS{
		cfg:      cfg,
		client:   testClientWrapper{api: api},
		queueURL: queueURL,
		bufCh:    make(chan *sqstypes.Message, cfg.BufSize),
		cancel:   cancel,
	}
	s.queueURLPtr = &s.queueURL
	return s, ctx2
}

func newTestSource(ctx context.Context, api sqsAPI, queueURL string, cfg SourceSQSConfig) *SourceSQS {
	s, pollCtx := newTestSourceNoPollers(ctx, api, queueURL, cfg)
	s.startPollers(pollCtx)
	return s
}

func makeInternalMsg(src *SourceSQS, id, rh string) *message {
	return &message{
		src: src,
		m: &sqstypes.Message{
			MessageId:     aws.String(id),
			ReceiptHandle: aws.String(rh),
			Body:          aws.String("x"),
		},
	}
}

func TestSourceSQS_Receive_DeliversMessages(t *testing.T) {
	f := newFakeSQSAPI(10)
	cfg := DefaultSourceSQSConfig
	cfg.WaitTimeSeconds = 0
	cfg.Pollers = 1
	cfg.BufSize = 10

	if err := f.pushReceiveJSON(`{
	  "Messages": [
	    {"MessageId":"m1","ReceiptHandle":"rh1","Body":"a"},
	    {"MessageId":"m2","ReceiptHandle":"rh2","Body":"b"}
	  ]
	}`); err != nil {
		t.Fatalf("bad fixture: %v", err)
	}

	src := newTestSource(context.Background(), f, "q", cfg)
	defer src.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	m1, err := src.Receive(ctx)
	if err != nil {
		t.Fatalf("receive 1: %v", err)
	}
	m2, err := src.Receive(ctx)
	if err != nil {
		t.Fatalf("receive 2: %v", err)
	}

	if m1.Data().Payload != "a" || m2.Data().Payload != "b" {
		t.Fatalf("unexpected payloads: %v %v", m1.Data().Payload, m2.Data().Payload)
	}
}

func TestSourceSQS_Receive_ContextCancel(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	cfg.Pollers = 1
	cfg.BufSize = 1
	cfg.WaitTimeSeconds = 0

	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := src.Receive(ctx)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSourceSQS_Close_ReceiveEventuallyReturnsError(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	cfg.WaitTimeSeconds = 0
	cfg.Pollers = 1
	cfg.BufSize = 1

	src := newTestSource(context.Background(), f, "q", cfg)
	src.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for i := 0; i < 1000; i++ {
		_, err := src.Receive(ctx)
		if err != nil {
			return
		}
	}
	t.Fatalf("expected Receive to return an error after Close")
}

func TestSourceSQS_AckBatch_SendsAllInChunksOf10(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	msgs := make([]Message, 0, 25)
	for i := 0; i < 25; i++ {
		msgs = append(msgs, makeInternalMsg(src, fmt.Sprintf("id-%d", i), fmt.Sprintf("rh-%d", i)))
	}

	if err := src.AckBatch(context.Background(), msgs); err != nil {
		t.Fatalf("AckBatch: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.delCalls != 3 {
		t.Fatalf("expected 3 calls, got %d", f.delCalls)
	}
	if len(f.delBatches) != 3 || len(f.delBatches[0]) != 10 || len(f.delBatches[1]) != 10 || len(f.delBatches[2]) != 5 {
		t.Fatalf("unexpected batch sizes: %#v", f.delBatches)
	}
}

func TestSourceSQS_AckBatch_ReturnsErrorOnFailedEntry(t *testing.T) {
	f := newFakeSQSAPI(1)
	f.delFail = true
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	msgs := []Message{
		makeInternalMsg(src, "id-0", "rh-0"),
		makeInternalMsg(src, "id-1", "rh-1"),
	}

	if err := src.AckBatch(context.Background(), msgs); err == nil {
		t.Fatalf("expected error")
	}
}

func TestMessage_Fail_NoOpWhenConfigNil(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	cfg.FailVisibilityTimeoutSeconds = nil
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	m := makeInternalMsg(src, "id", "rh-x")
	if err := m.Fail(context.Background(), errors.New("x")); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.visCalls != 0 {
		t.Fatalf("expected visCalls=0, got %d", f.visCalls)
	}
}

func TestMessage_Fail_CallsChangeVisibilityWhenConfigured(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	to := int32(7)
	cfg.FailVisibilityTimeoutSeconds = &to
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	m := makeInternalMsg(src, "id", "rh-777")
	if err := m.Fail(context.Background(), errors.New("x")); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.visCalls != 1 {
		t.Fatalf("expected visCalls=1, got %d", f.visCalls)
	}
	if f.lastVisRH != "rh-777" {
		t.Fatalf("expected rh-777, got %q", f.lastVisRH)
	}
	if f.lastVisTO != 7 {
		t.Fatalf("expected vis timeout=7, got %d", f.lastVisTO)
	}
}

func BenchmarkSourceSQS_AckBatch(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := newFakeSQSAPI(1)
			cfg := DefaultSourceSQSConfig
			src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

			msgs := make([]Message, 0, n)
			for i := 0; i < n; i++ {
				msgs = append(msgs, makeInternalMsg(src, fmt.Sprintf("id-%d", i), fmt.Sprintf("rh-%d", i)))
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := src.AckBatch(ctx, msgs); err != nil {
					b.Fatalf("AckBatch err: %v", err)
				}
			}
		})
	}
}

func BenchmarkSourceSQS_ackHandlesBatch(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := newFakeSQSAPI(1)
			cfg := DefaultSourceSQSConfig
			src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

			handles := make([]ackHandle, 0, n)
			for i := 0; i < n; i++ {
				handles = append(handles, makeInternalMsg(src, fmt.Sprintf("id-%d", i), fmt.Sprintf("rh-%d", i)).AckHandle())
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := src.ackHandlesBatch(ctx, handles); err != nil {
					b.Fatalf("ackHandlesBatch err: %v", err)
				}
			}
		})
	}
}

func BenchmarkSourceSQS_AckBatch_NoCapture(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := &fakeSQSNoCapture{}
			cfg := DefaultSourceSQSConfig
			src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

			msgs := make([]Message, 0, n)
			for i := 0; i < n; i++ {
				msgs = append(msgs, makeInternalMsg(src, fmt.Sprintf("id-%d", i), fmt.Sprintf("rh-%d", i)))
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := src.AckBatch(ctx, msgs); err != nil {
					b.Fatalf("AckBatch err: %v", err)
				}
			}
		})
	}
}

func BenchmarkSourceSQS_ackHandlesBatch_NoCapture(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := &fakeSQSNoCapture{}
			cfg := DefaultSourceSQSConfig
			src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

			handles := make([]ackHandle, 0, n)
			for i := 0; i < n; i++ {
				handles = append(handles, makeInternalMsg(src, fmt.Sprintf("id-%d", i), fmt.Sprintf("rh-%d", i)).AckHandle())
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := src.ackHandlesBatch(ctx, handles); err != nil {
					b.Fatalf("ackHandlesBatch err: %v", err)
				}
			}
		})
	}
}

func BenchmarkSourceSQS_Receive(b *testing.B) {
	for _, batch := range []int{1, 10} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			f := newFakeSQSAPI(1024)
			cfg := DefaultSourceSQSConfig
			cfg.WaitTimeSeconds = 0
			cfg.Pollers = 1
			cfg.BufSize = 1024

			src := newTestSource(context.Background(), f, "q", cfg)
			defer src.Close()

			ctx := context.Background()

			payload := make([]sqstypes.Message, 0, batch)
			for i := 0; i < batch; i++ {
				payload = append(payload, sqstypes.Message{
					MessageId:     aws.String("m"),
					ReceiptHandle: aws.String("rh"),
					Body:          aws.String("x"),
				})
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				f.pushReceive(&sqs.ReceiveMessageOutput{Messages: payload})
				for j := 0; j < batch; j++ {
					if _, err := src.Receive(ctx); err != nil {
						b.Fatalf("receive err: %v", err)
					}
				}
			}
		})
	}
}

func BenchmarkSourceSQS_Fail(b *testing.B) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	to := int32(7)
	cfg.FailVisibilityTimeoutSeconds = &to

	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)
	m := makeInternalMsg(src, "id", "rh-1")

	ctx := context.Background()
	errX := errors.New("x")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := m.Fail(ctx, errX); err != nil {
			b.Fatalf("fail err: %v", err)
		}
	}
}

func BenchmarkSourceSQS_ackMetasBatch_NoCapture(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := &fakeSQSNoCapture{}
			cfg := DefaultSourceSQSConfig
			src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

			metas := make([]AckMetadata, 0, n)
			for i := 0; i < n; i++ {
				metas = append(metas, AckMetadata{ID: fmt.Sprintf("id-%d", i), Handle: fmt.Sprintf("rh-%d", i)})
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := src.AckBatchMeta(ctx, metas); err != nil {
					b.Fatalf("AckBatchMeta err: %v", err)
				}
			}
		})
	}
}

func BenchmarkAckGroup_Commit_WithSQS_NoCapture(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := &fakeSQSNoCapture{}
			cfg := DefaultSourceSQSConfig
			src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

			var g AckGroup
			for i := 0; i < n; i++ {
				g.Add(makeInternalMsg(src, fmt.Sprintf("id-%d", i), fmt.Sprintf("rh-%d", i)))
			}

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := g.Commit(ctx, src); err != nil {
					b.Fatalf("commit err: %v", err)
				}
			}
		})
	}
}
