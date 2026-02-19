package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
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

	delErr        error
	delFail       bool
	delCalls      int
	delBatchSizes []int
	delFirstIDs   []string // opcional, só pra debug/assert

	visErr    error
	visCalls  int
	lastVisRH string

	visBatchErr   error
	visBatchCalls int
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

	// Capture barato: só tamanho do batch e (opcional) o primeiro ID.
	f.delBatchSizes = append(f.delBatchSizes, len(in.Entries))
	if len(in.Entries) > 0 {
		f.delFirstIDs = append(f.delFirstIDs, aws.ToString(in.Entries[0].Id))
	} else {
		f.delFirstIDs = append(f.delFirstIDs, "")
	}

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
	if f.visErr != nil {
		return nil, f.visErr
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQSAPI) ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	f.mu.Lock()
	f.visBatchCalls++
	err := f.visBatchErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
}

// fakeSQSNoCapture is optimized for parallel benchmarks: it doesn't capture entries
// and only counts calls. It's thread-safe.
type fakeSQSNoCapture struct {
	mu sync.Mutex

	delErr   error
	delFail  bool
	delCalls int

	visErr   error
	visCalls int

	visBatchErr   error
	visBatchCalls int
}

func (f *fakeSQSNoCapture) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	// Not used in these ack benchmarks.
	return &sqs.ReceiveMessageOutput{}, nil
}

func (f *fakeSQSNoCapture) DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	f.mu.Lock()
	f.delCalls++
	err := f.delErr
	fail := f.delFail
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}

	out := &sqs.DeleteMessageBatchOutput{}
	if fail && len(in.Entries) > 0 {
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

func (f *fakeSQSNoCapture) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	f.visCalls++
	err := f.visErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQSNoCapture) ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	f.mu.Lock()
	f.visBatchCalls++
	err := f.visBatchErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
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

func (w testClientWrapper) ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	return w.api.ChangeMessageVisibilityBatch(ctx, in, optFns...)
}

func newTestSourceNoPollers(ctx context.Context, api sqsAPI, queueURL string, cfg SourceSQSConfig) (*SourceSQS, context.Context) {
	cfg.validate()
	ctx2, cancel := context.WithCancel(ctx)
	return &SourceSQS{
		cfg:      cfg,
		client:   testClientWrapper{api: api},
		queueURL: queueURL,
		bufCh:    make(chan *sqstypes.Message, cfg.BufSize),
		cancel:   cancel,
	}, ctx2
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

	for {
		_, err := src.Receive(ctx)
		if err != nil {
			return
		}
	}
}

func TestSourceSQS_AckBatch_SendsAllInChunksOf10(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	handles := make([]ackHandle, 0, 25)
	for i := 0; i < 25; i++ {
		handles = append(handles, makeInternalMsg(src, fmt.Sprintf("id-%d", i), fmt.Sprintf("rh-%d", i)).AckHandle())
	}

	if err := src.ackHandlesBatch(context.Background(), handles); err != nil {
		t.Fatalf("ackHandlesBatch: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.delCalls != 3 {
		t.Fatalf("expected 3 calls, got %d", f.delCalls)
	}
	if len(f.delBatchSizes) != 3 || f.delBatchSizes[0] != 10 || f.delBatchSizes[1] != 10 || f.delBatchSizes[2] != 5 {
		t.Fatalf("unexpected batch sizes: %#v", f.delBatchSizes)
	}
}

func TestSourceSQS_AckBatch_ReturnsErrorOnFailedEntry(t *testing.T) {
	f := newFakeSQSAPI(1)
	f.delFail = true
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	handles := []ackHandle{
		makeInternalMsg(src, "id-0", "rh-0").AckHandle(),
		makeInternalMsg(src, "id-1", "rh-1").AckHandle(),
	}

	if err := src.ackHandlesBatch(context.Background(), handles); err == nil {
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
}

func TestSourceSQS_Concurrent_AckAndClose_NoPanic(t *testing.T) {
	f := &fakeSQSNoCapture{}
	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	ctx := context.Background()

	metas := make([]AckMetadata, 0, 1000)
	for i := 0; i < 1000; i++ {
		metas = append(metas, AckMetadata{ID: "id", Handle: "rh"})
	}

	var wg sync.WaitGroup
	wg.Add(8)

	for i := 0; i < 7; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = src.AckBatchMeta(ctx, metas)
			}
		}()
	}

	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			src.Close()
		}
	}()

	wg.Wait()

	// basic sanity: should have made some calls
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.delCalls == 0 {
		t.Fatalf("expected some delete calls")
	}
	_ = atomic.LoadInt64(new(int64)) // keeps atomic import used if your linter complains
}

func BenchmarkSourceSQS_AckBatch(b *testing.B) {
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

// --- Parallel additions ---

func BenchmarkSourceSQS_ackHandlesBatch_NoCapture_Parallel(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := &fakeSQSNoCapture{}
			cfg := DefaultSourceSQSConfig
			src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

			handles := make([]ackHandle, 0, n)
			for i := 0; i < n; i++ {
				handles = append(handles, ackHandle{id: "id", receiptHandle: "rh"})
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := src.ackHandlesBatch(ctx, handles); err != nil {
						b.Fatalf("ackHandlesBatch err: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkSourceSQS_ackMetasBatch_NoCapture_Parallel(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := &fakeSQSNoCapture{}
			cfg := DefaultSourceSQSConfig
			src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

			metas := make([]AckMetadata, 0, n)
			for i := 0; i < n; i++ {
				metas = append(metas, AckMetadata{ID: "id", Handle: "rh"})
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := src.ackMetasBatch(ctx, metas); err != nil {
						b.Fatalf("ackMetasBatch err: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkSourceSQS_Receive_ParallelConsumers(b *testing.B) {
	f := &fakeSQSNoCapture{}
	cfg := DefaultSourceSQSConfig
	cfg.BufSize = 4096

	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	stop := make(chan struct{})
	defer close(stop)

	// Producer: keep buffer replenished (no pollers).
	go func() {
		msg := &sqstypes.Message{
			MessageId:     aws.String("m"),
			ReceiptHandle: aws.String("rh"),
			Body:          aws.String("x"),
		}
		for {
			select {
			case <-stop:
				return
			case src.bufCh <- msg:
			default:
				runtime.Gosched()
			}
		}
	}()

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := src.Receive(ctx); err != nil {
				b.Fatalf("receive err: %v", err)
			}
		}
	})
}
