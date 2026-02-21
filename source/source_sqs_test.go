package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

//
// Fakes
//

type fakeSQSAPI struct {
	recvCh  chan *sqs.ReceiveMessageOutput
	recvErr error

	mu sync.Mutex

	delErr        error
	delFail       bool
	delCalls      int
	delBatchSizes []int
	delFirstIDs   []string

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

//
// Test client wrapper + constructors
//

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

// Matches production pattern: queueURLPtr points to s.queueURL.
func newTestSourceNoPollers(ctx context.Context, api sqsAPI, queueURL string, cfg SourceSQSConfig) (*SourceSQS, context.Context) {
	cfg.validate()
	pollCtx, cancel := context.WithCancel(ctx)

	s := &SourceSQS{
		cfg:      cfg,
		client:   testClientWrapper{api: api},
		queueURL: queueURL,
		bufCh:    make(chan *sqstypes.Message, cfg.BufSize),
		cancel:   cancel,
	}
	s.queueURLPtr = &s.queueURL
	return s, pollCtx
}

func newTestSource(ctx context.Context, api sqsAPI, queueURL string, cfg SourceSQSConfig) *SourceSQS {
	s, pollCtx := newTestSourceNoPollers(ctx, api, queueURL, cfg)
	s.startPollers(pollCtx)
	return s
}

//
// Tests
//

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

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
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

func TestSourceSQS_Close_ReceiveEventuallyReturnsErrClosed(t *testing.T) {
	f := newFakeSQSAPI(1)

	cfg := DefaultSourceSQSConfig
	cfg.WaitTimeSeconds = 0
	cfg.Pollers = 1
	cfg.BufSize = 1

	src := newTestSource(context.Background(), f, "q", cfg)
	src.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	for tries := 0; tries < 10_000; tries++ {
		_, err := src.Receive(ctx)
		if err != nil {
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("expected ErrClosed, got %v", err)
			}
			return
		}
		runtime.Gosched()
	}

	t.Fatalf("Receive did not return ErrClosed within expected attempts")
}

func TestSourceSQS_AckBatchMeta_SendsAllInChunksOf10(t *testing.T) {
	f := newFakeSQSAPI(1)

	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	metas := make([]AckMetadata, 0, 25)
	for i := 0; i < 25; i++ {
		metas = append(metas, AckMetadata{
			ID:     fmt.Sprintf("id-%d", i),
			Handle: fmt.Sprintf("rh-%d", i),
		})
	}

	if err := src.AckBatchMeta(context.Background(), metas); err != nil {
		t.Fatalf("AckBatchMeta: %v", err)
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

func TestSourceSQS_AckBatchMeta_ReturnsErrorOnFailedEntry(t *testing.T) {
	f := newFakeSQSAPI(1)
	f.delFail = true

	cfg := DefaultSourceSQSConfig
	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	metas := []AckMetadata{
		{ID: "id-0", Handle: "rh-0"},
		{ID: "id-1", Handle: "rh-1"},
	}

	if err := src.AckBatchMeta(context.Background(), metas); err == nil {
		t.Fatalf("expected error")
	}
}

func TestMessage_Fail_NoOpWhenConfigNil(t *testing.T) {
	f := newFakeSQSAPI(1)

	cfg := DefaultSourceSQSConfig
	cfg.FailVisibilityTimeoutSeconds = nil

	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	m := &message{
		src: src,
		m: &sqstypes.Message{
			MessageId:     aws.String("id"),
			ReceiptHandle: aws.String("rh-x"),
			Body:          aws.String("x"),
		},
	}

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

	m := &message{
		src: src,
		m: &sqstypes.Message{
			MessageId:     aws.String("id"),
			ReceiptHandle: aws.String("rh-777"),
			Body:          aws.String("x"),
		},
	}

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

func TestSourceSQS_Concurrent_AckMetaAndClose_NoPanic(t *testing.T) {
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

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.delCalls == 0 {
		t.Fatalf("expected some delete calls")
	}
}
