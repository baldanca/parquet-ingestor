package source

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// ErrClosed is returned when Receive is called after the source has been closed.
var ErrClosed = errors.New("source closed")

type SourceSQSConfig struct {
	WaitTimeSeconds int32
	MaxMessages     int32
	VisibilityTO    int32

	Pollers int
	BufSize int

	FailVisibilityTimeoutSeconds *int32
}

func (c *SourceSQSConfig) validate() {
	if c.WaitTimeSeconds < 0 || c.WaitTimeSeconds > 20 {
		panic("wait time seconds must be between 0 and 20")
	}
	if c.MaxMessages < 1 || c.MaxMessages > 10 {
		panic("max messages must be between 1 and 10")
	}
	if c.VisibilityTO < 0 {
		panic("visibility timeout must be non-negative")
	}
	if c.Pollers < 1 {
		panic("pollers must be at least 1")
	}
	if c.BufSize < 1 {
		panic("buffer size must be at least 1")
	}
	if c.FailVisibilityTimeoutSeconds != nil && *c.FailVisibilityTimeoutSeconds < 0 {
		panic("fail visibility timeout seconds must be non-negative")
	}
}

var DefaultSourceSQSConfig = SourceSQSConfig{
	WaitTimeSeconds: 20,
	MaxMessages:     10,
	VisibilityTO:    30,
	Pollers:         3,
	BufSize:         256,
}

type sqsAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
}

type SourceSQS struct {
	cfg SourceSQSConfig

	client      sqsAPI
	queueURL    string
	queueURLPtr *string

	bufCh chan *sqstypes.Message

	closeOnce sync.Once
	cancel    context.CancelFunc

	wg sync.WaitGroup
}

func NewWithConfig(ctx context.Context, client sqsAPI, queueURL string, cfg SourceSQSConfig) *SourceSQS {
	if client == nil {
		panic("sqs client is required")
	}
	if queueURL == "" {
		panic("queue url is required")
	}
	cfg.validate()

	ctx, cancel := context.WithCancel(ctx)

	s := &SourceSQS{
		cfg:      cfg,
		client:   client,
		queueURL: queueURL,
		bufCh:    make(chan *sqstypes.Message, cfg.BufSize),
		cancel:   cancel,
	}
	s.queueURLPtr = &s.queueURL

	s.startPollers(ctx)
	return s
}

func (s *SourceSQS) startPollers(ctx context.Context) {
	s.wg.Add(s.cfg.Pollers)
	for i := 0; i < s.cfg.Pollers; i++ {
		go func() {
			defer s.wg.Done()
			s.pollLoop(ctx)
		}()
	}
	go func() {
		s.wg.Wait()
		close(s.bufCh)
	}()
}

func (s *SourceSQS) pollLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reqCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WaitTimeSeconds+5)*time.Second)
		out, err := s.client.ReceiveMessage(reqCtx, &sqs.ReceiveMessageInput{
			QueueUrl:              s.queueURLPtr,
			MaxNumberOfMessages:   s.cfg.MaxMessages,
			WaitTimeSeconds:       s.cfg.WaitTimeSeconds,
			VisibilityTimeout:     s.cfg.VisibilityTO,
			MessageAttributeNames: []string{"All"},
			AttributeNames:        []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
		})
		cancel()

		if err != nil {
			select {
			case <-time.After(250 * time.Millisecond):
				continue
			case <-ctx.Done():
				return
			}
		}

		for i := range out.Messages {
			msg := &out.Messages[i]
			select {
			case s.bufCh <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *SourceSQS) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
	})
}

func (s *SourceSQS) Receive(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m, ok := <-s.bufCh:
		if !ok {
			return nil, ErrClosed
		}
		return &message{src: s, m: m}, nil
	}
}

type ackable interface {
	AckHandle() ackHandle
}

func (s *SourceSQS) AckBatch(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	// Avoid building an intermediate slice of handles: build DeleteMessageBatch entries directly.
	// This significantly reduces allocations on hot acknowledgement paths.
	const max = 10
	entries := make([]sqstypes.DeleteMessageBatchRequestEntry, 0, max)
	in := sqs.DeleteMessageBatchInput{QueueUrl: s.queueURLPtr}

	for i := 0; i < len(msgs); i += max {
		end := i + max
		if end > len(msgs) {
			end = len(msgs)
		}

		entries = entries[:0]
		var ids [max]string
		var rhs [max]string

		for j := i; j < end; j++ {
			m := msgs[j]
			if m == nil {
				continue
			}
			am, ok := m.(ackable)
			if !ok {
				return fmt.Errorf("message does not support AckHandle(): %T", m)
			}
			h := am.AckHandle()
			k := len(entries)
			ids[k] = h.id
			rhs[k] = h.receiptHandle
			entries = append(entries, sqstypes.DeleteMessageBatchRequestEntry{Id: &ids[k], ReceiptHandle: &rhs[k]})
		}

		if len(entries) == 0 {
			continue
		}

		in.Entries = entries
		out, err := s.client.DeleteMessageBatch(ctx, &in)
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			return fmt.Errorf("sqs delete failed id=%s code=%s message=%s",
				aws.ToString(f.Id), aws.ToString(f.Code), aws.ToString(f.Message))
		}
	}
	return nil
}

func (s *SourceSQS) ackHandlesBatch(ctx context.Context, handles []ackHandle) error {
	const max = 10

	entries := make([]sqstypes.DeleteMessageBatchRequestEntry, 0, max)
	in := sqs.DeleteMessageBatchInput{QueueUrl: s.queueURLPtr}

	for i := 0; i < len(handles); i += max {
		end := i + max
		if end > len(handles) {
			end = len(handles)
		}

		entries = entries[:0]
		for j := i; j < end; j++ {
			// ponteiro direto pros campos do slice
			entries = append(entries, sqstypes.DeleteMessageBatchRequestEntry{
				Id:            &handles[j].id,
				ReceiptHandle: &handles[j].receiptHandle,
			})
		}

		in.Entries = entries
		out, err := s.client.DeleteMessageBatch(ctx, &in)
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			return fmt.Errorf("sqs delete failed id=%s code=%s message=%s",
				aws.ToString(f.Id), aws.ToString(f.Code), aws.ToString(f.Message))
		}
	}
	return nil
}

// AckBatchMeta is an optional, source-agnostic fast path used by AckGroup when
// available. It acknowledges messages using AckMetadata (ID/Handle).
func (s *SourceSQS) AckBatchMeta(ctx context.Context, metas []AckMetadata) error {
	if len(metas) == 0 {
		return nil
	}
	return s.ackMetasBatch(ctx, metas)
}

func (s *SourceSQS) ackMetasBatch(ctx context.Context, metas []AckMetadata) error {
	const max = 10

	entries := make([]sqstypes.DeleteMessageBatchRequestEntry, 0, max)
	in := sqs.DeleteMessageBatchInput{QueueUrl: s.queueURLPtr}

	for i := 0; i < len(metas); i += max {
		end := i + max
		if end > len(metas) {
			end = len(metas)
		}

		entries = entries[:0]
		for j := i; j < end; j++ {
			entries = append(entries, sqstypes.DeleteMessageBatchRequestEntry{
				Id:            &metas[j].ID,
				ReceiptHandle: &metas[j].Handle,
			})
		}

		in.Entries = entries
		out, err := s.client.DeleteMessageBatch(ctx, &in)
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			return fmt.Errorf("sqs delete failed id=%s code=%s message=%s",
				aws.ToString(f.Id), aws.ToString(f.Code), aws.ToString(f.Message))
		}
	}
	return nil
}

type ackHandle struct {
	id            string
	receiptHandle string
}

type message struct {
	src *SourceSQS
	m   *sqstypes.Message
}

func (m *message) Data() Envelope {
	return Envelope{Payload: aws.ToString(m.m.Body)}
}

func (m *message) AckHandle() ackHandle {
	id := ""
	if m.m.MessageId != nil {
		id = *m.m.MessageId
	}
	if id == "" {
		id = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return ackHandle{
		id:            id,
		receiptHandle: aws.ToString(m.m.ReceiptHandle),
	}
}

func (m *message) AckMeta() (AckMetadata, bool) {
	id := ""
	if m.m.MessageId != nil {
		id = *m.m.MessageId
	}
	if id == "" {
		id = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	rh := aws.ToString(m.m.ReceiptHandle)
	if rh == "" {
		return AckMetadata{}, false
	}
	return AckMetadata{ID: id, Handle: rh}, true
}

func (m *message) EstimatedSizeBytes() (int64, bool) {
	body := aws.ToString(m.m.Body)
	return int64(len(body)), true
}

func (m *message) Fail(ctx context.Context, err error) error {
	if m.src.cfg.FailVisibilityTimeoutSeconds == nil {
		return nil
	}
	_, callErr := m.src.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          m.src.queueURLPtr,
		ReceiptHandle:     m.m.ReceiptHandle,
		VisibilityTimeout: *m.src.cfg.FailVisibilityTimeoutSeconds,
	})
	if callErr != nil && !errors.Is(callErr, context.Canceled) && !errors.Is(callErr, context.DeadlineExceeded) {
		return callErr
	}
	return nil
}

func (s *SourceSQS) ExtendVisibilityBatch(ctx context.Context, metas []AckMetadata, visibilityTimeoutSeconds int32) error {
	if len(metas) == 0 {
		return nil
	}

	const max = 10
	in := sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: s.queueURLPtr,
	}

	entries := make([]sqstypes.ChangeMessageVisibilityBatchRequestEntry, 0, max)

	for i := 0; i < len(metas); i += max {
		end := i + max
		if end > len(metas) {
			end = len(metas)
		}

		entries = entries[:0]
		var ids [max]string
		var rhs [max]string

		for j := i; j < end; j++ {
			k := j - i
			ids[k] = metas[j].ID
			rhs[k] = metas[j].Handle

			entries = append(entries, sqstypes.ChangeMessageVisibilityBatchRequestEntry{
				Id:                &ids[k],
				ReceiptHandle:     &rhs[k],
				VisibilityTimeout: visibilityTimeoutSeconds,
			})
		}

		in.Entries = entries
		out, err := s.client.ChangeMessageVisibilityBatch(ctx, &in)
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			return fmt.Errorf("sqs visibility batch failed id=%s code=%s message=%s",
				aws.ToString(f.Id), aws.ToString(f.Code), aws.ToString(f.Message))
		}
	}

	return nil
}
