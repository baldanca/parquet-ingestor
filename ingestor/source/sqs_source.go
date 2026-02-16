package source

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Source struct {
	client   *sqs.Client
	queueURL string

	waitTimeSeconds int32
	maxMessages     int32
	visibilityTO    int32

	bufCh chan *sqstypes.Message

	failVisibilityTimeoutSeconds *int32

	closeOnce sync.Once
	closed    chan struct{}
}

type Option func(*Source)

func WithWaitTimeSeconds(v int32) Option {
	return func(s *Source) { s.waitTimeSeconds = v }
}

func WithMaxMessages(v int32) Option {
	return func(s *Source) { s.maxMessages = v }
}

func WithVisibilityTimeoutSeconds(v int32) Option {
	return func(s *Source) { s.visibilityTO = v }
}

func WithPrefetchBuffer(n int) Option {
	return func(s *Source) {
		if n < 1 {
			n = 1
		}
		s.bufCh = make(chan *sqstypes.Message, n)
	}
}

func WithFailVisibilityTimeoutSeconds(v int32) Option {
	return func(s *Source) { s.failVisibilityTimeoutSeconds = &v }
}

func New(client *sqs.Client, queueURL string, opts ...Option) *Source {
	s := &Source{
		client:          client,
		queueURL:        queueURL,
		waitTimeSeconds: 20,
		maxMessages:     10,
		visibilityTO:    30,
		bufCh:           make(chan *sqstypes.Message, 256),
		closed:          make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	// Clamp SQS API limits.
	if s.maxMessages < 1 {
		s.maxMessages = 1
	}
	if s.maxMessages > 10 {
		s.maxMessages = 10
	}
	if s.waitTimeSeconds < 0 {
		s.waitTimeSeconds = 0
	}
	if s.waitTimeSeconds > 20 {
		s.waitTimeSeconds = 20
	}
	go s.prefetchLoop()
	return s
}

func (s *Source) prefetchLoop() {
	defer close(s.bufCh)
	for {
		select {
		case <-s.closed:
			return
		default:
		}

		if len(s.bufCh) == cap(s.bufCh) {
			select {
			case <-time.After(50 * time.Millisecond):
				continue
			case <-s.closed:
				return
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.waitTimeSeconds+5)*time.Second)
		out, err := s.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(s.queueURL),
			MaxNumberOfMessages: s.maxMessages,
			WaitTimeSeconds:     s.waitTimeSeconds,
			VisibilityTimeout:   s.visibilityTO,
			MessageAttributeNames: []string{
				"All",
			},
			AttributeNames: []sqstypes.QueueAttributeName{
				sqstypes.QueueAttributeNameAll,
			},
		})
		cancel()
		if err != nil {
			select {
			case <-time.After(250 * time.Millisecond):
				continue
			case <-s.closed:
				return
			}
		}
		for i := range out.Messages {
			msg := out.Messages[i]
			select {
			case s.bufCh <- &msg:
			case <-s.closed:
				return
			}
		}
	}
}

func (s *Source) Close() {
	s.closeOnce.Do(func() { close(s.closed) })
}

func (s *Source) Receive(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m, ok := <-s.bufCh:
		if !ok {
			return nil, context.Canceled
		}
		return &message{src: s, m: m}, nil
	}
}

func (s *Source) AckBatch(ctx context.Context, handles []ackHandle) error {
	const max = 10
	for i := 0; i < len(handles); i += max {
		end := i + max
		if end > len(handles) {
			end = len(handles)
		}
		entries := make([]sqstypes.DeleteMessageBatchRequestEntry, 0, end-i)
		for j := i; j < end; j++ {
			h := handles[j]
			entries = append(entries, sqstypes.DeleteMessageBatchRequestEntry{
				Id:            aws.String(h.id),
				ReceiptHandle: aws.String(h.receiptHandle),
			})
		}
		out, err := s.client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(s.queueURL),
			Entries:  entries,
		})
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
	src *Source
	m   *sqstypes.Message
}

func (m *message) Data() Envelope {
	return Envelope{Payload: aws.ToString(m.m.Body)}
}

func (m *message) AckHandle() ackHandle {
	id := aws.ToString(m.m.MessageId)
	if id == "" {
		id = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return ackHandle{
		id:            id,
		receiptHandle: aws.ToString(m.m.ReceiptHandle),
	}
}

func (m *message) EstimatedSizeBytes() (int64, bool) {
	body := aws.ToString(m.m.Body)
	return int64(len(body)), true
}

func (m *message) Fail(ctx context.Context, err error) error {
	if m.src.failVisibilityTimeoutSeconds == nil {
		return nil
	}
	_, callErr := m.src.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(m.src.queueURL),
		ReceiptHandle:     m.m.ReceiptHandle,
		VisibilityTimeout: *m.src.failVisibilityTimeoutSeconds,
	})
	if callErr != nil && !errors.Is(callErr, context.Canceled) {
		return callErr
	}
	return nil
}
