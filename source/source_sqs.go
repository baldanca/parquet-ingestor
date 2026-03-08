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

// ErrClosed is returned when Receive is called after the source has been closed.
var ErrClosed = errors.New("source closed")

// sqsBatchIDPtrs provides stable pointers for DeleteMessageBatch/ChangeMessageVisibilityBatch Entry IDs.
// SQS only requires IDs to be unique within the request; they do not need to match MessageId.
var (
	sqsBatchIDStrings = [10]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	sqsBatchIDPtrs    = [10]*string{
		&sqsBatchIDStrings[0],
		&sqsBatchIDStrings[1],
		&sqsBatchIDStrings[2],
		&sqsBatchIDStrings[3],
		&sqsBatchIDStrings[4],
		&sqsBatchIDStrings[5],
		&sqsBatchIDStrings[6],
		&sqsBatchIDStrings[7],
		&sqsBatchIDStrings[8],
		&sqsBatchIDStrings[9],
	}
)

// SourceSQSConfig controls the SQS polling behavior.
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

// DefaultSourceSQSConfig provides sensible defaults for long polling.
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

// SourceSQS is an Amazon SQS source with internal long-polling workers.
//
// Close cancels pollers and will eventually make Receive return ErrClosed.
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

// NewSourceSQSWithConfig creates a SourceSQS.
func NewSourceSQSWithConfig(ctx context.Context, client sqsAPI, queueURL string, cfg SourceSQSConfig) *SourceSQS {
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

// NewSourceSQS creates a SourceSQS using DefaultSourceSQSConfig.
func NewSourceSQS(ctx context.Context, client sqsAPI, queueURL string) *SourceSQS {
	return NewSourceSQSWithConfig(ctx, client, queueURL, DefaultSourceSQSConfig)
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

// Close stops polling.
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

// AckBatch deletes messages from SQS in batches of up to 10.
// It tries a fast-path for internal *message values to avoid allocations.
func (s *SourceSQS) AckBatch(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	const max = 10
	entries := make([]sqstypes.DeleteMessageBatchRequestEntry, max)
	in := sqs.DeleteMessageBatchInput{QueueUrl: s.queueURLPtr}

	for i := 0; i < len(msgs); i += max {
		end := i + max
		if end > len(msgs) {
			end = len(msgs)
		}

		n := 0
		for j := i; j < end; j++ {
			m := msgs[j]
			if m == nil {
				continue
			}

			entries[n].Id = sqsBatchIDPtrs[n]

			// Fast path: internal message already has pointers compatible with AWS SDK.
			if im, ok := m.(*message); ok && im != nil && im.m != nil && im.m.ReceiptHandle != nil {
				entries[n].ReceiptHandle = im.m.ReceiptHandle
				n++
				continue
			}

			// Fallback: interface-based ack handle (may allocate depending on implementation).
			am, ok := m.(ackable)
			if !ok {
				return fmt.Errorf("message does not support AckHandle(): %T", m)
			}
			h := am.AckHandle()
			rh := h.receiptHandle
			entries[n].ReceiptHandle = &rh // escapes for non-internal messages
			n++
		}

		if n == 0 {
			continue
		}

		in.Entries = entries[:n]
		out, err := s.client.DeleteMessageBatch(ctx, &in)
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			code, msg := aws.ToString(f.Code), aws.ToString(f.Message)
			return fmt.Errorf("delete message batch failed: code=%s message=%s", code, msg)
		}
	}

	return nil
}

// AckBatchMeta deletes messages from SQS in batches of up to 10 using AckMetadata.
// It uses stable Entry IDs and takes ReceiptHandle pointers directly from the metas slice to avoid per-entry heap allocations.
func (s *SourceSQS) AckBatchMeta(ctx context.Context, metas []AckMetadata) error {
	if len(metas) == 0 {
		return nil
	}

	const max = 10
	entries := make([]sqstypes.DeleteMessageBatchRequestEntry, max)
	in := sqs.DeleteMessageBatchInput{QueueUrl: s.queueURLPtr}

	for i := 0; i < len(metas); i += max {
		end := i + max
		if end > len(metas) {
			end = len(metas)
		}

		n := 0
		for j := i; j < end; j++ {
			rh := &metas[j].Handle
			if *rh == "" {
				continue
			}
			entries[n].Id = sqsBatchIDPtrs[n]
			entries[n].ReceiptHandle = rh
			n++
		}

		if n == 0 {
			continue
		}

		in.Entries = entries[:n]
		out, err := s.client.DeleteMessageBatch(ctx, &in)
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			code, msg := aws.ToString(f.Code), aws.ToString(f.Message)
			return fmt.Errorf("delete message batch failed: code=%s message=%s", code, msg)
		}
	}

	return nil
}

// ExtendVisibilityBatch extends the visibility timeout for a batch of messages.
// The request is sent in chunks of up to 10 entries.
func (s *SourceSQS) ExtendVisibilityBatch(ctx context.Context, metas []AckMetadata, timeoutSeconds int32) error {
	if len(metas) == 0 {
		return nil
	}
	if timeoutSeconds < 0 {
		return fmt.Errorf("timeoutSeconds must be non-negative")
	}

	const max = 10
	entries := make([]sqstypes.ChangeMessageVisibilityBatchRequestEntry, max)
	in := sqs.ChangeMessageVisibilityBatchInput{QueueUrl: s.queueURLPtr}

	for i := 0; i < len(metas); i += max {
		end := i + max
		if end > len(metas) {
			end = len(metas)
		}

		n := 0
		for j := i; j < end; j++ {
			rh := &metas[j].Handle
			if *rh == "" {
				continue
			}
			entries[n].Id = sqsBatchIDPtrs[n]
			entries[n].ReceiptHandle = rh
			entries[n].VisibilityTimeout = timeoutSeconds
			n++
		}

		if n == 0 {
			continue
		}

		in.Entries = entries[:n]
		out, err := s.client.ChangeMessageVisibilityBatch(ctx, &in)
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			code, msg := aws.ToString(f.Code), aws.ToString(f.Message)
			return fmt.Errorf("change visibility batch failed: code=%s message=%s", code, msg)
		}
	}

	return nil
}

type message struct {
	src *SourceSQS
	m   *sqstypes.Message
}

func (m *message) Data() Envelope {
	return Envelope{Payload: aws.ToString(m.m.Body)}
}

func (m *message) EstimatedSizeBytes() (int64, bool) {
	return int64(len(aws.ToString(m.m.Body))), true
}

func (m *message) Fail(ctx context.Context, reason error) error {
	if m.src.cfg.FailVisibilityTimeoutSeconds == nil {
		return nil
	}
	_, err := m.src.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          m.src.queueURLPtr,
		ReceiptHandle:     m.m.ReceiptHandle,
		VisibilityTimeout: *m.src.cfg.FailVisibilityTimeoutSeconds,
	})
	return err
}

type ackHandle struct {
	id            string
	receiptHandle string
}

// Improvement #3: avoid aws.ToString in hot-path extraction (no helper call, no extra work).
func (m *message) AckHandle() ackHandle {
	var id, rh string
	if m.m.MessageId != nil {
		id = *m.m.MessageId
	}
	if m.m.ReceiptHandle != nil {
		rh = *m.m.ReceiptHandle
	}
	return ackHandle{id: id, receiptHandle: rh}
}

// Improvement #3: same here (still returns strings, but avoids aws.ToString calls).
func (m *message) AckMeta() (AckMetadata, bool) {
	var id, rh string
	if m.m.MessageId != nil {
		id = *m.m.MessageId
	}
	if m.m.ReceiptHandle != nil {
		rh = *m.m.ReceiptHandle
	}
	return AckMetadata{ID: id, Handle: rh}, true
}
