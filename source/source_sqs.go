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
	"github.com/baldanca/parquet-ingestor/observability"
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
	Logger                       observability.Logger
	Metrics                      *observability.Registry
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
	Logger:          observability.NopLogger(),
	Metrics:         &observability.Registry{},
}

type sqsAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
}

// SourceSQS is an Amazon SQS source with internal long-polling workers.
type SourceSQS struct {
	cfg SourceSQSConfig

	client      sqsAPI
	queueURL    string
	queueURLPtr *string

	bufCh chan *sqstypes.Message

	closeOnce sync.Once
	rootCtx   context.Context
	cancel    context.CancelFunc

	wg sync.WaitGroup

	mu      sync.Mutex
	pollers []context.CancelFunc
	closed  bool
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
	if cfg.Logger == nil {
		cfg.Logger = observability.NopLogger()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = &observability.Registry{}
	}

	ctx, cancel := context.WithCancel(ctx)

	s := &SourceSQS{
		cfg:      cfg,
		client:   client,
		queueURL: queueURL,
		bufCh:    make(chan *sqstypes.Message, cfg.BufSize),
		rootCtx:  ctx,
		cancel:   cancel,
	}
	s.queueURLPtr = &s.queueURL
	s.cfg.Metrics.SetGauge("source_sqs_pollers", int64(cfg.Pollers))
	s.cfg.Metrics.SetGauge("source_sqs_buffer_capacity", int64(cfg.BufSize))
	s.startPollers(cfg.Pollers)
	return s
}

// NewSourceSQS creates a SourceSQS using DefaultSourceSQSConfig.
func NewSourceSQS(ctx context.Context, client sqsAPI, queueURL string) *SourceSQS {
	return NewSourceSQSWithConfig(ctx, client, queueURL, DefaultSourceSQSConfig)
}

func (s *SourceSQS) startPollers(n int) {
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(s.rootCtx)
		s.pollers = append(s.pollers, cancel)
		s.wg.Add(1)
		go func(id int, pollCtx context.Context) {
			defer s.wg.Done()
			s.cfg.Logger.Debug("source_sqs.poller.started", "poller_id", id)
			s.pollLoop(pollCtx)
			s.cfg.Logger.Debug("source_sqs.poller.stopped", "poller_id", id)
		}(len(s.pollers), ctx)
	}
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
			s.cfg.Metrics.AddCounter("source_sqs_receive_errors_total", 1)
			select {
			case <-time.After(250 * time.Millisecond):
				continue
			case <-ctx.Done():
				return
			}
		}
		s.cfg.Metrics.AddCounter("source_sqs_receive_calls_total", 1)
		s.cfg.Metrics.AddCounter("source_sqs_messages_received_total", int64(len(out.Messages)))

		for i := range out.Messages {
			msg := &out.Messages[i]
			select {
			case s.bufCh <- msg:
				s.cfg.Metrics.SetGauge("source_sqs_buffer_used", int64(len(s.bufCh)))
			case <-ctx.Done():
				return
			}
		}
	}
}

// SetPollers resizes the number of active SQS pollers at runtime.
func (s *SourceSQS) SetPollers(n int) {
	if n < 1 {
		n = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	current := len(s.pollers)
	if n == current {
		return
	}
	if n > current {
		s.cfg.Logger.Info("source_sqs.pollers.scale_up", "from", current, "to", n)
		s.startPollers(n - current)
	} else {
		s.cfg.Logger.Info("source_sqs.pollers.scale_down", "from", current, "to", n)
		for i := current - 1; i >= n; i-- {
			s.pollers[i]()
		}
		s.pollers = s.pollers[:n]
	}
	s.cfg.Metrics.SetGauge("source_sqs_pollers", int64(len(s.pollers)))
}

// Pollers returns the current number of active pollers.
func (s *SourceSQS) Pollers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pollers)
}

// BufferUsage returns current source buffer usage.
func (s *SourceSQS) BufferUsage() (used, capacity int) {
	return len(s.bufCh), cap(s.bufCh)
}

// Close stops polling and closes the internal message channel once all pollers exit.
func (s *SourceSQS) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.cancel()
		s.wg.Wait()
		close(s.bufCh)
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
		s.cfg.Metrics.SetGauge("source_sqs_buffer_used", int64(len(s.bufCh)))
		return &message{src: s, m: m}, nil
	}
}

type ackable interface{ AckHandle() ackHandle }

// AckBatch deletes messages from SQS in batches of up to 10.
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
			if im, ok := m.(*message); ok && im != nil && im.m != nil && im.m.ReceiptHandle != nil {
				entries[n].ReceiptHandle = im.m.ReceiptHandle
				n++
				continue
			}
			am, ok := m.(ackable)
			if !ok {
				return fmt.Errorf("message does not support AckHandle(): %T", m)
			}
			h := am.AckHandle()
			rh := h.receiptHandle
			entries[n].ReceiptHandle = &rh
			n++
		}
		if n == 0 {
			continue
		}
		in.Entries = entries[:n]
		out, err := s.client.DeleteMessageBatch(ctx, &in)
		if err != nil {
			s.cfg.Metrics.AddCounter("source_sqs_ack_errors_total", 1)
			return err
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			code, msg := aws.ToString(f.Code), aws.ToString(f.Message)
			s.cfg.Metrics.AddCounter("source_sqs_ack_errors_total", 1)
			return fmt.Errorf("delete batch failed: code=%s message=%s", code, msg)
		}
		s.cfg.Metrics.AddCounter("source_sqs_acked_total", int64(n))
	}
	return nil
}

// AckBatchMeta deletes messages using compact metadata handles.
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
			rh := metas[j].Handle
			if rh == "" {
				continue
			}
			entries[n].Id = sqsBatchIDPtrs[n]
			entries[n].ReceiptHandle = &rh
			n++
		}
		if n == 0 {
			continue
		}
		in.Entries = entries[:n]
		out, err := s.client.DeleteMessageBatch(ctx, &in)
		if err != nil {
			s.cfg.Metrics.AddCounter("source_sqs_ack_errors_total", 1)
			return err
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			code, msg := aws.ToString(f.Code), aws.ToString(f.Message)
			s.cfg.Metrics.AddCounter("source_sqs_ack_errors_total", 1)
			return fmt.Errorf("delete batch failed: code=%s message=%s", code, msg)
		}
		s.cfg.Metrics.AddCounter("source_sqs_acked_total", int64(n))
	}
	return nil
}

// ExtendVisibilityBatch updates visibility timeout in batch requests.
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
			s.cfg.Metrics.AddCounter("source_sqs_visibility_errors_total", 1)
			return err
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			code, msg := aws.ToString(f.Code), aws.ToString(f.Message)
			s.cfg.Metrics.AddCounter("source_sqs_visibility_errors_total", 1)
			return fmt.Errorf("change visibility batch failed: code=%s message=%s", code, msg)
		}
		s.cfg.Metrics.AddCounter("source_sqs_visibility_extensions_total", int64(n))
	}
	return nil
}

type message struct {
	src *SourceSQS
	m   *sqstypes.Message
}

func (m *message) Data() Envelope                    { return Envelope{Payload: aws.ToString(m.m.Body)} }
func (m *message) EstimatedSizeBytes() (int64, bool) { return int64(len(aws.ToString(m.m.Body))), true }
func (m *message) Fail(ctx context.Context, reason error) error {
	if m.src.cfg.FailVisibilityTimeoutSeconds == nil {
		return nil
	}
	_, err := m.src.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          m.src.queueURLPtr,
		ReceiptHandle:     m.m.ReceiptHandle,
		VisibilityTimeout: *m.src.cfg.FailVisibilityTimeoutSeconds,
	})
	if err != nil {
		m.src.cfg.Metrics.AddCounter("source_sqs_fail_visibility_errors_total", 1)
	}
	return err
}

type ackHandle struct{ id, receiptHandle string }

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
