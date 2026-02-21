package ingestor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/sink"
	"github.com/baldanca/parquet-ingestor/source"
	"github.com/baldanca/parquet-ingestor/transformer"
)

//
// Test scaffolding / fakes
//

type tMsg struct {
	env    source.Envelope
	size   int64
	sizeOK bool

	meta source.AckMetadata

	failCalls int32
	lastFail  atomic.Value // stores error
}

func (m *tMsg) Data() source.Envelope             { return m.env }
func (m *tMsg) EstimatedSizeBytes() (int64, bool) { return m.size, m.sizeOK }
func (m *tMsg) Fail(ctx context.Context, reason error) error {
	atomic.AddInt32(&m.failCalls, 1)
	m.lastFail.Store(reason)
	return nil
}
func (m *tMsg) AckMeta() (source.AckMetadata, bool) { return m.meta, true }

var _ source.Message = (*tMsg)(nil)

type tSource struct {
	recvCh chan source.Message

	ackCalls int32
	ackFails int32 // number of times ack should fail

	writeDone atomic.Bool
}

func newTSource() *tSource {
	return &tSource{recvCh: make(chan source.Message, 1024)}
}

func (s *tSource) Receive(ctx context.Context) (source.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m := <-s.recvCh:
		return m, nil
	}
}

func (s *tSource) AckBatch(ctx context.Context, msgs []source.Message) error {
	if !s.writeDone.Load() {
		return errors.New("ack before write")
	}
	atomic.AddInt32(&s.ackCalls, 1)
	if atomic.LoadInt32(&s.ackFails) > 0 {
		atomic.AddInt32(&s.ackFails, -1)
		return errors.New("ack fail")
	}
	return nil
}

func (s *tSource) AckBatchMeta(ctx context.Context, metas []source.AckMetadata) error {
	if !s.writeDone.Load() {
		return errors.New("ack before write")
	}
	atomic.AddInt32(&s.ackCalls, 1)
	if atomic.LoadInt32(&s.ackFails) > 0 {
		atomic.AddInt32(&s.ackFails, -1)
		return errors.New("ack fail")
	}
	return nil
}

var _ source.Sourcer = (*tSource)(nil)

type tTransformer struct {
	fail bool
}

func (t tTransformer) Transform(ctx context.Context, in source.Envelope) (int, error) {
	if t.fail {
		return 0, errors.New("transform fail")
	}
	return 7, nil
}

var _ transformer.Transformer[int] = tTransformer{}

type tEncoder struct {
	ct  string
	ext string

	encodeCalls   int32
	encodeToCalls int32
}

func (e *tEncoder) ContentType() string   { return e.ct }
func (e *tEncoder) FileExtension() string { return e.ext }

func (e *tEncoder) Encode(ctx context.Context, items []int) ([]byte, error) {
	atomic.AddInt32(&e.encodeCalls, 1)
	return []byte("ENCODE"), nil
}

func (e *tEncoder) EncodeTo(ctx context.Context, items []int, w io.Writer) error {
	atomic.AddInt32(&e.encodeToCalls, 1)
	_, err := w.Write([]byte("ENCODE_TO"))
	return err
}

var _ encoder.Encoder[int] = (*tEncoder)(nil)
var _ encoder.StreamEncoder[int] = (*tEncoder)(nil)

type tSink struct {
	writeCalls       int32
	writeStreamCalls int32

	writeFails int32 // number of times write/stream should fail
}

func (s *tSink) Write(ctx context.Context, req sink.WriteRequest) error {
	atomic.AddInt32(&s.writeCalls, 1)
	if atomic.LoadInt32(&s.writeFails) > 0 {
		atomic.AddInt32(&s.writeFails, -1)
		return errors.New("write fail")
	}
	return nil
}

func (s *tSink) WriteStream(ctx context.Context, req sink.StreamWriteRequest) error {
	atomic.AddInt32(&s.writeStreamCalls, 1)
	if atomic.LoadInt32(&s.writeFails) > 0 {
		atomic.AddInt32(&s.writeFails, -1)
		return errors.New("write stream fail")
	}
	return req.Writer.Write(io.Discard)
}

var _ sink.Sinkr = (*tSink)(nil)
var _ sink.StreamSinkr = (*tSink)(nil)

type tSinkOnly struct {
	writeCalls int32
}

func (s *tSinkOnly) Write(ctx context.Context, req sink.WriteRequest) error {
	atomic.AddInt32(&s.writeCalls, 1)
	return nil
}

var _ sink.Sinkr = (*tSinkOnly)(nil)

//
// Retry helpers used by tests
//

type RetryPolicyFunc func(ctx context.Context, fn func(ctx context.Context) error) error

func (f RetryPolicyFunc) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	return f(ctx, fn)
}

//
// Tests
//

func TestIngestor_processMessage_TransformerFail_CallsFail(t *testing.T) {
	src := newTSource()
	tr := tTransformer{fail: true}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	keyFn := func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil }

	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = 10 * time.Second
	cfg.MaxEstimatedInputBytes = 1024

	ing, err := NewIngestor[int](cfg, src, tr, enc, sk, keyFn)
	if err != nil {
		t.Fatalf("NewIngestor: %v", err)
	}

	m := &tMsg{
		env:    source.Envelope{Payload: map[string]any{"a": 1}},
		size:   10,
		sizeOK: true,
		meta:   source.AckMetadata{ID: "id", Handle: "rh"},
	}
	flushNow, err := ing.processMessage(context.Background(), m)
	if err != nil {
		t.Fatalf("processMessage err: %v", err)
	}
	if flushNow {
		t.Fatalf("expected flushNow=false")
	}
	if atomic.LoadInt32(&m.failCalls) != 1 {
		t.Fatalf("Fail calls=%d want=1", m.failCalls)
	}
}

func TestIngestor_flush_UsesStreaming_WhenAvailable(t *testing.T) {
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = 1 * time.Hour
	cfg.MaxEstimatedInputBytes = 1 << 60

	src := newTSource()
	tr := tTransformer{}
	enc := &tEncoder{ct: "application/vnd.apache.parquet", ext: ".parquet"}
	sk := &tSink{}
	keyFn := func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil }

	ing, err := NewIngestor[int](cfg, src, tr, enc, sk, keyFn)
	if err != nil {
		t.Fatalf("NewIngestor: %v", err)
	}

	ing.SetRetryPolicy(RetryPolicyFunc(func(ctx context.Context, fn func(ctx context.Context) error) error {
		err := fn(ctx)
		if err == nil {
			src.writeDone.Store(true)
		}
		return err
	}))
	ing.SetAckRetryPolicy(nopRetry{})

	msg := &tMsg{
		env:    source.Envelope{Payload: "x"},
		size:   100,
		sizeOK: true,
		meta:   source.AckMetadata{ID: "id", Handle: "rh"},
	}
	for i := 0; i < 10; i++ {
		_, _ = ing.processMessage(context.Background(), msg)
	}

	if err := ing.flush(context.Background()); err != nil {
		t.Fatalf("flush err: %v", err)
	}

	if atomic.LoadInt32(&sk.writeStreamCalls) != 1 {
		t.Fatalf("WriteStreamCalls=%d want=1", sk.writeStreamCalls)
	}
	if atomic.LoadInt32(&sk.writeCalls) != 0 {
		t.Fatalf("WriteCalls=%d want=0", sk.writeCalls)
	}
	if atomic.LoadInt32(&enc.encodeToCalls) == 0 {
		t.Fatalf("expected EncodeTo to be called")
	}
	if atomic.LoadInt32(&enc.encodeCalls) != 0 {
		t.Fatalf("expected Encode not to be called")
	}
	if atomic.LoadInt32(&src.ackCalls) != 1 {
		t.Fatalf("ackCalls=%d want=1", src.ackCalls)
	}
}

func TestIngestor_flush_Fallback_WhenSinkNotStream(t *testing.T) {
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = 1 * time.Hour
	cfg.MaxEstimatedInputBytes = 1 << 60

	src := newTSource()
	tr := tTransformer{}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSinkOnly{}
	keyFn := func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil }

	ing, err := NewIngestor[int](cfg, src, tr, enc, sk, keyFn)
	if err != nil {
		t.Fatalf("NewIngestor: %v", err)
	}

	ing.SetRetryPolicy(RetryPolicyFunc(func(ctx context.Context, fn func(ctx context.Context) error) error {
		err := fn(ctx)
		if err == nil {
			src.writeDone.Store(true)
		}
		return err
	}))
	ing.SetAckRetryPolicy(nopRetry{})

	msg := &tMsg{
		env:    source.Envelope{Payload: "x"},
		size:   100,
		sizeOK: true,
		meta:   source.AckMetadata{ID: "id", Handle: "rh"},
	}
	for i := 0; i < 10; i++ {
		_, _ = ing.processMessage(context.Background(), msg)
	}
	if err := ing.flush(context.Background()); err != nil {
		t.Fatalf("flush err: %v", err)
	}

	if atomic.LoadInt32(&sk.writeCalls) != 1 {
		t.Fatalf("WriteCalls=%d want=1", sk.writeCalls)
	}
	if atomic.LoadInt32(&src.ackCalls) != 1 {
		t.Fatalf("ackCalls=%d want=1", src.ackCalls)
	}
}

func TestIngestor_flush_RetriesWriteAndAck(t *testing.T) {
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = 1 * time.Hour
	cfg.MaxEstimatedInputBytes = 1 << 60

	src := newTSource()
	src.ackFails = 2

	tr := tTransformer{}
	enc := &tEncoder{ct: "application/vnd.apache.parquet", ext: ".parquet"}
	sk := &tSink{writeFails: 2}

	keyFn := func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil }

	ing, err := NewIngestor[int](cfg, src, tr, enc, sk, keyFn)
	if err != nil {
		t.Fatalf("NewIngestor: %v", err)
	}

	ing.SetAckRetryPolicy(SimpleRetry{Attempts: 5, BaseDelay: 1, MaxDelay: 1, Jitter: false})

	ing.SetRetryPolicy(RetryPolicyFunc(func(ctx context.Context, fn func(ctx context.Context) error) error {
		return SimpleRetry{Attempts: 5, BaseDelay: 1, MaxDelay: 1, Jitter: false}.Do(ctx, func(ctx context.Context) error {
			err := fn(ctx)
			if err == nil {
				src.writeDone.Store(true)
			}
			return err
		})
	}))

	msg := &tMsg{
		env:    source.Envelope{Payload: "x"},
		size:   100,
		sizeOK: true,
		meta:   source.AckMetadata{ID: "id", Handle: "rh"},
	}
	for i := 0; i < 10; i++ {
		_, _ = ing.processMessage(context.Background(), msg)
	}

	if err := ing.flush(context.Background()); err != nil {
		t.Fatalf("flush err: %v", err)
	}

	if atomic.LoadInt32(&sk.writeStreamCalls) != 3 {
		t.Fatalf("WriteStreamCalls=%d want=3 (2 fails + 1 ok)", sk.writeStreamCalls)
	}
	if atomic.LoadInt32(&src.ackCalls) != 3 {
		t.Fatalf("AckCalls=%d want=3 (2 fails + 1 ok)", src.ackCalls)
	}
}

//
// Bench fakes
//

type bMsg struct {
	env    source.Envelope
	size   int64
	sizeOK bool
}

func (m *bMsg) Data() source.Envelope                        { return m.env }
func (m *bMsg) EstimatedSizeBytes() (int64, bool)            { return m.size, m.sizeOK }
func (m *bMsg) Fail(ctx context.Context, reason error) error { return nil }

type bSource struct{}

func (bSource) Receive(ctx context.Context) (source.Message, error) { return nil, context.Canceled }
func (bSource) AckBatch(ctx context.Context, msgs []source.Message) error {
	return nil
}
func (bSource) AckBatchMeta(ctx context.Context, metas []source.AckMetadata) error {
	return nil
}

var _ source.Sourcer = (*bSource)(nil)

type bTransformer struct{}

func (bTransformer) Transform(ctx context.Context, in source.Envelope) (int, error) { return 1, nil }

var _ transformer.Transformer[int] = bTransformer{}

type bEncoder struct {
	ct string
}

func (e *bEncoder) Encode(ctx context.Context, items []int) ([]byte, error) {
	return make([]byte, 32*1024), nil
}
func (e *bEncoder) FileExtension() string { return ".bin" }
func (e *bEncoder) ContentType() string   { return e.ct }

var _ encoder.Encoder[int] = (*bEncoder)(nil)

type bStreamEncoder struct {
	ct string
}

func (e *bStreamEncoder) Encode(ctx context.Context, items []int) ([]byte, error) {
	return make([]byte, 32*1024), nil
}
func (e *bStreamEncoder) EncodeTo(ctx context.Context, items []int, w io.Writer) error {
	_, err := w.Write([]byte("stream"))
	return err
}
func (e *bStreamEncoder) FileExtension() string { return ".parquet" }
func (e *bStreamEncoder) ContentType() string   { return e.ct }

var _ encoder.Encoder[int] = (*bStreamEncoder)(nil)
var _ encoder.StreamEncoder[int] = (*bStreamEncoder)(nil)

type bSink struct{}

func (bSink) Write(ctx context.Context, req sink.WriteRequest) error { return nil }

var _ sink.Sinkr = (*bSink)(nil)

type bStreamSink struct{}

func (bStreamSink) Write(ctx context.Context, req sink.WriteRequest) error { return nil }
func (bStreamSink) WriteStream(ctx context.Context, req sink.StreamWriteRequest) error {
	return req.Writer.Write(io.Discard)
}

var _ sink.Sinkr = (*bStreamSink)(nil)
var _ sink.StreamSinkr = (*bStreamSink)(nil)

func newCfg() batcher.BatcherConfig {
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = 1 * time.Hour
	cfg.MaxEstimatedInputBytes = 1 << 60
	cfg.ReuseBuffers = true
	return cfg
}

func fillN(ctx context.Context, ing *Ingestor[int], msgs []*bMsg, n int) {
	for i := 0; i < n; i++ {
		_, _ = ing.processMessage(ctx, msgs[i])
	}
}

func makeMsgs(n int) []*bMsg {
	msgs := make([]*bMsg, n)
	for i := 0; i < n; i++ {
		msgs[i] = &bMsg{
			env:    source.Envelope{Payload: "x"},
			size:   100,
			sizeOK: true,
		}
	}
	return msgs
}

func BenchmarkIngestor_FlushOnly_Fallback(b *testing.B) {
	for _, n := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			cfg := newCfg()
			ctx := context.Background()

			src := bSource{}
			tr := bTransformer{}
			enc := &bEncoder{ct: "application/octet-stream"}
			sk := &bSink{}
			keyFn := func(ctx context.Context, bb batcher.Batch[int]) (string, error) { return "k", nil }

			ing, err := NewIngestor[int](cfg, src, tr, enc, sk, keyFn)
			if err != nil {
				b.Fatalf("NewIngestor: %v", err)
			}

			msgs := makeMsgs(n)
			fillN(ctx, ing, msgs, n)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := ing.flush(ctx); err != nil {
					b.Fatalf("flush: %v", err)
				}
				b.StopTimer()
				fillN(ctx, ing, msgs, n)
				b.StartTimer()
			}
		})
	}
}

func BenchmarkIngestor_FlushOnly_Streaming(b *testing.B) {
	for _, n := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			cfg := newCfg()
			ctx := context.Background()

			src := bSource{}
			tr := bTransformer{}
			enc := &bStreamEncoder{ct: "application/vnd.apache.parquet"}
			sk := &bStreamSink{}
			keyFn := func(ctx context.Context, bb batcher.Batch[int]) (string, error) { return "k", nil }

			ing, err := NewIngestor[int](cfg, src, tr, enc, sk, keyFn)
			if err != nil {
				b.Fatalf("NewIngestor: %v", err)
			}

			msgs := makeMsgs(n)
			fillN(ctx, ing, msgs, n)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := ing.flush(ctx); err != nil {
					b.Fatalf("flush: %v", err)
				}
				b.StopTimer()
				fillN(ctx, ing, msgs, n)
				b.StartTimer()
			}
		})
	}
}

//
// Additional tests (fixed config fields)
//

func TestIngestor_flush_KeyFuncError_DoesNotAckAndReturnsError(t *testing.T) {
	src := newTSource()
	sk := &tSink{}
	tr := tTransformer{}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}

	src.writeDone.Store(true)

	bcfg := batcher.BatcherConfig{
		MaxEstimatedInputBytes: 1024,
		FlushInterval:          10 * time.Millisecond,
		MaxItems:               10_000,
		ReuseBuffers:           true,
	}

	keyErr := errors.New("key func error")
	keyFunc := func(ctx context.Context, b batcher.Batch[int]) (string, error) {
		return "", keyErr
	}

	ig, err := NewIngestor[int](bcfg, src, tr, enc, sk, keyFunc)
	if err != nil {
		t.Fatalf("NewIngestor: %v", err)
	}

	src.recvCh <- &tMsg{
		env:    source.Envelope{Payload: []byte(`{}`)},
		size:   1,
		sizeOK: true,
		meta:   source.AckMetadata{ID: "id", Handle: "rh"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	runErr := ig.Run(ctx, 1, 1)
	if runErr == nil || !errors.Is(runErr, keyErr) {
		t.Fatalf("expected key func error, got: %v", runErr)
	}

	if got := atomic.LoadInt32(&src.ackCalls); got != 0 {
		t.Fatalf("expected no acks, got %d", got)
	}
}

type failingStreamEncoder struct {
	ct string

	failFirst int32
}

func (e *failingStreamEncoder) Encode(ctx context.Context, items []int) ([]byte, error) {
	return []byte("fallback"), nil
}

func (e *failingStreamEncoder) EncodeTo(ctx context.Context, items []int, w io.Writer) error {
	if atomic.CompareAndSwapInt32(&e.failFirst, 1, 0) {
		return errors.New("encode-to fail")
	}
	_, err := w.Write([]byte("ok"))
	return err
}

func (e *failingStreamEncoder) FileExtension() string { return ".parquet" }
func (e *failingStreamEncoder) ContentType() string   { return e.ct }

var _ encoder.Encoder[int] = (*failingStreamEncoder)(nil)
var _ encoder.StreamEncoder[int] = (*failingStreamEncoder)(nil)

func TestIngestor_flush_Streaming_RetriesWhenEncodeToFails(t *testing.T) {
	src := newTSource()
	sk := &tSink{}
	tr := tTransformer{}
	enc := &failingStreamEncoder{ct: "application/octet-stream", failFirst: 1}

	src.writeDone.Store(true)

	bcfg := batcher.BatcherConfig{
		MaxEstimatedInputBytes: 1, // force flush quickly by size
		FlushInterval:          5 * time.Second,
		MaxItems:               10_000,
		ReuseBuffers:           true,
	}

	keyFunc := func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil }

	ig, err := NewIngestor[int](bcfg, src, tr, enc, sk, keyFunc)
	if err != nil {
		t.Fatalf("NewIngestor: %v", err)
	}

	ig.SetRetryPolicy(SimpleRetry{Attempts: 3, BaseDelay: 0, MaxDelay: 0, Jitter: false})
	ig.SetAckRetryPolicy(SimpleRetry{Attempts: 3, BaseDelay: 0, MaxDelay: 0, Jitter: false})

	src.recvCh <- &tMsg{
		env:    source.Envelope{Payload: []byte(`{}`)},
		size:   1024,
		sizeOK: true,
		meta:   source.AckMetadata{ID: "id", Handle: "rh"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = ig.Run(ctx, 1, 1)

	if got := atomic.LoadInt32(&sk.writeStreamCalls); got < 2 {
		t.Fatalf("expected streaming write retried at least once, got %d", got)
	}
	if got := atomic.LoadInt32(&src.ackCalls); got != 1 {
		t.Fatalf("expected exactly 1 ack, got %d", got)
	}
}

func TestIngestor_Run_Cancel_FlushesRemainingBatch(t *testing.T) {
	src := newTSource()
	sk := &tSink{}
	tr := tTransformer{}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}

	src.writeDone.Store(true)

	bcfg := batcher.BatcherConfig{
		MaxEstimatedInputBytes: 10 * 1024 * 1024,
		FlushInterval:          10 * time.Second, // avoid time flush
		MaxItems:               10_000,
		ReuseBuffers:           true,
	}

	keyFunc := func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil }

	ig, err := NewIngestor[int](bcfg, src, tr, enc, sk, keyFunc)
	if err != nil {
		t.Fatalf("NewIngestor: %v", err)
	}

	ig.SetRetryPolicy(nopRetry{})
	ig.SetAckRetryPolicy(nopRetry{})

	for i := 0; i < 10; i++ {
		src.recvCh <- &tMsg{
			env:    source.Envelope{Payload: []byte(`{}`)},
			size:   1,
			sizeOK: true,
			meta:   source.AckMetadata{ID: "id", Handle: "rh"},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	_ = ig.Run(ctx, 2, 16)

	if got := atomic.LoadInt32(&src.ackCalls); got != 1 {
		t.Fatalf("expected exactly 1 ack on shutdown flush, got %d", got)
	}
	if w := atomic.LoadInt32(&sk.writeCalls) + atomic.LoadInt32(&sk.writeStreamCalls); w != 1 {
		t.Fatalf("expected exactly 1 write on shutdown flush, got %d", w)
	}
}
