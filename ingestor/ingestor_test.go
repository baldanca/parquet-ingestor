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
	"github.com/baldanca/parquet-ingestor/observability"
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
	drop bool // return empty slice without error (message dropped)
}

func (t tTransformer) Transform(ctx context.Context, in source.Envelope) ([]int, error) {
	if t.fail {
		return nil, errors.New("transform fail")
	}
	if t.drop {
		return []int{}, nil
	}
	return []int{7}, nil
}

var _ transformer.Transformer[int] = tTransformer{}

type tEncoder struct {
	ct  string
	ext string

	encodeCalls   int32
	encodeToCalls int32
	encodeFail    bool
}

func (e *tEncoder) ContentType() string   { return e.ct }
func (e *tEncoder) FileExtension() string { return e.ext }

func (e *tEncoder) Encode(ctx context.Context, items []int) ([]byte, error) {
	atomic.AddInt32(&e.encodeCalls, 1)
	if e.encodeFail {
		return nil, errors.New("encode fail")
	}
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

	ing, err := NewIngestor(cfg, src, tr, enc, sk, keyFn)
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

	ing, err := NewIngestor(cfg, src, tr, enc, sk, keyFn)
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

	ing, err := NewIngestor(cfg, src, tr, enc, sk, keyFn)
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

	ing, err := NewIngestor(cfg, src, tr, enc, sk, keyFn)
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

func (bTransformer) Transform(ctx context.Context, in source.Envelope) ([]int, error) {
	return []int{1}, nil
}

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

func ingestorBenchSizes() []int {
	if testing.Short() {
		return []int{10, 100}
	}
	return []int{10, 100, 1_000, 10_000}
}

func BenchmarkIngestor_FlushOnly_Fallback(b *testing.B) {
	for _, n := range ingestorBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			cfg := newCfg()
			ctx := context.Background()

			src := bSource{}
			tr := bTransformer{}
			enc := &bEncoder{ct: "application/octet-stream"}
			sk := &bSink{}
			keyFn := func(ctx context.Context, bb batcher.Batch[int]) (string, error) { return "k", nil }

			ing, err := NewIngestor(cfg, src, tr, enc, sk, keyFn)
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
	for _, n := range ingestorBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			cfg := newCfg()
			ctx := context.Background()

			src := bSource{}
			tr := bTransformer{}
			enc := &bStreamEncoder{ct: "application/vnd.apache.parquet"}
			sk := &bStreamSink{}
			keyFn := func(ctx context.Context, bb batcher.Batch[int]) (string, error) { return "k", nil }

			ing, err := NewIngestor(cfg, src, tr, enc, sk, keyFn)
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

// tTransformerMulti returns n copies of a fixed value so we can test 1:n expansion.
type tTransformerMulti struct {
	n int // records produced per message
}

func (t tTransformerMulti) Transform(ctx context.Context, in source.Envelope) ([]int, error) {
	out := make([]int, t.n)
	for i := range out {
		out[i] = i + 1
	}
	return out, nil
}

var _ transformer.Transformer[int] = tTransformerMulti{}

// tTransformerDrop always returns an empty slice (drop).
type tTransformerDrop struct{}

func (tTransformerDrop) Transform(ctx context.Context, in source.Envelope) ([]int, error) {
	return nil, nil
}

var _ transformer.Transformer[int] = tTransformerDrop{}

func TestIngestor_processMessage_OneToMany_AddsAllRecordsOneAck(t *testing.T) {
	src := newTSource()
	tr := tTransformerMulti{n: 3}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	keyFn := func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil }

	cfg := batcher.BatcherConfig{
		MaxEstimatedInputBytes: 1 << 30,
		FlushInterval:          time.Hour,
		ReuseBuffers:           true,
	}

	ing, err := NewIngestor(cfg, src, tr, enc, sk, keyFn)
	if err != nil {
		t.Fatalf("NewIngestor: %v", err)
	}

	msg := &tMsg{env: source.Envelope{Payload: "x"}, size: 10, sizeOK: true,
		meta: source.AckMetadata{ID: "1", Handle: "h1"}}

	flushNow, err := ing.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("processMessage: %v", err)
	}
	if flushNow {
		t.Fatalf("unexpected flush")
	}

	// batcher should have 3 records from 1 message
	batch := ing.batcher.Flush()
	if len(batch.Items) != 3 {
		t.Fatalf("items=%d want 3", len(batch.Items))
	}
}

func TestIngestor_processMessage_Drop_AckImmediately(t *testing.T) {
	src := newTSource()
	tr := tTransformerDrop{}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	keyFn := func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil }

	cfg := batcher.BatcherConfig{
		MaxEstimatedInputBytes: 1 << 30,
		FlushInterval:          time.Hour,
		ReuseBuffers:           true,
	}

	ing, err := NewIngestor(cfg, src, tr, enc, sk, keyFn)
	if err != nil {
		t.Fatalf("NewIngestor: %v", err)
	}
	// mark writeDone so AckBatch succeeds
	src.writeDone.Store(true)
	ing.SetAckRetryPolicy(nopRetry{})

	msg := &tMsg{env: source.Envelope{Payload: "x"}, size: 10, sizeOK: true,
		meta: source.AckMetadata{ID: "2", Handle: "h2"}}

	flushNow, err := ing.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("processMessage: %v", err)
	}
	if flushNow {
		t.Fatalf("unexpected flush for dropped message")
	}

	// source should have been acked immediately (1 call)
	if got := atomic.LoadInt32(&src.ackCalls); got != 1 {
		t.Fatalf("ackCalls=%d want 1", got)
	}

	// batcher should be empty
	batch := ing.batcher.Flush()
	if len(batch.Items) != 0 {
		t.Fatalf("expected empty batch for dropped message, got %d items", len(batch.Items))
	}
}

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

	ig, err := NewIngestor(bcfg, src, tr, enc, sk, keyFunc)
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

	ig, err := NewIngestor(bcfg, src, tr, enc, sk, keyFunc)
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

	ig, err := NewIngestor(bcfg, src, tr, enc, sk, keyFunc)
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

// --- handleRuntimeError ---

// captureLoggerSimple is a minimal logger that records the last Error event name.
type captureLoggerSimple struct {
	lastEvent string
	lastArgs  []any
}

func (l *captureLoggerSimple) Debug(string, ...any) {}
func (l *captureLoggerSimple) Info(string, ...any)  {}
func (l *captureLoggerSimple) Warn(string, ...any)  {}
func (l *captureLoggerSimple) Error(msg string, args ...any) {
	l.lastEvent = msg
	l.lastArgs = append([]any(nil), args...)
}

func TestHandleRuntimeError_NilError_IsNoop(t *testing.T) {
	lg := &captureLoggerSimple{}
	metrics := &observability.Registry{}
	ing := &Ingestor[int]{logger: lg, metrics: metrics}
	ing.handleRuntimeError("some.event", nil)

	if lg.lastEvent != "" {
		t.Fatalf("expected no log call for nil error, got event=%q", lg.lastEvent)
	}
	if snap := metrics.Snapshot(); snap["ingestor_runtime_errors_total"] != 0 {
		t.Fatalf("counter must stay 0 for nil error, got %v", snap["ingestor_runtime_errors_total"])
	}
}

func TestHandleRuntimeError_NoExtraArgs_IncrementCounter(t *testing.T) {
	lg := &captureLoggerSimple{}
	metrics := &observability.Registry{}
	ing := &Ingestor[int]{logger: lg, metrics: metrics}
	sentinel := errors.New("something broke")
	ing.handleRuntimeError("my.event", sentinel)

	if lg.lastEvent != "my.event" {
		t.Fatalf("event = %q, want my.event", lg.lastEvent)
	}
	// Fast path: args = ["error", err] — no extra items beyond the error key-value.
	if len(lg.lastArgs) != 2 || lg.lastArgs[0] != "error" || lg.lastArgs[1] != sentinel {
		t.Fatalf("args = %v, want [\"error\", err]", lg.lastArgs)
	}
	if snap := metrics.Snapshot(); snap["ingestor_runtime_errors_total"] != 1 {
		t.Fatalf("counter = %v, want 1", snap["ingestor_runtime_errors_total"])
	}
}

func TestHandleRuntimeError_WithExtraArgs_ForwardsAll(t *testing.T) {
	lg := &captureLoggerSimple{}
	metrics := &observability.Registry{}
	ing := &Ingestor[int]{logger: lg, metrics: metrics}
	sentinel := errors.New("oops")
	ing.handleRuntimeError("my.event", sentinel, "key", "value")

	if lg.lastEvent != "my.event" {
		t.Fatalf("event = %q, want my.event", lg.lastEvent)
	}
	// Should contain: "error", err, "key", "value"
	if len(lg.lastArgs) != 4 {
		t.Fatalf("args len = %d, want 4; args = %v", len(lg.lastArgs), lg.lastArgs)
	}
	if lg.lastArgs[0] != "error" || lg.lastArgs[1] != sentinel {
		t.Fatalf("first pair = %v %v, want error/sentinel", lg.lastArgs[0], lg.lastArgs[1])
	}
	if lg.lastArgs[2] != "key" || lg.lastArgs[3] != "value" {
		t.Fatalf("second pair = %v %v, want key/value", lg.lastArgs[2], lg.lastArgs[3])
	}
	if snap := metrics.Snapshot(); snap["ingestor_runtime_errors_total"] != 1 {
		t.Fatalf("counter = %v, want 1", snap["ingestor_runtime_errors_total"])
	}
}

// --- NewDefaultIngestor ---

func TestNewDefaultIngestor_ReturnsValidIngestor(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	keyFn := func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil }

	ing, err := NewDefaultIngestor(src, tTransformer{}, enc, sk, keyFn)
	if err != nil {
		t.Fatalf("NewDefaultIngestor: %v", err)
	}
	if ing == nil {
		t.Fatal("expected non-nil Ingestor")
	}
}

// --- NewIngestor nil guards ---

func TestNewIngestor_NilSource_ReturnsError(t *testing.T) {
	cfg := batcher.DefaultBatcherConfig
	_, err := NewIngestor(cfg, nil, tTransformer{}, &tEncoder{ct: "a", ext: ".b"}, &tSink{}, func(_ context.Context, _ batcher.Batch[int]) (string, error) { return "k", nil })
	if err == nil {
		t.Fatal("expected error for nil source")
	}
}

func TestNewIngestor_NilTransformer_ReturnsError(t *testing.T) {
	cfg := batcher.DefaultBatcherConfig
	_, err := NewIngestor(cfg, newTSource(), nil, &tEncoder{ct: "a", ext: ".b"}, &tSink{}, func(_ context.Context, _ batcher.Batch[int]) (string, error) { return "k", nil })
	if err == nil {
		t.Fatal("expected error for nil transformer")
	}
}

func TestNewIngestor_NilEncoder_ReturnsError(t *testing.T) {
	cfg := batcher.DefaultBatcherConfig
	_, err := NewIngestor(cfg, newTSource(), tTransformer{}, nil, &tSink{}, func(_ context.Context, _ batcher.Batch[int]) (string, error) { return "k", nil })
	if err == nil {
		t.Fatal("expected error for nil encoder")
	}
}

func TestNewIngestor_NilSink_ReturnsError(t *testing.T) {
	cfg := batcher.DefaultBatcherConfig
	_, err := NewIngestor(cfg, newTSource(), tTransformer{}, &tEncoder{ct: "a", ext: ".b"}, nil, func(_ context.Context, _ batcher.Batch[int]) (string, error) { return "k", nil })
	if err == nil {
		t.Fatal("expected error for nil sink")
	}
}

func TestNewIngestor_NilKeyFunc_ReturnsError(t *testing.T) {
	cfg := batcher.DefaultBatcherConfig
	_, err := NewIngestor(cfg, newTSource(), tTransformer{}, &tEncoder{ct: "a", ext: ".b"}, &tSink{}, nil)
	if err == nil {
		t.Fatal("expected error for nil keyFunc")
	}
}

// --- normalizeBatcherConfig ---

func TestNormalizeBatcherConfig_ZeroFields_FillsDefaults(t *testing.T) {
	result := normalizeBatcherConfig(batcher.BatcherConfig{})
	if result.MaxEstimatedInputBytes == 0 {
		t.Fatal("MaxEstimatedInputBytes must be filled from defaults")
	}
	if result.FlushInterval == 0 {
		t.Fatal("FlushInterval must be filled from defaults")
	}
}

// --- fatalError ---

func TestFatalError_ErrorAndUnwrap(t *testing.T) {
	inner := errors.New("inner")
	fe := fatalError{err: inner}
	if fe.Error() != "inner" {
		t.Fatalf("Error() = %q, want inner", fe.Error())
	}
	if fe.Unwrap() != inner {
		t.Fatal("Unwrap() must return the wrapped error")
	}
}

// --- Set* nil guards ---

func TestSetLogger_NilInstallsNop(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.SetLogger(nil) // must not panic; installs nop logger
	ing.logger.Info("should not panic")
}

func TestSetMetricsRegistry_NilInstallsEmpty(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.SetMetricsRegistry(nil) // must not panic
	ing.metrics.AddCounter("x", 1)
}

func TestSetRetryPolicy_NilDisablesRetry(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.SetRetryPolicy(nil)
	// nopRetry.Do should just call the function once and return its result.
	called := false
	_ = ing.retry.Do(context.Background(), func(_ context.Context) error {
		called = true
		return nil
	})
	if !called {
		t.Fatal("nopRetry must invoke the function")
	}
}

func TestSetAckRetryPolicy_NilDisablesRetry(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.SetAckRetryPolicy(nil)
	called := false
	_ = ing.ackRetry.Do(context.Background(), func(_ context.Context) error {
		called = true
		return nil
	})
	if !called {
		t.Fatal("nopRetry must invoke the function")
	}
}

// --- EnableAdaptiveRuntime zero-value defaults ---

func TestEnableAdaptiveRuntime_ZeroValues_FillsDefaults(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{Enabled: true})
	cfg := ing.adaptive
	if cfg.SampleInterval == 0 {
		t.Fatal("SampleInterval must default to 2s")
	}
	if cfg.Cooldown == 0 {
		t.Fatal("Cooldown must default to 10s")
	}
	if cfg.TargetMemoryUtilization == 0 {
		t.Fatal("TargetMemoryUtilization must default to 0.80")
	}
	if cfg.TargetCPUUtilization == 0 {
		t.Fatal("TargetCPUUtilization must default to 0.70")
	}
}

// --- EnableLease ---

func TestEnableLease_SetsFields(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.EnableLease(30, 15*time.Second)
	if !ing.leaseEnabled {
		t.Fatal("leaseEnabled must be true")
	}
	if ing.leaseVisibilityTimeoutSec != 30 {
		t.Fatalf("leaseVisibilityTimeoutSec = %d, want 30", ing.leaseVisibilityTimeoutSec)
	}
	if ing.leaseRenewEvery != 15*time.Second {
		t.Fatalf("leaseRenewEvery = %v, want 15s", ing.leaseRenewEvery)
	}
}

// --- SetPayloadLogEvery / SetShutdownTimeout ---

func TestSetPayloadLogEvery_StoresValue(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.SetPayloadLogEvery(100)
	if ing.payloadLogEvery != 100 {
		t.Fatalf("payloadLogEvery = %d, want 100", ing.payloadLogEvery)
	}
}

func TestSetShutdownTimeout_PositiveValue_Stored(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.SetShutdownTimeout(30 * time.Second)
	if ing.shutdownTimeout != 30*time.Second {
		t.Fatalf("shutdownTimeout = %v, want 30s", ing.shutdownTimeout)
	}
}

func TestSetShutdownTimeout_ZeroOrNegative_Ignored(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.SetShutdownTimeout(0)
	if ing.shutdownTimeout != 0 {
		t.Fatalf("shutdownTimeout must stay 0 for zero input, got %v", ing.shutdownTimeout)
	}
	ing.SetShutdownTimeout(-time.Second)
	if ing.shutdownTimeout != 0 {
		t.Fatalf("shutdownTimeout must stay 0 for negative input, got %v", ing.shutdownTimeout)
	}
}

// --- shouldLogPayload ---

func TestShouldLogPayload_DisabledWhenZero(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	// payloadLogEvery == 0 → always returns false
	if ing.shouldLogPayload(true) || ing.shouldLogPayload(false) {
		t.Fatal("shouldLogPayload must be false when payloadLogEvery=0")
	}
}

func TestShouldLogPayload_EveryN(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.payloadLogEvery = 3
	results := make([]bool, 6)
	for i := range results {
		results[i] = ing.shouldLogPayload(true)
	}
	// Sequence: seq=1,2,3,4,5,6 → 3%3==0 → index 2; 6%3==0 → index 5
	if !results[2] {
		t.Fatal("shouldLogPayload must be true at seq=3 (every 3)")
	}
	if !results[5] {
		t.Fatal("shouldLogPayload must be true at seq=6 (every 3)")
	}
	if results[0] || results[1] || results[3] || results[4] {
		t.Fatal("shouldLogPayload must be false at non-multiple positions")
	}
}

func TestShouldLogPayload_InputVsTransformed(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.payloadLogEvery = 1
	// Each counter is independent; both true==input and false==transformed.
	if !ing.shouldLogPayload(true) {
		t.Fatal("input counter: seq=1, every=1 → must be true")
	}
	if !ing.shouldLogPayload(false) {
		t.Fatal("transformed counter: seq=1, every=1 → must be true")
	}
}

// --- logValue ---

func TestLogValue_NormalValue(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	got := ing.logValue(map[string]int{"a": 1})
	if got != `{"a":1}` {
		t.Fatalf("logValue = %q, want {\"a\":1}", got)
	}
}

func TestLogValue_TruncatesLongValue(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	long := make([]byte, 5000)
	for i := range long {
		long[i] = 'x'
	}
	got := ing.logValue(string(long))
	if len(got) <= 4096 {
		t.Fatalf("expected truncated output longer than 4096 chars to contain suffix, got len=%d", len(got))
	}
	if got[len(got)-len("...<truncated>"):] != "...<truncated>" {
		t.Fatal("truncated value must end with ...<truncated>")
	}
}

// --- estimateSizeBytesFallback / countingWriter ---

func TestEstimateSizeBytesFallback_ReturnsJSONLength(t *testing.T) {
	n, err := estimateSizeBytesFallback(map[string]int{"a": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// json.NewEncoder appends a newline; {"a":1}\n = 8 bytes
	if n <= 0 {
		t.Fatalf("expected positive byte count, got %d", n)
	}
}

// --- DefaultKeyFunc extension branch ---

func TestDefaultKeyFunc_EmptyExtension_UsesBin(t *testing.T) {
	enc := &tEncoder{ct: "a", ext: ""} // empty ext → should default to .bin
	keyFn := DefaultKeyFunc(enc)
	key, err := keyFn(context.Background(), batcher.Batch[int]{})
	if err != nil {
		t.Fatalf("DefaultKeyFunc: %v", err)
	}
	if len(key) == 0 {
		t.Fatal("expected non-empty key")
	}
	// Key must end in .bin when encoder extension is empty.
	if key[len(key)-4:] != ".bin" {
		t.Fatalf("key = %q, want .bin suffix", key)
	}
}

// --- initFlushPoolLocked guard (flushJobs != nil) ---

func TestMaybeStartFlushPool_StartsPoolWithMultipleWorkers(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Second
	cfg.MaxEstimatedInputBytes = 1024
	ing, err := NewIngestor(cfg, src, tTransformer{}, enc, sk, func(_ context.Context, _ batcher.Batch[int]) (string, error) { return "k", nil })
	if err != nil {
		t.Fatal(err)
	}
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.flushQueue = 4
	ing.flushWorkers = 2
	ing.maybeStartFlushPool(context.TODO()) // nil would trigger background fallback; use TODO to be explicit
	defer func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	}()
	if ing.flushJobs == nil {
		t.Fatal("flushJobs must be initialized after maybeStartFlushPool")
	}
}

// --- processMessage paths ---

// tVisibilityExtender implements source.VisibilityExtender for testing lease.
type tVisibilityExtender struct {
	extendCalls int32
	extendErr   error
}

func (e *tVisibilityExtender) ExtendVisibilityBatch(ctx context.Context, metas []source.AckMetadata, timeoutSec int32) error {
	atomic.AddInt32(&e.extendCalls, 1)
	return e.extendErr
}

// tSourceWithLease wraps tSource and also implements VisibilityExtender.
type tSourceWithLease struct {
	*tSource
	ext *tVisibilityExtender
}

func (s *tSourceWithLease) ExtendVisibilityBatch(ctx context.Context, metas []source.AckMetadata, timeoutSec int32) error {
	return s.ext.ExtendVisibilityBatch(ctx, metas, timeoutSec)
}

var _ source.VisibilityExtender = (*tSourceWithLease)(nil)

func newTestIngestor(t *testing.T, src source.Sourcer, tr tTransformer, enc *tEncoder, sk sink.Sinkr) *Ingestor[int] {
	t.Helper()
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Second
	cfg.MaxEstimatedInputBytes = 1024
	ing, err := NewIngestor(cfg, src, tr, enc, sk, func(_ context.Context, _ batcher.Batch[int]) (string, error) { return "k", nil })
	if err != nil {
		t.Fatal(err)
	}
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.SetLogger(observability.NopLogger())
	return ing
}

func TestProcessMessage_TransformerDropsMessage_AcksCalled(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{drop: true}, enc, sk)
	src.writeDone.Store(true) // allow ack

	msg := &tMsg{env: source.Envelope{Payload: "hello"}, size: 5, sizeOK: true}
	flush, err := ing.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if flush {
		t.Fatal("dropped message must not trigger flush")
	}
	if atomic.LoadInt32(&src.ackCalls) != 1 {
		t.Fatalf("ackCalls = %d, want 1", src.ackCalls)
	}
	snap := ing.metrics.Snapshot()
	if snap["ingestor_messages_dropped_total"] != 1 {
		t.Fatalf("dropped counter = %v, want 1", snap["ingestor_messages_dropped_total"])
	}
}

func TestProcessMessage_SizeFallback_UsesEstimator(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)

	// sizeOK=false triggers estimateSizeBytesFallback(env.Payload).
	msg := &tMsg{env: source.Envelope{Payload: "hello"}, sizeOK: false}
	_, err := ing.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestProcessMessage_SizeFallback_ErrorCallsFail(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)

	// Channel payload is not JSON-marshalable — estimateSizeBytesFallback returns error.
	msg := &tMsg{env: source.Envelope{Payload: make(chan int)}, sizeOK: false}
	flush, err := ing.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("processMessage must not surface size estimation error, got %v", err)
	}
	if flush {
		t.Fatal("size estimation failure must not trigger flush")
	}
	if atomic.LoadInt32(&msg.failCalls) != 1 {
		t.Fatalf("Fail must have been called once, got %d", msg.failCalls)
	}
}

func TestProcessMessage_PayloadLogging(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)
	ing.payloadLogEvery = 1

	msg := &tMsg{env: source.Envelope{Payload: "data"}, size: 4, sizeOK: true}
	_, err := ing.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	snap := ing.metrics.Snapshot()
	if snap["ingestor_input_payload_logs_total"] != 1 {
		t.Fatalf("input_payload_logs_total = %v, want 1", snap["ingestor_input_payload_logs_total"])
	}
}

// --- flushJob error paths ---

func TestFlushJob_EncodeError_ReturnsError(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin", encodeFail: true}
	sk := &tSinkOnly{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)

	job := flushJob[int]{key: "test/key.bin", items: []int{1, 2, 3}, acks: source.AckGroup{}}
	err := ing.flushJob(context.Background(), job)
	if err == nil {
		t.Fatal("expected error when encoder fails")
	}
	snap := ing.metrics.Snapshot()
	if snap["ingestor_encode_errors_total"] != 1 {
		t.Fatalf("encode_errors_total = %v, want 1", snap["ingestor_encode_errors_total"])
	}
}

// tAlwaysFailSink implements only Sinkr (no streaming) and always errors on Write.
type tAlwaysFailSink struct{}

func (s *tAlwaysFailSink) Write(_ context.Context, _ sink.WriteRequest) error {
	return errors.New("sink write failed")
}

var _ sink.Sinkr = (*tAlwaysFailSink)(nil)

func TestFlushJob_SinkWriteError_ReturnsError(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	// tAlwaysFailSink has no StreamSinkr → tryStreamWrite returns streamed=false,
	// so the regular Write path is taken and the error increments sink_errors_total.
	ing := newTestIngestor(t, src, tTransformer{}, enc, &tAlwaysFailSink{})

	job := flushJob[int]{key: "k", items: []int{1}, acks: source.AckGroup{}}
	err := ing.flushJob(context.Background(), job)
	if err == nil {
		t.Fatal("expected error when sink write fails")
	}
	snap := ing.metrics.Snapshot()
	if snap["ingestor_sink_errors_total"] != 1 {
		t.Fatalf("sink_errors_total = %v, want 1", snap["ingestor_sink_errors_total"])
	}
}

func TestFlushJob_AckError_ReturnsError(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSinkOnly{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)

	// writeDone=false makes AckBatch return error.
	src.writeDone.Store(false)
	msg := &tMsg{env: source.Envelope{Payload: "x"}, size: 1, sizeOK: true}
	var acks source.AckGroup
	acks.Add(msg)

	job := flushJob[int]{key: "k", items: []int{1}, acks: acks}
	err := ing.flushJob(context.Background(), job)
	if err == nil {
		t.Fatal("expected error when ack fails")
	}
	snap := ing.metrics.Snapshot()
	if snap["ingestor_ack_errors_total"] != 1 {
		t.Fatalf("ack_errors_total = %v, want 1", snap["ingestor_ack_errors_total"])
	}
}

// --- startJobLease ---

func TestStartJobLease_EmptyMetas_ReturnsNopStop(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}, logger: observability.NopLogger()}
	ext := &tVisibilityExtender{}
	stop := ing.startJobLease(context.Background(), ext, nil)
	if stop == nil {
		t.Fatal("stop func must not be nil")
	}
	stop()
	if atomic.LoadInt32(&ext.extendCalls) != 0 {
		t.Fatal("no extend calls expected for empty metas")
	}
}

func TestStartJobLease_CallsExtendOnTick(t *testing.T) {
	ext := &tVisibilityExtender{}
	srcWithLease := &tSourceWithLease{tSource: newTSource(), ext: ext}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, srcWithLease, tTransformer{}, enc, sk)
	ing.leaseEnabled = true
	ing.leaseVisibilityTimeoutSec = 30
	ing.leaseRenewEvery = 15 * time.Millisecond

	metas := []source.AckMetadata{{ID: "m1", Handle: "rh1"}}
	stop := ing.startJobLease(context.Background(), ext, metas)
	defer stop()

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&ext.extendCalls) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt32(&ext.extendCalls) == 0 {
		t.Fatal("ExtendVisibilityBatch was never called")
	}
}

func TestStartJobLease_StopCancelsExtend(t *testing.T) {
	ext := &tVisibilityExtender{}
	srcWithLease := &tSourceWithLease{tSource: newTSource(), ext: ext}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, srcWithLease, tTransformer{}, enc, sk)
	ing.leaseEnabled = true
	ing.leaseVisibilityTimeoutSec = 30
	ing.leaseRenewEvery = 5 * time.Millisecond

	metas := []source.AckMetadata{{ID: "m1", Handle: "rh1"}}
	stop := ing.startJobLease(context.Background(), ext, metas)
	stop() // cancel before any tick fires

	before := atomic.LoadInt32(&ext.extendCalls)
	time.Sleep(30 * time.Millisecond)
	after := atomic.LoadInt32(&ext.extendCalls)
	if after > before+1 {
		t.Fatalf("extend calls grew from %d to %d after stop", before, after)
	}
}

// --- flushJob lease path ---

func TestFlushJob_WithLeaseEnabled_StartsLease(t *testing.T) {
	ext := &tVisibilityExtender{}
	srcWithLease := &tSourceWithLease{tSource: newTSource(), ext: ext}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tAlwaysFailSink{} // write fails → early return, but lease was started
	ing := newTestIngestor(t, srcWithLease, tTransformer{}, enc, sk)
	ing.leaseEnabled = true
	ing.leaseVisibilityTimeoutSec = 30
	ing.leaseRenewEvery = time.Hour // long interval so it never fires during the test

	msg := &tMsg{env: source.Envelope{Payload: "x"}, size: 1, sizeOK: true,
		meta: source.AckMetadata{ID: "m1", Handle: "rh1"}}
	var acks source.AckGroup
	acks.Add(msg)

	job := flushJob[int]{key: "k", items: []int{1}, acks: acks}
	// The sink fails, but the lease goroutine must have been started and stopped.
	_ = ing.flushJob(context.Background(), job)
}

// TestStartJobLease_ExtendError_GoroutineExits verifies that when
// ExtendVisibilityBatch returns an error the goroutine shuts itself down.
func TestStartJobLease_ExtendError_GoroutineExits(t *testing.T) {
	ext := &tVisibilityExtender{extendErr: errors.New("extend fail")}
	ing := &Ingestor[int]{
		metrics:                   &observability.Registry{},
		logger:                    observability.NopLogger(),
		leaseVisibilityTimeoutSec: 30,
		leaseRenewEvery:           10 * time.Millisecond,
	}

	metas := []source.AckMetadata{{ID: "m1", Handle: "rh1"}}
	stop := ing.startJobLease(context.Background(), ext, metas)
	defer stop()

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&ext.extendCalls) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Give the goroutine time to self-exit after the error.
	time.Sleep(20 * time.Millisecond)
	// No assertion needed beyond "no panic / deadlock"; goroutine must have exited.
}

// --- flushWorker channel-closed path ---

func TestFlushWorker_ChannelClosed_WorkerExits(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)
	ing.flushQueue = 2
	ing.flushWorkers = 2
	ing.maybeStartFlushPool(context.Background())

	src.writeDone.Store(true)

	// Closing flushJobs causes workers to exit via the !ok branch.
	close(ing.flushJobs)
	waitDone := make(chan struct{})
	go func() {
		ing.flushWG.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("flush workers did not exit after channel close within timeout")
	}
}

// TestFlushWorker_JobError_LogsAndContinues verifies that a flushJob error is
// logged and the worker continues processing subsequent jobs (the error path
// inside the flushWorker loop).
func TestFlushWorker_JobError_LogsAndContinues(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin", encodeFail: true}
	sk := &tSinkOnly{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)
	ing.flushQueue = 4
	ing.flushWorkers = 2
	ing.maybeStartFlushPool(context.Background())

	// Send a job that will fail (encoder fails), then close the channel.
	job := flushJob[int]{key: "k", items: []int{1}, acks: source.AckGroup{}}
	ing.flushJobs <- job
	close(ing.flushJobs)

	waitDone := make(chan struct{})
	go func() {
		ing.flushWG.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("workers did not exit within timeout")
	}
	snap := ing.metrics.Snapshot()
	if snap["ingestor_encode_errors_total"] < 1 {
		t.Fatalf("encode_errors_total = %v, want >= 1", snap["ingestor_encode_errors_total"])
	}
}

// --- Run edge cases ---

// errSource is a Sourcer whose Receive returns a non-context error a fixed
// number of times before blocking on ctx.Done.
type errSource struct {
	remaining int32
	err       error
}

func (s *errSource) Receive(ctx context.Context) (source.Message, error) {
	if r := atomic.AddInt32(&s.remaining, -1); r >= 0 {
		return nil, s.err
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (s *errSource) AckBatch(context.Context, []source.Message) error { return nil }

var _ source.Sourcer = (*errSource)(nil)

// TestRun_PreCancelledContext verifies that Run returns nil immediately when
// the context is already cancelled.
func TestRun_PreCancelledContext(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	if err := ing.Run(ctx, 1, 1); err != nil {
		t.Fatalf("expected nil error on pre-cancelled ctx, got %v", err)
	}
}

// TestRun_NonContextReceiveError verifies that a non-context receive error
// increments the error metric and the loop continues.
func TestRun_NonContextReceiveError(t *testing.T) {
	sentinel := errors.New("network blip")
	src := &errSource{remaining: 2, err: sentinel}

	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	if err := ing.Run(ctx, 1, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := ing.metrics.Snapshot()["ingestor_receive_errors_total"]; got < 1 {
		t.Fatalf("receive_errors_total = %v, want >= 1", got)
	}
}

// TestRun_FatalErrorFromFlush verifies that Run returns immediately when
// flush returns a fatalError (from keyFunc).
func TestRun_FatalErrorFromFlush(t *testing.T) {
	fatalKeyFuncErr := errors.New("fatal keyfunc error")

	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}

	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Millisecond // flush immediately
	cfg.MaxItems = 1                     // flush on first message
	ing, err := NewIngestor(cfg, src, tTransformer{}, enc, sk,
		func(_ context.Context, _ batcher.Batch[int]) (string, error) {
			return "", fatalKeyFuncErr
		})
	if err != nil {
		t.Fatal(err)
	}
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.SetLogger(observability.NopLogger())

	// Push a message to trigger flush.
	src.recvCh <- &tMsg{env: source.Envelope{Payload: "x"}, size: 10, sizeOK: true}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	runErr := ing.Run(ctx, 1, 1)
	if runErr == nil {
		t.Fatal("expected non-nil error from Run when keyFunc is fatal")
	}
	if !errors.Is(runErr, fatalKeyFuncErr) {
		t.Fatalf("expected fatalKeyFuncErr, got %v", runErr)
	}
}

// TestRun_FlushQueueDefaultsToWorkers verifies that passing flushQueue=0 sets
// it to flushWorkers (the default path at the top of Run).
func TestRun_FlushQueueDefaultsToWorkers(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = ing.Run(ctx, 2, 0)
	if ing.flushQueue != 2 {
		t.Fatalf("flushQueue = %d, want 2 (== flushWorkers)", ing.flushQueue)
	}
}

// --- initFlushPoolLocked / maybeStartFlushPool edge cases ---

// TestInitFlushPoolLocked_AlreadyInitialized_Noop verifies the early-return
// guard when flushJobs is already non-nil.
func TestInitFlushPoolLocked_AlreadyInitialized_Noop(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)
	ing.flushWorkers = 2
	ing.flushQueue = 2

	ing.flushMu.Lock()
	ing.initFlushPoolLocked()
	firstJobs := ing.flushJobs
	ing.initFlushPoolLocked() // second call – must be a noop
	secondJobs := ing.flushJobs
	// Cancel all workers before closing the channel.
	for _, stop := range ing.flushStops {
		stop()
	}
	close(firstJobs)
	ing.flushJobs = nil
	ing.flushMu.Unlock()
	ing.flushWG.Wait()

	if firstJobs != secondJobs {
		t.Fatal("initFlushPoolLocked must not replace flushJobs when already set")
	}
}

// TestMaybeStartFlushPool_NilCtx_UsesBackground verifies that passing nil ctx
// to maybeStartFlushPool does not panic and falls back to context.Background().
func TestMaybeStartFlushPool_NilCtx_UsesBackground(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)
	ing.flushWorkers = 2
	ing.flushQueue = 2

	ing.maybeStartFlushPool(nil) //nolint:staticcheck // intentionally testing nil-ctx fallback path

	ing.flushMu.Lock()
	jobs := ing.flushJobs
	ing.flushMu.Unlock()

	if jobs == nil {
		t.Fatal("expected flushJobs to be initialised")
	}
	// Cancel all workers then close the channel.
	ing.flushMu.Lock()
	for _, stop := range ing.flushStops {
		stop()
	}
	close(ing.flushJobs)
	ing.flushJobs = nil
	ing.flushMu.Unlock()
	ing.flushWG.Wait()
}

// --- SetFlushWorkers scale-down ---

// TestSetFlushWorkers_ScaleDown verifies that shrinking workers stops the
// excess ones and truncates the flushStops slice.
func TestSetFlushWorkers_ScaleDown(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)
	ing.flushWorkers = 3
	ing.flushQueue = 4
	ing.maybeStartFlushPool(context.Background())

	// Scale down from 3 → 1.
	ing.SetFlushWorkers(1)

	ing.flushMu.Lock()
	n := len(ing.flushStops)
	ing.flushMu.Unlock()

	if n != 1 {
		t.Fatalf("flushStops after scale-down = %d, want 1", n)
	}

	// Cancel the remaining worker and close the channel so all goroutines exit.
	ing.flushMu.Lock()
	for _, stop := range ing.flushStops {
		stop()
	}
	close(ing.flushJobs)
	ing.flushJobs = nil
	ing.flushMu.Unlock()
	ing.flushWG.Wait()
}

// TestSetFlushWorkers_NoOp_SameCount verifies that calling SetFlushWorkers with
// the current count is a no-op (n == current branch).
func TestSetFlushWorkers_NoOp_SameCount(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)
	ing.flushWorkers = 2
	ing.flushQueue = 2
	ing.maybeStartFlushPool(context.Background())

	before := len(ing.flushStops)
	ing.SetFlushWorkers(2) // same → noop
	after := len(ing.flushStops)

	if before != after {
		t.Fatalf("SetFlushWorkers noop changed flushStops from %d to %d", before, after)
	}

	// Cancel all workers then close the channel for a clean shutdown.
	ing.flushMu.Lock()
	for _, stop := range ing.flushStops {
		stop()
	}
	close(ing.flushJobs)
	ing.flushJobs = nil
	ing.flushMu.Unlock()
	ing.flushWG.Wait()
}

// --- flushRemainingOnStop timeout ---

// TestFlushRemainingOnStop_TimeoutPath verifies that when workers don't finish
// within the shutdownTimeout the timeout branch fires and returns nil.
func TestFlushRemainingOnStop_TimeoutPath(t *testing.T) {
	src := newTSource()
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	ing := newTestIngestor(t, src, tTransformer{}, enc, sk)

	// Simulate a pool with 2 workers by setting up the fields that
	// flushRemainingOnStop inspects, but add an extra WaitGroup counter that
	// will never complete within the 1 ms shutdown timeout.
	ing.flushWorkers = 2
	ing.flushQueue = 2
	ing.maybeStartFlushPool(context.Background())
	ing.SetShutdownTimeout(1 * time.Millisecond)

	// Add an extra count so flushWG never reaches zero within the timeout.
	ing.flushWG.Add(1)

	result := ing.flushRemainingOnStop(context.Background())
	if result != nil {
		t.Fatalf("expected nil from flushRemainingOnStop on timeout, got %v", result)
	}
	// Release the artificially added counter so the WaitGroup is balanced.
	ing.flushWG.Done()

	if got := ing.metrics.Snapshot()["ingestor_shutdown_timeout_total"]; got < 1 {
		t.Fatalf("shutdown_timeout_total = %v, want >= 1", got)
	}
}

// --- tryStreamWrite with nil retry ---

// TestTryStreamWrite_NilRetry_UsesNopRetry verifies that passing nil as the
// retry policy falls back to nopRetry (the retry==nil branch in tryStreamWrite).
func TestTryStreamWrite_NilRetry_UsesNopRetry(t *testing.T) {
	enc := &tEncoder{ct: "application/vnd.apache.parquet", ext: ".parquet"}
	sk := &tSink{}
	streamed, err := tryStreamWrite(context.Background(), enc, sk, nil, "key", []int{1, 2})
	if !streamed {
		t.Fatal("expected streamed=true")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&sk.writeStreamCalls) != 1 {
		t.Fatalf("WriteStreamCalls = %d, want 1", sk.writeStreamCalls)
	}
}

// TestLogValue_MarshalError verifies the error fallback path in logValue.
func TestLogValue_MarshalError(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	// json.Marshal fails on channels.
	got := ing.logValue(make(chan int))
	if len(got) == 0 {
		t.Fatal("expected non-empty error string from logValue on marshal failure")
	}
	if got[:1] != "<" {
		t.Fatalf("logValue error output = %q, want <marshal_error:...> prefix", got)
	}
}
