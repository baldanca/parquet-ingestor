package ingestor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/sink"
)

type fakeEnc[T any] struct {
	ct  string
	ext string
	// counters
	encodeToCalls int32
}

func (e *fakeEnc[T]) ContentType() string   { return e.ct }
func (e *fakeEnc[T]) FileExtension() string { return e.ext }

func (e *fakeEnc[T]) Encode(ctx context.Context, items []T) ([]byte, string, error) {
	// not used in tryStreamWrite tests
	return []byte("x"), e.ct, nil
}

func (e *fakeEnc[T]) EncodeTo(ctx context.Context, items []T, w io.Writer) error {
	atomic.AddInt32(&e.encodeToCalls, 1)
	_, err := w.Write([]byte("parquet-bytes"))
	return err
}

// Ensure it implements both interfaces
var _ encoder.Encoder[int] = (*fakeEnc[int])(nil)
var _ encoder.StreamEncoder[int] = (*fakeEnc[int])(nil)

type fakeSink struct {
	writeStreamCalls int32
	// capture
	lastCT  string
	lastKey string
	buf     bytes.Buffer
	// fail control
	failTimes int32
}

func (s *fakeSink) Write(ctx context.Context, req sink.WriteRequest) error {
	// not used here
	return nil
}

func (s *fakeSink) WriteStream(ctx context.Context, req sink.StreamWriteRequest) error {
	atomic.AddInt32(&s.writeStreamCalls, 1)

	if atomic.LoadInt32(&s.failTimes) > 0 {
		atomic.AddInt32(&s.failTimes, -1)
		return errors.New("sink-fail")
	}

	s.lastCT = req.ContentType
	s.lastKey = req.Key

	s.buf.Reset()
	return req.Writer.WriteTo(&s.buf)
}

var _ sink.Sinkr = (*fakeSink)(nil)
var _ sink.StreamSinkr = (*fakeSink)(nil)

type fakeSinkNoStream struct{}

func (fakeSinkNoStream) Write(ctx context.Context, req sink.WriteRequest) error { return nil }

var _ sink.Sinkr = (*fakeSinkNoStream)(nil)

type fakeEncNoStream[T any] struct {
	ct  string
	ext string
}

func (e fakeEncNoStream[T]) ContentType() string   { return e.ct }
func (e fakeEncNoStream[T]) FileExtension() string { return e.ext }
func (e fakeEncNoStream[T]) Encode(ctx context.Context, items []T) ([]byte, string, error) {
	return []byte("x"), e.ct, nil
}

var _ encoder.Encoder[int] = (*fakeEncNoStream[int])(nil)

func TestTryStreamWrite_StreamedFalse_WhenEncoderNotStream(t *testing.T) {
	enc := fakeEncNoStream[int]{ct: "application/x", ext: ".x"}
	sk := &fakeSink{}
	streamed, err := tryStreamWrite[int](context.Background(), &enc, sk, nopRetry{}, "k", []int{1, 2})
	if streamed {
		t.Fatalf("expected streamed=false")
	}
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestTryStreamWrite_StreamedFalse_WhenSinkNotStream(t *testing.T) {
	enc := &fakeEnc[int]{ct: "application/x", ext: ".x"}
	sk := &fakeSinkNoStream{}
	streamed, err := tryStreamWrite[int](context.Background(), enc, sk, nopRetry{}, "k", []int{1, 2})
	if streamed {
		t.Fatalf("expected streamed=false")
	}
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestTryStreamWrite_UsesEncoderContentType(t *testing.T) {
	enc := &fakeEnc[int]{ct: "application/vnd.apache.parquet", ext: ".parquet"}
	sk := &fakeSink{}
	streamed, err := tryStreamWrite[int](context.Background(), enc, sk, nopRetry{}, "key-1", []int{1, 2, 3})
	if !streamed {
		t.Fatalf("expected streamed=true")
	}
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sk.lastCT != enc.ct {
		t.Fatalf("contentType=%q want=%q", sk.lastCT, enc.ct)
	}
	if sk.lastKey != "key-1" {
		t.Fatalf("key=%q want=%q", sk.lastKey, "key-1")
	}
	if sk.buf.Len() == 0 {
		t.Fatalf("expected streamed bytes")
	}
	if atomic.LoadInt32(&enc.encodeToCalls) != 1 {
		t.Fatalf("EncodeToCalls=%d want=1", enc.encodeToCalls)
	}
}

func TestTryStreamWrite_FallbackContentType_WhenEmpty(t *testing.T) {
	enc := &fakeEnc[int]{ct: "", ext: ".bin"}
	sk := &fakeSink{}
	streamed, err := tryStreamWrite[int](context.Background(), enc, sk, nopRetry{}, "k", []int{1})
	if !streamed {
		t.Fatalf("expected streamed=true")
	}
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sk.lastCT != "application/octet-stream" {
		t.Fatalf("contentType=%q want=%q", sk.lastCT, "application/octet-stream")
	}
}

func TestTryStreamWrite_RetryIsUsed(t *testing.T) {
	enc := &fakeEnc[int]{ct: "application/vnd.apache.parquet", ext: ".parquet"}
	sk := &fakeSink{failTimes: 2} // fail first 2 attempts

	r := SimpleRetry{Attempts: 5, BaseDelay: 1, MaxDelay: 1, Jitter: false}

	streamed, err := tryStreamWrite[int](context.Background(), enc, sk, r, "k", []int{1, 2})
	if !streamed {
		t.Fatalf("expected streamed=true")
	}
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// 2 failures + 1 success
	if atomic.LoadInt32(&sk.writeStreamCalls) != 3 {
		t.Fatalf("WriteStreamCalls=%d want=3", sk.writeStreamCalls)
	}
}

type benchEnc struct{}

func (benchEnc) ContentType() string   { return "application/vnd.apache.parquet" }
func (benchEnc) FileExtension() string { return ".parquet" }
func (benchEnc) Encode(ctx context.Context, items []int) ([]byte, string, error) {
	return []byte("x"), "application/vnd.apache.parquet", nil
}
func (benchEnc) EncodeTo(ctx context.Context, items []int, w io.Writer) error {
	// simulate some bytes
	_, err := w.Write([]byte("parquet-bytes"))
	return err
}

var _ encoder.Encoder[int] = (*benchEnc)(nil)
var _ encoder.StreamEncoder[int] = (*benchEnc)(nil)

type benchSink struct{}

func (benchSink) Write(ctx context.Context, req sink.WriteRequest) error { return nil }
func (benchSink) WriteStream(ctx context.Context, req sink.StreamWriteRequest) error {
	return req.Writer.WriteTo(io.Discard)
}

var _ sink.Sinkr = (*benchSink)(nil)
var _ sink.StreamSinkr = (*benchSink)(nil)

func BenchmarkTryStreamWrite(b *testing.B) {
	ctx := context.Background()
	enc := benchEnc{}
	sk := benchSink{}
	items := make([]int, 1000)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := tryStreamWrite[int](ctx, enc, sk, nopRetry{}, "k", items)
		if err != nil {
			b.Fatalf("err: %v", err)
		}
	}
}
