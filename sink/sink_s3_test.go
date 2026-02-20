package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeS3API struct {
	mu sync.Mutex

	putCalls int
	lastIn   *s3.PutObjectInput
	lastBody []byte

	putErr error
}

func (f *fakeS3API) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	f.putCalls++
	f.lastIn = in
	putErr := f.putErr
	f.mu.Unlock()

	if putErr != nil {
		return nil, putErr
	}

	if in.Body != nil {
		b, err := io.ReadAll(in.Body)
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
		f.lastBody = b
		f.mu.Unlock()
	}
	return &s3.PutObjectOutput{}, nil
}

type fakeS3NoCapture struct {
	mu       sync.Mutex
	putCalls int
	putErr   error
}

func (f *fakeS3NoCapture) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	f.putCalls++
	err := f.putErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}

	if in.Body != nil {
		_, derr := io.Copy(io.Discard, in.Body)
		if derr != nil {
			return nil, derr
		}
	}

	return &s3.PutObjectOutput{}, nil
}

type bytesStreamWriter struct {
	b []byte
}

func (w bytesStreamWriter) WriteTo(dst io.Writer) error {
	_, err := dst.Write(w.b)
	return err
}

type errStreamWriter struct {
	err error
}

func (w errStreamWriter) WriteTo(dst io.Writer) error {
	_ = dst
	return w.err
}

func TestSink_Write_BuildsKeyWithPrefixWithoutCleaning(t *testing.T) {
	f := &fakeS3API{}
	s := NewSinkS3(f, "bkt", "/pfx/")

	data := []byte("abc")
	err := s.Write(context.Background(), WriteRequest{
		Key:         "/a/../b/x.parquet",
		Data:        data,
		ContentType: "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.putCalls != 1 {
		t.Fatalf("expected 1 call, got %d", f.putCalls)
	}
	if aws.ToString(f.lastIn.Bucket) != "bkt" {
		t.Fatalf("bucket: %q", aws.ToString(f.lastIn.Bucket))
	}
	if aws.ToString(f.lastIn.Key) != "pfx/a/../b/x.parquet" {
		t.Fatalf("key: %q", aws.ToString(f.lastIn.Key))
	}
	if aws.ToString(f.lastIn.ContentType) != "application/octet-stream" {
		t.Fatalf("content-type: %q", aws.ToString(f.lastIn.ContentType))
	}
	if f.lastIn.ContentLength == nil || *f.lastIn.ContentLength != int64(len(data)) {
		t.Fatalf("content-length: %#v", f.lastIn.ContentLength)
	}
	if !bytes.Equal(f.lastBody, data) {
		t.Fatalf("body mismatch: %q", string(f.lastBody))
	}
}

func TestSink_Write_EmptyKeyReturnsError(t *testing.T) {
	f := &fakeS3API{}
	s := NewSinkS3(f, "bkt", "")
	if err := s.Write(context.Background(), WriteRequest{Key: ""}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestSink_Write_PropagatesPutError(t *testing.T) {
	boom := errors.New("boom")
	f := &fakeS3API{putErr: boom}
	s := NewSinkS3(f, "bkt", "p")
	if err := s.Write(context.Background(), WriteRequest{Key: "x", Data: []byte("1")}); !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
}

func TestSink_WriteStream_BuildsKeyWithPrefixWithoutCleaning(t *testing.T) {
	f := &fakeS3API{}
	s := NewSinkS3(f, "bkt", "/pfx/")

	data := []byte("stream-abc")
	err := s.WriteStream(context.Background(), StreamWriteRequest{
		Key:         "/a/../b/x.parquet",
		ContentType: "application/octet-stream",
		Writer:      bytesStreamWriter{b: data},
	})
	if err != nil {
		t.Fatalf("WriteStream: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.putCalls != 1 {
		t.Fatalf("expected 1 call, got %d", f.putCalls)
	}
	if aws.ToString(f.lastIn.Bucket) != "bkt" {
		t.Fatalf("bucket: %q", aws.ToString(f.lastIn.Bucket))
	}
	if aws.ToString(f.lastIn.Key) != "pfx/a/../b/x.parquet" {
		t.Fatalf("key: %q", aws.ToString(f.lastIn.Key))
	}
	if aws.ToString(f.lastIn.ContentType) != "application/octet-stream" {
		t.Fatalf("content-type: %q", aws.ToString(f.lastIn.ContentType))
	}
	if f.lastIn.ContentLength != nil {
		t.Fatalf("content-length should be nil for streaming, got %#v", f.lastIn.ContentLength)
	}
	if !bytes.Equal(f.lastBody, data) {
		t.Fatalf("body mismatch: %q", string(f.lastBody))
	}
}

func TestSink_WriteStream_EmptyKeyReturnsError(t *testing.T) {
	f := &fakeS3API{}
	s := NewSinkS3(f, "bkt", "")
	if err := s.WriteStream(context.Background(), StreamWriteRequest{Key: "", Writer: bytesStreamWriter{b: nil}}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestSink_WriteStream_PropagatesPutError(t *testing.T) {
	boom := errors.New("boom")
	f := &fakeS3API{putErr: boom}
	s := NewSinkS3(f, "bkt", "p")
	err := s.WriteStream(context.Background(), StreamWriteRequest{
		Key:    "x",
		Writer: bytesStreamWriter{b: []byte("1")},
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
}

func TestSink_WriteStream_PropagatesWriterError(t *testing.T) {
	boom := errors.New("writer boom")
	f := &fakeS3API{}
	s := NewSinkS3(f, "bkt", "p")
	err := s.WriteStream(context.Background(), StreamWriteRequest{
		Key:    "x",
		Writer: errStreamWriter{err: boom},
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected writer boom, got %v", err)
	}
}

func BenchmarkSink_Write_NoCapture(b *testing.B) {
	for _, size := range []int{0, 128, 1024, 16 * 1024, 256 * 1024} {
		b.Run(fmt.Sprintf("size=%s", strconv.Itoa(size)), func(b *testing.B) {
			f := &fakeS3NoCapture{}
			s := NewSinkS3(f, "bkt", "pfx")
			data := make([]byte, size)
			req := WriteRequest{Key: "x.parquet", Data: data, ContentType: "application/octet-stream"}
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Write(ctx, req); err != nil {
					b.Fatalf("write: %v", err)
				}
			}
		})
	}
}

func BenchmarkSink_Write_NoCapture_Parallel_StaticKey(b *testing.B) {
	for _, size := range []int{0, 128, 1024, 16 * 1024, 256 * 1024} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			f := &fakeS3NoCapture{}
			s := NewSinkS3(f, "bkt", "pfx")
			data := make([]byte, size)
			ctx := context.Background()

			req := WriteRequest{Key: "x.parquet", Data: data, ContentType: "application/octet-stream"}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := s.Write(ctx, req); err != nil {
						b.Fatalf("write: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkSink_Write_NoCapture_Parallel_PrecomputedKeys(b *testing.B) {
	const keyCount = 1 << 16
	keys := make([]string, 0, keyCount)
	for i := 0; i < keyCount; i++ {
		keys = append(keys, fmt.Sprintf("x/%d.parquet", i))
	}
	mask := uint64(keyCount - 1)

	for _, size := range []int{0, 128, 1024, 16 * 1024, 256 * 1024} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			f := &fakeS3NoCapture{}
			s := NewSinkS3(f, "bkt", "pfx")
			data := make([]byte, size)
			ctx := context.Background()

			var seq uint64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					idx := atomic.AddUint64(&seq, 1) & mask
					req := WriteRequest{Key: keys[idx], Data: data, ContentType: "application/octet-stream"}
					if err := s.Write(ctx, req); err != nil {
						b.Fatalf("write: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkSink_WriteStream_NoCapture(b *testing.B) {
	for _, size := range []int{0, 128, 1024, 16 * 1024, 256 * 1024} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			f := &fakeS3NoCapture{}
			s := NewSinkS3(f, "bkt", "pfx")
			data := make([]byte, size)
			ctx := context.Background()

			req := StreamWriteRequest{
				Key:         "x.parquet",
				ContentType: "application/octet-stream",
				Writer:      bytesStreamWriter{b: data},
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := s.WriteStream(ctx, req); err != nil {
					b.Fatalf("WriteStream: %v", err)
				}
			}
		})
	}
}

func BenchmarkSink_WriteStream_Parallel(b *testing.B) {
	for _, size := range []int{0, 128, 1024, 16 * 1024, 256 * 1024} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			f := &fakeS3NoCapture{}
			s := NewSinkS3(f, "bkt", "pfx")
			data := make([]byte, size)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					req := StreamWriteRequest{
						Key:         "x.parquet",
						ContentType: "application/octet-stream",
						Writer:      bytesStreamWriter{b: data},
					}
					if err := s.WriteStream(ctx, req); err != nil {
						b.Fatalf("WriteStream: %v", err)
					}
				}
			})
		})
	}
}
