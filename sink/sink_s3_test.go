package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
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
		b, _ := io.ReadAll(in.Body)
		f.mu.Lock()
		f.lastBody = b
		f.mu.Unlock()
	}
	return &s3.PutObjectOutput{}, nil
}

// no-capture fake for benchmarks: minimal overhead, no body reads/copies.
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
	return &s3.PutObjectOutput{}, nil
}

func TestSink_Write_BuildsKeyWithPrefixWithoutCleaning(t *testing.T) {
	f := &fakeS3API{}
	s := New(f, "bkt", "/pfx/")

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
	s := New(f, "bkt", "")
	if err := s.Write(context.Background(), WriteRequest{Key: ""}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestSink_Write_PropagatesPutError(t *testing.T) {
	boom := errors.New("boom")
	f := &fakeS3API{putErr: boom}
	s := New(f, "bkt", "p")
	if err := s.Write(context.Background(), WriteRequest{Key: "x", Data: []byte("1")}); !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
}

func BenchmarkSink_Write_NoCapture(b *testing.B) {
	for _, size := range []int{0, 128, 1024, 16 * 1024, 256 * 1024} {
		b.Run(fmt.Sprintf("size=%s", strconv.Itoa(size)), func(b *testing.B) {
			f := &fakeS3NoCapture{}
			s := New(f, "bkt", "pfx")
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
