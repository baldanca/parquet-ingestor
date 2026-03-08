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

	// PutObject path
	putCalls int
	lastPut  *s3.PutObjectInput
	lastBody []byte

	putErr error

	// Multipart path
	createCalls   int
	uploadCalls   int
	completeCalls int
	abortCalls    int

	lastCreate   *s3.CreateMultipartUploadInput
	lastComplete *s3.CompleteMultipartUploadInput

	uploadID string
	parts    map[int32][]byte

	mpErr error // opcional: erro forçado no fluxo multipart

	// Stubs counters (opcional)
	getCalls  int
	headCalls int
	listCalls int
}

func (f *fakeS3API) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	f.putCalls++
	f.lastPut = in
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

func (f *fakeS3API) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	f.mu.Lock()
	f.createCalls++
	f.lastCreate = in
	mpErr := f.mpErr
	if f.parts == nil {
		f.parts = make(map[int32][]byte)
	}
	f.uploadID = "upload-1"
	uploadID := f.uploadID
	f.mu.Unlock()

	if mpErr != nil {
		return nil, mpErr
	}

	return &s3.CreateMultipartUploadOutput{UploadId: &uploadID}, nil
}

func (f *fakeS3API) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	f.mu.Lock()
	f.uploadCalls++
	mpErr := f.mpErr
	f.mu.Unlock()

	if mpErr != nil {
		return nil, mpErr
	}

	var body []byte
	if in.Body != nil {
		b, err := io.ReadAll(in.Body)
		if err != nil {
			return nil, err
		}
		body = b
	}

	partNum := int32(0)
	if in.PartNumber != nil {
		partNum = *in.PartNumber
	}

	f.mu.Lock()
	if f.parts == nil {
		f.parts = make(map[int32][]byte)
	}
	f.parts[partNum] = body
	f.mu.Unlock()

	etag := fmt.Sprintf("etag-%d", partNum)
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (f *fakeS3API) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	f.mu.Lock()
	f.completeCalls++
	f.lastComplete = in
	mpErr := f.mpErr

	// snapshot parts (avoid holding lock while building)
	partsCopy := make(map[int32][]byte, len(f.parts))
	for k, v := range f.parts {
		partsCopy[k] = v
	}
	f.mu.Unlock()

	if mpErr != nil {
		return nil, mpErr
	}

	// Reconstruct final body in the order provided by CompleteMultipartUpload
	var buf bytes.Buffer
	if in.MultipartUpload != nil {
		for _, p := range in.MultipartUpload.Parts {
			if p.PartNumber == nil {
				continue
			}
			buf.Write(partsCopy[*p.PartNumber])
		}
	}

	f.mu.Lock()
	f.lastBody = buf.Bytes()
	f.mu.Unlock()

	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeS3API) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.mu.Lock()
	f.abortCalls++
	mpErr := f.mpErr
	f.mu.Unlock()

	if mpErr != nil {
		return nil, mpErr
	}
	return &s3.AbortMultipartUploadOutput{}, nil
}

// ---- Required stubs for your interface ----

func (f *fakeS3API) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	f.getCalls++
	f.mu.Unlock()
	return nil, errors.New("GetObject not implemented in fake")
}

func (f *fakeS3API) HeadObject(ctx context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.mu.Lock()
	f.headCalls++
	f.mu.Unlock()
	return nil, errors.New("HeadObject not implemented in fake")
}

func (f *fakeS3API) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	f.listCalls++
	f.mu.Unlock()
	return &s3.ListObjectsV2Output{}, nil
}

type fakeS3NoCapture struct {
	mu sync.Mutex

	putCalls int
	putErr   error

	// multipart calls (for benches)
	createCalls   int
	uploadCalls   int
	completeCalls int
	abortCalls    int

	mpErr error

	// stubs
	getCalls  int
	headCalls int
	listCalls int
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
		if _, derr := io.Copy(io.Discard, in.Body); derr != nil {
			return nil, derr
		}
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3NoCapture) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	f.mu.Lock()
	f.createCalls++
	err := f.mpErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	id := "upload-1"
	return &s3.CreateMultipartUploadOutput{UploadId: &id}, nil
}

func (f *fakeS3NoCapture) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	f.mu.Lock()
	f.uploadCalls++
	err := f.mpErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	if in.Body != nil {
		if _, derr := io.Copy(io.Discard, in.Body); derr != nil {
			return nil, derr
		}
	}
	etag := "etag"
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (f *fakeS3NoCapture) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	f.mu.Lock()
	f.completeCalls++
	err := f.mpErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeS3NoCapture) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.mu.Lock()
	f.abortCalls++
	err := f.mpErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (f *fakeS3NoCapture) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	f.getCalls++
	f.mu.Unlock()
	return nil, errors.New("GetObject not implemented in fake")
}

func (f *fakeS3NoCapture) HeadObject(ctx context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.mu.Lock()
	f.headCalls++
	f.mu.Unlock()
	return nil, errors.New("HeadObject not implemented in fake")
}

func (f *fakeS3NoCapture) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	f.listCalls++
	f.mu.Unlock()
	return &s3.ListObjectsV2Output{}, nil
}

type bytesStreamWriter struct {
	b []byte
}

func (w bytesStreamWriter) Write(dst io.Writer) error {
	_, err := dst.Write(w.b)
	return err
}

type errStreamWriter struct {
	err error
}

func (w errStreamWriter) Write(dst io.Writer) error {
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
	if f.lastPut == nil {
		t.Fatalf("expected lastPut to be set")
	}

	if aws.ToString(f.lastPut.Bucket) != "bkt" {
		t.Fatalf("bucket: %q", aws.ToString(f.lastPut.Bucket))
	}
	if aws.ToString(f.lastPut.Key) != "pfx/a/../b/x.parquet" {
		t.Fatalf("key: %q", aws.ToString(f.lastPut.Key))
	}
	if aws.ToString(f.lastPut.ContentType) != "application/octet-stream" {
		t.Fatalf("content-type: %q", aws.ToString(f.lastPut.ContentType))
	}
	if f.lastPut.ContentLength == nil || *f.lastPut.ContentLength != int64(len(data)) {
		t.Fatalf("content-length: %#v", f.lastPut.ContentLength)
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

	// WriteStream uses PutObject in the current implementation
	if f.putCalls != 1 {
		t.Fatalf("expected 1 PutObject call, got %d", f.putCalls)
	}
	if f.lastPut == nil {
		t.Fatalf("expected lastPut to be set")
	}

	if aws.ToString(f.lastPut.Bucket) != "bkt" {
		t.Fatalf("bucket: %q", aws.ToString(f.lastPut.Bucket))
	}
	if aws.ToString(f.lastPut.Key) != "pfx/a/../b/x.parquet" {
		t.Fatalf("key: %q", aws.ToString(f.lastPut.Key))
	}
	if aws.ToString(f.lastPut.ContentType) != "application/octet-stream" {
		t.Fatalf("content-type: %q", aws.ToString(f.lastPut.ContentType))
	}

	// Streaming via io.Pipe() can't know Content-Length upfront.
	// Current behavior: ContentLength is nil.
	if f.lastPut.ContentLength != nil {
		t.Fatalf("content-length should be nil for WriteStream PutObject path, got %#v", f.lastPut.ContentLength)
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
