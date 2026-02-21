package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3Sink uploads objects to Amazon S3.
//
// It supports both buffered (Write) and streaming (WriteStream) uploads.
type S3Sink struct {
	client s3API

	bucket    string
	bucketPtr *string
	prefix    string

	pool sync.Pool
}

// Sink is kept for backward compatibility.
type Sink = S3Sink

// NewSinkS3 creates an S3 sink that uploads to the given bucket and optional prefix.
//
// Prefix is joined with the request key using a single '/' separator.
func NewSinkS3(client s3API, bucket, prefix string) *S3Sink {
	if client == nil {
		panic("s3 client is required")
	}
	if strings.TrimSpace(bucket) == "" {
		panic("bucket is required")
	}

	s := &S3Sink{
		client: client,
		bucket: bucket,
		prefix: strings.Trim(prefix, "/"),
	}
	s.bucketPtr = &s.bucket

	s.pool.New = func() any { return new(putScratch) }
	return s
}

func (s *S3Sink) Write(ctx context.Context, req WriteRequest) error {
	if req.Key == "" {
		return fmt.Errorf("key is empty")
	}

	sc := s.pool.Get().(*putScratch)
	defer s.pool.Put(sc)

	key := trimLeftSlashes(req.Key)
	sc.key = joinPrefix(s.prefix, key, &sc.sb)

	sc.cl = int64(len(req.Data))

	if req.ContentType != "" {
		sc.ct = req.ContentType
		sc.in.ContentType = &sc.ct
	} else {
		sc.ct = ""
		sc.in.ContentType = nil
	}

	sc.body.Reset(req.Data)

	sc.in.Bucket = s.bucketPtr
	sc.in.Key = &sc.key
	sc.in.Body = &sc.body
	sc.in.ContentLength = &sc.cl

	_, err := s.client.PutObject(ctx, &sc.in)
	if err != nil {
		return fmt.Errorf("put s3 object key=%q: %w", key, err)
	}
	return nil
}

func (s *S3Sink) WriteStream(ctx context.Context, req StreamWriteRequest) error {
	if req.Key == "" {
		return fmt.Errorf("key is empty")
	}
	if req.Writer == nil {
		return fmt.Errorf("writer is nil")
	}

	sc := s.pool.Get().(*putScratch)
	defer s.pool.Put(sc)

	key := trimLeftSlashes(req.Key)
	sc.key = joinPrefix(s.prefix, key, &sc.sb)

	if req.ContentType != "" {
		sc.ct = req.ContentType
		sc.in.ContentType = &sc.ct
	} else {
		sc.ct = ""
		sc.in.ContentType = nil
	}

	pr, pw := io.Pipe()
	defer pr.Close()

	var (
		wg   sync.WaitGroup
		werr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := req.Writer.Write(pw)
		werr = err
		_ = pw.CloseWithError(err)
	}()

	sc.in.Bucket = s.bucketPtr
	sc.in.Key = &sc.key
	sc.in.Body = pr
	sc.in.ContentLength = nil

	_, err := s.client.PutObject(ctx, &sc.in)
	if err != nil {
		_ = pr.CloseWithError(err)
		wg.Wait()
		return fmt.Errorf("put s3 object key=%q: %w", key, err)
	}

	wg.Wait()
	if werr != nil {
		return fmt.Errorf("stream producer error key=%q: %w", key, werr)
	}
	return nil
}

type putScratch struct {
	in   s3.PutObjectInput
	key  string
	ct   string
	cl   int64
	body bytes.Reader
	sb   strings.Builder
}

func joinPrefix(prefix, key string, b *strings.Builder) string {
	if prefix == "" {
		return key
	}
	b.Reset()
	b.Grow(len(prefix) + 1 + len(key))
	b.WriteString(prefix)
	b.WriteByte('/')
	b.WriteString(key)
	return b.String()
}

func trimLeftSlashes(s string) string {
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	return s
}
