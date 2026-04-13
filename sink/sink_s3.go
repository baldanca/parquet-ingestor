package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Sink uploads objects to Amazon S3.
//
// It supports both buffered (Write) and streaming (WriteStream) uploads.
type S3Sink struct {
	client  transfermanager.S3APIClient
	manager *transfermanager.Client

	bucket    string
	bucketPtr *string
	prefix    string

	pool sync.Pool
}

// Sink is kept for backward compatibility.
type Sink = S3Sink

// NewSinkS3 creates an S3 sink that uploads objects to bucket, optionally
// under prefix. The prefix is trimmed of leading/trailing slashes and joined
// with the object key with a single '/' separator.
//
// NewSinkS3 panics on nil client or blank bucket because those are
// configuration errors that should be caught at startup, not at write time.
func NewSinkS3(client transfermanager.S3APIClient, bucket, prefix string) *S3Sink {
	if client == nil {
		panic("s3 client is required")
	}
	if strings.TrimSpace(bucket) == "" {
		panic("bucket is required")
	}

	s := &S3Sink{
		client:  client,
		manager: transfermanager.New(client),
		bucket:  bucket,
		prefix:  strings.Trim(prefix, "/"),
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

	key := trimLeftSlashes(req.Key)
	fullKey := joinPrefix(s.prefix, key, &strings.Builder{})

	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()

	var (
		wg   sync.WaitGroup
		werr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		werr = req.Writer.Write(pw)
		_ = pw.CloseWithError(werr)
	}()

	in := &transfermanager.UploadObjectInput{
		Bucket: s.bucketPtr,
		Key:    &fullKey,
		Body:   pr,
	}
	if req.ContentType != "" {
		ct := req.ContentType
		in.ContentType = &ct
	}

	_, err := s.manager.UploadObject(ctx, in)
	if err != nil {
		_ = pr.CloseWithError(err)
		wg.Wait()
		return fmt.Errorf("upload s3 object key=%q: %w", key, err)
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

// ResolvePath returns the fully-qualified S3 object path for the given key.
func (s *S3Sink) ResolvePath(key string) string {
	cleanKey := trimLeftSlashes(key)
	fullKey := joinPrefix(s.prefix, cleanKey, &strings.Builder{})
	return "s3://" + s.bucket + "/" + fullKey
}

func trimLeftSlashes(s string) string {
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	return s
}
