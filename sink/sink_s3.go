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

type Sink struct {
	client s3API

	bucket    string
	bucketPtr *string
	prefix    string

	pool sync.Pool
}

func New(client s3API, bucket, prefix string) *Sink {
	if client == nil {
		panic("s3 client is required")
	}
	if strings.TrimSpace(bucket) == "" {
		panic("bucket is required")
	}

	s := &Sink{
		client: client,
		bucket: bucket,
		prefix: strings.Trim(prefix, "/"),
	}
	// Ponteiro estável.
	s.bucketPtr = &s.bucket

	// Scratch objects para evitar allocs por Write devido a “address-of-local escape”.
	s.pool.New = func() any { return new(putScratch) }
	return s
}

func (s *Sink) Write(ctx context.Context, req WriteRequest) error {
	if req.Key == "" {
		return fmt.Errorf("empty key")
	}

	// Use pooled scratch to avoid per-call heap allocations from escaping locals.
	sc := s.pool.Get().(*putScratch)
	defer s.pool.Put(sc)

	// Build key sem path-clean.
	key := trimLeftSlashes(req.Key)
	if s.prefix != "" {
		sc.b.Reset()
		sc.b.Grow(len(s.prefix) + 1 + len(key))
		sc.b.WriteString(s.prefix)
		sc.b.WriteByte('/')
		sc.b.WriteString(key)
		sc.key = sc.b.String()
	} else {
		sc.key = key
	}

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

// WriteStream streams the object body directly to S3 without buffering the full payload in memory.
// ContentLength is not set; the AWS SDK will stream the body as it is produced.
func (s *Sink) WriteStream(ctx context.Context, req StreamWriteRequest) error {
	if req.Key == "" {
		return fmt.Errorf("empty key")
	}
	if req.Writer == nil {
		return fmt.Errorf("nil writer")
	}

	// Use pooled scratch to avoid per-call heap allocations from escaping locals.
	sc := s.pool.Get().(*putScratch)
	defer s.pool.Put(sc)

	// Build key sem path-clean.
	key := trimLeftSlashes(req.Key)
	if s.prefix != "" {
		sc.sb.Reset()
		sc.sb.Grow(len(s.prefix) + 1 + len(key))
		sc.sb.WriteString(s.prefix)
		sc.sb.WriteByte('/')
		sc.sb.WriteString(key)
		sc.key = sc.sb.String()
	} else {
		sc.key = key
	}

	if req.ContentType != "" {
		sc.ct = req.ContentType
		sc.in.ContentType = &sc.ct
	} else {
		sc.ct = ""
		sc.in.ContentType = nil
	}

	pr, pw := io.Pipe()
	// If PutObject returns early (ctx cancel / network error), close reader to unblock writer.
	defer pr.Close()

	var (
		wg   sync.WaitGroup
		werr error
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := req.Writer.WriteTo(pw)
		werr = err
		_ = pw.CloseWithError(err)
	}()

	sc.in.Bucket = s.bucketPtr
	sc.in.Key = &sc.key
	sc.in.Body = pr
	// Unknown length for streaming.
	sc.in.ContentLength = nil

	_, err := s.client.PutObject(ctx, &sc.in)
	if err != nil {
		_ = pr.CloseWithError(err)
		wg.Wait()
		return fmt.Errorf("put s3 object key=%q: %w", key, err)
	}

	// Ensure producer finished (and surface producer errors even if S3 accepted fast).
	wg.Wait()
	if werr != nil {
		return fmt.Errorf("stream write key=%q: %w", key, werr)
	}
	return nil
}

type putScratch struct {
	in   s3.PutObjectInput
	key  string
	ct   string
	cl   int64
	body bytes.Reader
	b    strings.Builder
	sb   strings.Builder
}

func trimLeftSlashes(s string) string {
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	return s
}
