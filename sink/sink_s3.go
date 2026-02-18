package sink

import (
	"bytes"
	"context"
	"fmt"
	"strings"

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
	// Pointer estável (sem aws.String que aloca).
	s.bucketPtr = &s.bucket
	return s
}

func (s *Sink) Write(ctx context.Context, req WriteRequest) error {
	if req.Key == "" {
		return fmt.Errorf("empty key")
	}

	// Mantém semântica do S3 (não faz path-clean).
	key := strings.TrimLeft(req.Key, "/")
	if s.prefix != "" {
		key = s.prefix + "/" + key
	}

	// Evita aws.String/aws.Int64 (alocam).
	keyVar := key
	cl := int64(len(req.Data))

	var ct string
	if req.ContentType != "" {
		ct = req.ContentType
	}

	// Evita alocação do bytes.NewReader.
	var body bytes.Reader
	body.Reset(req.Data)

	input := s3.PutObjectInput{
		Bucket:        s.bucketPtr,
		Key:           &keyVar,
		Body:          &body,
		ContentLength: &cl,
	}
	if ct != "" {
		input.ContentType = &ct
	}

	_, err := s.client.PutObject(ctx, &input)
	if err != nil {
		return fmt.Errorf("put s3 object key=%q: %w", key, err)
	}
	return nil
}
