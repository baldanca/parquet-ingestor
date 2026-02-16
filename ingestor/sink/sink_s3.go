package sink

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Sink struct {
	client *s3.Client

	bucket string
	prefix string
}

func New(client *s3.Client, bucket, prefix string) *Sink {
	return &Sink{
		client: client,
		bucket: bucket,
		prefix: strings.Trim(prefix, "/"),
	}
}

func (s *Sink) Write(ctx context.Context, req WriteRequest) error {
	if req.Key == "" {
		return fmt.Errorf("empty key")
	}
	key := req.Key
	if s.prefix != "" {
		key = path.Join(s.prefix, req.Key)
	}
	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(string(req.Data)),
		ContentType: aws.String(req.ContentType),
	}

	_, err := s.client.PutObject(ctx, input)
	return err
}
