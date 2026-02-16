package ingestor

import "context"

type WriteRequest struct {
	Key         string
	Data        []byte
	ContentType string
	Meta        map[string]string
}

type Sink interface {
	Write(ctx context.Context, req WriteRequest) error
}
