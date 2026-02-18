package sink

import "context"

type WriteRequest struct {
	Key         string
	Data        []byte
	ContentType string
}

type Sinkr interface {
	Write(ctx context.Context, req WriteRequest) error
}
