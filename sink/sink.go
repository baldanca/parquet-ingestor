package sink

import (
	"context"
	"io"
)

type WriteRequest struct {
	Key         string
	Data        []byte
	ContentType string
}

type StreamWriteRequest struct {
	Key         string
	ContentType string
	// Write is called with a writer that streams directly to the destination.
	// The implementation must return when done writing.
	Write func(w io.Writer) error
}

type Sinkr interface {
	Write(ctx context.Context, req WriteRequest) error
}

// StreamSinkr is an optional interface implemented by sinks that can stream data directly
// to the destination without buffering the full payload in memory.
type StreamSinkr interface {
	WriteStream(ctx context.Context, req StreamWriteRequest) error
}
