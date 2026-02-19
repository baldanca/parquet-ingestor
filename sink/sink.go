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

// StreamWriter represents something that can write its contents to a destination writer.
// This avoids allocating function closures in hot paths.
type StreamWriter interface {
	WriteTo(w io.Writer) error
}

type StreamWriteRequest struct {
	Key         string
	ContentType string
	// Writer streams directly to the destination.
	// Implementations must return when done writing.
	Writer StreamWriter
}

type Sinkr interface {
	Write(ctx context.Context, req WriteRequest) error
}

// StreamSinkr is an optional interface implemented by sinks that can stream data directly
// to the destination without buffering the full payload in memory.
type StreamSinkr interface {
	WriteStream(ctx context.Context, req StreamWriteRequest) error
}
