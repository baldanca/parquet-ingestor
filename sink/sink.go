package sink

import (
	"context"
	"io"
)

// WriteRequest is a fully-buffered write to a sink destination.
type WriteRequest struct {
	Key         string
	Data        []byte
	ContentType string
}

// StreamWriter writes a payload to the provided writer.
//
// Implementations should return only after all bytes were written or an error
// occurred.
type StreamWriter interface {
	WriteTo(w io.Writer) error
}

// StreamWriteRequest describes a streaming write to a sink destination.
type StreamWriteRequest struct {
	Key         string
	ContentType string
	Writer      StreamWriter
}

// Sinkr persists files produced by the encoder.
type Sinkr interface {
	Write(ctx context.Context, req WriteRequest) error
}

// StreamSinkr is an optional interface for sinks that can upload content using
// an io stream, avoiding buffering the full payload in memory.
type StreamSinkr interface {
	WriteStream(ctx context.Context, req StreamWriteRequest) error
}
