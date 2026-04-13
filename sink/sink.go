package sink

import (
	"context"
	"io"
)

// WriteRequest describes a fully-buffered write to a sink destination.
type WriteRequest struct {
	// Key is the destination object key or path (e.g. an S3 object key).
	Key string
	// Data is the encoded payload to write.
	Data []byte
	// ContentType is the MIME type of the payload, forwarded as-is to the
	// underlying storage (e.g. S3 Content-Type header).
	ContentType string
}

// StreamWriter produces a payload by writing to the provided io.Writer.
// It must return only after all bytes have been written or an error occurred.
// The ingestor passes an io.Writer backed by the sink's upload stream, so
// Write must not close or retain the writer after returning.
type StreamWriter interface {
	Write(w io.Writer) error
}

// StreamWriteRequest describes a streaming write to a sink destination.
type StreamWriteRequest struct {
	Key         string
	ContentType string
	// Writer produces the payload on demand. The sink calls Writer.Write once
	// and pipes the output directly to the storage backend.
	Writer StreamWriter
}

// Sinkr persists encoded data to a storage backend. Implementations must be
// safe for concurrent use: multiple flush workers may call Write simultaneously.
type Sinkr interface {
	Write(ctx context.Context, req WriteRequest) error
}

// StreamSinkr is an optional interface for sinks that support streaming
// uploads, which avoids buffering the full encoded payload in memory. When
// both the encoder (StreamEncoder) and the sink implement this interface, the
// ingestor uses the streaming path automatically.
type StreamSinkr interface {
	WriteStream(ctx context.Context, req StreamWriteRequest) error
}
