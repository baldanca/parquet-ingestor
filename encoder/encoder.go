package encoder

import (
	"context"
	"io"
)

// Encoder converts a slice of typed records into a binary payload.
//
// Implementations must be safe for concurrent use: multiple flush workers may
// call Encode simultaneously with different item slices.
type Encoder[iType any] interface {
	// Encode serialises items and returns the encoded bytes.
	Encode(ctx context.Context, items []iType) (data []byte, err error)
	// FileExtension returns the conventional file suffix including the leading
	// dot (e.g. ".parquet", ".json"). Used by DefaultKeyFunc when building
	// object keys.
	FileExtension() string
	// ContentType returns the MIME type of the encoded payload
	// (e.g. "application/vnd.apache.parquet"). Used as the Content-Type header
	// on sink writes. Return "" to fall back to "application/octet-stream".
	ContentType() string
}

// StreamEncoder is an optional interface that encoders may implement to write
// output directly to an io.Writer, avoiding an intermediate in-memory buffer.
// This is especially valuable for large batches or memory-constrained
// environments.
//
// When both the encoder and the sink implement StreamEncoder / StreamSinkr, the
// ingestor uses the streaming path automatically. Encoders that implement
// StreamEncoder should also implement Encoder as a buffered fallback.
type StreamEncoder[iType any] interface {
	// EncodeTo writes the encoded representation of items to w.
	// It must return only after all bytes have been written or an error occurs.
	EncodeTo(ctx context.Context, items []iType, w io.Writer) error
	FileExtension() string
	ContentType() string
}
