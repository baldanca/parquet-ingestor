package encoder

import (
	"context"
	"io"
)

// Encoder converts a slice of typed records into a binary payload.
//
// Implementations must be safe for concurrent use unless documented otherwise.
type Encoder[iType any] interface {
	Encode(ctx context.Context, items []iType) (data []byte, err error)
	FileExtension() string
	ContentType() string
}

// StreamEncoder is an optional interface for encoders that can write directly
// to an io.Writer to avoid buffering the full output in memory.
type StreamEncoder[iType any] interface {
	EncodeTo(ctx context.Context, items []iType, w io.Writer) error
	FileExtension() string
	ContentType() string
}
