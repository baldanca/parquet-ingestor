package encoder

import (
	"context"
	"io"
)

type Encoder[iType any] interface {
	Encode(ctx context.Context, items []iType) (data []byte, err error)
	FileExtension() string
	ContentType() string
}

// StreamEncoder is an optional interface for encoders that can write directly to an io.Writer.
type StreamEncoder[iType any] interface {
	EncodeTo(ctx context.Context, items []iType, w io.Writer) error
	FileExtension() string
	ContentType() string
}
