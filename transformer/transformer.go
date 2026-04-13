package transformer

import (
	"context"

	"github.com/baldanca/parquet-ingestor/source"
)

// Transformer converts a source envelope into zero or more typed records.
//
// Returning an empty slice is valid and means the message should be silently
// dropped (the source message will still be acknowledged). Returning a
// non-nil error signals a processing failure; the source message will be
// failed (not acknowledged) and the error will be counted in metrics.
type Transformer[O any] interface {
	Transform(ctx context.Context, in source.Envelope) ([]O, error)
}
