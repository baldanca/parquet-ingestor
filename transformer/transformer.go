package transformer

import (
	"context"

	"github.com/baldanca/parquet-ingestor/source"
)

// Transformer converts one value into another.
//
// In this project it typically converts a source.Envelope into a typed record.
type Transformer[O any] interface {
	Transform(ctx context.Context, in source.Envelope) (O, error)
}
