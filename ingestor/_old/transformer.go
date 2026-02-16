package ingestor

import "context"

type Transformer[I any, O any] interface {
	Transform(ctx context.Context, in I) (O, error)
}
