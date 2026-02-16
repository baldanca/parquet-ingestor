package ingestor

import "context"

type Encoder[iType any] interface {
	Encode(ctx context.Context, items []iType) (data []byte, contentType string, err error)
}
