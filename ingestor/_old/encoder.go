package ingestor

import "context"

type Encoder[iType any] interface {
	Encode(ctx context.Context, items []iType) (data []byte, contentType string, err error)
	// FileExtension returns the default file extension (including the dot), e.g. ".parquet" or ".ndjson".
	FileExtension() string
}
