package encoder

import (
	"bytes"
	"context"
	"fmt"

	"github.com/parquet-go/parquet-go"
)

type ParquetEncoder[iType any] struct {
	// Compression (optional): "", "snappy", "gzip", "zstd"
	Compression string
}

func (e ParquetEncoder[iType]) FileExtension() string { return ".parquet" }

func (e ParquetEncoder[iType]) Encode(ctx context.Context, items []iType) ([]byte, string, error) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		default:
		}
	}

	output := &bytes.Buffer{}
	options := make([]parquet.WriterOption, 0, 1)

	switch e.Compression {
	case "":
		// no compression
	case "snappy":
		options = append(options, parquet.Compression(&parquet.Snappy))
	case "gzip":
		options = append(options, parquet.Compression(&parquet.Gzip))
	case "zstd":
		options = append(options, parquet.Compression(&parquet.Zstd))
	default:
		return nil, "", fmt.Errorf("unsupported parquet compression: %q", e.Compression)
	}

	w := parquet.NewGenericWriter[iType](output, options...)

	if _, err := w.Write(items); err != nil {
		_ = w.Close()
		return nil, "", err
	}

	if err := w.Close(); err != nil {
		return nil, "", err
	}

	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		default:
		}
	}

	return output.Bytes(), "application/vnd.apache.parquet", nil
}
