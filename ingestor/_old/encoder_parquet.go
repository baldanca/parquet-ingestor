package ingestor

import (
	"bytes"
	"context"

	"github.com/parquet-go/parquet-go"
)

type ParquetEncoder[iType any] struct {
	// Compression (opcional): "snappy", "gzip", "zstd"
	Compression string
}

func (e ParquetEncoder[iType]) FileExtension() string { return ".parquet" }

func (e ParquetEncoder[iType]) Encode(ctx context.Context, items []iType) ([]byte, string, error) {
	output := &bytes.Buffer{}

	options := []parquet.WriterOption{}

	switch e.Compression {
	case "snappy":
		options = append(options, parquet.Compression(&parquet.Snappy))
	case "gzip":
		options = append(options, parquet.Compression(&parquet.Gzip))
	case "zstd":
		options = append(options, parquet.Compression(&parquet.Zstd))
	}

	w := parquet.NewGenericWriter[iType](output, options...)

	if _, err := w.Write(items); err != nil {
		_ = w.Close()
		return nil, "", err
	}

	if err := w.Close(); err != nil {
		return nil, "", err
	}

	return output.Bytes(), "application/vnd.apache.parquet", nil
}
