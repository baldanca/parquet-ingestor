package encoder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
)

const parquetContentType = "application/vnd.apache.parquet"

var parquetBufferPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// ParquetCompression selects the compression codec used by ParquetEncoder.
type ParquetCompression string

const (
	ParquetCompressionNone   ParquetCompression = ""
	ParquetCompressionSnappy ParquetCompression = "snappy"
	ParquetCompressionGzip   ParquetCompression = "gzip"
	ParquetCompressionZstd   ParquetCompression = "zstd"
)

// ParseParquetCompression converts a user string to a ParquetCompression.
//
// The parsing is case-insensitive and trims surrounding whitespace.
func ParseParquetCompression(s string) (ParquetCompression, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return ParquetCompressionNone, nil
	}
	s = strings.ToLower(s)
	switch ParquetCompression(s) {
	case ParquetCompressionSnappy, ParquetCompressionGzip, ParquetCompressionZstd:
		return ParquetCompression(s), nil
	default:
		return "", fmt.Errorf("unsupported parquet compression: %q", s)
	}
}

// ParquetEncoder encodes records into an Apache Parquet file using parquet-go.
//
// The record type must be compatible with parquet-go's GenericWriter.
type ParquetEncoder[iType any] struct {
	Compression ParquetCompression
}

// NewParquetEncoder returns a ParquetEncoder configured with the given codec.
func NewParquetEncoder[iType any](compression ParquetCompression) ParquetEncoder[iType] {
	return ParquetEncoder[iType]{Compression: compression}
}

func (e ParquetEncoder[iType]) FileExtension() string { return ".parquet" }

func (e ParquetEncoder[iType]) ContentType() string { return parquetContentType }

func (e ParquetEncoder[iType]) EncodeTo(ctx context.Context, items []iType, w io.Writer) error {
	if w == nil {
		return fmt.Errorf("writer is nil")
	}
	if err := ctxErr(ctx); err != nil {
		return err
	}

	var pw *parquet.GenericWriter[iType]
	switch e.Compression {
	case ParquetCompressionNone:
		pw = parquet.NewGenericWriter[iType](w)
	case ParquetCompressionSnappy:
		pw = parquet.NewGenericWriter[iType](w, parquet.Compression(&parquet.Snappy))
	case ParquetCompressionGzip:
		pw = parquet.NewGenericWriter[iType](w, parquet.Compression(&parquet.Gzip))
	case ParquetCompressionZstd:
		pw = parquet.NewGenericWriter[iType](w, parquet.Compression(&parquet.Zstd))
	default:
		return fmt.Errorf("unsupported parquet compression: %q", e.Compression)
	}

	if _, err := pw.Write(items); err != nil {
		_ = pw.Close()
		return err
	}

	if err := pw.Close(); err != nil {
		return err
	}

	return ctxErr(ctx)
}

func (e ParquetEncoder[iType]) Encode(ctx context.Context, items []iType) ([]byte, error) {
	buf := parquetBufferPool.Get().(*bytes.Buffer)
	buf.Reset()

	if n := len(items); n > 0 {
		grow := n * 128
		if grow > 4<<20 {
			grow = 4 << 20
		}
		buf.Grow(grow)
	}

	if err := e.EncodeTo(ctx, items, buf); err != nil {
		parquetBufferPool.Put(buf)
		return nil, err
	}

	out := append([]byte(nil), buf.Bytes()...)
	parquetBufferPool.Put(buf)
	return out, nil
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
