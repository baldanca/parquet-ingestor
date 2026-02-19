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

var parquetBufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

type ParquetEncoder[iType any] struct {
	Compression string
}

func (e ParquetEncoder[iType]) FileExtension() string { return ".parquet" }

func (e ParquetEncoder[iType]) ContentType() string {
	return parquetContentType
}

func (e ParquetEncoder[iType]) EncodeTo(ctx context.Context, items []iType, w io.Writer) error {
	if w == nil {
		return fmt.Errorf("nil writer")
	}

	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	comp := strings.TrimSpace(e.Compression)

	var pw *parquet.GenericWriter[iType]
	switch {
	case comp == "":
		pw = parquet.NewGenericWriter[iType](w)
	case strings.EqualFold(comp, "snappy"):
		pw = parquet.NewGenericWriter[iType](w, parquet.Compression(&parquet.Snappy))
	case strings.EqualFold(comp, "gzip"):
		pw = parquet.NewGenericWriter[iType](w, parquet.Compression(&parquet.Gzip))
	case strings.EqualFold(comp, "zstd"):
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

	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
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

	err := e.EncodeTo(ctx, items, buf)
	if err != nil {
		parquetBufferPool.Put(buf)
		return nil, err
	}

	out := append([]byte(nil), buf.Bytes()...)
	parquetBufferPool.Put(buf)

	return out, nil
}
