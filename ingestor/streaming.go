package ingestor

import (
	"context"
	"io"

	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/sink"
)

type encodeToWriter[iType any] struct {
	ctx   context.Context
	se    encoder.StreamEncoder[iType]
	items []iType
}

func (w encodeToWriter[iType]) WriteTo(dst io.Writer) error {
	return w.se.EncodeTo(w.ctx, w.items, dst)
}

func tryStreamWrite[iType any](
	ctx context.Context,
	enc encoder.Encoder[iType],
	s sink.Sinkr,
	retry RetryPolicy,
	key string,
	items []iType,
) (streamed bool, err error) {

	se, ok := enc.(encoder.StreamEncoder[iType])
	if !ok {
		return false, nil
	}
	ss, ok := s.(sink.StreamSinkr)
	if !ok {
		return false, nil
	}

	if retry == nil {
		retry = nopRetry{}
	}

	ct := enc.ContentType()
	if ct == "" {
		ct = "application/octet-stream"
	}

	req := sink.StreamWriteRequest{
		Key:         key,
		ContentType: ct,
		Writer:      encodeToWriter[iType]{ctx: ctx, se: se, items: items},
	}

	err = retry.Do(ctx, func(ctx context.Context) error {
		return ss.WriteStream(ctx, req)
	})

	return true, err
}
