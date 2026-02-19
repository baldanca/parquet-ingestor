package ingestor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/sink"
	"github.com/baldanca/parquet-ingestor/source"
	"github.com/baldanca/parquet-ingestor/transformer"
)

type KeyFunc[iType any] func(ctx context.Context, batch batcher.Batch[iType]) (key string, err error)

type Ingestor[iType any] struct {
	batcherConfig batcher.BatcherConfig
	source        source.Sourcer
	transformer   transformer.Transformer[source.Envelope, iType]
	encoder       encoder.Encoder[iType]
	sink          sink.Sinkr
	keyFunc       KeyFunc[iType]

	// retries
	retry    RetryPolicy // for sink write
	ackRetry RetryPolicy // for ack commit

	batcher *batcher.Batcher[iType]
}

func NewIngestor[iType any](
	batcherConfig batcher.BatcherConfig,
	source source.Sourcer,
	transformer transformer.Transformer[source.Envelope, iType],
	encoder encoder.Encoder[iType],
	sink sink.Sinkr,
	keyFunc KeyFunc[iType],
) (*Ingestor[iType], error) {
	if source == nil {
		return nil, fmt.Errorf("source is nil")
	}
	if transformer == nil {
		return nil, fmt.Errorf("transformer is nil")
	}
	if encoder == nil {
		return nil, fmt.Errorf("encoder is nil")
	}
	if sink == nil {
		return nil, fmt.Errorf("sink is nil")
	}
	if keyFunc == nil {
		return nil, fmt.Errorf("keyFunc is nil")
	}

	b, err := batcher.NewBatcher[iType](batcherConfig)
	if err != nil {
		return nil, err
	}

	i := &Ingestor[iType]{
		batcherConfig: batcherConfig,
		source:        source,
		transformer:   transformer,
		encoder:       encoder,
		sink:          sink,
		keyFunc:       keyFunc,
		retry:         nopRetry{},
		ackRetry:      nopRetry{},
		batcher:       b,
	}

	return i, nil
}

func NewDefaultIngestor[iType any](
	source source.Sourcer,
	transformer transformer.Transformer[source.Envelope, iType],
	encoder encoder.Encoder[iType],
	sink sink.Sinkr,
	keyFunc KeyFunc[iType],
) (*Ingestor[iType], error) {
	return NewIngestor(batcher.DefaultBatcherConfig, source, transformer, encoder, sink, keyFunc)
}

// Optional setters to avoid breaking the constructor signature.
func (i *Ingestor[iType]) SetRetryPolicy(p RetryPolicy) {
	if p == nil {
		i.retry = nopRetry{}
		return
	}
	i.retry = p
}

func (i *Ingestor[iType]) SetAckRetryPolicy(p RetryPolicy) {
	if p == nil {
		i.ackRetry = nopRetry{}
		return
	}
	i.ackRetry = p
}

func (i *Ingestor[iType]) RunWithWorkers(ctx context.Context, workers int) error {
	if workers <= 1 {
		return i.Run(ctx)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		worker, err := i.cloneWithNewBatcher()
		if err != nil {
			return err
		}
		go func() {
			errCh <- worker.Run(ctx)
		}()
	}

	var firstErr error
	for w := 0; w < workers; w++ {
		err := <-errCh
		if err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	return firstErr
}

func (i *Ingestor[iType]) cloneWithNewBatcher() (*Ingestor[iType], error) {
	b, err := batcher.NewBatcher[iType](i.batcherConfig)
	if err != nil {
		return nil, err
	}
	return &Ingestor[iType]{
		batcherConfig: i.batcherConfig,
		source:        i.source,
		transformer:   i.transformer,
		encoder:       i.encoder,
		sink:          i.sink,
		keyFunc:       i.keyFunc,
		retry:         i.retry,
		ackRetry:      i.ackRetry,
		batcher:       b,
	}, nil
}

func (i *Ingestor[iType]) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return i.flushRemainingOnStop(ctx)
		}

		recvCtx := ctx
		var cancel context.CancelFunc
		if deadline, ok := i.batcher.Deadline(); ok {
			recvCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		msg, err := i.source.Receive(recvCtx)
		if cancel != nil {
			cancel()
		}

		if err != nil {
			// Deadline hit => time-based flush.
			if errors.Is(err, context.DeadlineExceeded) {
				if err := i.flush(ctx); err != nil {
					return err
				}
				continue
			}
			if ctx.Err() != nil {
				return i.flushRemainingOnStop(ctx)
			}
			return err
		}

		flushNow, err := i.processMessage(ctx, msg)
		if err != nil {
			return err
		}
		if flushNow {
			if err := i.flush(ctx); err != nil {
				return err
			}
		}
	}
}

func (i *Ingestor[iType]) processMessage(ctx context.Context, msg source.Message) (flushNow bool, err error) {
	env := msg.Data()

	out, err := i.transformer.Transform(ctx, env)
	if err != nil {
		_ = msg.Fail(ctx, err)
		return false, nil
	}

	sizeBytes := int64(0)
	if n, ok := msg.EstimatedSizeBytes(); ok {
		sizeBytes = n
	} else {
		var sizeErr error
		sizeBytes, sizeErr = estimateSizeBytesFallback(env.Payload)
		if sizeErr != nil {
			_ = msg.Fail(ctx, sizeErr)
			return false, nil
		}
	}

	now := time.Now()
	flushNow = i.batcher.Add(now, out, msg, sizeBytes)
	return flushNow, nil
}

func (i *Ingestor[iType]) flush(ctx context.Context) error {
	batch := i.batcher.Flush()
	if len(batch.Items) == 0 {
		return nil
	}

	key, err := i.keyFunc(ctx, batch)
	if err != nil {
		return err
	}

	// 1) Prefer streaming when both encoder + sink support it.
	if streamed, err := tryStreamWrite[iType](ctx, i.encoder, i.sink, i.retry, key, batch.Items); streamed {
		if err != nil {
			return err
		}
		// Ack only after successful write (with retries).
		if err := i.ackRetry.Do(ctx, func(ctx context.Context) error {
			return batch.Acks.Commit(ctx, i.source)
		}); err != nil {
			return err
		}
		return nil
	}

	// 2) Fallback: in-memory Encode + Write.
	data, contentType, err := i.encoder.Encode(ctx, batch.Items)
	if err != nil {
		return err
	}

	// Prefer encoder.ContentType() if Encode returned empty.
	if contentType == "" {
		contentType = i.encoder.ContentType()
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	writeReq := sink.WriteRequest{Key: key, Data: data, ContentType: contentType}

	// Retry sink write
	if err := i.retry.Do(ctx, func(ctx context.Context) error {
		return i.sink.Write(ctx, writeReq)
	}); err != nil {
		return err
	}

	// Ack only after successful write (with retries)
	if err := i.ackRetry.Do(ctx, func(ctx context.Context) error {
		return batch.Acks.Commit(ctx, i.source)
	}); err != nil {
		return err
	}

	return nil
}

func (i *Ingestor[iType]) flushRemainingOnStop(ctx context.Context) error {
	// Best effort: keep values/deadlines but ignore cancellation, and don't block forever.
	base := context.WithoutCancel(ctx)
	stopCtx, cancel := context.WithTimeout(base, 10*time.Second)
	defer cancel()
	return i.flush(stopCtx)
}

// DefaultKeyFunc partitions by time and avoids collisions (good for multiple workers).
func DefaultKeyFunc[iType any](enc encoder.Encoder[iType]) KeyFunc[iType] {
	ext := enc.FileExtension()
	if ext == "" || ext[0] != '.' {
		// Defensive: keep keys consistent.
		ext = ".bin"
	}
	return func(ctx context.Context, batch batcher.Batch[iType]) (string, error) {
		_ = ctx
		_ = batch

		now := time.Now().UTC()
		suffix, err := randomHex(8) // 16 chars
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%04d/%02d/%02d/%02d/%d-%s%s",
			now.Year(), int(now.Month()), now.Day(), now.Hour(), now.UnixNano(), suffix, ext,
		), nil
	}
}

func estimateSizeBytesFallback(v any) (int64, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return int64(len(b)), nil
}

func randomHex(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
