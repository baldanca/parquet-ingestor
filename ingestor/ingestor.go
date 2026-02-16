package ingestor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type KeyFunc[iType any] func(ctx context.Context, batch Batch[iType]) (key string, meta map[string]string, err error)

type Ingestor[iType any] struct {
	cfg Config

	source      Source
	transformer Transformer[Envelope, iType]
	encoder     Encoder[iType]
	sink        Sink
	keyFunc     KeyFunc[iType]

	batcher *Batcher[iType]
}

func NewIngestor[iType any](
	cfg Config,
	source Source,
	transformer Transformer[Envelope, iType],
	encoder Encoder[iType],
	sink Sink,
	keyFunc KeyFunc[iType],
) (*Ingestor[iType], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
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

	b, err := NewBatcher[iType](cfg)
	if err != nil {
		return nil, err
	}

	i := &Ingestor[iType]{
		cfg:         cfg,
		source:      source,
		transformer: transformer,
		encoder:     encoder,
		sink:        sink,
		keyFunc:     keyFunc,
		batcher:     b,
	}
	if i.keyFunc == nil {
		i.keyFunc = i.defaultKeyFunc
	}
	return i, nil
}

func NewDefaultIngestor[iType any](
	source Source,
	transformer Transformer[Envelope, iType],
	encoder Encoder[iType],
	sink Sink,
	keyFunc KeyFunc[iType],
) (*Ingestor[iType], error) {
	return NewIngestor(DefaultConfig, source, transformer, encoder, sink, keyFunc)
}

// RunWithWorkers roda N workers em paralelo, cada um com seu próprio Batcher.
// Isso aumenta throughput (principalmente em Source=SQS).
//
// Requisitos:
// - Source deve ser seguro para chamadas concorrentes de Receive/AckBatch.
// - Sink/Encoder/Transformer também devem tolerar concorrência (ou você pode envolver com mutex externamente).
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
	b, err := NewBatcher[iType](i.cfg)
	if err != nil {
		return nil, err
	}
	return &Ingestor[iType]{
		cfg:         i.cfg,
		source:      i.source,
		transformer: i.transformer,
		encoder:     i.encoder,
		sink:        i.sink,
		keyFunc:     i.keyFunc,
		batcher:     b,
	}, nil
}

func (i *Ingestor[iType]) Run(ctx context.Context) error {
	for {
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
			// Se estamos encerrando, faz flush best-effort e sai.
			if ctx.Err() != nil {
				return i.flushRemainingOnStop()
			}

			// Se deadline estourou, flush e segue.
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				if flushErr := i.flush(ctx); flushErr != nil {
					return flushErr
				}
				continue
			}

			return err
		}

		flushNow, procErr := i.processMessage(ctx, msg)
		if procErr != nil {
			return procErr
		}

		if flushNow {
			if flushErr := i.flush(ctx); flushErr != nil {
				return flushErr
			}
		}
	}
}

func (i *Ingestor[iType]) processMessage(ctx context.Context, msg Message) (flushNow bool, err error) {
	env := msg.Data()

	out, err := i.transformer.Transform(ctx, env)
	if err != nil {
		_ = msg.Fail(ctx, err)
		return false, nil
	}

	sizeBytes, sizeErr := estimateSizeBytes(env.Payload)
	if sizeErr != nil {
		_ = msg.Fail(ctx, sizeErr)
		return false, nil
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

	key, meta, err := i.keyFunc(ctx, batch)
	if err != nil {
		return err
	}

	data, contentType, err := i.encoder.Encode(ctx, batch.Items)
	if err != nil {
		return err
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if err := i.sink.Write(ctx, WriteRequest{
		Key:         key,
		Data:        data,
		ContentType: contentType,
		Meta:        meta,
	}); err != nil {
		return err
	}

	if err := batch.Acks.Commit(ctx, i.source); err != nil {
		return err
	}

	return nil
}

func (i *Ingestor[iType]) flushRemainingOnStop() error {
	return i.flush(context.Background())
}

func (i *Ingestor[iType]) defaultKeyFunc(ctx context.Context, batch Batch[iType]) (string, map[string]string, error) {
	_ = ctx
	_ = batch

	now := time.Now().UTC()
	key := fmt.Sprintf("ingestor/%04d/%02d/%02d/%02d/%d.parquet",
		now.Year(), now.Month(), now.Day(), now.Hour(), now.UnixNano(),
	)
	return key, nil, nil
}

func estimateSizeBytes(v any) (int64, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return int64(len(b)), nil
}
