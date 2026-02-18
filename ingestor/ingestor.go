package ingestor

/*
type KeyFunc[iType any] func(ctx context.Context, batch Batch[iType]) (key string, err error)

type Ingestor[iType any] struct {
	cfg Config

	source      Source
	transformer Transformer[Envelope, iType]
	encoder     Encoder[iType]
	sink        Sink
	keyFunc     KeyFunc[iType]
	retry       RetryPolicy
	ackRetry    RetryPolicy

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
	if keyFunc == nil {
		return nil, fmt.Errorf("keyFunc is nil")
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

	if i.cfg.RetryPolicy != nil {
		i.retry = i.cfg.RetryPolicy
	} else {
		i.retry = nopRetry{}
	}
	if i.cfg.AckRetryPolicy != nil {
		i.ackRetry = i.cfg.AckRetryPolicy
	} else {
		i.ackRetry = nopRetry{}
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
//
// Requisitos:
// - Source deve ser seguro para chamadas concorrentes de Receive/AckBatch.
// - Transformer/Encoder/Sink também devem tolerar concorrência (ou envolva com mutex externamente).
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
		retry:       i.retry,
		ackRetry:    i.ackRetry,
		batcher:     b,
	}, nil
}

// Run processa mensagens e faz flush por:
// - tamanho/quantidade (via b.batcher.Add)
// - tempo (deadline aplicada ao Receive)
//
// Backpressure: o loop é síncrono. Se Transformer/Encoder/Sink forem lentos, o Receive será naturalmente pressionado
// (menos chamadas por segundo), evitando crescimento de memória.
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

func (i *Ingestor[iType]) processMessage(ctx context.Context, msg Message) (flushNow bool, err error) {
	env := msg.Data()

	out, err := i.transformer.Transform(ctx, env)
	if err != nil {
		wrapped := fmt.Errorf("%w: %v", ErrTransform, err)
		_ = msg.Fail(ctx, wrapped)
		return false, nil
	}

	sizeBytes := int64(0)
	if n, ok := msg.EstimatedSizeBytes(); ok {
		sizeBytes = n
	} else {
		// Slow fallback – prefer implementing EstimatedSizeBytes in your Source.
		var sizeErr error
		sizeBytes, sizeErr = estimateSizeBytesFallback(env.Payload)
		if sizeErr != nil {
			wrapped := fmt.Errorf("%w: %v", ErrTransform, sizeErr)
			_ = msg.Fail(ctx, wrapped)
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

	data, contentType, err := i.encoder.Encode(ctx, batch.Items)
	if err != nil {
		wrapped := fmt.Errorf("%w: %v", ErrEncode, err)
		return wrapped
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	writeReq := WriteRequest{Key: key, Data: data, ContentType: contentType}
	if err := i.retry.Do(ctx, func(ctx context.Context) error {
		return i.sink.Write(ctx, writeReq)
	}); err != nil {
		wrapped := fmt.Errorf("%w: %v", ErrSinkWrite, err)
		return wrapped
	}

	// Só ack depois do write com sucesso
	if err := i.ackRetry.Do(ctx, func(ctx context.Context) error {
		return batch.Acks.Commit(ctx, i.source)
	}); err != nil {
		wrapped := fmt.Errorf("%w: %v", ErrAck, err)
		return wrapped
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
func DefaultKeyFunc[iType any](enc Encoder[iType]) KeyFunc[iType] {
	ext := enc.FileExtension()
	if ext == "" || ext[0] != '.' {
		// Defensive: keep keys consistent.
		ext = ".bin"
	}
	return func(ctx context.Context, batch Batch[iType]) (string, error) {
		_ = ctx
		_ = batch

		now := time.Now().UTC()
		suffix, err := randomHex(8) // 16 chars
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("ingestor/%04d/%02d/%02d/%02d/%d-%s%s",
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
*/
