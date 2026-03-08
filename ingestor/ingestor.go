package ingestor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/sink"
	"github.com/baldanca/parquet-ingestor/source"
	"github.com/baldanca/parquet-ingestor/transformer"
)

// KeyFunc builds the destination key for a flushed batch.
type KeyFunc[iType any] func(ctx context.Context, batch batcher.Batch[iType]) (key string, err error)

// Ingestor pulls messages from a Source, transforms them into typed records,
// batches them, encodes the batch, writes it to a Sink, and acknowledges the
// consumed messages.
//
// The ingest loop is single-threaded. Flush (encode/write/ack) can optionally
// run concurrently via a worker pool configured in Run.
type Ingestor[iType any] struct {
	batcherConfig batcher.BatcherConfig
	source        source.Sourcer
	transformer   transformer.Transformer[iType]
	encoder       encoder.Encoder[iType]
	sink          sink.Sinkr
	keyFunc       KeyFunc[iType]

	retry    RetryPolicy
	ackRetry RetryPolicy

	batcher *batcher.Batcher[iType]

	flushOnce    sync.Once
	flushJobs    chan flushJob[iType]
	flushErrCh   chan error
	flushCancel  context.CancelFunc
	flushWorkers int
	flushQueue   int
	flushWG      sync.WaitGroup

	leaseEnabled              bool
	leaseVisibilityTimeoutSec int32
	leaseRenewEvery           time.Duration
}

type flushJob[iType any] struct {
	key   string
	items []iType
	acks  source.AckGroup
}

// NewIngestor builds an Ingestor.
func NewIngestor[iType any](
	batcherConfig batcher.BatcherConfig,
	src source.Sourcer,
	tr transformer.Transformer[iType],
	enc encoder.Encoder[iType],
	sk sink.Sinkr,
	keyFunc KeyFunc[iType],
) (*Ingestor[iType], error) {
	if src == nil {
		return nil, fmt.Errorf("source is nil")
	}
	if tr == nil {
		return nil, fmt.Errorf("transformer is nil")
	}
	if enc == nil {
		return nil, fmt.Errorf("encoder is nil")
	}
	if sk == nil {
		return nil, fmt.Errorf("sink is nil")
	}
	if keyFunc == nil {
		return nil, fmt.Errorf("keyFunc is nil")
	}

	b, err := batcher.NewBatcher[iType](batcherConfig)
	if err != nil {
		return nil, err
	}

	return &Ingestor[iType]{
		batcherConfig: batcherConfig,
		source:        src,
		transformer:   tr,
		encoder:       enc,
		sink:          sk,
		keyFunc:       keyFunc,
		retry:         nopRetry{},
		ackRetry:      nopRetry{},
		batcher:       b,
	}, nil
}

// NewDefaultIngestor builds an Ingestor using batcher.DefaultBatcherConfig.
func NewDefaultIngestor[iType any](
	src source.Sourcer,
	tr transformer.Transformer[iType],
	enc encoder.Encoder[iType],
	sk sink.Sinkr,
	keyFunc KeyFunc[iType],
) (*Ingestor[iType], error) {
	return NewIngestor(batcher.DefaultBatcherConfig, src, tr, enc, sk, keyFunc)
}

// SetRetryPolicy sets the retry policy for sink writes.
func (i *Ingestor[iType]) SetRetryPolicy(p RetryPolicy) {
	if p == nil {
		i.retry = nopRetry{}
		return
	}
	i.retry = p
}

// SetAckRetryPolicy sets the retry policy for acknowledgements.
func (i *Ingestor[iType]) SetAckRetryPolicy(p RetryPolicy) {
	if p == nil {
		i.ackRetry = nopRetry{}
		return
	}
	i.ackRetry = p
}

// EnableLease enables periodic visibility extensions while a flush job is running.
//
// This requires the Source to also implement source.VisibilityExtender.
func (i *Ingestor[iType]) EnableLease(visibilityTimeoutSec int32, renewEvery time.Duration) {
	i.leaseEnabled = true
	i.leaseVisibilityTimeoutSec = visibilityTimeoutSec
	i.leaseRenewEvery = renewEvery
}

// Run starts the ingest loop.
//
// If flushWorkers > 1, flush operations run concurrently via a worker pool.
// flushQueue bounds the number of in-flight flushes.
func (i *Ingestor[iType]) Run(ctx context.Context, flushWorkers, flushQueue int) error {
	if flushWorkers < 1 {
		flushWorkers = 1
	}
	if flushQueue < 1 {
		flushQueue = flushWorkers
	}

	i.flushWorkers = flushWorkers
	i.flushQueue = flushQueue
	i.maybeStartFlushPool(ctx)

	for {
		// If a flush worker failed, return immediately (graceful flush can't succeed if worker pool is poisoned).
		if err := i.pollFlushErr(); err != nil {
			return err
		}

		// Any stop condition: attempt final flush (graceful) before returning.
		if ctx.Err() != nil {
			return i.flushRemainingOnStop(context.Background())
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
			// If the per-receive deadline fired, we flush the current batch.
			// BUT if the main ctx is already done, we prefer the graceful shutdown path.
			if errors.Is(err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					return i.flushRemainingOnStop(context.Background())
				}
				if err := i.flush(ctx); err != nil {
					return err
				}
				continue
			}

			// Treat any cancellation as a stop signal and attempt a final flush.
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return i.flushRemainingOnStop(context.Background())
			}

			return err
		}

		flushNow, err := i.processMessage(ctx, msg)
		if err != nil {
			return err
		}
		if flushNow {
			if err := i.flush(ctx); err != nil {
				// If ctx was canceled during flush trigger, still try final flush.
				if ctx.Err() != nil {
					return i.flushRemainingOnStop(context.Background())
				}
				return err
			}
		}
	}
}

func (i *Ingestor[iType]) maybeStartFlushPool(ctx context.Context) {
	if i.flushWorkers <= 1 {
		return
	}

	i.flushOnce.Do(func() {
		base := context.WithoutCancel(ctx)
		flushCtx, cancel := context.WithCancel(base)

		i.flushCancel = cancel

		i.flushJobs = make(chan flushJob[iType], i.flushQueue)
		i.flushErrCh = make(chan error, 1)

		for w := 0; w < i.flushWorkers; w++ {
			i.flushWG.Add(1)
			go func() {
				defer i.flushWG.Done()
				i.flushWorker(flushCtx)
			}()
		}
	})
}

func (i *Ingestor[iType]) flushWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-i.flushJobs:
			if !ok {
				return
			}
			if err := i.flushJob(ctx, job); err != nil {
				select {
				case i.flushErrCh <- err:
				default:
				}
				if i.flushCancel != nil {
					i.flushCancel()
				}
				return
			}
		}
	}
}

func (i *Ingestor[iType]) pollFlushErr() error {
	if i.flushErrCh == nil {
		return nil
	}
	select {
	case err := <-i.flushErrCh:
		return err
	default:
		return nil
	}
}

func (i *Ingestor[iType]) processMessage(ctx context.Context, msg source.Message) (flushNow bool, err error) {
	env := msg.Data()

	out, err := i.transformer.Transform(ctx, env)
	if err != nil {
		_ = msg.Fail(ctx, err)
		return false, nil
	}

	var sizeBytes int64
	if n, ok := msg.EstimatedSizeBytes(); ok {
		sizeBytes = n
	} else {
		n, sizeErr := estimateSizeBytesFallback(env.Payload)
		if sizeErr != nil {
			_ = msg.Fail(ctx, sizeErr)
			return false, nil
		}
		sizeBytes = n
	}

	now := time.Now()
	flushNow = i.batcher.Add(now, out, msg, sizeBytes)
	return flushNow, nil
}

func (i *Ingestor[iType]) flush(ctx context.Context) error {
	if err := i.pollFlushErr(); err != nil {
		return err
	}

	batch := i.batcher.Flush()
	if len(batch.Items) == 0 {
		return nil
	}

	key, err := i.keyFunc(ctx, batch)
	if err != nil {
		return err
	}

	if i.flushJobs != nil && i.flushWorkers > 1 {
		items := append([]iType(nil), batch.Items...)
		acks := batch.Acks.Snapshot()

		job := flushJob[iType]{key: key, items: items, acks: acks}

		select {
		case i.flushJobs <- job:
			return nil
		case err := <-i.flushErrCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return i.flushJob(ctx, flushJob[iType]{key: key, items: batch.Items, acks: batch.Acks})
}

func (i *Ingestor[iType]) flushJob(ctx context.Context, job flushJob[iType]) error {
	var stopLease func()
	if i.leaseEnabled {
		if ext, ok := i.source.(source.VisibilityExtender); ok {
			stopLease = i.startJobLease(ctx, ext, job.acks.Metas())
		}
	}
	if stopLease != nil {
		defer stopLease()
	}

	if streamed, err := tryStreamWrite(ctx, i.encoder, i.sink, i.retry, job.key, job.items); streamed {
		if err != nil {
			return err
		}
		return i.ackRetry.Do(ctx, func(ctx context.Context) error {
			return job.acks.Commit(ctx, i.source)
		})
	}

	data, err := i.encoder.Encode(ctx, job.items)
	if err != nil {
		return err
	}

	contentType := i.encoder.ContentType()
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	writeReq := sink.WriteRequest{Key: job.key, Data: data, ContentType: contentType}

	if err := i.retry.Do(ctx, func(ctx context.Context) error {
		return i.sink.Write(ctx, writeReq)
	}); err != nil {
		return err
	}

	return i.ackRetry.Do(ctx, func(ctx context.Context) error {
		return job.acks.Commit(ctx, i.source)
	})
}

func (i *Ingestor[iType]) startJobLease(parent context.Context, ext source.VisibilityExtender, metas []source.AckMetadata) (stop func()) {
	if len(metas) == 0 {
		return func() {}
	}

	renewevery := i.leaseRenewEvery
	if renewevery <= 0 {
		renewevery = 20 * time.Second
	}

	ctx, cancel := context.WithCancel(parent)

	go func() {
		t := time.NewTicker(renewevery)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := ext.ExtendVisibilityBatch(ctx, metas, i.leaseVisibilityTimeoutSec); err != nil {
					select {
					case i.flushErrCh <- err:
					default:
					}
					if i.flushCancel != nil {
						i.flushCancel()
					}
					return
				}
			}
		}
	}()

	return cancel
}

func (i *Ingestor[iType]) flushRemainingOnStop(ctx context.Context) error {
	// "Graceful flush": do not inherit cancellation from the caller.
	// This ctx is expected to be Background() from Run(), but we keep this safe anyway.
	base := context.WithoutCancel(ctx)
	stopCtx, cancel := context.WithTimeout(base, 10*time.Second)
	defer cancel()

	// Try one last flush (whatever is currently buffered).
	if err := i.flush(stopCtx); err != nil {
		return err
	}

	// If no worker pool, we're done.
	if i.flushJobs == nil || i.flushWorkers <= 1 {
		return nil
	}

	// Drain/stop worker pool after ensuring the last flush was enqueued.
	close(i.flushJobs)

	done := make(chan struct{})
	go func() {
		i.flushWG.Wait()
		close(done)
	}()

	select {
	case err := <-i.flushErrCh:
		return err
	case <-done:
		return nil
	case <-stopCtx.Done():
		return stopCtx.Err()
	}
}

// DefaultKeyFunc builds time-partitioned keys and appends a random suffix to
// avoid collisions.
func DefaultKeyFunc[iType any](enc encoder.Encoder[iType]) KeyFunc[iType] {
	ext := enc.FileExtension()
	if ext == "" || ext[0] != '.' {
		ext = ".bin"
	}
	return func(ctx context.Context, batch batcher.Batch[iType]) (string, error) {
		_ = ctx
		_ = batch

		now := time.Now().UTC()
		suffix, err := randomHex(8)
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
