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

	// flush workers (enabled via Run(ctx, workers, queue))
	flushOnce    sync.Once
	flushJobs    chan flushJob[iType]
	flushErrCh   chan error
	flushCancel  context.CancelFunc
	flushWorkers int
	flushQueue   int

	// lease
	leaseEnabled              bool
	leaseVisibilityTimeoutSec int32
	leaseRenewEvery           time.Duration
}

type flushJob[iType any] struct {
	key   string
	items []iType
	acks  source.AckGroup
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

	return &Ingestor[iType]{
		batcherConfig: batcherConfig,
		source:        source,
		transformer:   transformer,
		encoder:       encoder,
		sink:          sink,
		keyFunc:       keyFunc,
		retry:         nopRetry{},
		ackRetry:      nopRetry{},
		batcher:       b,
	}, nil
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

func (i *Ingestor[iType]) EnableLease(visibilityTimeoutSec int32, renewEvery time.Duration) {
	i.leaseEnabled = true
	i.leaseVisibilityTimeoutSec = visibilityTimeoutSec
	i.leaseRenewEvery = renewEvery
}

// Run starts the ingest loop. If flushWorkers > 1, flush (encode/write/ack) is done
// concurrently by a worker pool and the ingest loop only enqueues flush jobs.
// flushQueue bounds the number of in-flight flushes (memory bound). Fail-fast.
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
		// surface worker errors quickly
		if err := i.pollFlushErr(); err != nil {
			return err
		}

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

			// ✅ IMPORTANT: graceful stop signaled by source (ex: nil sentinel => context.Canceled)
			// Flush remaining buffered items before exiting.
			if errors.Is(err, context.Canceled) && ctx.Err() == nil {
				return i.flushRemainingOnStop(ctx)
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

func (i *Ingestor[iType]) maybeStartFlushPool(ctx context.Context) {
	if i.flushWorkers <= 1 {
		return
	}

	i.flushOnce.Do(func() {
		flushCtx, cancel := context.WithCancel(ctx)
		i.flushCancel = cancel

		i.flushJobs = make(chan flushJob[iType], i.flushQueue)
		i.flushErrCh = make(chan error, 1) // first error wins (fail-fast)

		for w := 0; w < i.flushWorkers; w++ {
			go i.flushWorker(flushCtx)
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
				// fail-fast: report first error and cancel
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
	// surface worker errors quickly
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

	// If pool enabled: snapshot and enqueue.
	if i.flushJobs != nil && i.flushWorkers > 1 {
		items := append([]iType(nil), batch.Items...) // snapshot slice
		acks := batch.Acks.Snapshot()                 // snapshot internal slices

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

	// Single-threaded fallback (no pool)
	return i.flushJob(ctx, flushJob[iType]{key: key, items: batch.Items, acks: batch.Acks})
}

func (i *Ingestor[iType]) flushJob(ctx context.Context, job flushJob[iType]) error {
	// Start lease if enabled and source supports it.
	var stopLease func()
	if i.leaseEnabled {
		if ext, ok := i.source.(source.VisibilityExtender); ok {
			stopLease = i.startJobLease(ctx, ext, job.acks.Metas())
		}
	}
	if stopLease != nil {
		defer stopLease()
	}

	// 1) Prefer streaming when both encoder + sink support it.
	if streamed, err := tryStreamWrite(ctx, i.encoder, i.sink, i.retry, job.key, job.items); streamed {
		if err != nil {
			return err
		}
		// Ack only after successful write (with retries).
		return i.ackRetry.Do(ctx, func(ctx context.Context) error {
			return job.acks.Commit(ctx, i.source)
		})
	}

	// 2) Fallback: in-memory Encode + Write.
	data, err := i.encoder.Encode(ctx, job.items)
	if err != nil {
		return err
	}

	contentType := i.encoder.ContentType()
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	writeReq := sink.WriteRequest{Key: job.key, Data: data, ContentType: contentType}

	// Retry sink write
	if err := i.retry.Do(ctx, func(ctx context.Context) error {
		return i.sink.Write(ctx, writeReq)
	}); err != nil {
		return err
	}

	// Ack only after successful write (with retries)
	return i.ackRetry.Do(ctx, func(ctx context.Context) error {
		return job.acks.Commit(ctx, i.source)
	})
}

func (i *Ingestor[iType]) startJobLease(
	parent context.Context,
	ext source.VisibilityExtender,
	metas []source.AckMetadata,
) (stop func()) {
	if len(metas) == 0 {
		return func() {}
	}

	renewEvery := i.leaseRenewEvery
	if renewEvery <= 0 {
		renewEvery = 20 * time.Second // fallback seguro
	}

	ctx, cancel := context.WithCancel(parent)

	go func() {
		t := time.NewTicker(renewEvery)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				// best-effort: se falhar, derruba o job (fail-fast) via flushErrCh
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
	// Best effort: keep values/deadlines but ignore cancellation, and don't block forever.
	base := context.WithoutCancel(ctx)
	stopCtx, cancel := context.WithTimeout(base, 10*time.Second)
	defer cancel()

	// flush what's currently in the batcher (enqueue or inline)
	if err := i.flush(stopCtx); err != nil {
		return err
	}

	// If no pool, done.
	if i.flushJobs == nil || i.flushWorkers <= 1 {
		return nil
	}

	// close queue so workers exit after draining
	close(i.flushJobs)

	// wait for first error or timeout; otherwise best-effort success
	select {
	case err := <-i.flushErrCh:
		return err
	case <-stopCtx.Done():
		return stopCtx.Err()
	default:
		return nil
	}
}

// DefaultKeyFunc partitions by time and avoids collisions (good for concurrent flush workers).
func DefaultKeyFunc[iType any](enc encoder.Encoder[iType]) KeyFunc[iType] {
	ext := enc.FileExtension()
	if ext == "" || ext[0] != '.' {
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
