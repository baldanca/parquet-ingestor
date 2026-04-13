package ingestor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/observability"
	"github.com/baldanca/parquet-ingestor/sink"
	"github.com/baldanca/parquet-ingestor/source"
	"github.com/baldanca/parquet-ingestor/transformer"
)

// KeyFunc builds the destination key for a flushed batch. It is called once
// per flush, synchronously, before encoding. The returned key is passed to
// the Sink verbatim.
//
// An error from KeyFunc is treated as fatal: Run will stop and return the
// error immediately, without acknowledging the batch.
type KeyFunc[iType any] func(ctx context.Context, batch batcher.Batch[iType]) (key string, err error)

// SinkPathResolver is an optional interface that sinks may implement to expose
// a human-readable path (e.g. "s3://bucket/prefix/key") for structured log
// output. When not implemented, the raw key is logged instead.
type SinkPathResolver interface {
	ResolvePath(key string) string
}

// Ingestor orchestrates a source → transform → batch → encode → sink pipeline.
//
// # Concurrency model
//
// The ingest loop (Run) is single-threaded: one message is received and
// processed at a time. Flush operations (encode + write + ack) can run
// concurrently when flushWorkers > 1, using an internal worker pool.
//
// Configuration methods (SetLogger, SetMetricsRegistry, EnableAdaptiveRuntime,
// etc.) must be called before Run; they are not safe for concurrent use with
// Run or with each other.
//
// # Error handling
//
// Runtime errors (encode failures, sink write failures, ack failures) are
// non-fatal by default: they are logged and counted but do not stop the loop.
// Only configuration errors returned by KeyFunc are fatal.
//
// # Acknowledgement guarantee
//
// Source messages are acknowledged only after a successful sink write. If the
// sink write fails, the message will be re-delivered by the source according
// to its own visibility or re-queue policy.
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

	flushJobs    chan flushJob[iType]
	flushBaseCtx context.Context
	flushWorkers int
	flushQueue   int
	flushWG      sync.WaitGroup
	flushMu      sync.Mutex
	flushStops   []context.CancelFunc

	logger       observability.Logger
	metrics      *observability.Registry
	adaptive     *AdaptiveRuntimeConfig
	adaptiveStop context.CancelFunc // cancels the adaptive goroutine; nil when adaptive is disabled

	inputLogSeq       atomic.Uint64
	transformedLogSeq atomic.Uint64
	payloadLogEvery   uint64

	shutdownTimeout time.Duration

	leaseEnabled              bool
	leaseVisibilityTimeoutSec int32
	leaseRenewEvery           time.Duration
}

type flushJob[iType any] struct {
	key   string
	items []iType
	acks  source.AckGroup
}

type fatalError struct {
	err error
}

func (e fatalError) Error() string { return e.err.Error() }
func (e fatalError) Unwrap() error { return e.err }

func normalizeBatcherConfig(cfg batcher.BatcherConfig) batcher.BatcherConfig {
	if cfg.MaxEstimatedInputBytes == 0 {
		cfg.MaxEstimatedInputBytes = batcher.DefaultBatcherConfig.MaxEstimatedInputBytes
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = batcher.DefaultBatcherConfig.FlushInterval
	}
	return cfg
}

func isFatalError(err error) bool {
	var fe fatalError
	return errors.As(err, &fe)
}

// NewIngestor constructs an Ingestor with a custom BatcherConfig.
//
// All parameters are required; NewIngestor returns an error if any is nil or
// if batcherConfig is invalid (see BatcherConfig.validate).
//
// After construction, call the Set*/Enable* methods to configure logging,
// metrics, retries, adaptive runtime, and leases, then call Run to start
// processing.
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

	batcherConfig = normalizeBatcherConfig(batcherConfig)

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
		logger:        observability.NopLogger(),
		metrics:       &observability.Registry{},
	}, nil
}

// NewDefaultIngestor constructs an Ingestor using batcher.DefaultBatcherConfig
// (5 MiB batch size, 5-minute flush interval, buffer reuse enabled).
func NewDefaultIngestor[iType any](
	src source.Sourcer,
	tr transformer.Transformer[iType],
	enc encoder.Encoder[iType],
	sk sink.Sinkr,
	keyFunc KeyFunc[iType],
) (*Ingestor[iType], error) {
	return NewIngestor(batcher.DefaultBatcherConfig, src, tr, enc, sk, keyFunc)
}

// SetLogger sets the structured logger. Passing nil installs a no-op logger.
func (i *Ingestor[iType]) SetLogger(l observability.Logger) {
	if l == nil {
		i.logger = observability.NopLogger()
		return
	}
	i.logger = l
}

// SetMetricsRegistry sets the metrics registry. Passing nil installs an empty
// no-op registry. Use Registry.AddAdapter to forward metrics to Datadog or
// other backends.
func (i *Ingestor[iType]) SetMetricsRegistry(r *observability.Registry) {
	if r == nil {
		i.metrics = &observability.Registry{}
		return
	}
	i.metrics = r
}

// EnableAdaptiveRuntime enables automatic scaling of flush workers and source
// pollers based on CPU utilisation, memory pressure, and queue backlog.
//
// Zero-value fields in cfg are replaced with sensible defaults
// (SampleInterval=2s, Cooldown=10s, TargetCPU=0.70, TargetMemory=0.80).
// The source must implement source.PollerScaler for poller scaling to take
// effect; flush worker scaling works for all sources.
func (i *Ingestor[iType]) EnableAdaptiveRuntime(cfg AdaptiveRuntimeConfig) {
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = 2 * time.Second
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 10 * time.Second
	}
	if cfg.TargetMemoryUtilization <= 0 {
		cfg.TargetMemoryUtilization = 0.80
	}
	if cfg.TargetCPUUtilization <= 0 {
		cfg.TargetCPUUtilization = 0.70
	}
	i.adaptive = &cfg
}

// SetRetryPolicy sets the retry policy applied to sink write operations.
// Passing nil disables retries (single attempt). Use SimpleRetry for
// exponential backoff with optional jitter.
func (i *Ingestor[iType]) SetRetryPolicy(p RetryPolicy) {
	if p == nil {
		i.retry = nopRetry{}
		return
	}
	i.retry = p
}

// SetAckRetryPolicy sets the retry policy applied to source acknowledgements.
// Passing nil disables retries. Ack failures are non-fatal but leave messages
// eligible for re-delivery by the source.
func (i *Ingestor[iType]) SetAckRetryPolicy(p RetryPolicy) {
	if p == nil {
		i.ackRetry = nopRetry{}
		return
	}
	i.ackRetry = p
}

// EnableLease enables periodic visibility-timeout extensions for SQS messages
// while a flush job is in progress. This prevents messages from becoming
// visible again (and being re-delivered) during a long encode or upload.
//
// visibilityTimeoutSec is the new visibility timeout set on each renewal.
// renewEvery controls how often the extension is sent; it must be shorter
// than visibilityTimeoutSec to avoid gaps. A zero or negative renewEvery
// defaults to 20 seconds.
//
// The Source must implement source.VisibilityExtender; if it does not,
// EnableLease is silently ignored at runtime.
func (i *Ingestor[iType]) EnableLease(visibilityTimeoutSec int32, renewEvery time.Duration) {
	i.leaseEnabled = true
	i.leaseVisibilityTimeoutSec = visibilityTimeoutSec
	i.leaseRenewEvery = renewEvery
}

// Run starts the ingest loop and blocks until ctx is cancelled or a fatal
// error occurs.
//
// flushWorkers controls how many encode/write/ack operations may run
// concurrently (minimum 1). flushQueue is the depth of the internal job
// channel; when full, the ingest loop blocks until a worker slot frees up.
//
// Run attempts a final flush of any buffered records on shutdown, waiting up
// to the configured shutdown timeout (default 10 s; see SetShutdownTimeout).
// It returns nil on clean shutdown and a non-nil error only when KeyFunc fails.
func (i *Ingestor[iType]) Run(ctx context.Context, flushWorkers, flushQueue int) error {
	if flushWorkers < 1 {
		flushWorkers = 1
	}
	if flushQueue < 1 {
		flushQueue = flushWorkers
	}

	i.flushWorkers = flushWorkers
	i.flushQueue = flushQueue
	i.metrics.SetGauge("ingestor_flush_queue_capacity", int64(flushQueue))
	i.maybeStartFlushPool(ctx)
	i.startAdaptiveLoop(ctx)
	i.logger.Info("ingestor.run.started", "flush_workers", flushWorkers, "flush_queue", flushQueue, "adaptive_enabled", i.adaptive != nil && i.adaptive.Enabled)

	for {
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
			if errors.Is(err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					return i.flushRemainingOnStop(context.Background())
				}
				if err := i.flush(ctx); err != nil {
					if isFatalError(err) {
						return err
					}
					i.handleRuntimeError("ingestor.flush.failed", err)
				}
				continue
			}

			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return i.flushRemainingOnStop(context.Background())
			}

			i.metrics.AddCounter("ingestor_receive_errors_total", 1)
			i.handleRuntimeError("ingestor.receive.failed", err)
			continue
		}

		flushNow, err := i.processMessage(ctx, msg)
		if err != nil {
			i.handleRuntimeError("ingestor.process.failed", err)
			continue
		}
		if flushNow {
			if err := i.flush(ctx); err != nil {
				if isFatalError(err) {
					return err
				}
				i.handleRuntimeError("ingestor.flush.failed", err)
				continue
			}
		}
	}
}

func (i *Ingestor[iType]) currentFlushWorkers() int {
	i.flushMu.Lock()
	defer i.flushMu.Unlock()
	if len(i.flushStops) == 0 {
		return i.flushWorkers
	}
	return len(i.flushStops)
}

// SetFlushWorkers resizes the flush worker pool at runtime.
func (i *Ingestor[iType]) SetFlushWorkers(n int) {
	if n < 1 {
		n = 1
	}
	i.flushMu.Lock()
	defer i.flushMu.Unlock()
	if i.flushJobs == nil {
		i.flushWorkers = n
		if n > 1 {
			i.initFlushPoolLocked()
		}
		return
	}
	current := len(i.flushStops)
	if n == current {
		return
	}
	if n > current {
		base := context.Background()
		for w := current; w < n; w++ {
			ctx, cancel := context.WithCancel(base)
			i.flushStops = append(i.flushStops, cancel)
			i.flushWG.Add(1)
			go func(workerID int, workerCtx context.Context) {
				defer i.flushWG.Done()
				i.logger.Debug("ingestor.flush_worker.started", "worker_id", workerID)
				i.flushWorker(workerCtx)
				i.logger.Debug("ingestor.flush_worker.stopped", "worker_id", workerID)
			}(w+1, ctx)
		}
	} else {
		for w := current - 1; w >= n; w-- {
			i.flushStops[w]()
		}
		i.flushStops = i.flushStops[:n]
	}
	i.flushWorkers = n
	i.metrics.SetGauge("ingestor_flush_workers", int64(n))
}

func (i *Ingestor[iType]) maybeStartFlushPool(ctx context.Context) {
	i.flushMu.Lock()
	defer i.flushMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	i.flushBaseCtx = context.WithoutCancel(ctx)
	if i.flushWorkers <= 1 || i.flushJobs != nil {
		return
	}
	i.initFlushPoolLocked()
}

func (i *Ingestor[iType]) initFlushPoolLocked() {
	if i.flushJobs != nil {
		return
	}
	if i.flushBaseCtx == nil {
		i.flushBaseCtx = context.Background()
	}
	base := context.WithoutCancel(i.flushBaseCtx)
	i.flushJobs = make(chan flushJob[iType], i.flushQueue)
	current := len(i.flushStops)
	for w := current; w < i.flushWorkers; w++ {
		ctx, stop := context.WithCancel(base)
		i.flushStops = append(i.flushStops, stop)
		i.flushWG.Add(1)
		go func(workerID int, workerCtx context.Context) {
			defer i.flushWG.Done()
			i.logger.Debug("ingestor.flush_worker.started", "worker_id", workerID)
			i.flushWorker(workerCtx)
			i.logger.Debug("ingestor.flush_worker.stopped", "worker_id", workerID)
		}(w+1, ctx)
	}
	i.metrics.SetGauge("ingestor_flush_workers", int64(i.flushWorkers))
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
				i.handleRuntimeError("ingestor.flush.job_failed", err, "key", job.key)
				continue
			}
		}
	}
}

func (i *Ingestor[iType]) processMessage(ctx context.Context, msg source.Message) (flushNow bool, err error) {
	env := msg.Data()
	i.metrics.AddCounter("ingestor_messages_received_total", 1)
	if i.shouldLogPayload(true) {
		i.logger.Debug("ingestor.message.received", "payload", i.logValue(env.Payload))
		i.metrics.AddCounter("ingestor_input_payload_logs_total", 1)
	}

	outs, err := i.transformer.Transform(ctx, env)
	if err != nil {
		i.metrics.AddCounter("ingestor_transform_errors_total", 1)
		i.logger.Error("ingestor.transform.failed", "error", err)
		_ = msg.Fail(ctx, err)
		return false, nil
	}

	// An empty result means the transformer deliberately dropped the message
	// (e.g. deduplication or filtering). Acknowledge it immediately so it is
	// not re-delivered, and skip batching.
	if len(outs) == 0 {
		i.metrics.AddCounter("ingestor_messages_dropped_total", 1)
		i.logger.Debug("ingestor.message.dropped", "reason", "transformer_returned_empty")
		if ackErr := i.ackRetry.Do(ctx, func(ctx context.Context) error {
			return i.source.AckBatch(ctx, []source.Message{msg})
		}); ackErr != nil {
			i.handleRuntimeError("ingestor.drop.ack_failed", ackErr)
		}
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
	if i.shouldLogPayload(false) {
		for _, out := range outs {
			i.logger.Debug("ingestor.message.transformed", "payload", i.logValue(out))
		}
		i.metrics.AddCounter("ingestor_transformed_payload_logs_total", int64(len(outs)))
	}

	flushNow = i.batcher.AddBatch(now, outs, msg, sizeBytes)
	i.metrics.AddCounter("ingestor_messages_buffered_total", 1)
	i.metrics.AddCounter("ingestor_records_buffered_total", int64(len(outs)))
	return flushNow, nil
}

func (i *Ingestor[iType]) flush(ctx context.Context) error {
	batch := i.batcher.Flush()
	if len(batch.Items) == 0 {
		return nil
	}
	i.metrics.AddCounter("ingestor_flush_triggered_total", 1)
	i.metrics.SetGauge("ingestor_last_flush_items", int64(len(batch.Items)))

	key, err := i.keyFunc(ctx, batch)
	if err != nil {
		return fatalError{err: err}
	}

	if i.flushJobs != nil && i.flushWorkers > 1 {
		items := append([]iType(nil), batch.Items...)
		acks := batch.Acks.Snapshot()

		job := flushJob[iType]{key: key, items: items, acks: acks}

		if len(i.flushJobs) == cap(i.flushJobs) {
			i.metrics.AddCounter("ingestor_flush_queue_backpressure_total", 1)
		}
		select {
		case i.flushJobs <- job:
			return nil
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

	start := time.Now()

	if streamed, err := tryStreamWrite(ctx, i.encoder, i.sink, i.retry, job.key, job.items); streamed {
		if err != nil {
			i.metrics.AddCounter("ingestor_flush_errors_total", 1)
			i.logger.Error("ingestor.flush.stream_write_failed", "key", job.key, "error", err)
			return err
		}
		i.metrics.AddCounter("ingestor_stream_writes_total", 1)
		if err := i.ackRetry.Do(ctx, func(ctx context.Context) error {
			return job.acks.Commit(ctx, i.source)
		}); err != nil {
			i.metrics.AddCounter("ingestor_ack_errors_total", 1)
			return err
		}
		i.metrics.AddCounter("ingestor_flush_completed_total", 1)
		i.metrics.AddCounter("ingestor_acked_total", int64(len(job.items)))
		i.metrics.SetGauge("ingestor_last_flush_duration_ms", time.Since(start).Milliseconds())
		i.logSinkWrite(job.key, len(job.items), -1)
		return nil
	}

	data, err := i.encoder.Encode(ctx, job.items)
	if err != nil {
		i.metrics.AddCounter("ingestor_encode_errors_total", 1)
		i.logger.Error("ingestor.flush.encode_failed", "key", job.key, "error", err)
		return err
	}
	i.metrics.AddCounter("ingestor_encoded_bytes_total", int64(len(data)))

	contentType := i.encoder.ContentType()
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	writeReq := sink.WriteRequest{Key: job.key, Data: data, ContentType: contentType}

	if err := i.retry.Do(ctx, func(ctx context.Context) error {
		return i.sink.Write(ctx, writeReq)
	}); err != nil {
		i.metrics.AddCounter("ingestor_sink_errors_total", 1)
		i.logger.Error("ingestor.flush.sink_write_failed", "key", job.key, "error", err)
		return err
	}
	if err := i.ackRetry.Do(ctx, func(ctx context.Context) error {
		return job.acks.Commit(ctx, i.source)
	}); err != nil {
		i.metrics.AddCounter("ingestor_ack_errors_total", 1)
		return err
	}
	i.metrics.AddCounter("ingestor_flush_completed_total", 1)
	i.metrics.AddCounter("ingestor_acked_total", int64(len(job.items)))
	i.metrics.AddCounter("ingestor_sink_writes_total", 1)
	i.metrics.SetGauge("ingestor_last_flush_duration_ms", time.Since(start).Milliseconds())
	i.logSinkWrite(job.key, len(job.items), len(data))
	return nil
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
					i.handleRuntimeError("ingestor.lease.extend_failed", err, "message_count", len(metas))
					return
				}
			}
		}
	}()

	return cancel
}

func (i *Ingestor[iType]) flushRemainingOnStop(ctx context.Context) error {
	// Stop the adaptive loop first so it cannot call SetFlushWorkers or
	// SetPollers concurrently with the channel close below.
	if i.adaptiveStop != nil {
		i.adaptiveStop()
	}

	timeout := i.shutdownTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	base := context.WithoutCancel(ctx)
	stopCtx, cancel := context.WithTimeout(base, timeout)
	defer cancel()

	if err := i.flush(stopCtx); err != nil {
		i.handleRuntimeError("ingestor.flush.shutdown_failed", err)
	}

	if i.flushJobs == nil || i.flushWorkers <= 1 {
		i.logger.Info("ingestor.run.stopped")
		return nil
	}

	flushJobs := i.flushJobs
	if flushJobs != nil {
		close(flushJobs)
	}

	done := make(chan struct{})
	go func() {
		i.flushWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		i.logger.Info("ingestor.run.stopped")
		return nil
	case <-stopCtx.Done():
		for _, stop := range i.flushStops {
			stop()
		}
		i.metrics.AddCounter("ingestor_shutdown_timeout_total", 1)
		i.logger.Warn("ingestor.run.stop_timeout", "error", stopCtx.Err(), "timeout", timeout)
		return nil
	}
}

// SetPayloadLogEvery enables debug-level payload logging every n messages.
// A value of 0 (default) disables payload logging entirely, which is the
// recommended setting for production to avoid logging sensitive data.
func (i *Ingestor[iType]) SetPayloadLogEvery(n uint64) {
	i.payloadLogEvery = n
}

// SetShutdownTimeout overrides the maximum time the ingestor will wait for
// in-flight flush workers to finish on graceful shutdown. Defaults to 10s.
func (i *Ingestor[iType]) SetShutdownTimeout(d time.Duration) {
	if d > 0 {
		i.shutdownTimeout = d
	}
}

func (i *Ingestor[iType]) shouldLogPayload(input bool) bool {
	if i.payloadLogEvery == 0 {
		return false
	}
	var seq uint64
	if input {
		seq = i.inputLogSeq.Add(1)
	} else {
		seq = i.transformedLogSeq.Add(1)
	}
	return seq%i.payloadLogEvery == 0
}

func (i *Ingestor[iType]) handleRuntimeError(event string, err error, args ...any) {
	if err == nil {
		return
	}
	i.metrics.AddCounter("ingestor_runtime_errors_total", 1)
	// Fast path: avoid a heap allocation when no extra key-value pairs are
	// provided, which is the case for most call sites.
	if len(args) == 0 {
		i.logger.Error(event, "error", err)
		return
	}
	attrs := make([]any, 0, 2+len(args))
	attrs = append(attrs, "error", err)
	attrs = append(attrs, args...)
	i.logger.Error(event, attrs...)
}

func (i *Ingestor[iType]) logValue(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<marshal_error:%v>", err)
	}
	out := string(b)
	const maxLogValueLen = 4096
	if len(out) > maxLogValueLen {
		return out[:maxLogValueLen] + "...<truncated>"
	}
	return out
}

func (i *Ingestor[iType]) logSinkWrite(key string, items int, bytes int) {
	resolved := key
	if r, ok := i.sink.(SinkPathResolver); ok {
		resolved = r.ResolvePath(key)
	}
	i.logger.Info("ingestor.flush.sink_write_succeeded",
		"key", key,
		"path", resolved,
		"file_name", path.Base(key),
		"items", items,
		"bytes", bytes,
	)
}

// DefaultKeyFunc returns a KeyFunc that generates time-partitioned, collision-
// safe object keys using the encoder's file extension.
//
// Keys follow the format:
//
//	YYYY/MM/DD/HH/<UnixNano>-<8-byte random hex><.ext>
//
// The timestamp is always UTC. The random suffix prevents collisions when
// multiple ingestor instances flush at the same nanosecond.
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

// countingWriter counts bytes written to it without allocating storage.
type countingWriter struct{ n int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

func estimateSizeBytesFallback(v any) (int64, error) {
	var cw countingWriter
	if err := json.NewEncoder(&cw).Encode(v); err != nil {
		return 0, err
	}
	return cw.n, nil
}

func randomHex(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
