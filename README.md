# parquet-ingestor

`parquet-ingestor` is a Go pipeline for reading messages from a source, transforming them into typed records, batching them, encoding them, and writing the result to a sink.

This version includes an adaptive runtime that can automatically resize flush workers and source pollers based on backlog, CPU pressure, and memory pressure.

## Highlights

- source → transform → batch → encode → sink pipeline
- adaptive runtime for flush workers and source pollers
- structured logging through a pluggable logger
- generic metrics registry with adapter fanout
- Datadog / DogStatsD adapter included
- runtime errors treated as non-fatal by default
- unit tests, integration tests, and adaptive benchmarks

## Pipeline

1. receive a message from a source
2. transform the input envelope into the target record type
3. append the record to the batcher
4. flush when the batch is full or the flush interval is reached
5. encode the batch
6. write the payload to the sink
7. ack the source messages only after a successful write

## Adaptive runtime

The adaptive runtime lets you configure resource boundaries instead of manually guessing the best worker and poller counts.

```go
ing.EnableAdaptiveRuntime(ingestor.AdaptiveRuntimeConfig{
    Enabled:                 true,
    MinWorkers:              1,
    MaxWorkers:              16,
    MinPollers:              1,
    MaxPollers:              4,
    TargetCPUUtilization:    0.70,
    TargetMemoryUtilization: 0.80,
    MaxMemoryBytes:          512 * 1024 * 1024,
    SampleInterval:          2 * time.Second,
    Cooldown:                10 * time.Second,
})
```

### Design goals

- memory pressure is more important than aggressive scale up
- workers are the primary scaling lever
- pollers scale more conservatively to avoid over-pulling from queues such as SQS
- runtime decisions are observable through logs and metrics
- invalid configuration remains fatal, but operational failures are logged and counted instead of killing the process

## Quick start

```go
cfg := batcher.DefaultBatcherConfig
cfg.MaxItems = 1000
cfg.MaxEstimatedInputBytes = 8 * 1024 * 1024
cfg.FlushInterval = 5 * time.Second

ing, err := ingestor.NewIngestor(
    cfg,
    src,
    transformer,
    encoder,
    sink,
    ingestor.DefaultKeyFunc(encoder),
)
if err != nil {
    return err
}

ing.SetLogger(observability.NewSlogLogger(slog.New(
    slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
)))

ing.SetMetricsRegistry(&observability.Registry{})

ing.EnableAdaptiveRuntime(ingestor.AdaptiveRuntimeConfig{
    Enabled:                 true,
    MinWorkers:              1,
    MaxWorkers:              8,
    MinPollers:              1,
    MaxPollers:              4,
    TargetCPUUtilization:    0.70,
    TargetMemoryUtilization: 0.80,
    MaxMemoryBytes:          512 * 1024 * 1024,
    SampleInterval:          2 * time.Second,
    Cooldown:                10 * time.Second,
})

return ing.Run(ctx, 2, 8)
```

## Manual runtime

You can still run the project in manual mode.

```go
err := ing.Run(ctx, 4, 16)
```

The adaptive runtime is optional.

## SQS source runtime scaling

If the source implements `source.PollerScaler`, the adaptive runtime can resize the number of active pollers at runtime.

This is especially useful for pull-based systems such as SQS, where receiving too aggressively can increase in-flight pressure and visibility timeout risk.

## Logging

Logging is now controlled only by the logger level.

There is no ingestor-specific log switch for payloads or sink-write visibility anymore.

Recommended level behavior:

- `Debug`:
  - input payload
  - transformed payload
  - flush worker lifecycle
- `Info`:
  - run start / stop
  - successful sink writes with key, resolved path, file name, item count, and bytes
  - adaptive scale-up events
- `Warn`:
  - adaptive scale-down events
  - shutdown timeout
- `Error`:
  - transform failures
  - sink write failures
  - ack failures
  - receive failures
  - lease extension failures
  - any other non-fatal runtime error

Because payload visibility is on the `Debug` level, production environments can keep payload logs disabled simply by running with `Info` or higher.

Example with `Info` level:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))
ing.SetLogger(observability.NewSlogLogger(logger))
```

Example with `Debug` level for investigations:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))
ing.SetLogger(observability.NewSlogLogger(logger))
```

## Metrics

The project exposes a generic in-process registry:

```go
metrics := &observability.Registry{}
ing.SetMetricsRegistry(metrics)
```

The registry keeps local counters and gauges, and can fan them out to one or more adapters.

Common metrics emitted by the ingestor include:

- `ingestor_messages_received_total`
- `ingestor_messages_buffered_total`
- `ingestor_flush_triggered_total`
- `ingestor_flush_completed_total`
- `ingestor_flush_errors_total`
- `ingestor_sink_errors_total`
- `ingestor_ack_errors_total`
- `ingestor_runtime_errors_total`
- `ingestor_memory_bytes`
- `ingestor_cpu_utilization`
- `ingestor_flush_workers`
- `ingestor_source_pollers`
- `ingestor_scale_up_total`
- `ingestor_scale_down_total`

### Datadog / DogStatsD

A Datadog-compatible adapter is included.

```go
type dogstatsdClient interface {
    Count(name string, value int64, tags []string, rate float64) error
    Gauge(name string, value float64, tags []string, rate float64) error
}

metrics := &observability.Registry{}
metrics.AddAdapter(observability.DatadogAdapter{
    Client: myDogStatsDClient,
    Prefix: "parquet_ingestor",
    Tags:   []string{"env:prod", "service:parquet-ingestor"},
    Rate:   1,
})

ing.SetMetricsRegistry(metrics)
```

The Datadog integration is covered by tests for:

- counter forwarding
- gauge forwarding
- float gauge forwarding
- metric prefixing
- tag forwarding
- sample-rate normalization
- nil client safety

## How the adaptive runtime decides

### Scale up workers

The runtime tends to add flush workers when:

- the flush queue is filling up
- CPU is below the configured target
- memory is below the configured target

### Scale down workers

The runtime tends to reduce flush workers when:

- memory pressure reaches the configured limit
- CPU is materially above the configured target

### Scale up pollers

The runtime tends to add pollers when:

- the source buffer is underused
- the flush queue is not under pressure
- CPU and memory still have room

### Scale down pollers

The runtime tends to reduce pollers when:

- source buffering pressure is high
- the system is under CPU or memory pressure

## Runtime error behavior

Configuration errors are still fatal. Examples:

- nil source
- nil transformer
- nil encoder
- nil sink
- nil key function
- invalid batcher configuration
- key function failure during flush, because the destination path cannot be resolved safely

Operational runtime failures are treated as non-fatal. They are logged at `Error` level and counted in metrics.

Examples:

- source receive failures
- transform failures
- encoder failures
- sink write failures
- ack failures
- lease-renewal failures
- flush worker job failures

This keeps the process alive while preserving observability.

## Testing

The repository includes:

- unit tests for batcher, source, encoder, sink, retry, and adaptive runtime
- integration tests for end-to-end ingest behavior
- benchmarks for encoder, source, sink, and adaptive runtime decisions

Adaptive runtime coverage includes tests for:

- worker pool resize
- scale-up decisions
- scale-down decisions
- runtime metric publication

Adaptive runtime benchmarks include:

- scale-up decision path
- scale-down decision path

## Backward compatibility

The manual `Run(ctx, flushWorkers, flushQueue)` API is unchanged.

If adaptive runtime is disabled, the project behaves like a manually tuned ingestor.

## Roadmap

- adaptive tuning for batch size and flush interval
- additional metric adapters
- more source-specific control heuristics
- richer backpressure controls for pull-based queues

## Benchmarks

Run the full benchmark suite locally:

```bash
go test ./... -bench=./... -benchmem
```

Run a fast CI-style benchmark sweep:

```bash
go test ./encoder ./ingestor ./sink ./source -short -run=^$ -bench=. -benchmem -benchtime=100ms
```

Benchmarks use `testing.Short()` to reduce heavy matrices in CI while keeping the full matrix available for local performance analysis.
