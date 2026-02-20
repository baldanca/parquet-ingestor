# parquet-ingestor

A small, dependency-light Go library that turns streaming messages into **time-partitioned Parquet files**.

It is designed to feel familiar if you have used managed delivery services (e.g. Kinesis Firehose):

- Pull messages from a **Source** (e.g. SQS)
- Transform each message into a typed record
- Batch by **bytes / item count / time**
- Encode the batch (e.g. Parquet)
- Write the file to a **Sink** (e.g. S3)
- Acknowledge the consumed messages

## Features

- Generic pipeline (`source` → `transformer` → `batcher` → `encoder` → `sink`)
- Optional **streaming encode + streaming upload** to reduce peak memory
- Concurrent flush workers (`Run(ctx, workers, queue)`)
- Optional SQS lease extension while flushing (visibility renew)
- Simple retry policies for writes and acks

## Installation

```bash
go get github.com/baldanca/parquet-ingestor
```

## Quick start

The record type must be compatible with `parquet-go`'s `GenericWriter` (struct tags are supported).

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/ingestor"
	"github.com/baldanca/parquet-ingestor/sink"
	"github.com/baldanca/parquet-ingestor/source"
	"github.com/baldanca/parquet-ingestor/transformer"
)

type Event struct {
	ID        string    `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`
	CreatedAt time.Time `parquet:"name=created_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Payload   string    `parquet:"name=payload, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type eventTransformer struct{}

func (eventTransformer) Transform(ctx context.Context, in source.Envelope) (Event, error) {
	// Example assumes the SQS body is a JSON string. Replace with your decoding.
	_ = ctx
	return Event{ID: "example", CreatedAt: time.Now().UTC(), Payload: in.Payload.(string)}, nil
}

func main() {
	ctx := context.Background()

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	sqsClient := sqs.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg)

	src := source.New(ctx, sqsClient, "https://sqs.us-east-1.amazonaws.com/123/queue")
	sk := sink.New(s3Client, "my-bucket", "events")

	enc := encoder.NewParquetEncoder[Event](encoder.ParquetCompressionSnappy)
	keyFn := ingestor.DefaultKeyFunc[Event](enc)

	cfg := batcher.DefaultBatcherConfig
	cfg.MaxEstimatedInputBytes = 5 * 1024 * 1024
	cfg.FlushInterval = 5 * time.Minute

	ing, err := ingestor.NewIngestor[Event](cfg, src, transformer.Transformer[source.Envelope, Event](eventTransformer{}), enc, sk, keyFn)
	if err != nil {
		log.Fatal(err)
	}

	ing.SetRetryPolicy(ingestor.SimpleRetry{Attempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second, Jitter: true})
	ing.SetAckRetryPolicy(ingestor.SimpleRetry{Attempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: 1 * time.Second, Jitter: true})

	// Optional: if flush can take longer than the SQS visibility timeout.
	// ing.EnableLease(60, 20*time.Second)

	if err := ing.Run(ctx, 4, 64); err != nil {
		log.Fatal(err)
	}
}
```

## Package overview

- `source`: input connectors (currently includes SQS)
- `transformer`: generic transform interface
- `batcher`: size/time-based batch accumulator
- `encoder`: output encoders (currently includes Parquet)
- `sink`: output connectors (currently includes S3)
- `ingestor`: orchestration, worker pool, retries, lease renew

## Design notes

- **Acknowledgements happen only after a successful sink write.**
- When both the encoder implements `encoder.StreamEncoder` and the sink implements `sink.StreamSinkr`, the ingestor will stream the Parquet bytes directly to the sink.
- `Run(ctx, workers, queue)` allows high throughput by overlapping flushes.

## Testing

```bash
go test ./...
```

Benchmarks:

```bash
go test ./... -bench=./... -benchmem
```

## Contributing

Issues and PRs are welcome. Keep changes small and include tests/benchmarks when relevant.

## License

Add a license that fits your project (MIT/Apache-2.0 are common). This repository currently does not ship one.


## Examples

- `examples/basic`: in-memory end-to-end run (no AWS dependencies at runtime).
- `examples/sqs_to_s3`: wiring SQS -> Parquet -> S3 using the AWS SDK.
