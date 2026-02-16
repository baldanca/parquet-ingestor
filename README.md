# parquet-ingestor

High-throughput event ingestion library in Go that:

1) receives messages from a `Source` (e.g. SQS)
2) transforms them into a typed domain model
3) batches by time/size/count
4) encodes to Parquet (Snappy) or NDJSON
5) writes to a `Sink` (e.g. S3)

The core is intentionally small and dependency-free (you provide adapters for AWS SDK / logging / metrics).

## Architecture

```
Source → Transformer → Batcher → Encoder → Sink
```

## Guarantees

- **At-least-once**: batches are acked only after a successful sink write.
- **Ordering**: not guaranteed across workers; within a batch depends on your `Source`.
- **Backpressure**: the processing loop is synchronous; slow Transform/Encode/Write naturally reduces Receive rate.

## Failure model

- Transform error:
  - `Message.Fail()` is called and processing continues.
- Encode error:
  - batch is not acked; redelivery depends on source semantics.
- Sink write error:
  - batch is not acked; redelivery is expected (duplicates possible).
- Ack error:
  - write already happened; duplicates are possible (at-least-once).

Error taxonomy is exposed via sentinels: `ErrTransform`, `ErrEncode`, `ErrSinkWrite`, `ErrAck`.

## Configuration

Batch flush triggers:

- `MaxEstimatedInputBytes` (cheap estimate; for SQS use `len(body)`)
- `MaxItems` (optional safety cap)
- `FlushInterval`

## Observability

Use `Config.Telemetry` to plug metrics/logs/traces without forcing a dependency.

## Retries

Use `Config.RetryPolicy` for sink writes (e.g. S3 throttling) and `Config.AckRetryPolicy` for ack retries.

## Example (SQS → S3)

This repo includes a minimal AWS SDK v2 example:

```bash
export AWS_REGION=us-east-1
export SQS_QUEUE_URL='https://sqs.us-east-1.amazonaws.com/123456789012/my-queue'
export S3_BUCKET='my-bucket'
export S3_PREFIX='events'

go run ./examples/sqs_to_s3_parquet \
  -workers 4 \
  -flush 5s \
  -max-items 5000 \
  -max-est-mb 12
```

Tuning note (Parquet+Snappy targeting ~5MB objects): start with `-max-est-mb 12` and adjust based on the
observed `batch.encoded_bytes` telemetry.
