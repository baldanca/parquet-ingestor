package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/encoder"
	"github.com/baldanca/parquet-ingestor/ingestor"
	"github.com/baldanca/parquet-ingestor/sink"
	"github.com/baldanca/parquet-ingestor/source"
)

// This example shows how to wire SQS -> transformer -> Parquet -> S3.
// It expects standard AWS credentials in the environment (or any AWS SDK provider chain).
func main() {
	queueURL := os.Getenv("SQS_QUEUE_URL")
	bucket := os.Getenv("S3_BUCKET")
	prefix := os.Getenv("S3_PREFIX")
	if prefix == "" {
		prefix = "parquet-ingestor/"
	}
	if queueURL == "" || bucket == "" {
		fmt.Println("Set SQS_QUEUE_URL and S3_BUCKET (optional: S3_PREFIX).")
		os.Exit(2)
	}

	ctx := context.Background()

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}

	sqsClient := sqs.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg)

	// NOTE: constructors/configs in this repo are stable for interfaces,
	// but some example-only constructors changed over time.
	// This example assumes your current repo still exposes these.
	src := source.NewSourceSQSWithConfig(ctx, sqsClient, queueURL, source.SourceSQSConfig{
		WaitTimeSeconds: 20,
		MaxMessages:     10,
		VisibilityTO:    30,
		Pollers:         4,
		BufSize:         256,
		// Optional: if a message fails transform/flush, extend visibility so it won't be re-delivered immediately.
		FailVisibilityTimeoutSeconds: ptrI32(300),
	})

	sk := sink.NewSinkS3(s3Client, bucket, prefix)

	tr := jsonTransformer{}

	// NEW API: NewParquetEncoder returns only the encoder (no error) and
	// does not take ParquetConfig anymore.
	enc := encoder.NewParquetEncoder[Record](encoder.ParquetCompressionSnappy)

	// NEW API: BatcherConfig fields were renamed.
	bcfg := batcher.BatcherConfig{
		MaxEstimatedInputBytes: 5 * 1024 * 1024,
		FlushInterval:          5 * time.Minute,
		MaxItems:               100_000,
		ReuseBuffers:           true,
	}

	keyFunc := func(ctx context.Context, b batcher.Batch[Record]) (string, error) {
		_ = ctx
		_ = b
		// NOTE: make keys unique. You can include timestamps / partitions / random suffixes.
		return fmt.Sprintf("%s%04d.parquet", prefix, time.Now().UnixNano()), nil
	}

	ig, err := ingestor.NewIngestor[Record](bcfg, src, tr, enc, sk, keyFunc)
	if err != nil {
		panic(err)
	}

	// Run with a small worker pool for flushes.
	if err := ig.Run(ctx, 4, 256); err != nil {
		panic(err)
	}
}

type Record struct {
	ID    int64   `parquet:"name=id"`
	Name  string  `parquet:"name=name"`
	Value float64 `parquet:"name=value"`
}

type jsonTransformer struct{}

func (jsonTransformer) Transform(ctx context.Context, env source.Envelope) (Record, error) {
	var out Record
	b, ok := env.Payload.([]byte)
	if !ok {
		return out, fmt.Errorf("payload is not []byte")
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return out, err
	}
	return out, nil
}

func ptrI32(v int32) *int32 { return &v }
