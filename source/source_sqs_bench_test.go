package source

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func BenchmarkSourceSQS_AckBatchMeta(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := &fakeSQSNoCapture{}

			cfg := DefaultSourceSQSConfig
			src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

			metas := make([]AckMetadata, 0, n)
			for i := 0; i < n; i++ {
				metas = append(metas, AckMetadata{ID: "id", Handle: "rh"})
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := src.AckBatchMeta(ctx, metas); err != nil {
					b.Fatalf("AckBatchMeta err: %v", err)
				}
			}
		})
	}
}

func BenchmarkSourceSQS_Receive(b *testing.B) {
	for _, batch := range []int{1, 10} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			f := newFakeSQSAPI(1024)

			cfg := DefaultSourceSQSConfig
			cfg.WaitTimeSeconds = 0
			cfg.Pollers = 1
			cfg.BufSize = 1024

			src := newTestSource(context.Background(), f, "q", cfg)
			defer src.Close()

			ctx := context.Background()

			payload := make([]sqstypes.Message, 0, batch)
			for i := 0; i < batch; i++ {
				payload = append(payload, sqstypes.Message{
					MessageId:     aws.String("m"),
					ReceiptHandle: aws.String("rh"),
					Body:          aws.String("x"),
				})
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				f.pushReceive(&sqs.ReceiveMessageOutput{Messages: payload})
				for j := 0; j < batch; j++ {
					if _, err := src.Receive(ctx); err != nil {
						b.Fatalf("receive err: %v", err)
					}
				}
			}
		})
	}
}

func BenchmarkSourceSQS_Fail(b *testing.B) {
	f := newFakeSQSAPI(1)

	cfg := DefaultSourceSQSConfig
	to := int32(7)
	cfg.FailVisibilityTimeoutSeconds = &to

	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	m := &message{
		src: src,
		m: &sqstypes.Message{
			MessageId:     aws.String("id"),
			ReceiptHandle: aws.String("rh-1"),
			Body:          aws.String("x"),
		},
	}

	ctx := context.Background()
	errX := errors.New("x")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := m.Fail(ctx, errX); err != nil {
			b.Fatalf("fail err: %v", err)
		}
	}
}

func BenchmarkSourceSQS_AckBatchMeta_NoCapture_Parallel(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f := &fakeSQSNoCapture{}

			cfg := DefaultSourceSQSConfig
			src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

			metas := make([]AckMetadata, 0, n)
			for i := 0; i < n; i++ {
				metas = append(metas, AckMetadata{ID: "id", Handle: "rh"})
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := src.AckBatchMeta(ctx, metas); err != nil {
						b.Fatalf("AckBatchMeta err: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkSourceSQS_Receive_ParallelConsumers(b *testing.B) {
	f := &fakeSQSNoCapture{}

	cfg := DefaultSourceSQSConfig
	cfg.BufSize = 4096

	src, _ := newTestSourceNoPollers(context.Background(), f, "q", cfg)

	stop := make(chan struct{})
	defer close(stop)

	go func() {
		msg := &sqstypes.Message{
			MessageId:     aws.String("m"),
			ReceiptHandle: aws.String("rh"),
			Body:          aws.String("x"),
		}
		for {
			select {
			case <-stop:
				return
			case src.bufCh <- msg:
			default:
				runtime.Gosched()
			}
		}
	}()

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := src.Receive(ctx); err != nil {
				b.Fatalf("receive err: %v", err)
			}
		}
	})
}
