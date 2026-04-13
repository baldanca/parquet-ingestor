package ingestor

import (
	"context"
	"testing"
	"time"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/observability"
)

func newAdaptiveBenchmarkIngestor(b *testing.B, pollers, used, cap int) (*Ingestor[int], *adaptiveSource) {
	b.Helper()

	src := &adaptiveSource{tSource: newTSource(), pollers: pollers, used: used, cap: cap}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Second
	cfg.MaxEstimatedInputBytes = 1024
	ing, err := NewIngestor(cfg, src, tTransformer{}, enc, sk, func(ctx context.Context, bt batcher.Batch[int]) (string, error) { return "k", nil })
	if err != nil {
		b.Fatal(err)
	}
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{Enabled: true, MinWorkers: 1, MaxWorkers: 8, MinPollers: 1, MaxPollers: 4, TargetCPUUtilization: 0.8, TargetMemoryUtilization: 0.8, MaxMemoryBytes: 64 << 20, SampleInterval: time.Second, Cooldown: time.Second})
	ing.flushQueue = 8
	ing.flushWorkers = 2
	ing.maybeStartFlushPool(context.Background())
	b.Cleanup(func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	})
	return ing, src
}

func BenchmarkIngestorAdaptiveDecision(b *testing.B) {
	ing, src := newAdaptiveBenchmarkIngestor(b, 1, 1, 32)
	benchAdaptiveDecision(b, ing, src, runtimeSnapshot{memBytes: 1 << 20, cpuUtil: 0.3, flushQueueLen: 6, flushQueueCap: 8, sourceUsed: 2, sourceCap: 32})
}

func BenchmarkIngestorAdaptiveDecisionScaleUpWorkers(b *testing.B) {
	ing, src := newAdaptiveBenchmarkIngestor(b, 1, 1, 32)
	benchAdaptiveDecision(b, ing, src, runtimeSnapshot{memBytes: 1 << 20, cpuUtil: 0.2, flushQueueLen: 7, flushQueueCap: 8, sourceUsed: 1, sourceCap: 32})
}

func BenchmarkIngestorAdaptiveDecisionScaleDownWorkers(b *testing.B) {
	ing, src := newAdaptiveBenchmarkIngestor(b, 2, 24, 32)
	benchAdaptiveDecision(b, ing, src, runtimeSnapshot{memBytes: 80 << 20, cpuUtil: 0.96, flushQueueLen: 2, flushQueueCap: 8, sourceUsed: 24, sourceCap: 32})
}

func BenchmarkIngestorAdaptiveDecisionScaleUpPollers(b *testing.B) {
	ing, src := newAdaptiveBenchmarkIngestor(b, 1, 0, 32)
	benchAdaptiveDecision(b, ing, src, runtimeSnapshot{memBytes: 1 << 20, cpuUtil: 0.25, flushQueueLen: 0, flushQueueCap: 8, sourceUsed: 0, sourceCap: 32, workers: 2, pollers: 1})
}

func BenchmarkIngestorAdaptiveDecisionScaleDownPollers(b *testing.B) {
	ing, src := newAdaptiveBenchmarkIngestor(b, 4, 30, 32)
	benchAdaptiveDecision(b, ing, src, runtimeSnapshot{memBytes: 1 << 20, cpuUtil: 0.2, flushQueueLen: 0, flushQueueCap: 8, sourceUsed: 30, sourceCap: 32, workers: 2, pollers: 4})
}

func BenchmarkIngestorAdaptivePublishRuntimeMetrics(b *testing.B) {
	ing, _ := newAdaptiveBenchmarkIngestor(b, 2, 3, 32)
	snap := runtimeSnapshot{memBytes: 4 << 20, cpuUtil: 0.37, flushQueueLen: 5, flushQueueCap: 8, sourceUsed: 3, sourceCap: 32, workers: 2, pollers: 2}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ing.publishRuntimeMetrics(snap)
	}
}

func BenchmarkIngestorAdaptiveSnapshotRuntime(b *testing.B) {
	ing, _ := newAdaptiveBenchmarkIngestor(b, 2, 3, 32)
	prevTS := time.Now().Add(-time.Second)
	prevCPU := readCPUTimeSeconds()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = ing.snapshotRuntime(time.Now(), prevTS, prevCPU)
	}
}

func benchAdaptiveDecision(b *testing.B, ing *Ingestor[int], src *adaptiveSource, base runtimeSnapshot) {
	b.Helper()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		snap := base
		snap.workers = ing.currentFlushWorkers()
		snap.pollers = src.pollers
		_ = ing.applyAdaptiveDecision(snap)
	}
}

func BenchmarkIngestorAdaptiveDecisionNoop(b *testing.B) {
	ing, src := newAdaptiveBenchmarkIngestor(b, 2, 8, 32)
	benchAdaptiveDecision(b, ing, src, runtimeSnapshot{memBytes: 16 << 20, cpuUtil: 0.55, flushQueueLen: 2, flushQueueCap: 8, sourceUsed: 8, sourceCap: 32, workers: 2, pollers: 2})
}

func BenchmarkIngestorAdaptiveDecisionSeverePressure(b *testing.B) {
	ing, src := newAdaptiveBenchmarkIngestor(b, 4, 30, 32)
	benchAdaptiveDecision(b, ing, src, runtimeSnapshot{memBytes: 96 << 20, cpuUtil: 1.15, flushQueueLen: 7, flushQueueCap: 8, sourceUsed: 30, sourceCap: 32, workers: 4, pollers: 4})
}

func BenchmarkIngestorAdaptiveScaleWorkersUpDown(b *testing.B) {
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Second
	cfg.MaxEstimatedInputBytes = 1024
	src := &adaptiveSource{tSource: newTSource(), pollers: 2, used: 2, cap: 32}
	ing, err := NewIngestor(cfg, src, tTransformer{}, &tEncoder{ct: "application/octet-stream", ext: ".bin"}, &tSink{}, func(ctx context.Context, bt batcher.Batch[int]) (string, error) { return "k", nil })
	if err != nil {
		b.Fatal(err)
	}
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.flushQueue = 8
	ing.flushWorkers = 2
	ing.maybeStartFlushPool(context.Background())
	b.Cleanup(func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ing.SetFlushWorkers(4)
		ing.SetFlushWorkers(2)
	}
}
