package ingestor

import (
	"context"
	"testing"
	"time"

	"github.com/baldanca/parquet-ingestor/batcher"
	"github.com/baldanca/parquet-ingestor/observability"
	"github.com/baldanca/parquet-ingestor/source"
)

type adaptiveSource struct {
	*tSource
	pollers int
	used    int
	cap     int
}

func (s *adaptiveSource) SetPollers(n int) { s.pollers = n }
func (s *adaptiveSource) Pollers() int     { return s.pollers }
func (s *adaptiveSource) BufferUsage() (used, capacity int) {
	return s.used, s.cap
}

var _ source.PollerScaler = (*adaptiveSource)(nil)
var _ source.BufferStats = (*adaptiveSource)(nil)

func TestIngestor_SetFlushWorkers_ResizesPool(t *testing.T) {
	src := &adaptiveSource{tSource: newTSource(), pollers: 1, used: 0, cap: 10}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Second
	cfg.MaxEstimatedInputBytes = 1024
	ing, err := NewIngestor(cfg, src, tTransformer{}, enc, sk, func(ctx context.Context, b batcher.Batch[int]) (string, error) {
		return "k", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.flushQueue = 4
	ing.flushWorkers = 2
	ing.maybeStartFlushPool(context.Background())
	if got := ing.currentFlushWorkers(); got != 2 {
		t.Fatalf("workers=%d want=2", got)
	}
	ing.SetFlushWorkers(4)
	if got := ing.currentFlushWorkers(); got != 4 {
		t.Fatalf("workers=%d want=4", got)
	}
	ing.SetFlushWorkers(1)
	if got := ing.currentFlushWorkers(); got != 1 {
		t.Fatalf("workers=%d want=1", got)
	}
	for _, stop := range ing.flushStops {
		stop()
	}
	close(ing.flushJobs)
}

func TestIngestor_AdaptiveDecision_ScalesUpWorkersAndPollersWhenSourceIsUnderPressure(t *testing.T) {
	src := &adaptiveSource{tSource: newTSource(), pollers: 1, used: 1, cap: 10}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Second
	cfg.MaxEstimatedInputBytes = 1024
	ing, err := NewIngestor(cfg, src, tTransformer{}, enc, sk, func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil })
	if err != nil {
		t.Fatal(err)
	}
	metrics := &observability.Registry{}
	ing.SetMetricsRegistry(metrics)
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{Enabled: true, MinWorkers: 1, MaxWorkers: 4, MinPollers: 1, MaxPollers: 3, TargetCPUUtilization: 0.8, TargetMemoryUtilization: 0.8, MaxMemoryBytes: 1024 * 1024, SampleInterval: time.Second, Cooldown: time.Second})
	ing.flushQueue = 4
	ing.flushWorkers = 1
	ing.maybeStartFlushPool(context.Background())
	// trigger worker scale up
	if !ing.applyAdaptiveDecision(runtimeSnapshot{memBytes: 100, cpuUtil: 0.2, flushQueueLen: 3, flushQueueCap: 4, sourceUsed: 1, sourceCap: 10, workers: 1, pollers: 1}) {
		t.Fatalf("expected worker scale up")
	}
	if got := ing.currentFlushWorkers(); got != 2 {
		t.Fatalf("workers=%d want=2", got)
	}
	// trigger poller scale up only when there is real source-side pressure
	if !ing.applyAdaptiveDecision(runtimeSnapshot{memBytes: 100, cpuUtil: 0.2, flushQueueLen: 1, flushQueueCap: 4, sourceUsed: 7, sourceCap: 10, workers: 2, pollers: 1}) {
		t.Fatalf("expected poller scale up")
	}
	if src.pollers != 2 {
		t.Fatalf("pollers=%d want=2", src.pollers)
	}
	for _, stop := range ing.flushStops {
		stop()
	}
	close(ing.flushJobs)
}


func TestIngestor_AdaptiveDecision_DoesNotScaleUpPollersWhenIdle(t *testing.T) {
	src := &adaptiveSource{tSource: newTSource(), pollers: 1, used: 0, cap: 10}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Second
	cfg.MaxEstimatedInputBytes = 1024
	ing, err := NewIngestor(cfg, src, tTransformer{}, enc, sk, func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil })
	if err != nil {
		t.Fatal(err)
	}
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{Enabled: true, MinWorkers: 1, MaxWorkers: 4, MinPollers: 1, MaxPollers: 3, TargetCPUUtilization: 0.8, TargetMemoryUtilization: 0.8, MaxMemoryBytes: 1024 * 1024, SampleInterval: time.Second, Cooldown: time.Second})
	ing.flushQueue = 4
	ing.flushWorkers = 2
	ing.maybeStartFlushPool(context.Background())
	defer func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	}()

	if ing.applyAdaptiveDecision(runtimeSnapshot{memBytes: 100, cpuUtil: 0.2, flushQueueLen: 0, flushQueueCap: 4, sourceUsed: 0, sourceCap: 10, workers: 2, pollers: 1}) {
		t.Fatalf("expected no scale up while idle")
	}
	if src.pollers != 1 {
		t.Fatalf("pollers=%d want=1", src.pollers)
	}
}

func TestIngestor_AdaptiveDecision_ScalesDownWorkersAndPollers(t *testing.T) {
	src := &adaptiveSource{tSource: newTSource(), pollers: 3, used: 9, cap: 10}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Second
	cfg.MaxEstimatedInputBytes = 1024
	ing, err := NewIngestor(cfg, src, tTransformer{}, enc, sk, func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil })
	if err != nil {
		t.Fatal(err)
	}
	metrics := &observability.Registry{}
	ing.SetMetricsRegistry(metrics)
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{Enabled: true, MinWorkers: 1, MaxWorkers: 4, MinPollers: 1, MaxPollers: 3, TargetCPUUtilization: 0.7, TargetMemoryUtilization: 0.8, MaxMemoryBytes: 1024, SampleInterval: time.Second, Cooldown: time.Second})
	ing.flushQueue = 4
	ing.flushWorkers = 3
	ing.maybeStartFlushPool(context.Background())
	if !ing.applyAdaptiveDecision(runtimeSnapshot{memBytes: 2048, cpuUtil: 0.95, flushQueueLen: 1, flushQueueCap: 4, sourceUsed: 9, sourceCap: 10, workers: 3, pollers: 3}) {
		t.Fatalf("expected scale down")
	}
	if got := ing.currentFlushWorkers(); got != 2 {
		t.Fatalf("workers=%d want=2", got)
	}
	if !ing.applyAdaptiveDecision(runtimeSnapshot{memBytes: 100, cpuUtil: 0.2, flushQueueLen: 0, flushQueueCap: 4, sourceUsed: 9, sourceCap: 10, workers: 2, pollers: 3}) {
		t.Fatalf("expected poller scale down")
	}
	if src.pollers != 2 {
		t.Fatalf("pollers=%d want=2", src.pollers)
	}
	for _, stop := range ing.flushStops {
		stop()
	}
	if ing.flushJobs != nil {
		close(ing.flushJobs)
	}
}

func TestIngestor_PublishRuntimeMetrics(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	ing.publishRuntimeMetrics(runtimeSnapshot{memBytes: 123, cpuUtil: 0.5, flushQueueLen: 2, flushQueueCap: 4, sourceUsed: 3, sourceCap: 8, workers: 2, pollers: 1})
	snap := ing.metrics.Snapshot()
	if snap["ingestor_memory_bytes"] != 123 {
		t.Fatalf("memory metric = %v, want 123", snap["ingestor_memory_bytes"])
	}
	if snap["ingestor_cpu_utilization"] != 0.5 {
		t.Fatalf("cpu metric = %v, want 0.5", snap["ingestor_cpu_utilization"])
	}
	if snap["ingestor_flush_workers"] != 2 {
		t.Fatalf("workers metric = %v, want 2", snap["ingestor_flush_workers"])
	}
	if snap["ingestor_source_pollers"] != 1 {
		t.Fatalf("pollers metric = %v, want 1", snap["ingestor_source_pollers"])
	}
}

func TestIngestor_AdaptiveDecision_NoOpWithinHealthyWindow(t *testing.T) {
	src := &adaptiveSource{tSource: newTSource(), pollers: 2, used: 4, cap: 16}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Second
	cfg.MaxEstimatedInputBytes = 1024
	ing, err := NewIngestor(cfg, src, tTransformer{}, enc, sk, func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil })
	if err != nil {
		t.Fatal(err)
	}
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{Enabled: true, MinWorkers: 1, MaxWorkers: 4, MinPollers: 1, MaxPollers: 4, TargetCPUUtilization: 0.8, TargetMemoryUtilization: 0.8, MaxMemoryBytes: 1024 * 1024, SampleInterval: time.Second, Cooldown: time.Second})
	ing.flushQueue = 4
	ing.flushWorkers = 2
	ing.maybeStartFlushPool(context.Background())
	defer func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	}()
	if ing.applyAdaptiveDecision(runtimeSnapshot{memBytes: 128 << 10, cpuUtil: 0.55, flushQueueLen: 1, flushQueueCap: 4, sourceUsed: 4, sourceCap: 16, workers: 2, pollers: 2}) {
		t.Fatalf("expected no scaling decision")
	}
	if got := ing.currentFlushWorkers(); got != 2 {
		t.Fatalf("workers=%d want=2", got)
	}
	if src.pollers != 2 {
		t.Fatalf("pollers=%d want=2", src.pollers)
	}
}

func TestIngestor_AdaptiveDecision_SeverePressurePrefersWorkersDownFirst(t *testing.T) {
	src := &adaptiveSource{tSource: newTSource(), pollers: 3, used: 15, cap: 16}
	enc := &tEncoder{ct: "application/octet-stream", ext: ".bin"}
	sk := &tSink{}
	cfg := batcher.DefaultBatcherConfig
	cfg.FlushInterval = time.Second
	cfg.MaxEstimatedInputBytes = 1024
	ing, err := NewIngestor(cfg, src, tTransformer{}, enc, sk, func(ctx context.Context, b batcher.Batch[int]) (string, error) { return "k", nil })
	if err != nil {
		t.Fatal(err)
	}
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{Enabled: true, MinWorkers: 1, MaxWorkers: 4, MinPollers: 1, MaxPollers: 4, TargetCPUUtilization: 0.7, TargetMemoryUtilization: 0.8, MaxMemoryBytes: 1024, SampleInterval: time.Second, Cooldown: time.Second})
	ing.flushQueue = 4
	ing.flushWorkers = 3
	ing.maybeStartFlushPool(context.Background())
	defer func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	}()
	if !ing.applyAdaptiveDecision(runtimeSnapshot{memBytes: 2048, cpuUtil: 1.05, flushQueueLen: 3, flushQueueCap: 4, sourceUsed: 15, sourceCap: 16, workers: 3, pollers: 3}) {
		t.Fatalf("expected scale down")
	}
	if got := ing.currentFlushWorkers(); got != 2 {
		t.Fatalf("workers=%d want=2", got)
	}
	if src.pollers != 3 {
		t.Fatalf("pollers=%d want=3", src.pollers)
	}
}
