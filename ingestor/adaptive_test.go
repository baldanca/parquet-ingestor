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

func TestIngestor_AdaptiveDecision_IdleScalesWorkersDown(t *testing.T) {
	// When workers are above minimum and the flush queue is idle, the adaptive
	// runtime should scale workers back down (not scale pollers up).
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

	// workers=2 > minWorkers=1, flushUsage=0 < 0.20 → should scale workers down
	if !ing.applyAdaptiveDecision(runtimeSnapshot{memBytes: 100, cpuUtil: 0.2, flushQueueLen: 0, flushQueueCap: 4, sourceUsed: 0, sourceCap: 10, workers: 2, pollers: 1}) {
		t.Fatal("expected idle scale-down of workers")
	}
	if got := ing.currentFlushWorkers(); got != 1 {
		t.Fatalf("workers=%d want=1", got)
	}
	// pollers must NOT have been scaled up
	if src.pollers != 1 {
		t.Fatalf("pollers=%d want=1 (must not scale up while idle)", src.pollers)
	}
}

func TestIngestor_AdaptiveDecision_AtMinimumNoIdleScaleDown(t *testing.T) {
	// When already at minimum workers and pollers, idle state should not trigger
	// any scaling action.
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
	ing.flushWorkers = 1
	ing.maybeStartFlushPool(context.Background())
	defer func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	}()

	// workers=1==minWorkers, pollers=1==minPollers → no scaling should occur
	if ing.applyAdaptiveDecision(runtimeSnapshot{memBytes: 100, cpuUtil: 0.2, flushQueueLen: 0, flushQueueCap: 4, sourceUsed: 0, sourceCap: 10, workers: 1, pollers: 1}) {
		t.Fatal("expected no action when already at minimum workers and pollers")
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

// TestIngestor_AdaptiveDecision_NilConfig verifies that applyAdaptiveDecision
// returns false immediately when no adaptive config is set.
func TestIngestor_AdaptiveDecision_NilConfig(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	if ing.applyAdaptiveDecision(runtimeSnapshot{workers: 2, pollers: 1}) {
		t.Fatal("expected false when adaptive config is nil")
	}
}

// TestRatio covers the zero-capacity and normal cases of the ratio helper.
func TestRatio(t *testing.T) {
	if got := ratio(5, 0); got != 0 {
		t.Fatalf("ratio(5,0) = %v, want 0", got)
	}
	if got := ratio(0, 0); got != 0 {
		t.Fatalf("ratio(0,0) = %v, want 0", got)
	}
	if got := ratio(3, 10); got != 0.3 {
		t.Fatalf("ratio(3,10) = %v, want 0.3", got)
	}
	if got := ratio(10, 10); got != 1.0 {
		t.Fatalf("ratio(10,10) = %v, want 1.0", got)
	}
}

// TestClampInt covers below-min, above-max, and in-range cases.
func TestClampInt(t *testing.T) {
	if got := clampInt(0, 1, 5); got != 1 {
		t.Fatalf("clampInt(0,1,5) = %d, want 1", got)
	}
	if got := clampInt(10, 1, 5); got != 5 {
		t.Fatalf("clampInt(10,1,5) = %d, want 5", got)
	}
	if got := clampInt(3, 1, 5); got != 3 {
		t.Fatalf("clampInt(3,1,5) = %d, want 3", got)
	}
	if got := clampInt(1, 1, 5); got != 1 {
		t.Fatalf("clampInt(1,1,5) = %d, want 1 (at min)", got)
	}
	if got := clampInt(5, 1, 5); got != 5 {
		t.Fatalf("clampInt(5,1,5) = %d, want 5 (at max)", got)
	}
}

// TestIngestor_SnapshotRuntime verifies that snapshotRuntime returns a valid
// snapshot and a non-negative cpuNow value.
func TestIngestor_SnapshotRuntime(t *testing.T) {
	src := &adaptiveSource{tSource: newTSource(), pollers: 2, used: 5, cap: 10}
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
	ing.flushQueue = 4
	ing.flushWorkers = 2
	ing.maybeStartFlushPool(context.Background())
	defer func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		close(ing.flushJobs)
	}()

	prevTS := time.Now().Add(-time.Second)
	prevCPU := readCPUTimeSeconds()
	snap, cpuNow := ing.snapshotRuntime(time.Now(), prevTS, prevCPU)

	if cpuNow < 0 {
		t.Fatalf("cpuNow = %v, want >= 0", cpuNow)
	}
	if snap.memBytes == 0 {
		// memBytes should reflect live heap; allow zero only on trivial programs
		// but at minimum the field must be readable without panic.
		t.Log("memBytes = 0 (acceptable on minimal test heap)")
	}
	if snap.workers != 2 {
		t.Fatalf("workers = %d, want 2", snap.workers)
	}
	if snap.pollers != 2 {
		t.Fatalf("pollers = %d, want 2", snap.pollers)
	}
	if snap.sourceUsed != 5 || snap.sourceCap != 10 {
		t.Fatalf("sourceUsed=%d sourceCap=%d, want 5/10", snap.sourceUsed, snap.sourceCap)
	}
	if snap.flushQueueCap != 4 {
		t.Fatalf("flushQueueCap = %d, want 4", snap.flushQueueCap)
	}
}

// TestIngestor_SnapshotRuntime_NoPrevCPU verifies the cpuUtil clamp to zero
// when prevCPUTime is greater than cpuNow (clock jump / counter reset).
func TestIngestor_SnapshotRuntime_NegativeCPUClampedToZero(t *testing.T) {
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

	// Pass a ridiculously large prevCPUTime to force a negative delta.
	snap, _ := ing.snapshotRuntime(time.Now(), time.Now().Add(-time.Second), 1e18)
	if snap.cpuUtil < 0 {
		t.Fatalf("cpuUtil = %v, should be clamped to >= 0", snap.cpuUtil)
	}
}

// TestIngestor_StartAdaptiveLoop_PublishesMetricsOnTick verifies that the
// adaptive goroutine fires at least once and records runtime metrics.
func TestIngestor_StartAdaptiveLoop_PublishesMetricsOnTick(t *testing.T) {
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
	metrics := &observability.Registry{}
	ing.SetMetricsRegistry(metrics)
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{
		Enabled:                 true,
		MinWorkers:              1,
		MaxWorkers:              4,
		MinPollers:              1,
		MaxPollers:              4,
		TargetCPUUtilization:    0.8,
		TargetMemoryUtilization: 0.8,
		MaxMemoryBytes:          64 << 20,
		SampleInterval:          20 * time.Millisecond,
		Cooldown:                time.Second,
	})
	ing.flushQueue = 4
	ing.flushWorkers = 1
	ing.maybeStartFlushPool(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	ing.startAdaptiveLoop(ctx)

	// Wait for at least one tick to populate the metrics.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		snap := metrics.Snapshot()
		if _, ok := snap["ingestor_memory_bytes"]; ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	// Give the goroutine time to exit.
	time.Sleep(30 * time.Millisecond)

	snap := metrics.Snapshot()
	if _, ok := snap["ingestor_memory_bytes"]; !ok {
		t.Fatal("ingestor_memory_bytes metric was never published")
	}
	if _, ok := snap["ingestor_flush_workers"]; !ok {
		t.Fatal("ingestor_flush_workers metric was never published")
	}

	for _, stop := range ing.flushStops {
		stop()
	}
	if ing.flushJobs != nil {
		close(ing.flushJobs)
	}
}

// TestIngestor_StartAdaptiveLoop_Disabled verifies that calling startAdaptiveLoop
// when adaptive is nil (disabled) is a safe no-op.
func TestIngestor_StartAdaptiveLoop_Disabled(t *testing.T) {
	ing := &Ingestor[int]{metrics: &observability.Registry{}}
	// adaptive is nil → should return immediately without starting a goroutine.
	ing.startAdaptiveLoop(context.Background())
	if ing.adaptiveStop != nil {
		t.Fatal("adaptiveStop must remain nil when adaptive is disabled")
	}
}

// TestIngestor_AdaptiveDecision_SingleWorkerMode_ScalesUpViaSourcePressure
// covers the new scale-up path: when flushQueueCap==0 (no flush queue yet),
// sourceUsage >= 0.60 should trigger a worker scale-up.
func TestIngestor_AdaptiveDecision_SingleWorkerMode_ScalesUpViaSourcePressure(t *testing.T) {
	src := &adaptiveSource{tSource: newTSource(), pollers: 1, used: 7, cap: 10}
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
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{
		Enabled:                 true,
		MinWorkers:              1,
		MaxWorkers:              4,
		MinPollers:              1,
		MaxPollers:              3,
		TargetCPUUtilization:    0.8,
		TargetMemoryUtilization: 0.8,
		MaxMemoryBytes:          1024 * 1024,
		SampleInterval:          time.Second,
		Cooldown:                time.Second,
	})
	// flushQueue=0 → flushJobs will be nil → flushQueueCap=0 in snapshot.
	ing.flushWorkers = 1
	ing.maybeStartFlushPool(context.Background())
	defer func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	}()

	// sourceUsage = 7/10 = 0.70 >= 0.60, cpuUtil=0.2 < 0.8, flushQueueCap=0.
	if !ing.applyAdaptiveDecision(runtimeSnapshot{
		memBytes: 100, cpuUtil: 0.2,
		flushQueueLen: 0, flushQueueCap: 0,
		sourceUsed: 7, sourceCap: 10,
		workers: 1, pollers: 1,
	}) {
		t.Fatal("expected scale-up via source pressure in single-worker mode")
	}
	if got := ing.currentFlushWorkers(); got != 2 {
		t.Fatalf("workers = %d, want 2", got)
	}
}

// TestIngestor_AdaptiveDecision_SingleWorkerMode_NoScaleWhenSourceIdle verifies
// that when the source buffer is not pressured, no scale-up occurs even with an
// empty flush queue.
func TestIngestor_AdaptiveDecision_SingleWorkerMode_NoScaleWhenSourceIdle(t *testing.T) {
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
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{
		Enabled:                 true,
		MinWorkers:              1,
		MaxWorkers:              4,
		MinPollers:              1,
		MaxPollers:              3,
		TargetCPUUtilization:    0.8,
		TargetMemoryUtilization: 0.8,
		MaxMemoryBytes:          1024 * 1024,
		SampleInterval:          time.Second,
		Cooldown:                time.Second,
	})
	ing.flushWorkers = 1
	ing.maybeStartFlushPool(context.Background())
	defer func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	}()

	// sourceUsage = 1/10 = 0.10 < 0.60 → no scale-up.
	if ing.applyAdaptiveDecision(runtimeSnapshot{
		memBytes: 100, cpuUtil: 0.2,
		flushQueueLen: 0, flushQueueCap: 0,
		sourceUsed: 1, sourceCap: 10,
		workers: 1, pollers: 1,
	}) {
		t.Fatal("expected no scaling when source is idle in single-worker mode")
	}
	if got := ing.currentFlushWorkers(); got != 1 {
		t.Fatalf("workers = %d, want 1", got)
	}
}

// TestIngestor_AdaptiveDecision_SingleWorkerMode_AlreadyAtMax verifies that
// the single-worker scale-up path is a no-op when MaxWorkers == MinWorkers == 1.
// In this configuration clampInt(2, 1, 1) == 1 == current, so adjustFlushWorkers
// returns false and the entire decision returns false.
func TestIngestor_AdaptiveDecision_SingleWorkerMode_AlreadyAtMax(t *testing.T) {
	src := &adaptiveSource{tSource: newTSource(), pollers: 1, used: 7, cap: 10}
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
	// MaxWorkers=MinWorkers=1, MaxPollers=MinPollers=1 → every adjust call
	// clamps to current → no action possible in any branch.
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{
		Enabled: true, MinWorkers: 1, MaxWorkers: 1,
		MinPollers: 1, MaxPollers: 1,
		TargetCPUUtilization: 0.8, TargetMemoryUtilization: 0.8,
		MaxMemoryBytes: 1024 * 1024, SampleInterval: time.Second, Cooldown: time.Second,
	})
	ing.flushWorkers = 1
	ing.maybeStartFlushPool(context.Background())
	defer func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	}()

	// sourceUsage=0.70 >= 0.60, flushQueueCap=0, but MaxWorkers=1 → no change.
	if ing.applyAdaptiveDecision(runtimeSnapshot{
		memBytes: 100, cpuUtil: 0.2,
		flushQueueLen: 0, flushQueueCap: 0,
		sourceUsed: 7, sourceCap: 10,
		workers: 1, pollers: 1,
	}) {
		t.Fatal("expected no scaling when MaxWorkers=MinWorkers=1")
	}
}

// TestIngestor_AdaptiveDecision_ZeroWorkersInSnapshot exercises the fallback
// branch that reads the current worker count when the snapshot reports workers=0.
func TestIngestor_AdaptiveDecision_ZeroWorkersInSnapshot(t *testing.T) {
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
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{
		Enabled: true, MinWorkers: 1, MaxWorkers: 4,
		MinPollers: 1, MaxPollers: 3,
		TargetCPUUtilization: 0.8, TargetMemoryUtilization: 0.8,
		MaxMemoryBytes: 1024 * 1024, SampleInterval: time.Second, Cooldown: time.Second,
	})
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

	// workers=0 and pollers=0 in snapshot → fallback reads current values.
	// flushUsage=0 < 0.20, workers(actual)=2 > minWorkers=1 → idle scale-down.
	if !ing.applyAdaptiveDecision(runtimeSnapshot{
		memBytes: 100, cpuUtil: 0.2,
		flushQueueLen: 0, flushQueueCap: 4,
		sourceUsed: 0, sourceCap: 10,
		workers: 0, pollers: 0, // zero → uses live values
	}) {
		t.Fatal("expected idle scale-down when workers=0 in snapshot (reads live workers=2)")
	}
	if got := ing.currentFlushWorkers(); got != 1 {
		t.Fatalf("workers = %d, want 1 after idle scale-down", got)
	}
}

// TestIngestor_AdaptiveDecision_SeverePressure_ScalesPollerWhenWorkersAtMin
// covers the adjustPollers branch inside the severe-pressure block: when workers
// are already at minimum the decision must fall through to scaling pollers down.
func TestIngestor_AdaptiveDecision_SeverePressure_ScalesPollerWhenWorkersAtMin(t *testing.T) {
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
	ing.SetMetricsRegistry(&observability.Registry{})
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{
		Enabled: true, MinWorkers: 1, MaxWorkers: 4,
		MinPollers: 1, MaxPollers: 4,
		TargetCPUUtilization: 0.7, TargetMemoryUtilization: 0.8,
		MaxMemoryBytes: 1024, SampleInterval: time.Second, Cooldown: time.Second,
	})
	ing.flushQueue = 4
	ing.flushWorkers = 1 // already at MinWorkers
	ing.maybeStartFlushPool(context.Background())
	defer func() {
		for _, stop := range ing.flushStops {
			stop()
		}
		if ing.flushJobs != nil {
			close(ing.flushJobs)
		}
	}()

	// severeMem fires (memPressure = 2048/1024 = 2.0 >= 1.15).
	// workers=1 == minWorkers=1 → adjustFlushWorkers returns false.
	// Falls through to adjustPollers in the severe block → scales pollers 3→2.
	if !ing.applyAdaptiveDecision(runtimeSnapshot{
		memBytes: 2048, cpuUtil: 0.3,
		flushQueueLen: 1, flushQueueCap: 4,
		sourceUsed: 9, sourceCap: 10,
		workers: 1, pollers: 3,
	}) {
		t.Fatal("expected poller scale-down from severe block when workers at min")
	}
	if src.pollers != 2 {
		t.Fatalf("pollers = %d, want 2", src.pollers)
	}
}

// TestIngestor_AdaptiveDecision_ModeratePressure_ScalesWorkersWhenPollersAtMin
// covers the adjustFlushWorkers branch inside the moderate-pressure block: when
// pollers are already at minimum the decision must fall through to workers.
func TestIngestor_AdaptiveDecision_ModeratePressure_ScalesWorkersWhenPollersAtMin(t *testing.T) {
	src := &adaptiveSource{tSource: newTSource(), pollers: 1, used: 9, cap: 10}
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
	ing.EnableAdaptiveRuntime(AdaptiveRuntimeConfig{
		Enabled: true, MinWorkers: 1, MaxWorkers: 4,
		MinPollers: 1, MaxPollers: 4,
		TargetCPUUtilization: 0.7, TargetMemoryUtilization: 0.8,
		MaxMemoryBytes: 1024 * 1024, SampleInterval: time.Second, Cooldown: time.Second,
	})
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

	// moderateMem fires (memPressure = 900000/1000000 < 1.0 → no; use sourcePressure).
	// sourceUsage = 9/10 = 0.90 >= 0.90 → moderate block fires.
	// pollers=1 == minPollers=1 → adjustPollers returns false.
	// Falls through to adjustFlushWorkers in the moderate block → scales 3→2.
	if !ing.applyAdaptiveDecision(runtimeSnapshot{
		memBytes: 100, cpuUtil: 0.3,
		flushQueueLen: 1, flushQueueCap: 4,
		sourceUsed: 9, sourceCap: 10,
		workers: 3, pollers: 1,
	}) {
		t.Fatal("expected worker scale-down from moderate block when pollers at min")
	}
	if got := ing.currentFlushWorkers(); got != 2 {
		t.Fatalf("workers = %d, want 2", got)
	}
}

// TestAdjustPollers_NoPollerScaler verifies that adjustPollers is a safe no-op
// when the source does not implement source.PollerScaler.
func TestAdjustPollers_NoPollerScaler(t *testing.T) {
	// tSource does NOT implement PollerScaler.
	src := newTSource()
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

	scaled, from, to := ing.adjustPollers(2, +1, 1, 4)
	if scaled {
		t.Fatalf("adjustPollers must return false when source has no PollerScaler, got scaled=true from=%d to=%d", from, to)
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
