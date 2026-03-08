package ingestor

import (
	"context"
	"runtime"
	runtimemetrics "runtime/metrics"
	"time"

	"github.com/baldanca/parquet-ingestor/source"
)

// AdaptiveRuntimeConfig enables dynamic scaling based on memory, CPU, and backlog.
type AdaptiveRuntimeConfig struct {
	Enabled                 bool
	MinWorkers              int
	MaxWorkers              int
	MinPollers              int
	MaxPollers              int
	TargetCPUUtilization    float64
	TargetMemoryUtilization float64
	MaxMemoryBytes          uint64
	SampleInterval          time.Duration
	Cooldown                time.Duration
}

type runtimeSnapshot struct {
	memBytes      uint64
	cpuUtil       float64
	flushQueueLen int
	flushQueueCap int
	sourceUsed    int
	sourceCap     int
	workers       int
	pollers       int
}

func (i *Ingestor[iType]) startAdaptiveLoop(ctx context.Context) {
	if i.adaptive == nil || !i.adaptive.Enabled {
		return
	}
	go func() {
		ticker := time.NewTicker(i.adaptive.SampleInterval)
		defer ticker.Stop()
		var lastScale time.Time
		prevCPUTime := readCPUTimeSeconds()
		prevTS := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				snap := i.snapshotRuntime(now, prevTS, prevCPUTime)
				prevTS = now
				prevCPUTime = readCPUTimeSeconds()
				i.publishRuntimeMetrics(snap)
				if !lastScale.IsZero() && now.Sub(lastScale) < i.adaptive.Cooldown {
					continue
				}
				if i.applyAdaptiveDecision(snap) {
					lastScale = now
				}
			}
		}
	}()
}

func (i *Ingestor[iType]) snapshotRuntime(now, prevTS time.Time, prevCPUTime float64) runtimeSnapshot {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	used := ms.Alloc
	cpuNow := readCPUTimeSeconds()
	elapsed := now.Sub(prevTS).Seconds()
	cpuUtil := 0.0
	if elapsed > 0 {
		cpuUtil = (cpuNow - prevCPUTime) / (elapsed * float64(runtime.GOMAXPROCS(0)))
		if cpuUtil < 0 {
			cpuUtil = 0
		}
	}
	snap := runtimeSnapshot{memBytes: used, cpuUtil: cpuUtil, workers: i.currentFlushWorkers()}
	if i.flushJobs != nil {
		snap.flushQueueLen = len(i.flushJobs)
		snap.flushQueueCap = cap(i.flushJobs)
	}
	if bs, ok := i.source.(source.BufferStats); ok {
		snap.sourceUsed, snap.sourceCap = bs.BufferUsage()
	}
	if ps, ok := i.source.(source.PollerScaler); ok {
		snap.pollers = ps.Pollers()
	}
	return snap
}

func (i *Ingestor[iType]) publishRuntimeMetrics(s runtimeSnapshot) {
	i.metrics.SetGauge("ingestor_memory_bytes", int64(s.memBytes))
	i.metrics.SetGaugeFloat("ingestor_cpu_utilization", s.cpuUtil)
	i.metrics.SetGauge("ingestor_flush_queue_used", int64(s.flushQueueLen))
	i.metrics.SetGauge("ingestor_flush_queue_capacity", int64(s.flushQueueCap))
	i.metrics.SetGauge("ingestor_source_buffer_used", int64(s.sourceUsed))
	i.metrics.SetGauge("ingestor_source_buffer_capacity", int64(s.sourceCap))
	i.metrics.SetGauge("ingestor_flush_workers", int64(s.workers))
	i.metrics.SetGauge("ingestor_source_pollers", int64(s.pollers))
}

func (i *Ingestor[iType]) applyAdaptiveDecision(s runtimeSnapshot) bool {
	cfg := i.adaptive
	if cfg == nil || !cfg.Enabled {
		return false
	}

	workers := s.workers
	pollers := s.pollers
	maxWorkers := cfg.MaxWorkers
	if maxWorkers < 1 {
		maxWorkers = max(1, runtime.GOMAXPROCS(0)*4)
	}
	minWorkers := cfg.MinWorkers
	if minWorkers < 1 {
		minWorkers = 1
	}
	maxPollers := cfg.MaxPollers
	if maxPollers < 1 {
		maxPollers = max(1, runtime.GOMAXPROCS(0))
	}
	minPollers := cfg.MinPollers
	if minPollers < 1 {
		minPollers = 1
	}

	flushUsage := ratio(s.flushQueueLen, s.flushQueueCap)
	sourceUsage := ratio(s.sourceUsed, s.sourceCap)
	memPressure := 0.0
	if cfg.MaxMemoryBytes > 0 {
		memPressure = float64(s.memBytes) / float64(cfg.MaxMemoryBytes)
	}

	if workers == 0 {
		workers = i.currentFlushWorkers()
	}
	if pollers == 0 {
		if scaler, ok := i.source.(source.PollerScaler); ok {
			pollers = scaler.Pollers()
		}
	}

	severeCPU := s.cpuUtil > cfg.TargetCPUUtilization+0.30
	moderateCPU := s.cpuUtil > cfg.TargetCPUUtilization+0.15
	severeMem := cfg.MaxMemoryBytes > 0 && memPressure >= 1.15
	moderateMem := cfg.MaxMemoryBytes > 0 && memPressure >= 1.0

	if severeCPU || severeMem {
		if scaled, from, to := i.adjustFlushWorkers(workers, -1, minWorkers, maxWorkers); scaled {
			i.metrics.AddCounter("ingestor_scale_down_total", 1)
			i.logger.Warn("ingestor.adaptive.scale_down_workers", "from", from, "to", to, "cpu_utilization", s.cpuUtil, "memory_bytes", s.memBytes, "flush_usage", flushUsage)
			return true
		}
		if scaled, from, to := i.adjustPollers(pollers, -1, minPollers, maxPollers); scaled {
			i.metrics.AddCounter("ingestor_scale_down_total", 1)
			i.logger.Warn("ingestor.adaptive.scale_down_pollers", "from", from, "to", to, "source_usage", sourceUsage, "cpu_utilization", s.cpuUtil, "memory_bytes", s.memBytes)
			return true
		}
	}

	if moderateCPU || moderateMem || sourceUsage >= 0.90 {
		if scaled, from, to := i.adjustPollers(pollers, -1, minPollers, maxPollers); scaled {
			i.metrics.AddCounter("ingestor_scale_down_total", 1)
			i.logger.Warn("ingestor.adaptive.scale_down_pollers", "from", from, "to", to, "source_usage", sourceUsage)
			return true
		}
		if scaled, from, to := i.adjustFlushWorkers(workers, -1, minWorkers, maxWorkers); scaled {
			i.metrics.AddCounter("ingestor_scale_down_total", 1)
			i.logger.Warn("ingestor.adaptive.scale_down_workers", "from", from, "to", to, "cpu_utilization", s.cpuUtil, "memory_bytes", s.memBytes)
			return true
		}
	}

	if flushUsage >= 0.85 && s.cpuUtil < cfg.TargetCPUUtilization-0.05 && (cfg.MaxMemoryBytes == 0 || memPressure < cfg.TargetMemoryUtilization-0.05) {
		if scaled, from, to := i.adjustFlushWorkers(workers, +1, minWorkers, maxWorkers); scaled {
			i.metrics.AddCounter("ingestor_scale_up_total", 1)
			i.logger.Info("ingestor.adaptive.scale_up_workers", "from", from, "to", to, "flush_usage", flushUsage)
			return true
		}
	}

	if flushUsage >= 0.70 && s.cpuUtil < cfg.TargetCPUUtilization && (cfg.MaxMemoryBytes == 0 || memPressure < cfg.TargetMemoryUtilization) {
		if scaled, from, to := i.adjustFlushWorkers(workers, +1, minWorkers, maxWorkers); scaled {
			i.metrics.AddCounter("ingestor_scale_up_total", 1)
			i.logger.Info("ingestor.adaptive.scale_up_workers", "from", from, "to", to, "flush_usage", flushUsage)
			return true
		}
	}

	if flushUsage <= 0.25 && sourceUsage <= 0.10 && s.cpuUtil < cfg.TargetCPUUtilization-0.1 && (cfg.MaxMemoryBytes == 0 || memPressure < cfg.TargetMemoryUtilization-0.1) {
		if scaled, from, to := i.adjustPollers(pollers, +1, minPollers, maxPollers); scaled {
			i.metrics.AddCounter("ingestor_scale_up_total", 1)
			i.logger.Info("ingestor.adaptive.scale_up_pollers", "from", from, "to", to, "source_usage", sourceUsage)
			return true
		}
	}

	return false
}

func ratio(used, capacity int) float64 {
	if capacity <= 0 {
		return 0
	}
	return float64(used) / float64(capacity)
}

func clampInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func (i *Ingestor[iType]) adjustFlushWorkers(current, delta, minWorkers, maxWorkers int) (bool, int, int) {
	target := clampInt(current+delta, minWorkers, maxWorkers)
	if target == current {
		return false, current, current
	}
	i.SetFlushWorkers(target)
	return true, current, target
}

func (i *Ingestor[iType]) adjustPollers(current, delta, minPollers, maxPollers int) (bool, int, int) {
	scaler, ok := i.source.(source.PollerScaler)
	if !ok {
		return false, current, current
	}
	target := clampInt(current+delta, minPollers, maxPollers)
	if target == current {
		return false, current, current
	}
	scaler.SetPollers(target)
	return true, current, target
}

func readCPUTimeSeconds() float64 {
	var samples [1]runtimemetrics.Sample
	samples[0].Name = "/cpu/classes/total:cpu-seconds"
	runtimemetrics.Read(samples[:])
	return samples[0].Value.Float64()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
