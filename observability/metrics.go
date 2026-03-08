package observability

import (
	"math"
	"sync"
	"sync/atomic"
)

type intMetric struct{ v atomic.Int64 }
type floatMetric struct{ v atomic.Uint64 }

// Metrics is the common metrics contract used by the project.
type Metrics interface {
	AddCounter(name string, delta int64)
	SetGauge(name string, value int64)
	SetGaugeFloat(name string, value float64)
}

// Adapter is an external metrics sink that receives the same metric updates
// recorded in the local registry. Examples: DogStatsD/Datadog, OTEL bridges,
// custom internal collectors.
type Adapter interface {
	Metrics
}

// Registry is a lightweight in-process metrics registry.
//
// It stores metrics locally and can fan them out to optional adapters.
type Registry struct {
	counters sync.Map // map[string]*intMetric
	gauges   sync.Map // map[string]*intMetric
	floats   sync.Map // map[string]*floatMetric

	adaptersMu sync.RWMutex
	adapters   []Adapter
}

func (r *Registry) intCounter(name string) *intMetric {
	if v, ok := r.counters.Load(name); ok {
		return v.(*intMetric)
	}
	m := &intMetric{}
	actual, _ := r.counters.LoadOrStore(name, m)
	return actual.(*intMetric)
}

func (r *Registry) intGauge(name string) *intMetric {
	if v, ok := r.gauges.Load(name); ok {
		return v.(*intMetric)
	}
	m := &intMetric{}
	actual, _ := r.gauges.LoadOrStore(name, m)
	return actual.(*intMetric)
}

func (r *Registry) floatGauge(name string) *floatMetric {
	if v, ok := r.floats.Load(name); ok {
		return v.(*floatMetric)
	}
	m := &floatMetric{}
	actual, _ := r.floats.LoadOrStore(name, m)
	return actual.(*floatMetric)
}

// AddAdapter registers an external metrics adapter.
func (r *Registry) AddAdapter(a Adapter) {
	if a == nil {
		return
	}
	r.adaptersMu.Lock()
	defer r.adaptersMu.Unlock()
	r.adapters = append(r.adapters, a)
}

func (r *Registry) fanout(fn func(Adapter)) {
	r.adaptersMu.RLock()
	adapters := append([]Adapter(nil), r.adapters...)
	r.adaptersMu.RUnlock()
	for _, a := range adapters {
		fn(a)
	}
}

func (r *Registry) AddCounter(name string, delta int64) {
	r.intCounter(name).v.Add(delta)
	r.fanout(func(a Adapter) { a.AddCounter(name, delta) })
}

func (r *Registry) SetGauge(name string, value int64) {
	r.intGauge(name).v.Store(value)
	r.fanout(func(a Adapter) { a.SetGauge(name, value) })
}

func (r *Registry) SetGaugeFloat(name string, value float64) {
	r.floatGauge(name).v.Store(math.Float64bits(value))
	r.fanout(func(a Adapter) { a.SetGaugeFloat(name, value) })
}

func (r *Registry) Snapshot() map[string]float64 {
	out := map[string]float64{}
	r.counters.Range(func(k, v any) bool {
		out[k.(string)] = float64(v.(*intMetric).v.Load())
		return true
	})
	r.gauges.Range(func(k, v any) bool {
		out[k.(string)] = float64(v.(*intMetric).v.Load())
		return true
	})
	r.floats.Range(func(k, v any) bool {
		out[k.(string)] = math.Float64frombits(v.(*floatMetric).v.Load())
		return true
	})
	return out
}

// DatadogClient is the minimal contract required by DatadogAdapter.
type DatadogClient interface {
	Count(name string, value int64, tags []string, rate float64) error
	Gauge(name string, value float64, tags []string, rate float64) error
}

// DatadogAdapter forwards metrics to a DogStatsD-compatible client.
type DatadogAdapter struct {
	Client DatadogClient
	Prefix string
	Tags   []string
	Rate   float64
}

func (d DatadogAdapter) metricName(name string) string {
	if d.Prefix == "" {
		return name
	}
	return d.Prefix + "." + name
}

func (d DatadogAdapter) AddCounter(name string, delta int64) {
	if d.Client == nil {
		return
	}
	_ = d.Client.Count(d.metricName(name), delta, d.Tags, d.sampleRate())
}

func (d DatadogAdapter) SetGauge(name string, value int64) {
	if d.Client == nil {
		return
	}
	_ = d.Client.Gauge(d.metricName(name), float64(value), d.Tags, d.sampleRate())
}

func (d DatadogAdapter) SetGaugeFloat(name string, value float64) {
	if d.Client == nil {
		return
	}
	_ = d.Client.Gauge(d.metricName(name), value, d.Tags, d.sampleRate())
}

func (d DatadogAdapter) sampleRate() float64 {
	if d.Rate <= 0 || d.Rate > 1 {
		return 1
	}
	return d.Rate
}
