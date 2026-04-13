package observability

import (
	"testing"
)

// BenchmarkRegistry_AddCounter_NoAdapters measures the hot path: a registry
// with no adapters registered. This is the most common production case when
// only the in-process Snapshot is consumed (e.g. no Datadog sidecar).
func BenchmarkRegistry_AddCounter_NoAdapters(b *testing.B) {
	r := &Registry{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.AddCounter("ingestor_messages_received_total", 1)
	}
}

// BenchmarkRegistry_AddCounter_WithAdapter measures the fanout path when one
// adapter is registered. Validates that the fast-path guard does not regress
// the adapter case.
func BenchmarkRegistry_AddCounter_WithAdapter(b *testing.B) {
	r := &Registry{}
	r.AddAdapter(DatadogAdapter{Client: &nopDDClient{}})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.AddCounter("ingestor_messages_received_total", 1)
	}
}

type nopDDClient struct{}

func (nopDDClient) Count(string, int64, []string, float64) error   { return nil }
func (nopDDClient) Gauge(string, float64, []string, float64) error { return nil }

func TestRegistrySnapshot(t *testing.T) {
	r := &Registry{}
	r.AddCounter("events_total", 3)
	r.SetGauge("queue_depth", 7)
	r.SetGaugeFloat("cpu_utilization", 0.55)

	snap := r.Snapshot()
	if got := snap["events_total"]; got != 3 {
		t.Fatalf("events_total = %v, want 3", got)
	}
	if got := snap["queue_depth"]; got != 7 {
		t.Fatalf("queue_depth = %v, want 7", got)
	}
	if got := snap["cpu_utilization"]; got != 0.55 {
		t.Fatalf("cpu_utilization = %v, want 0.55", got)
	}
}

// TestRegistry_MetricCacheHit exercises the fast-path branches inside
// intCounter, intGauge, and floatGauge that return the already-stored metric
// when the same name is used more than once.
func TestRegistry_MetricCacheHit(t *testing.T) {
	r := &Registry{}
	// First call creates the entry; second call must hit the Load fast-path.
	r.AddCounter("cache_total", 1)
	r.AddCounter("cache_total", 2)
	if got := r.Snapshot()["cache_total"]; got != 3 {
		t.Fatalf("counter after two increments = %v, want 3", got)
	}

	r.SetGauge("depth", 10)
	r.SetGauge("depth", 20)
	if got := r.Snapshot()["depth"]; got != 20 {
		t.Fatalf("gauge = %v, want 20", got)
	}

	r.SetGaugeFloat("ratio", 0.1)
	r.SetGaugeFloat("ratio", 0.9)
	if got := r.Snapshot()["ratio"]; got != 0.9 {
		t.Fatalf("float gauge = %v, want 0.9", got)
	}
}

// TestRegistry_AddAdapter_NilIsIgnored verifies that passing nil to AddAdapter
// does not panic and does not set the hasAdapters flag.
func TestRegistry_AddAdapter_NilIsIgnored(t *testing.T) {
	r := &Registry{}
	r.AddAdapter(nil) // must not panic
	if r.hasAdapters.Load() {
		t.Fatal("hasAdapters must stay false after adding nil adapter")
	}
}

// TestRegistry_HasAdapters_SetAfterFirstAdd verifies that the atomic fast-path
// flag transitions from false to true once a real adapter is registered.
func TestRegistry_HasAdapters_SetAfterFirstAdd(t *testing.T) {
	r := &Registry{}
	if r.hasAdapters.Load() {
		t.Fatal("hasAdapters should be false on zero-value Registry")
	}
	r.AddAdapter(DatadogAdapter{Client: &nopDDClient{}})
	if !r.hasAdapters.Load() {
		t.Fatal("hasAdapters should be true after AddAdapter")
	}
}

// TestDatadogAdapter_MetricName_NoPrefix verifies that when Prefix is empty the
// metric name is returned unchanged.
func TestDatadogAdapter_MetricName_NoPrefix(t *testing.T) {
	d := DatadogAdapter{}
	if got := d.metricName("foo_total"); got != "foo_total" {
		t.Fatalf("metricName = %q, want %q", got, "foo_total")
	}
}

// TestDatadogAdapter_SampleRate_OutOfRange verifies that a Rate > 1 is clamped
// to 1.0 so DogStatsD receives a valid sample rate.
func TestDatadogAdapter_SampleRate_OutOfRange(t *testing.T) {
	dd := &captureDD{}
	a := DatadogAdapter{Client: dd, Rate: 1.5}
	a.AddCounter("x_total", 1)
	if dd.lastRate != 1.0 {
		t.Fatalf("sample rate = %v, want 1.0 for Rate=1.5", dd.lastRate)
	}
}
