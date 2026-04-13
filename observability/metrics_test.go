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

func (nopDDClient) Count(string, int64, []string, float64) error  { return nil }
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
