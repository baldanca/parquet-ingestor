package observability

import "testing"

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
