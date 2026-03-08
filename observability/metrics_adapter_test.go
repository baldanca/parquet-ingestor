package observability

import "testing"

type captureDD struct {
	counts    map[string]int64
	gauges    map[string]float64
	lastTags  []string
	lastRate  float64
	lastNames []string
}

func (c *captureDD) Count(name string, value int64, tags []string, rate float64) error {
	if c.counts == nil {
		c.counts = map[string]int64{}
	}
	c.counts[name] += value
	c.lastTags = append([]string(nil), tags...)
	c.lastRate = rate
	c.lastNames = append(c.lastNames, name)
	return nil
}
func (c *captureDD) Gauge(name string, value float64, tags []string, rate float64) error {
	if c.gauges == nil {
		c.gauges = map[string]float64{}
	}
	c.gauges[name] = value
	c.lastTags = append([]string(nil), tags...)
	c.lastRate = rate
	c.lastNames = append(c.lastNames, name)
	return nil
}

func TestRegistryAdaptersFanoutToDatadog(t *testing.T) {
	dd := &captureDD{}
	r := &Registry{}
	r.AddAdapter(DatadogAdapter{Client: dd, Prefix: "parquet_ingestor", Tags: []string{"env:test", "service:pi"}, Rate: 0.5})

	r.AddCounter("messages_total", 2)
	r.SetGauge("workers", 4)
	r.SetGaugeFloat("cpu", 0.75)

	if got := dd.counts["parquet_ingestor.messages_total"]; got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
	if got := dd.gauges["parquet_ingestor.workers"]; got != 4 {
		t.Fatalf("workers gauge = %v, want 4", got)
	}
	if got := dd.gauges["parquet_ingestor.cpu"]; got != 0.75 {
		t.Fatalf("cpu gauge = %v, want 0.75", got)
	}
	if dd.lastRate != 0.5 {
		t.Fatalf("sample rate = %v, want 0.5", dd.lastRate)
	}
	if len(dd.lastTags) != 2 || dd.lastTags[0] != "env:test" || dd.lastTags[1] != "service:pi" {
		t.Fatalf("tags = %#v, want forwarded tags", dd.lastTags)
	}
}

func TestDatadogAdapter_DefaultSampleRateAndNilClient(t *testing.T) {
	var nilClient DatadogAdapter
	nilClient.AddCounter("ignored", 1)
	nilClient.SetGauge("ignored", 1)
	nilClient.SetGaugeFloat("ignored", 1.0)

	dd := &captureDD{}
	adapter := DatadogAdapter{Client: dd, Prefix: "pi", Rate: 0}
	adapter.AddCounter("events", 1)
	if dd.lastRate != 1 {
		t.Fatalf("default sample rate = %v, want 1", dd.lastRate)
	}
	if len(dd.lastNames) == 0 || dd.lastNames[0] != "pi.events" {
		t.Fatalf("metric names = %#v, want prefixed metric", dd.lastNames)
	}
}
