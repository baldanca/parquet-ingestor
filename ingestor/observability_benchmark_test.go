package ingestor

import "testing"

func BenchmarkIngestor_AdaptiveDecisionScaleUp(b *testing.B) {
	BenchmarkIngestorAdaptiveDecisionScaleUpWorkers(b)
}

func BenchmarkIngestor_AdaptiveDecisionScaleDown(b *testing.B) {
	BenchmarkIngestorAdaptiveDecisionScaleDownWorkers(b)
}
