package source

import (
	"context"
	"testing"
	"time"
)

func TestSourceSQS_SetPollers(t *testing.T) {
	api := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	cfg.WaitTimeSeconds = 0
	cfg.Pollers = 1
	cfg.BufSize = 8
	s := NewSourceSQSWithConfig(context.Background(), api, "queue-url", cfg)
	defer s.Close()
	if got := s.Pollers(); got != 1 {
		t.Fatalf("pollers=%d want=1", got)
	}
	s.SetPollers(3)
	if got := s.Pollers(); got != 3 {
		t.Fatalf("pollers=%d want=3", got)
	}
	s.SetPollers(1)
	if got := s.Pollers(); got != 1 {
		t.Fatalf("pollers=%d want=1", got)
	}
	_, cap := s.BufferUsage()
	if cap != 8 {
		t.Fatalf("buffer cap=%d want=8", cap)
	}
	// give canceled pollers a moment to exit cleanly in test environments
	time.Sleep(20 * time.Millisecond)
}
