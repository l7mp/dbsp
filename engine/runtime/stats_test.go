package runtime

import (
	"testing"

	"github.com/go-logr/logr"
)

func TestStats(t *testing.T) {
	rt := NewRuntime("stats-test", logr.Discard())
	rt.CountStat("test.counter")
	rt.AddStat("test.counter", 2)
	CountStat("test.process")

	s := rt.Stats()
	if got := s.Counters["test.counter"]; got != 3 {
		t.Fatalf("test.counter = %d, want 3", got)
	}
	if got := s.ProcessCounters["test.process"]; got < 1 {
		t.Fatalf("test.process = %d, want >= 1", got)
	}
	s.Counters["test.counter"] = 99
	if got := rt.Stats().Counters["test.counter"]; got != 3 {
		t.Fatalf("snapshot must be a copy, counter = %d", got)
	}
	if s.Runtime.Goroutines <= 0 || s.Runtime.HeapAllocBytes == 0 {
		t.Fatalf("runtime figures missing: %+v", s.Runtime)
	}
}
