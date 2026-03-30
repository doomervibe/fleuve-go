package metrics

import "testing"

func TestNewFleuveMetrics(t *testing.T) {
	m := NewFleuveMetrics()
	if m == nil || m.EventsProcessedTotal == nil {
		t.Fatal("metrics not initialized")
	}
}
