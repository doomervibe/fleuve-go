package stream

import (
	"context"
	"testing"
	"time"
)

func TestSleeperReset(t *testing.T) {
	s := NewSleeper(time.Millisecond, time.Hour)
	s.attempts = 5
	s.Reset()
	if s.attempts != 0 {
		t.Fatalf("attempts=%d", s.attempts)
	}
}

func TestSleeperRespectsContextCancel(t *testing.T) {
	s := NewSleeper(time.Hour, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Sleep(ctx); err != context.Canceled {
		t.Fatalf("got %v", err)
	}
}

func TestConsumedEventShape(t *testing.T) {
	ev := &ConsumedEvent{
		GlobalID:     9,
		WorkflowID:   "w",
		WorkflowType: "t",
		EventNo:      3,
		EventType:    "e",
		Metadata:     map[string]any{"tags": []string{"a"}},
	}
	if ev.GlobalID != 9 || ev.WorkflowID != "w" {
		t.Fatal("fields")
	}
}
