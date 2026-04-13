package runner

import "testing"

func TestInflightTracker_Sequential(t *testing.T) {
	tr := NewInflightTracker()

	tr.Register(1)
	tr.Register(2)
	tr.Register(3)

	tr.MarkDone(1)
	if got := tr.CommittableOffset(); got != 1 {
		t.Fatalf("expected 1, got %d", got)
	}

	tr.MarkDone(2)
	if got := tr.CommittableOffset(); got != 2 {
		t.Fatalf("expected 2, got %d", got)
	}

	tr.MarkDone(3)
	if got := tr.CommittableOffset(); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
	if tr.Size() != 0 {
		t.Fatalf("expected empty, got size %d", tr.Size())
	}
}

func TestInflightTracker_OutOfOrder(t *testing.T) {
	tr := NewInflightTracker()

	tr.Register(10)
	tr.Register(11)
	tr.Register(12)

	// Complete out of order — offset must not advance past the gap.
	tr.MarkDone(12)
	if got := tr.CommittableOffset(); got != 0 {
		t.Fatalf("expected 0 (gap at 10), got %d", got)
	}

	tr.MarkDone(10)
	if got := tr.CommittableOffset(); got != 10 {
		t.Fatalf("expected 10 (gap at 11), got %d", got)
	}

	tr.MarkDone(11)
	if got := tr.CommittableOffset(); got != 12 {
		t.Fatalf("expected 12 (all done), got %d", got)
	}
}

func TestInflightTracker_Empty(t *testing.T) {
	tr := NewInflightTracker()
	if got := tr.CommittableOffset(); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}
