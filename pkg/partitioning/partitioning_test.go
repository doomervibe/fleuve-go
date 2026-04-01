package partitioning

import (
	"testing"
)

func TestGetPartitionIndex_nonPositive(t *testing.T) {
	if GetPartitionIndex("any", 0) != 0 {
		t.Fatal("totalPartitions 0 should yield 0")
	}
	if GetPartitionIndex("any", -3) != 0 {
		t.Fatal("negative totalPartitions should yield 0")
	}
}

func TestPartitionedReaderName(t *testing.T) {
	got := PartitionedReaderName("order_workflow", 0, 3)
	want := "order_workflow_runner_partition_0_of_3"
	if got != want {
		t.Fatalf("PartitionedReaderName = %q, want %q", got, want)
	}
}

func TestGetPartitionIndex_singlePartition(t *testing.T) {
	if idx := GetPartitionIndex("any-id", 1); idx != 0 {
		t.Fatalf("single partition: got %d, want 0", idx)
	}
}

func TestGetPartitionIndex_rangeAndStable(t *testing.T) {
	id := "workflow-abc-123"
	for _, n := range []int{2, 3, 7, 16} {
		idx := GetPartitionIndex(id, n)
		if idx < 0 || idx >= n {
			t.Fatalf("GetPartitionIndex(%q, %d) = %d, want in [0,%d)", id, n, idx, n)
		}
	}
	first := GetPartitionIndex(id, 5)
	for i := 0; i < 20; i++ {
		if GetPartitionIndex(id, 5) != first {
			t.Fatalf("expected stable index for same id")
		}
	}
}

func TestIsMine_matchesGetPartitionIndex(t *testing.T) {
	id := "shard-test"
	total := 4
	idx := GetPartitionIndex(id, total)
	if !IsMine(id, idx, total) {
		t.Fatal("IsMine should be true for GetPartitionIndex result")
	}
	for p := 0; p < total; p++ {
		if p != idx && IsMine(id, p, total) {
			t.Fatalf("IsMine(%q, %d, %d) should be false when partition is %d", id, p, total, idx)
		}
	}
}
