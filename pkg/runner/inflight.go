package runner

import "sort"

// InflightTracker tracks in-flight event processing for safe checkpointing.
//
// Maintains a sliding window of dispatched event global_ids and their
// completion status. The committable offset is the highest *contiguous*
// completed global_id from the bottom (TCP receive-window semantics).
//
// Mirrors Python fleuve InflightTracker.
type InflightTracker struct {
	pending   map[int64]bool
	committed int64
}

// NewInflightTracker creates a new InflightTracker.
func NewInflightTracker() *InflightTracker {
	return &InflightTracker{
		pending: make(map[int64]bool),
	}
}

// Register marks a global_id as dispatched but not yet completed.
func (t *InflightTracker) Register(globalID int64) {
	t.pending[globalID] = false
}

// MarkDone marks a global_id as completed and advances the committable offset
// past any contiguous completed entries.
func (t *InflightTracker) MarkDone(globalID int64) {
	t.pending[globalID] = true
	t.advance()
}

// CommittableOffset returns the highest contiguous completed global_id.
func (t *InflightTracker) CommittableOffset() int64 {
	return t.committed
}

// Size returns the number of in-flight (not yet committable) entries.
func (t *InflightTracker) Size() int {
	return len(t.pending)
}

func (t *InflightTracker) advance() {
	ids := make([]int64, 0, len(t.pending))
	for gid := range t.pending {
		ids = append(ids, gid)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, gid := range ids {
		if t.pending[gid] {
			t.committed = gid
			delete(t.pending, gid)
		} else {
			break
		}
	}
}
