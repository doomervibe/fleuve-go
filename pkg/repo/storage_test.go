package repo

import (
	"context"
	"sync"
	"testing"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

func TestInProcessEphemeralStoragePutGetRemove(t *testing.T) {
	ctx := context.Background()
	s := NewInProcessEphemeralStorage(100)
	st := &model.StoredState{ID: "wf-1", Version: 1}
	if err := s.PutState(ctx, st); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetState(ctx, "wf-1")
	if err != nil || got == nil || got.ID != "wf-1" {
		t.Fatalf("get: %#v err=%v", got, err)
	}
	if got, _ := s.GetState(ctx, "missing"); got != nil {
		t.Fatal("expected nil")
	}
	if err := s.RemoveState(ctx, "wf-1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetState(ctx, "wf-1"); got != nil {
		t.Fatal("after remove")
	}
}

func TestInProcessEphemeralStorageLRUEviction(t *testing.T) {
	ctx := context.Background()
	s := NewInProcessEphemeralStorage(2)
	for i := 0; i < 3; i++ {
		_ = s.PutState(ctx, &model.StoredState{ID: string(rune('a' + i)), Version: 1})
	}
	if got, _ := s.GetState(ctx, "a"); got != nil {
		t.Fatal("oldest 'a' should be evicted")
	}
	if got, _ := s.GetState(ctx, "b"); got == nil {
		t.Fatal("expected b")
	}
	if got, _ := s.GetState(ctx, "c"); got == nil {
		t.Fatal("expected c")
	}
}

func TestInProcessEphemeralStorageUpdateExistingNoExtraKey(t *testing.T) {
	ctx := context.Background()
	s := NewInProcessEphemeralStorage(2)
	_ = s.PutState(ctx, &model.StoredState{ID: "x", Version: 1})
	_ = s.PutState(ctx, &model.StoredState{ID: "x", Version: 2})
	if got, _ := s.GetState(ctx, "x"); got == nil || got.Version != 2 {
		t.Fatalf("version: %#v", got)
	}
	// Still only one logical entry; room for one more before eviction of x
	_ = s.PutState(ctx, &model.StoredState{ID: "y", Version: 1})
	_ = s.PutState(ctx, &model.StoredState{ID: "z", Version: 1})
	if got, _ := s.GetState(ctx, "x"); got != nil {
		t.Fatal("x should be evicted as LRU when adding y,z after refresh x")
	}
}

func TestInProcessEphemeralStorageRingBufferEviction(t *testing.T) {
	ctx := context.Background()
	s := NewInProcessEphemeralStorage(3)
	// Fill the ring
	for i := 0; i < 3; i++ {
		_ = s.PutState(ctx, &model.StoredState{ID: string(rune('a' + i)), Version: 1})
	}
	// Remove middle entry
	_ = s.RemoveState(ctx, "b")
	// Add two more — should evict 'a', then skip empty 'b' slot
	_ = s.PutState(ctx, &model.StoredState{ID: "d", Version: 1})
	_ = s.PutState(ctx, &model.StoredState{ID: "e", Version: 1})
	// 'a' should be evicted
	if got, _ := s.GetState(ctx, "a"); got != nil {
		t.Fatal("'a' should have been evicted")
	}
	// 'c', 'd', 'e' should be present (b was already removed)
	for _, id := range []string{"c", "d", "e"} {
		if got, _ := s.GetState(ctx, id); got == nil {
			t.Fatalf("expected %q to be present", id)
		}
	}
}

func TestInProcessEphemeralStorageEvictionOrderWraps(t *testing.T) {
	ctx := context.Background()
	s := NewInProcessEphemeralStorage(2)
	// a, b fill the cache
	_ = s.PutState(ctx, &model.StoredState{ID: "a", Version: 1})
	_ = s.PutState(ctx, &model.StoredState{ID: "b", Version: 1})
	// c evicts a
	_ = s.PutState(ctx, &model.StoredState{ID: "c", Version: 1})
	if got, _ := s.GetState(ctx, "a"); got != nil {
		t.Fatal("a should be evicted")
	}
	// d evicts b
	_ = s.PutState(ctx, &model.StoredState{ID: "d", Version: 1})
	if got, _ := s.GetState(ctx, "b"); got != nil {
		t.Fatal("b should be evicted")
	}
	// c and d survive
	if got, _ := s.GetState(ctx, "c"); got == nil {
		t.Fatal("c should remain")
	}
	if got, _ := s.GetState(ctx, "d"); got == nil {
		t.Fatal("d should remain")
	}
}

type mapStorage struct {
	mu   sync.Mutex
	data map[string]*model.StoredState
}

func newMapStorage() *mapStorage {
	return &mapStorage{data: make(map[string]*model.StoredState)}
}

func (m *mapStorage) PutState(ctx context.Context, st *model.StoredState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[st.ID] = st
	return nil
}

func (m *mapStorage) GetState(ctx context.Context, id string) (*model.StoredState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[id], nil
}

func (m *mapStorage) RemoveState(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, id)
	return nil
}

func TestTieredEphemeralStorageL2Fallback(t *testing.T) {
	ctx := context.Background()
	l1 := NewInProcessEphemeralStorage(10)
	l2 := newMapStorage()
	_ = l2.PutState(ctx, &model.StoredState{ID: "remote", Version: 5})
	tiered := NewTieredEphemeralStorage(l1, l2)
	got, err := tiered.GetState(ctx, "remote")
	if err != nil || got == nil || got.Version != 5 {
		t.Fatalf("got %#v err=%v", got, err)
	}
	// Promoted to L1
	if got, _ := l1.GetState(ctx, "remote"); got == nil {
		t.Fatal("expected L1 promotion")
	}
}

func TestTieredEphemeralStorageRemoveClearsBoth(t *testing.T) {
	ctx := context.Background()
	l1 := NewInProcessEphemeralStorage(10)
	l2 := newMapStorage()
	tiered := NewTieredEphemeralStorage(l1, l2)
	if err := tiered.PutState(ctx, &model.StoredState{ID: "w", Version: 1}); err != nil {
		t.Fatal(err)
	}
	if err := tiered.RemoveState(ctx, "w"); err != nil {
		t.Fatal(err)
	}
	if got, _ := l1.GetState(ctx, "w"); got != nil {
		t.Fatal("L1")
	}
	if got, _ := l2.GetState(ctx, "w"); got != nil {
		t.Fatal("L2")
	}
}
