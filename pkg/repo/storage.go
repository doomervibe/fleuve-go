package repo

import (
	"context"
	"sync"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

type EphemeralStorage interface {
	PutState(ctx context.Context, new *model.StoredState) error
	GetState(ctx context.Context, workflowID string) (*model.StoredState, error)
	RemoveState(ctx context.Context, workflowID string) error
}

type InProcessEphemeralStorage struct {
	mu      sync.RWMutex
	cache   map[string]*model.StoredState
	ring    []string // ring buffer for eviction order
	head    int      // next write position
	size    int      // current number of entries in ring
	maxSize int
}

func NewInProcessEphemeralStorage(maxSize int) *InProcessEphemeralStorage {
	return &InProcessEphemeralStorage{
		cache:   make(map[string]*model.StoredState),
		ring:    make([]string, maxSize),
		maxSize: maxSize,
	}
}

func (s *InProcessEphemeralStorage) PutState(ctx context.Context, new *model.StoredState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.cache[new.ID]; !exists {
		if s.size >= s.maxSize {
			// Evict the oldest entry at the current head (ring wraps around).
			// Skip slots that were already removed.
			oldest := s.ring[s.head]
			if oldest != "" {
				delete(s.cache, oldest)
			}
		} else {
			s.size++
		}
		s.ring[s.head] = new.ID
		s.head = (s.head + 1) % s.maxSize
	}
	s.cache[new.ID] = new
	return nil
}

func (s *InProcessEphemeralStorage) GetState(ctx context.Context, workflowID string) (*model.StoredState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state, ok := s.cache[workflowID]
	if !ok {
		return nil, nil
	}
	return state, nil
}

func (s *InProcessEphemeralStorage) RemoveState(ctx context.Context, workflowID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.cache, workflowID)
	// Mark the slot as empty in the ring; eviction will skip empty/stale entries.
	for i := 0; i < s.size; i++ {
		idx := (s.head - s.size + i + s.maxSize) % s.maxSize
		if s.ring[idx] == workflowID {
			s.ring[idx] = ""
			break
		}
	}
	return nil
}

type TieredEphemeralStorage struct {
	l1 *InProcessEphemeralStorage
	l2 EphemeralStorage
}

func NewTieredEphemeralStorage(l1 *InProcessEphemeralStorage, l2 EphemeralStorage) *TieredEphemeralStorage {
	return &TieredEphemeralStorage{l1: l1, l2: l2}
}

func (s *TieredEphemeralStorage) PutState(ctx context.Context, new *model.StoredState) error {
	if err := s.l1.PutState(ctx, new); err != nil {
		return err
	}
	return s.l2.PutState(ctx, new)
}

func (s *TieredEphemeralStorage) GetState(ctx context.Context, workflowID string) (*model.StoredState, error) {
	state, err := s.l1.GetState(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	if state != nil {
		return state, nil
	}
	state, err = s.l2.GetState(ctx, workflowID)
	if err != nil || state == nil {
		return state, err
	}
	_ = s.l1.PutState(ctx, state)
	return state, nil
}

func (s *TieredEphemeralStorage) RemoveState(ctx context.Context, workflowID string) error {
	_ = s.l1.RemoveState(ctx, workflowID)
	return s.l2.RemoveState(ctx, workflowID)
}
