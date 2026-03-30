package repo

import (
	"context"
	"sync"

	"github.com/fleuve/fleuve-go/pkg/model"
)

type EphemeralStorage interface {
	PutState(ctx context.Context, new *model.StoredState) error
	GetState(ctx context.Context, workflowID string) (*model.StoredState, error)
	RemoveState(ctx context.Context, workflowID string) error
}

type InProcessEphemeralStorage struct {
	mu      sync.RWMutex
	cache   map[string]*model.StoredState
	keys    []string
	maxSize int
}

func NewInProcessEphemeralStorage(maxSize int) *InProcessEphemeralStorage {
	return &InProcessEphemeralStorage{
		cache:   make(map[string]*model.StoredState),
		keys:    make([]string, 0),
		maxSize: maxSize,
	}
}

func (s *InProcessEphemeralStorage) PutState(ctx context.Context, new *model.StoredState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.cache[new.ID]; !exists {
		if len(s.keys) >= s.maxSize {
			oldest := s.keys[0]
			delete(s.cache, oldest)
			s.keys = s.keys[1:]
		}
		s.keys = append(s.keys, new.ID)
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
	for i, k := range s.keys {
		if k == workflowID {
			s.keys = append(s.keys[:i], s.keys[i+1:]...)
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
