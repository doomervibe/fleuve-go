package repo

import (
	"container/list"
	"context"
	"sync"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

// EphemeralStorage is the interface for workflow state caching.
// Implementations provide fast access to workflow state without hitting the database.
type EphemeralStorage interface {
	// PutState stores a workflow's state in the cache.
	PutState(ctx context.Context, state *model.StoredState) error
	// GetState retrieves a workflow's state from the cache.
	// Returns nil if the workflow is not in the cache.
	GetState(ctx context.Context, workflowID string) (*model.StoredState, error)
	// RemoveState removes a workflow's state from the cache.
	RemoveState(ctx context.Context, workflowID string) error
}

// =============================================================================
// InProcessEuphemeralStorage (L1 Cache)
// =============================================================================

// InProcessEuphemeralStorage is an in-memory LRU cache for workflow state.
// Key characteristics:
//   - LRU eviction when size exceeds max_size
//   - Zero serialization overhead - stores Go objects directly
//   - Thread-safe via mutex
//   - Best for: Partitioned runners where each worker handles a fixed subset of workflow IDs
type InProcessEuphemeralStorage struct {
	mu      sync.RWMutex
	items   map[string]*list.Element
	order   *list.List // Front = most recently used, Back = least recently used
	maxSize int
	hits    uint64
	misses  uint64
}

type lruEntry struct {
	key   string
	value *model.StoredState
}

// NewInProcessEuphemeralStorage creates a new LRU cache with the given maximum size.
// If maxSize is 0 or negative, defaults to 1000.
func NewInProcessEuphemeralStorage(maxSize int) *InProcessEuphemeralStorage {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &InProcessEuphemeralStorage{
		items:   make(map[string]*list.Element),
		order:   list.New(),
		maxSize: maxSize,
	}
}

// PutState stores a workflow's state in the cache.
// If the cache is full, evicts the least recently used item.
func (s *InProcessEuphemeralStorage) PutState(ctx context.Context, state *model.StoredState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, exists := s.items[state.ID]; exists {
		// Update existing entry and move to front
		s.order.MoveToFront(elem)
		elem.Value.(*lruEntry).value = state
		return nil
	}

	// Evict LRU if at capacity
	if s.order.Len() >= s.maxSize {
		s.evictLRU()
	}

	// Insert new entry at front
	entry := &lruEntry{key: state.ID, value: state}
	elem := s.order.PushFront(entry)
	s.items[state.ID] = elem
	return nil
}

// GetState retrieves a workflow's state from the cache.
// Returns nil if not found. Moves accessed item to front (MRU).
func (s *InProcessEuphemeralStorage) GetState(ctx context.Context, workflowID string) (*model.StoredState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, exists := s.items[workflowID]; exists {
		s.order.MoveToFront(elem)
		s.hits++
		return elem.Value.(*lruEntry).value, nil
	}

	s.misses++
	return nil, nil
}

// RemoveState removes a workflow's state from the cache.
func (s *InProcessEuphemeralStorage) RemoveState(ctx context.Context, workflowID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, exists := s.items[workflowID]; exists {
		s.order.Remove(elem)
		delete(s.items, workflowID)
	}
	return nil
}

// evictLRU removes the least recently used item from the cache.
// Must be called with lock held.
func (s *InProcessEuphemeralStorage) evictLRU() {
	if s.order.Len() == 0 {
		return
	}
	// Back of list is LRU
	elem := s.order.Back()
	if elem != nil {
		entry := s.order.Remove(elem).(*lruEntry)
		delete(s.items, entry.key)
	}
}

// Size returns the current number of items in the cache.
func (s *InProcessEuphemeralStorage) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.order.Len()
}

// Stats returns cache hit/miss statistics.
func (s *InProcessEuphemeralStorage) Stats() (hits, misses uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hits, s.misses
}

// Clear removes all items from the cache.
func (s *InProcessEuphemeralStorage) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[string]*list.Element)
	s.order.Init()
}

// =============================================================================
// TieredEuphemeralStorage (L1 + L2)
// =============================================================================

// TieredEuphemeralStorage combines L1 (in-memory) and L2 (external) caches.
// Key characteristics:
//   - GetState: Try L1 first (zero cost), then L2 (network). On L2 hit, promote to L1.
//   - PutState: Write to BOTH tiers simultaneously
//   - RemoveState: Remove from BOTH tiers
type TieredEuphemeralStorage struct {
	l1 EphemeralStorage
	l2 EphemeralStorage
}

// NewTieredEuphemeralStorage creates a new tiered cache with L1 and L2.
func NewTieredEuphemeralStorage(l1, l2 EphemeralStorage) *TieredEuphemeralStorage {
	return &TieredEuphemeralStorage{
		l1: l1,
		l2: l2,
	}
}

// PutState stores a workflow's state in BOTH L1 and L2.
func (t *TieredEuphemeralStorage) PutState(ctx context.Context, state *model.StoredState) error {
	// Write to both, but don't fail L1 if L2 fails (L1 is local and more reliable)
	l1Err := t.l1.PutState(ctx, state)
	l2Err := t.l2.PutState(ctx, state)

	if l2Err != nil {
		return l2Err
	}
	return l1Err
}

// GetState retrieves a workflow's state, trying L1 first then L2.
// On L2 hit, the state is promoted to L1.
func (t *TieredEuphemeralStorage) GetState(ctx context.Context, workflowID string) (*model.StoredState, error) {
	// Try L1 first (zero network cost)
	state, err := t.l1.GetState(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	if state != nil {
		return state, nil
	}

	// Try L2 (external cache)
	state, err = t.l2.GetState(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, nil
	}

	// Promote to L1 on hit (best-effort, don't fail if L1 has issues)
	_ = t.l1.PutState(ctx, state)

	return state, nil
}

// RemoveState removes a workflow's state from BOTH L1 and L2.
func (t *TieredEuphemeralStorage) RemoveState(ctx context.Context, workflowID string) error {
	l1Err := t.l1.RemoveState(ctx, workflowID)
	l2Err := t.l2.RemoveState(ctx, workflowID)

	// Return first non-nil error
	if l1Err != nil {
		return l1Err
	}
	return l2Err
}

// L1 returns the L1 cache for direct access (e.g., for stats).
func (t *TieredEuphemeralStorage) L1() EphemeralStorage {
	return t.l1
}

// L2 returns the L2 cache for direct access.
func (t *TieredEuphemeralStorage) L2() EphemeralStorage {
	return t.l2
}

// =============================================================================
// NoopEuphemeralStorage
// =============================================================================

// NoopEuphemeralStorage is a cache that does nothing.
// Useful for testing or when caching is disabled.
type NoopEuphemeralStorage struct{}

// NewNoopEuphemeralStorage creates a new no-op cache.
func NewNoopEuphemeralStorage() *NoopEuphemeralStorage {
	return &NoopEuphemeralStorage{}
}

func (n *NoopEuphemeralStorage) PutState(ctx context.Context, state *model.StoredState) error {
	return nil
}

func (n *NoopEuphemeralStorage) GetState(ctx context.Context, workflowID string) (*model.StoredState, error) {
	return nil, nil
}

func (n *NoopEuphemeralStorage) RemoveState(ctx context.Context, workflowID string) error {
	return nil
}
