package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

// Mock workflow for benchmarking
type BenchmarkState struct {
	Count     int64                `json:"count"`
	Data      string               `json:"data"`
	Fields    map[string]string    `json:"fields"`
	Lifecycle model.LifecycleState `json:"lifecycle"`
}

func (s *BenchmarkState) GetSubscriptions() []model.Sub                 { return nil }
func (s *BenchmarkState) GetExternalSubscriptions() []model.ExternalSub { return nil }
func (s *BenchmarkState) GetLifecycle() model.LifecycleState            { return s.Lifecycle }
func (s *BenchmarkState) GetSchedules() []model.Schedule                { return nil }
func (s *BenchmarkState) Copy() model.State {
	fields := make(map[string]string)
	for k, v := range s.Fields {
		fields[k] = v
	}
	return &BenchmarkState{
		Count:     s.Count,
		Data:      s.Data,
		Fields:    fields,
		Lifecycle: s.Lifecycle,
	}
}

type BenchmarkEvent struct {
	Type     string         `json:"type"`
	Amount   int64          `json:"amount"`
	Data     string         `json:"data"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

func (e *BenchmarkEvent) GetType() string              { return e.Type }
func (e *BenchmarkEvent) GetMetadata() map[string]any  { return e.Metadata }
func (e *BenchmarkEvent) SetMetadata(m map[string]any) { e.Metadata = m }

type BenchmarkCommand struct {
	Type   string `json:"type"`
	Amount int64  `json:"amount"`
}

type BenchmarkWorkflow struct {
	name string
}

func (w *BenchmarkWorkflow) Name() string       { return w.name }
func (w *BenchmarkWorkflow) SchemaVersion() int { return 1 }
func (w *BenchmarkWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	return rawData
}

func (w *BenchmarkWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	var s *BenchmarkState
	if state != nil {
		var ok bool
		s, ok = state.(*BenchmarkState)
		if !ok {
			return nil, &model.Rejection{Msg: "invalid state type"}
		}
	} else {
		s = &BenchmarkState{Lifecycle: model.LifecycleActive, Fields: make(map[string]string)}
	}

	if s.Lifecycle != model.LifecycleActive {
		return nil, &model.Rejection{Msg: "workflow not active"}
	}

	switch c := cmd.(type) {
	case *BenchmarkCommand:
		return []model.Event{&BenchmarkEvent{
			Type:   "benchmark_event",
			Amount: c.Amount,
			Data:   fmt.Sprintf("data-%d", c.Amount),
		}}, nil
	}

	return nil, nil
}

func (w *BenchmarkWorkflow) Evolve(state model.State, event model.Event) model.State {
	var s *BenchmarkState
	if state != nil {
		s = state.(*BenchmarkState).Copy().(*BenchmarkState)
	} else {
		s = &BenchmarkState{Lifecycle: model.LifecycleActive, Fields: make(map[string]string)}
	}

	switch e := event.(type) {
	case *BenchmarkEvent:
		s.Count += e.Amount
		s.Data = e.Data
	}

	return s
}

func (w *BenchmarkWorkflow) EventToCmd(e model.Event) model.Command {
	return nil
}

func (w *BenchmarkWorkflow) IsFinalEvent(e model.Event) bool {
	return false
}

// generateStateWithFields generates a state with the specified number of fields
func generateStateWithFields(numFields int) *BenchmarkState {
	fields := make(map[string]string)
	for i := 0; i < numFields; i++ {
		fields[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("value_%d", i)
	}
	return &BenchmarkState{
		Count:     0,
		Fields:    fields,
		Lifecycle: model.LifecycleActive,
	}
}

// generateEventWithData generates an event with specified data size (approximate)
func generateEventWithData(dataSize int) *BenchmarkEvent {
	data := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = byte('a' + (i % 26))
	}
	return &BenchmarkEvent{
		Type:   "benchmark_event",
		Amount: 1,
		Data:   string(data),
	}
}

// BenchmarkProcessCommand benchmarks command processing
// Varies: event body size and concurrent writers
func BenchmarkProcessCommand(b *testing.B) {
	ctx := context.Background()
	workflow := &BenchmarkWorkflow{name: "BenchmarkWorkflow"}
	storage := NewInProcessEphemeralStorage(10000)

	// Test different event body sizes
	sizes := []struct {
		name string
		size int
	}{
		{"100B", 100},
		{"1KB", 1024},
		{"10KB", 10 * 1024},
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("BodySize_%s", size.name), func(b *testing.B) {
			// Create in-memory repo for benchmarking
			repo := NewInMemoryRepo(workflow, storage)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := fmt.Sprintf("bench-%d", time.Now().UnixNano())
				for pb.Next() {
					_, _ = repo.ProcessCommand(ctx, id, &BenchmarkCommand{
						Type:   "increment",
						Amount: 1,
					})
				}
			})
		})
	}

	// Test concurrent writers
	concurrencyLevels := []int{1, 10, 100}
	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrent_%d", concurrency), func(b *testing.B) {
			repo := NewInMemoryRepo(workflow, storage)

			b.ResetTimer()
			var wg sync.WaitGroup
			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for j := 0; j < b.N/concurrency; j++ {
						id := fmt.Sprintf("bench-worker-%d-item-%d", workerID, j)
						_, _ = repo.ProcessCommand(ctx, id, &BenchmarkCommand{
							Type:   "increment",
							Amount: 1,
						})
					}
				}(i)
			}
			wg.Wait()
		})
	}
}

// BenchmarkStateEvolution benchmarks the Evolve() call
// Varies: state size (number of fields)
func BenchmarkStateEvolution(b *testing.B) {
	workflow := &BenchmarkWorkflow{}

	stateSizes := []struct {
		name   string
		fields int
	}{
		{"10Fields", 10},
		{"100Fields", 100},
		{"1000Fields", 1000},
	}

	for _, size := range stateSizes {
		b.Run(size.name, func(b *testing.B) {
			state := generateStateWithFields(size.fields)
			event := &BenchmarkEvent{
				Type:   "benchmark_event",
				Amount: 1,
				Data:   "benchmark",
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = workflow.Evolve(state, event)
			}
		})
	}

	// Test with nil state (initial state evolution)
	b.Run("NilState", func(b *testing.B) {
		event := &BenchmarkEvent{
			Type:   "benchmark_event",
			Amount: 1,
			Data:   "benchmark",
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = workflow.Evolve(nil, event)
		}
	})
}

// BenchmarkCacheGetPut benchmarks LRU cache operations
// Varies: cache size
func BenchmarkCacheGetPut(b *testing.B) {
	cacheSizes := []int{100, 1000, 10000}

	for _, cacheSize := range cacheSizes {
		b.Run(fmt.Sprintf("CacheSize_%d", cacheSize), func(b *testing.B) {
			cache := NewInProcessEphemeralStorage(cacheSize)
			ctx := context.Background()

			// Pre-populate cache
			for i := 0; i < cacheSize; i++ {
				id := fmt.Sprintf("item-%d", i)
				_ = cache.PutState(ctx, &model.StoredState{
					ID: id,
					State: &BenchmarkState{
						Count:     int64(i),
						Lifecycle: model.LifecycleActive,
					},
				})
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					id := fmt.Sprintf("item-%d", i%cacheSize)
					state, _ := cache.GetState(ctx, id)
					if state == nil {
						_ = cache.PutState(ctx, &model.StoredState{
							ID: id,
							State: &BenchmarkState{
								Count:     int64(i),
								Lifecycle: model.LifecycleActive,
							},
						})
					}
					i++
				}
			})
		})
	}

	// Benchmark cache misses
	b.Run("CacheMisses", func(b *testing.B) {
		cache := NewInProcessEphemeralStorage(1000)
		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := fmt.Sprintf("miss-item-%d", i)
			state, _ := cache.GetState(ctx, id)
			if state == nil {
				_ = cache.PutState(ctx, &model.StoredState{
					ID: id,
					State: &BenchmarkState{
						Count:     int64(i),
						Lifecycle: model.LifecycleActive,
					},
				})
			}
		}
	})
}

// BenchmarkEventSerialization benchmarks JSON marshal/unmarshal operations
// Compares: stdlib json (can be extended with json-iterator or sonic)
func BenchmarkEventSerialization(b *testing.B) {
	eventSizes := []struct {
		name string
		size int
	}{
		{"Small_100B", 100},
		{"Medium_1KB", 1024},
		{"Large_10KB", 10 * 1024},
	}

	for _, size := range eventSizes {
		event := generateEventWithData(size.size)

		// Benchmark stdlib JSON marshal
		b.Run(fmt.Sprintf("Stdlib_Marshal_%s", size.name), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = json.Marshal(event)
			}
		})

		// Benchmark stdlib JSON unmarshal
		b.Run(fmt.Sprintf("Stdlib_Unmarshal_%s", size.name), func(b *testing.B) {
			data, _ := json.Marshal(event)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var e BenchmarkEvent
				_ = json.Unmarshal(data, &e)
			}
		})

		// Benchmark round-trip
		b.Run(fmt.Sprintf("Stdlib_RoundTrip_%s", size.name), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, _ := json.Marshal(event)
				var e BenchmarkEvent
				_ = json.Unmarshal(data, &e)
			}
		})
	}
}

// InMemoryRepo is a simple in-memory repository for benchmarking
type InMemoryRepo struct {
	workflow model.Workflow
	storage  EphemeralStorage
	mu       sync.RWMutex
	states   map[string]*model.StoredState
}

func NewInMemoryRepo(workflow model.Workflow, storage EphemeralStorage) *InMemoryRepo {
	return &InMemoryRepo{
		workflow: workflow,
		storage:  storage,
		states:   make(map[string]*model.StoredState),
	}
}

func (r *InMemoryRepo) ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, exists := r.states[id]
	if !exists {
		state = &model.StoredState{
			ID:      id,
			Version: 0,
			State:   nil,
		}
	}

	events, _ := r.workflow.Decide(state.State, cmd)

	for _, event := range events {
		state.State = r.workflow.Evolve(state.State, event)
		state.Version++
	}

	r.states[id] = state
	return state, nil
}

func (r *InMemoryRepo) GetCurrentState(ctx context.Context, id string) (*model.StoredState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.states[id], nil
}

// BenchmarkWorkflowDecide benchmarks the Decide() method
func BenchmarkWorkflowDecide(b *testing.B) {
	workflow := &BenchmarkWorkflow{}

	scenarios := []struct {
		name  string
		state *BenchmarkState
	}{
		{"NilState", nil},
		{"SmallState", generateStateWithFields(10)},
		{"LargeState", generateStateWithFields(1000)},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			cmd := &BenchmarkCommand{Type: "increment", Amount: 1}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = workflow.Decide(scenario.state, cmd)
			}
		})
	}
}

// BenchmarkConcurrentStateAccess benchmarks concurrent access to state
func BenchmarkConcurrentStateAccess(b *testing.B) {
	storage := NewInProcessEphemeralStorage(1000)
	ctx := context.Background()

	// Pre-populate some items
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("concurrent-%d", i)
		_ = storage.PutState(ctx, &model.StoredState{
			ID:    id,
			State: generateStateWithFields(10),
		})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			id := fmt.Sprintf("concurrent-%d", i%100)
			state, _ := storage.GetState(ctx, id)
			if state == nil {
				_ = storage.PutState(ctx, &model.StoredState{
					ID:    id,
					State: generateStateWithFields(10),
				})
			}
			i++
		}
	})
}
