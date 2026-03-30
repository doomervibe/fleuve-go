package runner

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fleuve/fleuve-go/pkg/model"
)

// BenchmarkEventThroughput benchmarks end-to-end event processing throughput
// Varies: batch size and max_inflight
// Compares: PostgreSQL reader vs NATS reader
func BenchmarkEventThroughput(b *testing.B) {
	// Test different batch sizes
	batchSizes := []int{10, 50, 100, 500}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			// Mock reader for benchmarking
			reader := &MockBenchmarkReader{
				events: generateBenchmarkEvents(batchSize),
			}

			runner := &BenchmarkRunner{
				reader:      reader,
				batchSize:   batchSize,
				maxInflight: 100,
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.Background()
				for pb.Next() {
					_ = runner.ProcessBatch(ctx)
				}
			})
		})
	}

	// Test different max_inflight settings
	maxInflightSettings := []int{10, 50, 100, 500, 1000}

	for _, maxInflight := range maxInflightSettings {
		b.Run(fmt.Sprintf("MaxInflight_%d", maxInflight), func(b *testing.B) {
			reader := &MockBenchmarkReader{
				events: generateBenchmarkEvents(100),
			}

			runner := &BenchmarkRunner{
				reader:      reader,
				batchSize:   10,
				maxInflight: maxInflight,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = runner.ProcessBatch(context.Background())
			}
		})
	}

	// Compare reader types
	b.Run("PostgreSQL_Reader", func(b *testing.B) {
		reader := &MockBenchmarkReader{
			events:     generateBenchmarkEvents(100),
			readerType: "postgres",
			latency:    1 * time.Millisecond, // Simulate DB latency
		}

		runner := &BenchmarkRunner{
			reader:      reader,
			batchSize:   10,
			maxInflight: 100,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = runner.ProcessBatch(context.Background())
		}
	})

	b.Run("NATS_Reader", func(b *testing.B) {
		reader := &MockBenchmarkReader{
			events:     generateBenchmarkEvents(100),
			readerType: "nats",
			latency:    100 * time.Microsecond, // Simulate NATS latency
		}

		runner := &BenchmarkRunner{
			reader:      reader,
			batchSize:   10,
			maxInflight: 100,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = runner.ProcessBatch(context.Background())
		}
	})
}

// BenchmarkConcurrentWorkflows benchmarks parallel workflow processing
// Measures: throughput with 10, 100, 1000 concurrent workflows
func BenchmarkConcurrentWorkflows(b *testing.B) {
	concurrencyLevels := []int{10, 100, 1000}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrent_%d", concurrency), func(b *testing.B) {
			runner := &BenchmarkRunner{
				reader:      &MockBenchmarkReader{events: generateBenchmarkEvents(concurrency)},
				batchSize:   concurrency / 10,
				maxInflight: concurrency,
			}

			b.ResetTimer()
			var wg sync.WaitGroup
			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func(workflowID int) {
					defer wg.Done()
					for j := 0; j < b.N/concurrency; j++ {
						_ = runner.ProcessWorkflow(context.Background(), fmt.Sprintf("workflow-%d", workflowID))
					}
				}(i)
			}
			wg.Wait()
		})
	}

	// Test with high contention
	b.Run("HighContention_100", func(b *testing.B) {
		runner := &BenchmarkRunner{
			reader:      &MockBenchmarkReader{events: generateBenchmarkEvents(100)},
			batchSize:   10,
			maxInflight: 10, // Low max_inflight creates contention
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			ctx := context.Background()
			for pb.Next() {
				_ = runner.ProcessBatch(ctx)
			}
		})
	})
}

// BenchmarkInflightTracker benchmarks the inflight event tracker
func BenchmarkInflightTracker(b *testing.B) {
	tracker := NewMockInflightTracker()

	b.Run("Add", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			eventID := fmt.Sprintf("event-%d", i)
			tracker.Add(eventID, fmt.Sprintf("workflow-%d", i%100))
		}
	})

	b.Run("Remove", func(b *testing.B) {
		// Pre-populate
		for i := 0; i < 10000; i++ {
			eventID := fmt.Sprintf("event-%d", i)
			tracker.Add(eventID, fmt.Sprintf("workflow-%d", i%100))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			eventID := fmt.Sprintf("event-%d", i%10000)
			tracker.Remove(eventID)
		}
	})

	b.Run("Get", func(b *testing.B) {
		// Pre-populate
		for i := 0; i < 10000; i++ {
			eventID := fmt.Sprintf("event-%d", i)
			tracker.Add(eventID, fmt.Sprintf("workflow-%d", i%100))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			eventID := fmt.Sprintf("event-%d", i%10000)
			_, _ = tracker.Get(eventID)
		}
	})

	b.Run("Concurrent_AddRemove", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				eventID := fmt.Sprintf("event-%d", i)
				if i%2 == 0 {
					tracker.Add(eventID, fmt.Sprintf("workflow-%d", i%100))
				} else {
					tracker.Remove(fmt.Sprintf("event-%d", i-1))
				}
				i++
			}
		})
	})
}

// BenchmarkEventProcessing benchmarks individual event processing
func BenchmarkEventProcessing(b *testing.B) {
	processor := &MockEventProcessor{}
	events := generateBenchmarkEvents(1000)

	b.Run("Sequential", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			event := events[i%len(events)]
			_ = processor.Process(context.Background(), event)
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				event := events[i%len(events)]
				_ = processor.Process(context.Background(), event)
				i++
			}
		})
	})
}

// BenchmarkWorkflowDecideWithEvolve benchmarks the full decide+evolve cycle
func BenchmarkWorkflowDecideWithEvolve(b *testing.B) {
	workflow := &MockBenchmarkWorkflow{}
	processor := &MockEventProcessor{workflow: workflow}

	states := []struct {
		name  string
		state model.State
	}{
		{"EmptyState", nil},
		{"SmallState", generateMockState(10)},
		{"LargeState", generateMockState(1000)},
	}

	for _, scenario := range states {
		b.Run(scenario.name, func(b *testing.B) {
			cmd := &MockCommand{Type: "increment", Amount: 1}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				events, _ := workflow.Decide(scenario.state, cmd)
				for _, event := range events {
					_ = processor.ProcessEvent(context.Background(), event)
				}
			}
		})
	}
}

// Helper types and functions

type MockBenchmarkReader struct {
	events     []*MockEventEntry
	readerType string
	latency    time.Duration
	offset     int
	mu         sync.Mutex
}

func (r *MockBenchmarkReader) Read(ctx context.Context, limit int) ([]*MockEventEntry, error) {
	if r.latency > 0 {
		time.Sleep(r.latency)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.offset >= len(r.events) {
		return nil, nil
	}

	end := r.offset + limit
	if end > len(r.events) {
		end = len(r.events)
	}

	batch := r.events[r.offset:end]
	r.offset = end

	return batch, nil
}

func (r *MockBenchmarkReader) Commit(ctx context.Context, offset int64) error {
	return nil
}

func (r *MockBenchmarkReader) Close() error {
	return nil
}

type BenchmarkRunner struct {
	reader      *MockBenchmarkReader
	batchSize   int
	maxInflight int
	tracker     *InflightTracker
	processor   *MockEventProcessor
	mu          sync.Mutex
}

func (r *BenchmarkRunner) ProcessBatch(ctx context.Context) error {
	events, err := r.reader.Read(ctx, r.batchSize)
	if err != nil {
		return err
	}

	if r.processor == nil {
		r.processor = &MockEventProcessor{}
	}

	var wg sync.WaitGroup
	for _, event := range events {
		wg.Add(1)
		go func(e *MockEventEntry) {
			defer wg.Done()
			_ = r.processor.Process(ctx, e)
		}(event)
	}
	wg.Wait()

	return nil
}

func (r *BenchmarkRunner) ProcessWorkflow(ctx context.Context, workflowID string) error {
	if r.processor == nil {
		r.processor = &MockEventProcessor{}
	}

	event := &MockEventEntry{
		WorkflowID: workflowID,
		EventType:  "benchmark_event",
	}

	return r.processor.Process(ctx, event)
}

type MockEventProcessor struct {
	workflow *MockBenchmarkWorkflow
}

func (p *MockEventProcessor) Process(ctx context.Context, event *MockEventEntry) error {
	// Simulate minimal processing
	return nil
}

func (p *MockEventProcessor) ProcessEvent(ctx context.Context, event model.Event) error {
	// Simulate event evolution
	return nil
}

// MockInflightTracker is a simplified tracker for benchmarking
type MockInflightTracker struct {
	events map[string]string
	mu     sync.RWMutex
}

func NewMockInflightTracker() *MockInflightTracker {
	return &MockInflightTracker{
		events: make(map[string]string),
	}
}

func (t *MockInflightTracker) Add(eventID, workflowID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events[eventID] = workflowID
}

func (t *MockInflightTracker) Remove(eventID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.events, eventID)
}

func (t *MockInflightTracker) Get(eventID string) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	workflowID, ok := t.events[eventID]
	return workflowID, ok
}

type MockBenchmarkWorkflow struct{}

func (w *MockBenchmarkWorkflow) Name() string       { return "BenchmarkWorkflow" }
func (w *MockBenchmarkWorkflow) SchemaVersion() int { return 1 }
func (w *MockBenchmarkWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	return rawData
}

func (w *MockBenchmarkWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return []model.Event{&MockEvent{Type: "benchmark_event"}}, nil
}

func (w *MockBenchmarkWorkflow) Evolve(state model.State, event model.Event) model.State {
	return &MockState{Count: 1}
}

func (w *MockBenchmarkWorkflow) EventToCmd(e model.Event) model.Command {
	return nil
}

func (w *MockBenchmarkWorkflow) IsFinalEvent(e model.Event) bool {
	return false
}

type MockEvent struct {
	Type string
}

func (e *MockEvent) GetType() string              { return e.Type }
func (e *MockEvent) GetMetadata() map[string]any  { return nil }
func (e *MockEvent) SetMetadata(m map[string]any) {}

type MockCommand struct {
	Type   string
	Amount int64
}

type MockState struct {
	Count int64
}

func (s *MockState) GetSubscriptions() []model.Sub                 { return nil }
func (s *MockState) GetExternalSubscriptions() []model.ExternalSub { return nil }
func (s *MockState) GetLifecycle() model.LifecycleState            { return model.LifecycleActive }
func (s *MockState) GetSchedules() []model.Schedule                { return nil }
func (s *MockState) Copy() model.State {
	return &MockState{Count: s.Count}
}

// MockEventEntry represents a mock event for benchmarking
type MockEventEntry struct {
	ID         string
	WorkflowID string
	EventType  string
	Version    int64
}

func generateBenchmarkEvents(count int) []*MockEventEntry {
	events := make([]*MockEventEntry, count)
	for i := 0; i < count; i++ {
		events[i] = &MockEventEntry{
			ID:         fmt.Sprintf("event-%d", i),
			WorkflowID: fmt.Sprintf("workflow-%d", i%100),
			EventType:  "benchmark_event",
			Version:    int64(i),
		}
	}
	return events
}

func generateMockState(fields int) model.State {
	return &MockState{Count: int64(fields)}
}
