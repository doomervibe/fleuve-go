// Package counterworkflow provides a minimal built-in workflow used by fleuve-runner and fleuve-gateway.
package counterworkflow

import (
	"encoding/json"

	"github.com/fleuve/fleuve-go/pkg/model"
)

// CounterState holds the workflow state.
type CounterState struct {
	Count     int64                `json:"count"`
	Lifecycle model.LifecycleState `json:"lifecycle"`
}

func (s *CounterState) GetSubscriptions() []model.Sub                 { return nil }
func (s *CounterState) GetExternalSubscriptions() []model.ExternalSub { return nil }
func (s *CounterState) GetLifecycle() model.LifecycleState            { return s.Lifecycle }
func (s *CounterState) GetSchedules() []model.Schedule                { return nil }
func (s *CounterState) Copy() model.State {
	return &CounterState{
		Count:     s.Count,
		Lifecycle: s.Lifecycle,
	}
}

// CounterIncremented is emitted when the counter increases.
type CounterIncremented struct {
	Amount int64 `json:"amount"`
}

func (e *CounterIncremented) GetType() string              { return "counter_incremented" }
func (e *CounterIncremented) GetMetadata() map[string]any  { return nil }
func (e *CounterIncremented) SetMetadata(m map[string]any) {}

// CounterReset clears the counter.
type CounterReset struct{}

func (e *CounterReset) GetType() string              { return "counter_reset" }
func (e *CounterReset) GetMetadata() map[string]any  { return nil }
func (e *CounterReset) SetMetadata(m map[string]any) {}

// IncrementCmd adds to the counter.
type IncrementCmd struct {
	Amount int64 `json:"amount"`
}

// ResetCmd resets the counter to zero.
type ResetCmd struct{}

// Workflow implements the counter aggregate.
type Workflow struct{}

// New returns a CounterWorkflow instance (name "CounterWorkflow").
func New() *Workflow { return &Workflow{} }

func (w *Workflow) Name() string       { return "CounterWorkflow" }
func (w *Workflow) SchemaVersion() int { return 1 }

func (w *Workflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	return rawData
}

func (w *Workflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	var s *CounterState
	if state != nil {
		s = state.(*CounterState)
	} else {
		s = &CounterState{Lifecycle: model.LifecycleActive}
	}

	if s.Lifecycle != model.LifecycleActive {
		return nil, &model.Rejection{Msg: "workflow not active"}
	}

	switch c := cmd.(type) {
	case *IncrementCmd:
		if c.Amount <= 0 {
			return nil, &model.Rejection{Msg: "amount must be positive"}
		}
		return []model.Event{&CounterIncremented{Amount: c.Amount}}, nil

	case *ResetCmd:
		return []model.Event{&CounterReset{}}, nil
	}

	return nil, nil
}

func (w *Workflow) Evolve(state model.State, event model.Event) model.State {
	var s *CounterState
	if state != nil {
		s = state.(*CounterState).Copy().(*CounterState)
	} else {
		s = &CounterState{Lifecycle: model.LifecycleActive}
	}

	switch e := event.(type) {
	case *CounterIncremented:
		s.Count += e.Amount
	case *CounterReset:
		s.Count = 0
	}

	return s
}

func (w *Workflow) EventToCmd(e model.Event) model.Command { return nil }

func (w *Workflow) IsFinalEvent(e model.Event) bool { return false }

func (w *Workflow) DecodeEvent(eventType string, schemaVersion int, raw map[string]any) (model.Event, error) {
	switch eventType {
	case "counter_incremented":
		b, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		var e CounterIncremented
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "counter_reset":
		return &CounterReset{}, nil
	default:
		return model.DecodeBuiltinReplayEvent(eventType, raw)
	}
}
