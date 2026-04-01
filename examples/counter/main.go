// Package main demonstrates a simple counter workflow using the fleuve-go framework.
//
// This example shows:
//   - Defining workflow state that embeds model.StateBase
//   - Creating domain events and commands
//   - Implementing the model.Workflow interface
//   - Running in-memory tests with WorkflowTestHarness
package main

import (
	"fmt"
	"log"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/testing"
)

// =============================================================================
// State
// =============================================================================

// CounterState holds the state for our counter workflow.
// It embeds model.StateBase which provides lifecycle management, subscriptions,
// and schedules support.
type CounterState struct {
	model.StateBase
	Count int `json:"count"`
}

// Copy returns a deep copy of the CounterState.
// This is required by the model.State interface to ensure immutable state transitions.
func (s *CounterState) Copy() model.State {
	if s == nil {
		return &CounterState{}
	}
	return &CounterState{
		StateBase: *s.StateBase.Copy().(*model.StateBase),
		Count:     s.Count,
	}
}

// =============================================================================
// Events
// =============================================================================

// CounterIncremented is emitted when the counter is incremented.
type CounterIncremented struct {
	model.EventBase
	Amount int `json:"amount"`
}

func (e *CounterIncremented) Type() string { return "counter_incremented" }

// CounterDecremented is emitted when the counter is decremented.
type CounterDecremented struct {
	model.EventBase
	Amount int `json:"amount"`
}

func (e *CounterDecremented) Type() string { return "counter_decremented" }

// CounterReset is emitted when the counter is reset to zero.
type CounterReset struct {
	model.EventBase
}

func (e *CounterReset) Type() string { return "counter_reset" }

// =============================================================================
// Commands
// =============================================================================

// IncrementCommand requests the counter be incremented by the given amount.
type IncrementCommand struct {
	Amount int `json:"amount"`
}

func (c *IncrementCommand) CommandType() string { return "increment" }

// DecrementCommand requests the counter be decremented by the given amount.
type DecrementCommand struct {
	Amount int `json:"amount"`
}

func (c *DecrementCommand) CommandType() string { return "decrement" }

// ResetCommand requests the counter be reset to zero.
type ResetCommand struct{}

func (c *ResetCommand) CommandType() string { return "reset" }

// =============================================================================
// Workflow Implementation
// =============================================================================

// CounterWorkflow implements model.Workflow for a simple counter.
type CounterWorkflow struct{}

// Name returns a unique identifier for this workflow type.
func (w *CounterWorkflow) Name() string {
	return "counter_workflow"
}

// SchemaVersion returns the current schema version.
// Increment this when evolving event schemas and implement Upcast to handle migrations.
func (w *CounterWorkflow) SchemaVersion() int {
	return 1
}

// Upcast transforms old event data to the current schema format.
// Called during state loading when the stored schema_version < current version.
func (w *CounterWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	// No schema migrations needed for version 1
	return rawData
}

// Decide is a pure function that maps commands to events.
// Given the current state and a command, it returns the events to persist
// or a rejection if the command is invalid.
func (w *CounterWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	switch c := cmd.(type) {
	case *IncrementCommand:
		if c.Amount <= 0 {
			return nil, &model.Rejection{Msg: "increment amount must be positive"}
		}
		return []model.Event{&CounterIncremented{Amount: c.Amount}}, nil

	case *DecrementCommand:
		if c.Amount <= 0 {
			return nil, &model.Rejection{Msg: "decrement amount must be positive"}
		}
		// Get current count, defaulting to 0 for new workflows
		currentCount := 0
		if state != nil {
			if cs, ok := state.(*CounterState); ok {
				currentCount = cs.Count
			}
		}
		if currentCount-c.Amount < 0 {
			return nil, &model.Rejection{Msg: "counter cannot go below zero"}
		}
		return []model.Event{&CounterDecremented{Amount: c.Amount}}, nil

	case *ResetCommand:
		return []model.Event{&CounterReset{}}, nil

	default:
		return nil, &model.Rejection{Msg: fmt.Sprintf("unknown command type: %T", cmd)}
	}
}

// Evolve applies a single event to produce new state.
// This is a pure state transition function - no side effects.
func (w *CounterWorkflow) Evolve(state model.State, event model.Event) model.State {
	// Initialize state if nil (first event for new workflow)
	var cs *CounterState
	if state == nil {
		cs = &CounterState{StateBase: *model.NewStateBase()}
	} else {
		cs = state.(*CounterState).Copy().(*CounterState)
	}

	switch e := event.(type) {
	case *CounterIncremented:
		cs.Count += e.Amount
	case *CounterDecremented:
		cs.Count -= e.Amount
	case *CounterReset:
		cs.Count = 0
	}

	return cs
}

// EventToCmd maps an external event to a command.
// Returns nil to ignore the event.
// This counter workflow doesn't react to external events.
func (w *CounterWorkflow) EventToCmd(e model.Event) model.Command {
	return nil
}

// IsFinalEvent returns true if the event represents workflow completion.
// The counter workflow never "completes" - it runs indefinitely.
func (w *CounterWorkflow) IsFinalEvent(e model.Event) bool {
	return false
}

// =============================================================================
// Main - Test Runner
// =============================================================================

func main() {
	workflow := &CounterWorkflow{}
	harness := testing.NewWorkflowTestHarness(workflow)

	fmt.Println("=== Counter Workflow Example ===")
	fmt.Println()

	// Test 1: Create a new counter with initial increment
	fmt.Println("Test 1: Create counter and increment by 5")
	state, events, err := harness.CreateNew("counter-1", &IncrementCommand{Amount: 5}, nil)
	if err != nil {
		log.Fatalf("Failed to create workflow: %v", err)
	}
	printResult(state, events)
	fmt.Println()

	// Test 2: Increment again
	fmt.Println("Test 2: Increment by 3")
	state, events, err = harness.SendCommand("counter-1", &IncrementCommand{Amount: 3})
	if err != nil {
		log.Fatalf("Failed to send command: %v", err)
	}
	printResult(state, events)
	fmt.Println()

	// Test 3: Decrement
	fmt.Println("Test 3: Decrement by 2")
	state, events, err = harness.SendCommand("counter-1", &DecrementCommand{Amount: 2})
	if err != nil {
		log.Fatalf("Failed to send command: %v", err)
	}
	printResult(state, events)
	fmt.Println()

	// Test 4: Try to decrement below zero (should be rejected)
	fmt.Println("Test 4: Try to decrement by 10 (should fail)")
	state, events, err = harness.SendCommand("counter-1", &DecrementCommand{Amount: 10})
	if err != nil {
		fmt.Printf("  Rejection: %s\n", err.Error())
	} else {
		printResult(state, events)
	}
	fmt.Println()

	// Test 5: Reset counter
	fmt.Println("Test 5: Reset counter")
	state, events, err = harness.SendCommand("counter-1", &ResetCommand{})
	if err != nil {
		log.Fatalf("Failed to send command: %v", err)
	}
	printResult(state, events)
	fmt.Println()

	// Test 6: Invalid increment (zero amount)
	fmt.Println("Test 6: Try to increment by 0 (should fail)")
	_, _, err = harness.SendCommand("counter-1", &IncrementCommand{Amount: 0})
	if err != nil {
		fmt.Printf("  Rejection: %s\n", err.Error())
	}
	fmt.Println()

	// Test 7: Simulate without mutating state
	fmt.Println("Test 7: Simulate increment by 100 (no state change)")
	simEvents, rejection := harness.Simulate("counter-1", &IncrementCommand{Amount: 100})
	fmt.Printf("  Would produce %d event(s), rejection: %v\n", len(simEvents), rejection)
	currentState := harness.GetState("counter-1")
	fmt.Printf("  Current count (unchanged): %d\n", currentState.(*CounterState).Count)
	fmt.Println()

	fmt.Println("=== All tests passed! ===")
}

// printResult prints the current state and produced events in a readable format.
func printResult(state model.State, events []model.Event) {
	if state == nil {
		fmt.Println("  State: nil")
	} else {
		fmt.Printf("  State: count=%d\n", state.(*CounterState).Count)
	}
	fmt.Printf("  Events (%d):\n", len(events))
	for _, ev := range events {
		switch e := ev.(type) {
		case *CounterIncremented:
			fmt.Printf("    - %s{Amount: %d}\n", e.Type(), e.Amount)
		case *CounterDecremented:
			fmt.Printf("    - %s{Amount: %d}\n", e.Type(), e.Amount)
		case *CounterReset:
			fmt.Printf("    - %s{}\n", e.Type())
		default:
			fmt.Printf("    - %s\n", e.Type())
		}
	}
}
