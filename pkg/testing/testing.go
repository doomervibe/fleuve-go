package testing

import (
	"fmt"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

type WorkflowTestHarness struct {
	workflow model.Workflow
	adapter  model.Adapter
	state    model.State
	version  int64
	events   []model.Event
}

func NewWorkflowTestHarness(workflow model.Workflow, adapter model.Adapter) *WorkflowTestHarness {
	return &WorkflowTestHarness{
		workflow: workflow,
		adapter:  adapter,
		version:  0,
		events:   make([]model.Event, 0),
	}
}

func (h *WorkflowTestHarness) CreateNew(cmd model.Command) ([]model.Event, *model.Rejection) {
	events, rejection := h.workflow.Decide(nil, cmd)
	if rejection != nil {
		return nil, rejection
	}

	h.state = h.workflow.Evolve(nil, events[0])
	for _, e := range events[1:] {
		h.state = h.workflow.Evolve(h.state, e)
	}
	h.events = append(h.events, events...)
	h.version = int64(len(events))

	return events, nil
}

func (h *WorkflowTestHarness) SendCommand(cmd model.Command) ([]model.Event, *model.Rejection) {
	events, rejection := h.workflow.Decide(h.state, cmd)
	if rejection != nil {
		return nil, rejection
	}

	for _, e := range events {
		h.state = h.workflow.Evolve(h.state, e)
	}
	h.events = append(h.events, events...)
	h.version += int64(len(events))

	return events, nil
}

func (h *WorkflowTestHarness) GetState() model.State {
	return h.state
}

func (h *WorkflowTestHarness) GetVersion() int64 {
	return h.version
}

func (h *WorkflowTestHarness) GetEvents() []model.Event {
	return h.events
}

func (h *WorkflowTestHarness) Simulate(cmd model.Command) ([]model.Event, model.State, *model.Rejection) {
	events, rejection := h.workflow.Decide(h.state, cmd)
	if rejection != nil {
		return nil, nil, rejection
	}

	simState := h.state
	for _, e := range events {
		simState = h.workflow.Evolve(simState, e)
	}

	return events, simState, nil
}

func (h *WorkflowTestHarness) AssertSubscriptions(expected []model.Sub) error {
	stateBase, ok := h.state.(interface{ GetSubscriptions() []model.Sub })
	if !ok {
		return fmt.Errorf("state does not expose subscriptions")
	}

	subs := stateBase.GetSubscriptions()
	if len(subs) != len(expected) {
		return fmt.Errorf("expected %d subscriptions, got %d", len(expected), len(subs))
	}

	for i, exp := range expected {
		if subs[i].WorkflowID != exp.WorkflowID || subs[i].EventType != exp.EventType {
			return fmt.Errorf("subscription %d mismatch: expected %+v, got %+v", i, exp, subs[i])
		}
	}

	return nil
}

func (h *WorkflowTestHarness) AssertLifecycle(expected model.LifecycleState) error {
	stateBase, ok := h.state.(interface{ GetLifecycle() model.LifecycleState })
	if !ok {
		return fmt.Errorf("state does not expose lifecycle")
	}

	actual := stateBase.GetLifecycle()
	if actual != expected {
		return fmt.Errorf("expected lifecycle %s, got %s", expected, actual)
	}

	return nil
}

func (h *WorkflowTestHarness) AssertSchedules(expected int) error {
	stateBase, ok := h.state.(interface{ GetSchedules() []model.Schedule })
	if !ok {
		return fmt.Errorf("state does not expose schedules")
	}

	schedules := stateBase.GetSchedules()
	if len(schedules) != expected {
		return fmt.Errorf("expected %d schedules, got %d", expected, len(schedules))
	}

	return nil
}

func (h *WorkflowTestHarness) Reset() {
	h.state = nil
	h.version = 0
	h.events = make([]model.Event, 0)
}

func (h *WorkflowTestHarness) ReplayFrom(events []model.Event) model.State {
	var state model.State
	for _, e := range events {
		state = h.workflow.Evolve(state, e)
	}
	return state
}

func (h *WorkflowTestHarness) IsCompleted() bool {
	if len(h.events) == 0 {
		return false
	}
	return h.workflow.IsFinalEvent(h.events[len(h.events)-1])
}

type MockAdapter struct {
	ToBeActOnFn func(event *model.ConsumedEvent) bool
	ActOnFn     func(ctx interface{}, event *model.ConsumedEvent, context *model.ActionContext) (<-chan model.ActionYield, error)
	SyncDBFn    func(ctx interface{}, workflowID string, oldState, newState model.State, events []model.Event) error
}

func (a *MockAdapter) ToBeActOn(event *model.ConsumedEvent) bool {
	if a.ToBeActOnFn != nil {
		return a.ToBeActOnFn(event)
	}
	return false
}

func (a *MockAdapter) ActOn(ctx interface{}, event *model.ConsumedEvent, context *model.ActionContext) (<-chan model.ActionYield, error) {
	if a.ActOnFn != nil {
		return a.ActOnFn(ctx, event, context)
	}
	ch := make(chan model.ActionYield)
	close(ch)
	return ch, nil
}

func (a *MockAdapter) SyncDB(ctx interface{}, workflowID string, oldState, newState model.State, events []model.Event) error {
	if a.SyncDBFn != nil {
		return a.SyncDBFn(ctx, workflowID, oldState, newState, events)
	}
	return nil
}
