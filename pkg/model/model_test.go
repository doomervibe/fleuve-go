package model

import (
	"encoding/json"
	"testing"
	"time"
)

type testState struct {
	StateBase
	Counter int `json:"counter"`
}

func (s *testState) GetSubscriptions() []Sub                 { return s.Subscriptions }
func (s *testState) GetExternalSubscriptions() []ExternalSub { return s.ExternalSubscriptions }
func (s *testState) GetLifecycle() LifecycleState            { return s.Lifecycle }
func (s *testState) GetSchedules() []Schedule                { return s.Schedules }
func (s *testState) Copy() State {
	c := *s
	c.StateBase = *s.StateBase.Copy()
	return &c
}

type incCommand struct {
	N int `json:"n"`
}

type incEvent struct {
	EventBase
	Type   string `json:"type"`
	Amount int    `json:"amount"`
}

func (e *incEvent) GetType() string { return e.Type }

type testWorkflow struct{}

func (w *testWorkflow) Name() string { return "test_workflow" }

func (w *testWorkflow) SchemaVersion() int { return 1 }

func (w *testWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	return rawData
}

func (w *testWorkflow) Decide(state State, cmd Command) ([]Event, *Rejection) {
	c, ok := cmd.(*incCommand)
	if !ok {
		return nil, nil
	}
	if c.N < 0 {
		return nil, &Rejection{Msg: "negative increment"}
	}
	if c.N == 0 {
		return nil, nil
	}
	return []Event{&incEvent{Type: "inc", Amount: c.N}}, nil
}

func (w *testWorkflow) Evolve(state State, event Event) State {
	var s *testState
	if state != nil {
		s = state.(*testState).Copy().(*testState)
	} else {
		s = &testState{StateBase: *NewStateBase()}
	}
	switch e := event.(type) {
	case *incEvent:
		s.Counter += e.Amount
	}
	return s
}

func (w *testWorkflow) EventToCmd(e Event) Command { return nil }

func (w *testWorkflow) IsFinalEvent(e Event) bool {
	if ev, ok := e.(*incEvent); ok && ev.Amount == 999 {
		return true
	}
	return false
}

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	if p.MaxRetries != 3 {
		t.Errorf("MaxRetries: got %d", p.MaxRetries)
	}
	if p.BackoffStrategy != "exponential" {
		t.Errorf("BackoffStrategy: got %q", p.BackoffStrategy)
	}
	if p.BackoffFactor != 2 {
		t.Errorf("BackoffFactor: got %v", p.BackoffFactor)
	}
	if p.BackoffMax != 60*time.Second || p.BackoffMin != time.Second {
		t.Errorf("BackoffMax/Min: got %v / %v", p.BackoffMax, p.BackoffMin)
	}
	if p.BackoffJitter != 0.5 {
		t.Errorf("BackoffJitter: got %v", p.BackoffJitter)
	}
}

func TestRetryPolicyCalculateDelay_exponential(t *testing.T) {
	p := DefaultRetryPolicy()
	d1 := p.CalculateDelay(0)
	d2 := p.CalculateDelay(1)
	if d2 <= d1 {
		t.Errorf("exponential backoff: d1=%v d2=%v", d1, d2)
	}
	if d1 < p.BackoffMin {
		t.Errorf("d1 below min: %v", d1)
	}
}

func TestRetryPolicyCalculateDelay_linear(t *testing.T) {
	p := RetryPolicy{
		MaxRetries:      5,
		BackoffStrategy: "linear",
		BackoffFactor:   1,
		BackoffMax:      120 * time.Second,
		BackoffMin:      2 * time.Second,
	}
	if got := p.CalculateDelay(0); got != 2*time.Second {
		t.Errorf("linear retry 0: got %v", got)
	}
	if got := p.CalculateDelay(1); got != 4*time.Second {
		t.Errorf("linear retry 1: got %v", got)
	}
}

func TestRetryPolicyCalculateDelay_unknownStrategyUsesMin(t *testing.T) {
	p := RetryPolicy{BackoffStrategy: "weird", BackoffMin: 3 * time.Second}
	if got := p.CalculateDelay(5); got != 3*time.Second {
		t.Errorf("got %v", got)
	}
}

func TestRejectionError(t *testing.T) {
	r := &Rejection{Msg: "no"}
	if r.Error() != "no" {
		t.Errorf("got %q", r.Error())
	}
}

func TestWorkflowNotFoundError(t *testing.T) {
	e := &WorkflowNotFound{ID: "x", WorkflowType: "T"}
	if e.Error() != "workflow x of type T not found" {
		t.Errorf("got %q", e.Error())
	}
}

func TestWorkflowDecideEvolve(t *testing.T) {
	w := &testWorkflow{}
	cmd := &incCommand{N: 7}
	events, rej := w.Decide(nil, cmd)
	if rej != nil {
		t.Fatalf("rejection: %v", rej)
	}
	if len(events) != 1 || events[0].(*incEvent).Amount != 7 {
		t.Fatalf("events: %#v", events)
	}
	state := w.Evolve(nil, events[0])
	if state.(*testState).Counter != 7 {
		t.Errorf("counter: %d", state.(*testState).Counter)
	}
}

func TestWorkflowDecideRejection(t *testing.T) {
	w := &testWorkflow{}
	_, rej := w.Decide(nil, &incCommand{N: -1})
	if rej == nil || rej.Msg != "negative increment" {
		t.Fatalf("got %#v", rej)
	}
}

func TestWorkflowDecideNoEvents(t *testing.T) {
	w := &testWorkflow{}
	events, rej := w.Decide(nil, &incCommand{N: 0})
	if rej != nil || len(events) != 0 {
		t.Fatalf("rej=%v events=%d", rej, len(events))
	}
}

func TestSubMatchesTags(t *testing.T) {
	sub := &Sub{EventType: "order_created", WorkflowID: "*", Tags: []string{"urgent"}}
	if !sub.MatchesTags([]string{"urgent"}, nil) {
		t.Error("expected match urgent")
	}
	if sub.MatchesTags([]string{"normal"}, nil) {
		t.Error("unexpected match")
	}
	subAll := &Sub{EventType: "order_created", WorkflowID: "*", TagsAll: []string{"urgent", "priority"}}
	if !subAll.MatchesTags([]string{"urgent", "priority"}, nil) {
		t.Error("expected TagsAll match")
	}
	if subAll.MatchesTags([]string{"urgent"}, nil) {
		t.Error("TagsAll should require both")
	}
}

func TestStateBaseCopy(t *testing.T) {
	b := NewStateBase()
	b.Subscriptions = []Sub{{EventType: "e", WorkflowID: "w"}}
	b.Lifecycle = LifecyclePaused
	c := b.Copy()
	c.Subscriptions[0].EventType = "changed"
	if b.Subscriptions[0].EventType != "e" {
		t.Error("original mutated")
	}
	if c.Lifecycle != LifecyclePaused {
		t.Errorf("lifecycle %q", c.Lifecycle)
	}
}

func TestStateBaseCopyNilReceiver(t *testing.T) {
	var sb *StateBase
	c := sb.Copy()
	if c == nil || c.Lifecycle != LifecycleActive {
		t.Fatalf("Copy nil receiver: %#v", c)
	}
}

func TestEventBaseMetadata(t *testing.T) {
	var e EventBase
	m := e.GetMetadata()
	m["k"] = 1
	if e.GetMetadata()["k"] != 1 {
		t.Fatal("metadata not shared")
	}
	e.SetMetadata(map[string]any{"a": true})
	if e.GetMetadata()["a"] != true {
		t.Fatal("SetMetadata")
	}
}

func TestBuiltinEventGetTypes(t *testing.T) {
	cases := []struct {
		e    Event
		want string
	}{
		{&EvDelayComplete{}, "delay_complete"},
		{&EvDelay{}, "delay"},
		{&EvCancelSchedule{}, "cancel_schedule"},
		{&EvActionCancel{}, "action_cancel"},
		{&EvSubscriptionAdded{}, "subscription_added"},
		{&EvSystemPause{}, "system_pause"},
		{&EvSystemResume{}, "system_resume"},
		{&EvSystemCancel{}, "system_cancel"},
		{&EvContinueAsNew{}, "system_continue_as_new"},
	}
	for _, tc := range cases {
		if tc.e.GetType() != tc.want {
			t.Errorf("%T: got %q want %q", tc.e, tc.e.GetType(), tc.want)
		}
	}
}

func TestActionContextFields(t *testing.T) {
	p := DefaultRetryPolicy()
	ctx := ActionContext{
		WorkflowID:  "wf-1",
		EventNumber: 5,
		Checkpoint:  map[string]any{"step": 2.0},
		RetryCount:  1,
		RetryPolicy: p,
	}
	if ctx.WorkflowID != "wf-1" || ctx.EventNumber != 5 {
		t.Fatal("fields")
	}
	if ctx.RetryPolicy.MaxRetries != 3 {
		t.Fatal("policy")
	}
}

func TestCommandYieldVariants(t *testing.T) {
	cy := CommandYield{Cmd: &incCommand{N: 1}}
	if !cy.IsCommand() || cy.GetCommand() == nil || cy.IsCheckpoint() || cy.IsTimeout() {
		t.Fatal("CommandYield")
	}
	cp := CheckpointYieldWrapper{CP: &CheckpointYield{Data: map[string]any{"x": 1}}}
	if !cp.IsCheckpoint() || cp.GetCheckpoint().Data["x"] != 1 {
		t.Fatal("CheckpointYieldWrapper")
	}
	tm := TimeoutYield{T: &ActionTimeout{Seconds: 1.5}}
	if !tm.IsTimeout() || tm.GetTimeout().Seconds != 1.5 {
		t.Fatal("TimeoutYield")
	}
}

func TestStoredStateMarshalJSON(t *testing.T) {
	s := &testState{StateBase: *NewStateBase(), Counter: 42}
	ss := &StoredState{ID: "wf-1", Version: 3, State: s}
	b, err := json.Marshal(ss)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	if string(raw["id"]) != `"wf-1"` || string(raw["version"]) != `3` {
		t.Fatalf("top-level: %s", b)
	}
}

func TestIsFinalEvent(t *testing.T) {
	w := &testWorkflow{}
	if w.IsFinalEvent(&incEvent{Type: "inc", Amount: 1}) {
		t.Error("non-final")
	}
	if !w.IsFinalEvent(&incEvent{Type: "inc", Amount: 999}) {
		t.Error("final")
	}
}
