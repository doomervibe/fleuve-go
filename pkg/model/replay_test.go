package model

import (
	"encoding/json"
	"testing"
)

func TestShouldUseUpcastPath(t *testing.T) {
	if ShouldUseUpcastPath(nil, 2) {
		t.Fatal("empty events: should be false")
	}
	events := []RawStoredEvent{
		{SchemaVersion: 2},
	}
	if ShouldUseUpcastPath(events, 2) {
		t.Fatal("all at current version: should be false")
	}
	old := []RawStoredEvent{
		{SchemaVersion: 1},
	}
	if !ShouldUseUpcastPath(old, 2) {
		t.Fatal("older schema: should be true")
	}
}

func TestReplayEvents_systemPause(t *testing.T) {
	wf := stubReplayWorkflow{}
	state := ReplayEvents(wf, nil, []Event{&EvSystemPause{}})
	if state == nil {
		t.Fatal("expected state")
	}
	sb, ok := state.(*StateBase)
	if !ok {
		t.Fatalf("type %T", state)
	}
	if sb.Lifecycle != LifecyclePaused {
		t.Fatalf("lifecycle = %v", sb.Lifecycle)
	}
}

type stubReplayWorkflow struct{}

func (stubReplayWorkflow) Name() string { return "stub" }
func (stubReplayWorkflow) SchemaVersion() int {
	return 2
}
func (stubReplayWorkflow) Upcast(string, int, map[string]any) map[string]any { return nil }
func (stubReplayWorkflow) Decide(State, Command) ([]Event, *Rejection) {
	return nil, nil
}
func (stubReplayWorkflow) Evolve(State, Event) State { return nil }
func (stubReplayWorkflow) EventToCmd(Event) Command  { return nil }
func (stubReplayWorkflow) IsFinalEvent(Event) bool   { return false }

func TestParseAndUpcastEvent_upcastNilError(t *testing.T) {
	wf := upcastNilWorkflow{}
	cfg := ReplayConfig{
		Workflow:             wf,
		Parser:               nil,
		CurrentSchemaVersion: 2,
	}
	raw := RawStoredEvent{
		EventType:     "x",
		SchemaVersion: 1,
		BodyRaw:       json.RawMessage(`{}`),
	}
	_, err := ParseAndUpcastEvent(cfg, raw)
	if err == nil {
		t.Fatal("expected error when Upcast returns nil")
	}
}

type upcastNilWorkflow struct{ stubReplayWorkflow }

func (upcastNilWorkflow) Upcast(string, int, map[string]any) map[string]any {
	return nil
}
