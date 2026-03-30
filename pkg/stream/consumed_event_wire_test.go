package stream

import (
	"testing"
	"time"
)

func TestMarshalUnmarshalConsumedEventWire(t *testing.T) {
	at := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	body := []byte(`{"type":"inc","amount":3}`)
	meta := []byte(`{"k":"v"}`)
	data, err := MarshalConsumedEventWire(42, "wf-1", "CounterWorkflow", 7, "inc", body, meta, at)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := UnmarshalConsumedEventMessage(data)
	if err != nil {
		t.Fatal(err)
	}
	if ev.GlobalID != 42 || ev.WorkflowID != "wf-1" || ev.WorkflowType != "CounterWorkflow" || ev.EventNo != 7 || ev.EventType != "inc" {
		t.Fatalf("fields: %+v", ev)
	}
	if !ev.At.Equal(at) {
		t.Fatalf("at: %v", ev.At)
	}
	if ev.Event == nil || ev.Event.GetType() != "inc" {
		t.Fatalf("event type: %#v", ev.Event)
	}
	if ev.Metadata["k"] != "v" {
		t.Fatalf("metadata: %#v", ev.Metadata)
	}
}
