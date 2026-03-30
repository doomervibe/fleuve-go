package postgres

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStoredEventJSONRoundTrip(t *testing.T) {
	at := time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC)
	se := StoredEvent{
		GlobalID:        9,
		WorkflowID:      "w1",
		WorkflowVersion: 2,
		EventType:       "e",
		WorkflowType:    "T",
		SchemaVersion:   1,
		Body:            []byte(`{"x":1}`),
		At:              at,
		Metadata:        []byte(`{}`),
		Pushed:          false,
	}
	b, err := json.Marshal(se)
	if err != nil {
		t.Fatal(err)
	}
	var out StoredEvent
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.WorkflowID != se.WorkflowID || out.GlobalID != se.GlobalID {
		t.Fatalf("%+v", out)
	}
}
