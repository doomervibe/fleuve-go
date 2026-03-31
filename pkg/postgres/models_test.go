package postgres

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

func TestRetryPolicyJSONRoundTrip(t *testing.T) {
	p := model.RetryPolicy{
		MaxRetries:      5,
		BackoffStrategy: "exponential",
		BackoffFactor:   2.0,
		BackoffMax:      30 * time.Second,
		BackoffMin:      1 * time.Second,
		BackoffJitter:   0.1,
	}
	b, err := RetryPolicyToJSON(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		t.Fatal("expected non-empty bytes from RetryPolicyToJSON")
	}
	got, err := JSONToRetryPolicy(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxRetries != p.MaxRetries || got.BackoffStrategy != p.BackoffStrategy || got.BackoffFactor != p.BackoffFactor {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, p)
	}
}

func TestJSONMarshalUnmarshalErrors(t *testing.T) {
	// Unmarshal invalid data should return error
	_, err := JSONToRetryPolicy([]byte(`{invalid`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

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
