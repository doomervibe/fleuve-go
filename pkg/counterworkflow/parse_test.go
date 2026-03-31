package counterworkflow

import (
	"testing"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

func TestParseGatewayCommand(t *testing.T) {
	cmd, err := ParseGatewayCommand("increment", map[string]any{"amount": float64(3)})
	if err != nil {
		t.Fatal(err)
	}
	inc, ok := cmd.(*IncrementCmd)
	if !ok || inc.Amount != 3 {
		t.Fatalf("cmd %#v", cmd)
	}
	_, err = ParseGatewayCommand("reset", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ParseGatewayCommand("nope", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWorkflowImplementsModel(t *testing.T) {
	var _ model.Workflow = New()
}

func TestCounterIncrementedMetadataNotNil(t *testing.T) {
	e := &CounterIncremented{Amount: 1}
	m := e.GetMetadata()
	if m == nil {
		t.Fatal("GetMetadata() returned nil, want non-nil map")
	}
	// Writing to the map should not panic.
	m["key"] = "value"
	if e.GetMetadata()["key"] != "value" {
		t.Fatal("metadata not persisted")
	}
}

func TestCounterResetMetadataNotNil(t *testing.T) {
	e := &CounterReset{}
	m := e.GetMetadata()
	if m == nil {
		t.Fatal("GetMetadata() returned nil, want non-nil map")
	}
	m["key"] = "value"
	if e.GetMetadata()["key"] != "value" {
		t.Fatal("metadata not persisted")
	}
}

func TestCounterEventSetMetadata(t *testing.T) {
	e := &CounterIncremented{Amount: 5}
	custom := map[string]any{"workflow_tags": []string{"vip"}}
	e.SetMetadata(custom)
	if e.GetMetadata()["workflow_tags"] == nil {
		t.Fatal("SetMetadata not applied")
	}
}
