package counterworkflow

import (
	"testing"

	"github.com/fleuve/fleuve-go/pkg/model"
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
