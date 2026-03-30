package counterworkflow

import (
	"fmt"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

// ParseGatewayCommand maps JSON command_type + payload to a model.Command (HTTP gateway).
func ParseGatewayCommand(cmdType string, payload map[string]any) (model.Command, error) {
	switch cmdType {
	case "increment":
		var amount float64
		if v, ok := payload["amount"].(float64); ok {
			amount = v
		} else if v, ok := payload["amount"].(int); ok {
			amount = float64(v)
		} else if v, ok := payload["amount"].(int64); ok {
			amount = float64(v)
		} else {
			return nil, fmt.Errorf("increment requires numeric amount")
		}
		if amount <= 0 {
			return nil, fmt.Errorf("amount must be positive")
		}
		return &IncrementCmd{Amount: int64(amount)}, nil
	case "reset":
		return &ResetCmd{}, nil
	default:
		return nil, fmt.Errorf("unknown command type: %s", cmdType)
	}
}
