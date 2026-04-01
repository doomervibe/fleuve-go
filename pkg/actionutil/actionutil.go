package actionutil

import (
	"github.com/doomervibe/fleuve-go/pkg/model"
)

// NewCommandYield creates an ActionYield that yields a command to be processed.
func NewCommandYield(cmd model.Command) model.ActionYield {
	return model.CommandYield{Cmd: cmd}
}

// NewCheckpointYield creates an ActionYield that yields checkpoint data to be saved.
// If saveNow is true, the checkpoint is persisted immediately.
// Otherwise, it is merged and persisted at the end of the action.
func NewCheckpointYield(data map[string]any, saveNow bool) model.ActionYield {
	return model.CheckpointYield{Data: data, SaveNow: saveNow}
}

// NewTimeoutYield creates an ActionYield that sets a timeout for all subsequent yields.
// The executor wraps all subsequent yields in the appropriate timeout.
// If the remainder of the action does not complete in time, the action retries.
func NewTimeoutYield(seconds float64) model.ActionYield {
	return model.ActionTimeout{Seconds: seconds}
}
