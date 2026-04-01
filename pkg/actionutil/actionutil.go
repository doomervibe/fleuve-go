package actionutil

import (
	"time"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

// run_with_background_check wraps an action generator with a background task
// that periodically calls condition(). When condition() returns true:
//   - Stop consuming the inner generator
//   - Optionally yield on_stop_cmd (if non-nil)
//   - Return
//
// This is used for actions that should be cancellable based on external state,
// such as waiting for an event while also checking if the workflow has been
// paused or canceled.
func run_with_background_check(
	gen <-chan model.ActionYield,
	condition func() bool,
	interval time.Duration,
	on_stop_cmd model.Command,
) <-chan model.ActionYield {
	out := make(chan model.ActionYield)

	go func() {
		defer close(out)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case yield, ok := <-gen:
				if !ok {
					return
				}
				out <- yield
			case <-ticker.C:
				if condition() {
					if on_stop_cmd != nil {
						out <- NewCommandYield(on_stop_cmd)
					}
					return
				}
			}
		}
	}()

	return out
}

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
