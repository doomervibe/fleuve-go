package actions

import (
	"context"

	"github.com/fleuve/fleuve-go/pkg/model"
)

type noopAdapter struct{}

// NoopModelAdapter is a model.Adapter that never runs side effects (activities).
func NoopModelAdapter() model.Adapter {
	return noopAdapter{}
}

func (noopAdapter) ActOn(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
	ch := make(chan model.ActionYield)
	close(ch)
	return ch, nil
}

func (noopAdapter) ToBeActOn(event *model.ConsumedEvent) bool { return false }

func (noopAdapter) SyncDB(ctx context.Context, workflowID string, oldState, newState model.State, events []model.Event) error {
	return nil
}
