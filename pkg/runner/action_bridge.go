package runner

import (
	"context"

	"github.com/fleuve/fleuve-go/pkg/actions"
	"github.com/fleuve/fleuve-go/pkg/stream"
)

// ActionExecutorSideEffects runs activities via an ActionExecutor when the adapter opts in.
type ActionExecutorSideEffects struct {
	Exec *actions.ActionExecutor
}

func (s *ActionExecutorSideEffects) MaybeActOn(ctx context.Context, event *stream.ConsumedEvent) error {
	if s == nil || s.Exec == nil || event == nil {
		return nil
	}
	me := StreamToModelConsumedEvent(event)
	if !s.Exec.ToBeActOn(me) {
		return nil
	}
	return s.Exec.ExecuteAction(ctx, me)
}
