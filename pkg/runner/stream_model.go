package runner

import (
	"github.com/fleuve/fleuve-go/pkg/model"
	"github.com/fleuve/fleuve-go/pkg/stream"
)

// StreamToModelConsumedEvent maps a stream event to the model shape used by adapters and actions.
func StreamToModelConsumedEvent(ev *stream.ConsumedEvent) *model.ConsumedEvent {
	if ev == nil {
		return nil
	}
	meta := ev.Metadata
	if meta == nil {
		meta = make(map[string]any)
	}
	return &model.ConsumedEvent{
		GlobalID:     ev.GlobalID,
		WorkflowID:   ev.WorkflowID,
		WorkflowType: ev.WorkflowType,
		EventNo:      ev.EventNo,
		EventType:    ev.EventType,
		Event:        ev.Event,
		At:           ev.At,
		Metadata_:    meta,
	}
}
