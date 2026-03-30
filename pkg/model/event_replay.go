package model

import (
	"encoding/json"
	"fmt"
)

// EventDecoder is optionally implemented by workflows so LoadState/replay can
// materialize domain event types from stored JSON.
type EventDecoder interface {
	Workflow
	DecodeEvent(eventType string, schemaVersion int, raw map[string]any) (Event, error)
}

// DecodeReplayEvent applies Upcast, then EventDecoder if present, otherwise known built-in event types.
func DecodeReplayEvent(wf Workflow, eventType string, schemaVersion int, raw map[string]any) (Event, error) {
	if raw == nil {
		return nil, fmt.Errorf("nil raw event body")
	}
	up := wf.Upcast(eventType, schemaVersion, raw)
	if d, ok := wf.(EventDecoder); ok {
		return d.DecodeEvent(eventType, schemaVersion, up)
	}
	return DecodeBuiltinReplayEvent(eventType, up)
}

// DecodeBuiltinReplayEvent unmarshals standard Fleuve event types (no domain events).
func DecodeBuiltinReplayEvent(eventType string, raw map[string]any) (Event, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var target Event
	switch eventType {
	case "delay_complete":
		var e EvDelayComplete
		err = json.Unmarshal(b, &e)
		target = &e
	case "delay":
		var e EvDelay
		err = json.Unmarshal(b, &e)
		target = &e
	case "cancel_schedule":
		var e EvCancelSchedule
		err = json.Unmarshal(b, &e)
		target = &e
	case "action_cancel":
		var e EvActionCancel
		err = json.Unmarshal(b, &e)
		target = &e
	case "subscription_added":
		var e EvSubscriptionAdded
		err = json.Unmarshal(b, &e)
		target = &e
	case "subscription_removed":
		var e EvSubscriptionRemoved
		err = json.Unmarshal(b, &e)
		target = &e
	case "external_subscription_added":
		var e EvExternalSubscriptionAdded
		err = json.Unmarshal(b, &e)
		target = &e
	case "external_subscription_removed":
		var e EvExternalSubscriptionRemoved
		err = json.Unmarshal(b, &e)
		target = &e
	case "schedule_added":
		var e EvScheduleAdded
		err = json.Unmarshal(b, &e)
		target = &e
	case "schedule_removed":
		var e EvScheduleRemoved
		err = json.Unmarshal(b, &e)
		target = &e
	case "system_pause":
		var e EvSystemPause
		err = json.Unmarshal(b, &e)
		target = &e
	case "system_resume":
		var e EvSystemResume
		err = json.Unmarshal(b, &e)
		target = &e
	case "system_cancel":
		var e EvSystemCancel
		err = json.Unmarshal(b, &e)
		target = &e
	case "system_continue_as_new":
		var e EvContinueAsNew
		err = json.Unmarshal(b, &e)
		target = &e
	case "direct_message":
		var e EvDirectMessage
		err = json.Unmarshal(b, &e)
		target = &e
	default:
		return nil, fmt.Errorf("replay: unknown event type %q (implement model.EventDecoder on the workflow)", eventType)
	}
	if err != nil {
		return nil, err
	}
	return target, nil
}
