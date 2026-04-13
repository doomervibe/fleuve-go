package model

import (
	"encoding/json"
	"fmt"
)

// ParseFrameworkEvent deserializes event types emitted by the framework into any
// workflow's stored_events row (subscriptions, schedules, delays, system events).
// Workflow-specific ParseEvent functions should fall back to this for replay.
func ParseFrameworkEvent(eventType string, raw json.RawMessage) (Event, error) {
	switch eventType {
	case EventTypeSubscriptionAdded:
		var e EvSubscriptionAdded
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvSubscriptionAdded: %w", err)
		}
		return &e, nil
	case EventTypeSubscriptionRemoved:
		var e EvSubscriptionRemoved
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvSubscriptionRemoved: %w", err)
		}
		return &e, nil
	case EventTypeExternalSubscriptionAdded:
		var e EvExternalSubscriptionAdded
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvExternalSubscriptionAdded: %w", err)
		}
		return &e, nil
	case EventTypeExternalSubscriptionRemoved:
		var e EvExternalSubscriptionRemoved
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvExternalSubscriptionRemoved: %w", err)
		}
		return &e, nil
	case EventTypeScheduleAdded:
		var e EvScheduleAdded
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvScheduleAdded: %w", err)
		}
		return &e, nil
	case EventTypeScheduleRemoved:
		var e EvScheduleRemoved
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvScheduleRemoved: %w", err)
		}
		return &e, nil
	case EventTypeCancelSchedule:
		var e EvCancelSchedule
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvCancelSchedule: %w", err)
		}
		return &e, nil
	case EventTypeActionCancel:
		var e EvActionCancel
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvActionCancel: %w", err)
		}
		return &e, nil
	case EventTypeDelay:
		// NextCmd is a Command interface — json.Unmarshal cannot populate it from
		// a stored "{}" value. During replay we only need the scheduling fields;
		// the NextCmd is not required to reconstruct workflow state.
		var body struct {
			ID             string    `json:"id"`
			DelayUntil     time.Time `json:"delay_until"`
			CronExpression string    `json:"cron_expression,omitempty"`
			Timezone       string    `json:"timezone,omitempty"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return nil, fmt.Errorf("parse EvDelay: %w", err)
		}
		return &EvDelay{
			ID:             body.ID,
			DelayUntil:     body.DelayUntil,
			CronExpression: body.CronExpression,
			Timezone:       body.Timezone,
		}, nil
	case EventTypeDirectMessage:
		var e EvDirectMessage
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvDirectMessage: %w", err)
		}
		return &e, nil
	case EventTypeDelayComplete:
		var e EvDelayComplete
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvDelayComplete: %w", err)
		}
		return &e, nil
	case EventTypeSystemPause:
		var e EvSystemPause
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvSystemPause: %w", err)
		}
		return &e, nil
	case EventTypeSystemResume:
		var e EvSystemResume
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvSystemResume: %w", err)
		}
		return &e, nil
	case EventTypeSystemCancel:
		var e EvSystemCancel
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvSystemCancel: %w", err)
		}
		return &e, nil
	case EventTypeContinueAsNew:
		var e EvContinueAsNew
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("parse EvContinueAsNew: %w", err)
		}
		return &e, nil
	default:
		return nil, fmt.Errorf("unknown framework event type %q", eventType)
	}
}
