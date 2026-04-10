package model

import "time"

// Event is the interface that all workflow events must implement.
// Every concrete event struct MUST implement Type() returning a stable string literal.
type Event interface {
	Type() string
}

// MetaEmitterWorkflowVersion is the metadata key the workflow runner sets on
// incoming events before Workflow.EventToCmd. The value is the emitter
// aggregate's stored_events.workflow_version (int64) for that event — used by
// subscribers (e.g. recipes) to bump target_version without ambiguous JSON.
const MetaEmitterWorkflowVersion = "emitter_workflow_version"

// MetaEmitterWorkflowID is the emitter workflow_id (UUID string) for the event
// being processed — used by subscribers that need the target aggregate id when
// the event body does not repeat it.
const MetaEmitterWorkflowID = "emitter_workflow_id"

// MetaEmitterWorkflowType is the emitter workflow_type (e.g. "domain", "project").
const MetaEmitterWorkflowType = "emitter_workflow_type"

// EventBase provides common fields for all events.
// The Metadata field is internal and NOT serialized to JSON - it carries
// injected workflow tags and other framework metadata alongside the event.
// User-defined events should embed EventBase.
type EventBase struct {
	Metadata map[string]any `json:"-"`
}

// GetMetadata returns the metadata map, initializing it if nil.
func (e *EventBase) GetMetadata() map[string]any {
	if e.Metadata == nil {
		e.Metadata = make(map[string]any)
	}
	return e.Metadata
}

// SetMetadata sets the metadata map.
func (e *EventBase) SetMetadata(m map[string]any) {
	e.Metadata = m
}

// =============================================================================
// System Events (Framework-Emitted)
// These events are NOT emitted by user workflows. They are emitted by the
// framework itself for lifecycle management, delays, and workflow reset.
// =============================================================================

// EvDelayComplete is emitted by DelayScheduler when a delay expires.
// It contains the delay_id, expiration time, and the next_cmd to execute.
// Workflows never emit this; event_to_cmd receives it and must return next_cmd.
type EvDelayComplete struct {
	EventBase
	DelayID string    `json:"delay_id"`
	At      time.Time `json:"at"`
	NextCmd Command   `json:"next_cmd"`
}

func (e *EvDelayComplete) Type() string { return "delay_complete" }

// EvSystemPause is emitted when pause_workflow() is called.
type EvSystemPause struct {
	EventBase
	Reason string `json:"reason,omitempty"`
}

func (e *EvSystemPause) Type() string { return "system_pause" }

// EvSystemResume is emitted when resume_workflow() is called.
type EvSystemResume struct {
	EventBase
}

func (e *EvSystemResume) Type() string { return "system_resume" }

// EvSystemCancel is emitted when cancel_workflow() is called.
// CRITICAL: This is NOT treated as a final event - state is kept for lifecycle checks.
type EvSystemCancel struct {
	EventBase
	Reason string `json:"reason,omitempty"`
}

func (e *EvSystemCancel) Type() string { return "system_cancel" }

// EvContinueAsNew is emitted by continue_as_new() to reset event history
// while preserving state. Optionally changes workflow type label.
type EvContinueAsNew struct {
	EventBase
	Reason          string `json:"reason,omitempty"`
	NewWorkflowType string `json:"new_workflow_type,omitempty"`
}

func (e *EvContinueAsNew) Type() string { return "system_continue_as_new" }

// =============================================================================
// Sync Events (User-Emitted from decide())
// These events have DUAL effects: they evolve state AND perform synchronous
// database operations in the same transaction as event insertion.
// =============================================================================

// EvSubscriptionAdded adds an internal subscription.
// DB Side Effect: INSERT into subscriptions table.
type EvSubscriptionAdded struct {
	EventBase
	Sub Sub `json:"sub"`
}

func (e *EvSubscriptionAdded) Type() string { return "subscription_added" }

// EvSubscriptionRemoved removes an internal subscription.
// DB Side Effect: DELETE from subscriptions table.
type EvSubscriptionRemoved struct {
	EventBase
	Sub Sub `json:"sub"`
}

func (e *EvSubscriptionRemoved) Type() string { return "subscription_removed" }

// EvExternalSubscriptionAdded adds an external subscription.
// DB Side Effect: INSERT into external_subscriptions table.
type EvExternalSubscriptionAdded struct {
	EventBase
	Sub ExternalSub `json:"sub"`
}

func (e *EvExternalSubscriptionAdded) Type() string { return "external_subscription_added" }

// EvExternalSubscriptionRemoved removes an external subscription.
// DB Side Effect: DELETE from external_subscriptions table.
type EvExternalSubscriptionRemoved struct {
	EventBase
	Topic string `json:"topic"`
}

func (e *EvExternalSubscriptionRemoved) Type() string { return "external_subscription_removed" }

// EvScheduleAdded adds a cron schedule.
// DB Side Effect: INSERT into delay_schedule table.
type EvScheduleAdded struct {
	EventBase
	Schedule Schedule `json:"schedule"`
}

func (e *EvScheduleAdded) Type() string { return "schedule_added" }

// EvScheduleRemoved removes a cron schedule.
// DB Side Effect: DELETE from delay_schedule table.
type EvScheduleRemoved struct {
	EventBase
	DelayID string `json:"delay_id"`
}

func (e *EvScheduleRemoved) Type() string { return "schedule_removed" }

// EvCancelSchedule cancels a delay schedule.
// DB Side Effect: DELETE from delay_schedule table.
type EvCancelSchedule struct {
	EventBase
	DelayID string `json:"delay_id"`
}

func (e *EvCancelSchedule) Type() string { return "cancel_schedule" }

// EvActionCancel cancels in-flight actions.
// No DB side effect - signals executor to cancel.
type EvActionCancel struct {
	EventBase
	EventNumbers []int `json:"event_numbers,omitempty"`
}

func (e *EvActionCancel) Type() string { return "action_cancel" }

// =============================================================================
// Delay Events
// =============================================================================

// EvDelay represents a delay request from decide().
// Two paths:
//   - One-shot (CronExpression is empty): Registered by DelayScheduler,
//     fires once, then deleted.
//   - Cron (CronExpression is set): Treated as a sync event - stored in
//     state.schedules AND delay_schedule table. Recurring with computed next fire time.
type EvDelay struct {
	EventBase
	ID             string    `json:"id"`
	DelayUntil     time.Time `json:"delay_until"`
	NextCmd        Command   `json:"next_cmd"`
	CronExpression string    `json:"cron_expression,omitempty"`
	Timezone       string    `json:"timezone,omitempty"`
}

func (e *EvDelay) Type() string { return "delay" }

// IsCron returns true if this is a recurring (cron) delay.
func (e *EvDelay) IsCron() bool {
	return e.CronExpression != ""
}

// =============================================================================
// Direct Message Event
// =============================================================================

// EvDirectMessage allows sending a message directly to a specific workflow.
// Used for targeted cross-workflow communication.
type EvDirectMessage struct {
	EventBase
	TargetWorkflowID   string `json:"target_workflow_id"`
	TargetWorkflowType string `json:"target_workflow_type"`
}

func (e *EvDirectMessage) Type() string { return "direct_message" }

// =============================================================================
// Event Type Constants for Reference
// =============================================================================

const (
	EventTypeDelayComplete               = "delay_complete"
	EventTypeSystemPause                 = "system_pause"
	EventTypeSystemResume                = "system_resume"
	EventTypeSystemCancel                = "system_cancel"
	EventTypeContinueAsNew               = "system_continue_as_new"
	EventTypeSubscriptionAdded           = "subscription_added"
	EventTypeSubscriptionRemoved         = "subscription_removed"
	EventTypeExternalSubscriptionAdded   = "external_subscription_added"
	EventTypeExternalSubscriptionRemoved = "external_subscription_removed"
	EventTypeScheduleAdded               = "schedule_added"
	EventTypeScheduleRemoved             = "schedule_removed"
	EventTypeCancelSchedule              = "cancel_schedule"
	EventTypeActionCancel                = "action_cancel"
	EventTypeDelay                       = "delay"
	EventTypeDirectMessage               = "direct_message"
)

// IsSystemEvent returns true if the event type is a system/framework event.
func IsSystemEvent(eventType string) bool {
	switch eventType {
	case EventTypeDelayComplete,
		EventTypeSystemPause,
		EventTypeSystemResume,
		EventTypeSystemCancel,
		EventTypeContinueAsNew:
		return true
	}
	return false
}

// IsSyncEvent returns true if the event type has synchronous DB side effects.
func IsSyncEvent(eventType string) bool {
	switch eventType {
	case EventTypeSubscriptionAdded,
		EventTypeSubscriptionRemoved,
		EventTypeExternalSubscriptionAdded,
		EventTypeExternalSubscriptionRemoved,
		EventTypeScheduleAdded,
		EventTypeScheduleRemoved,
		EventTypeCancelSchedule,
		EventTypeActionCancel:
		return true
	}
	return false
}
