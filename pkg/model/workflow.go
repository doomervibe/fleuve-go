package model

import (
	"context"
	"time"
)

type LifecycleState string

const (
	LifecycleActive    LifecycleState = "active"
	LifecyclePaused    LifecycleState = "paused"
	LifecycleCanceled  LifecycleState = "cancelled"
	LifecycleCompleted LifecycleState = "completed"
)

type Sub struct {
	EventType  string   `json:"event_type"`
	WorkflowID string   `json:"workflow_id"`
	Tags       []string `json:"tags,omitempty"`
	TagsAll    []string `json:"tags_all,omitempty"`
}

func (s *Sub) MatchesTags(eventTags, workflowTags []string) bool {
	allTags := make(map[string]struct{})
	for _, t := range eventTags {
		allTags[t] = struct{}{}
	}
	for _, t := range workflowTags {
		allTags[t] = struct{}{}
	}

	if len(s.Tags) > 0 {
		found := false
		for _, tag := range s.Tags {
			if _, ok := allTags[tag]; ok {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(s.TagsAll) > 0 {
		for _, tag := range s.TagsAll {
			if _, ok := allTags[tag]; !ok {
				return false
			}
		}
	}

	return true
}

type ExternalSub struct {
	Topic string `json:"topic"`
}

type Schedule struct {
	ID             string  `json:"id"`
	CronExpression string  `json:"cron_expression"`
	Timezone       string  `json:"timezone,omitempty"`
	NextCmd        Command `json:"next_cmd"`
}

type StateBase struct {
	Subscriptions         []Sub          `json:"subscriptions"`
	ExternalSubscriptions []ExternalSub  `json:"external_subscriptions,omitempty"`
	Lifecycle             LifecycleState `json:"lifecycle"`
	Schedules             []Schedule     `json:"schedules,omitempty"`
}

func NewStateBase() *StateBase {
	return &StateBase{
		Subscriptions:         []Sub{},
		ExternalSubscriptions: []ExternalSub{},
		Lifecycle:             LifecycleActive,
		Schedules:             []Schedule{},
	}
}

func (s *StateBase) Copy() *StateBase {
	if s == nil {
		return NewStateBase()
	}
	return &StateBase{
		Subscriptions:         append([]Sub{}, s.Subscriptions...),
		ExternalSubscriptions: append([]ExternalSub{}, s.ExternalSubscriptions...),
		Lifecycle:             s.Lifecycle,
		Schedules:             append([]Schedule{}, s.Schedules...),
	}
}

type Event interface {
	GetType() string
	GetMetadata() map[string]any
	SetMetadata(map[string]any)
}

type EventBase struct {
	Metadata_ map[string]any `json:"-"`
}

func (e *EventBase) GetMetadata() map[string]any {
	if e.Metadata_ == nil {
		e.Metadata_ = make(map[string]any)
	}
	return e.Metadata_
}

func (e *EventBase) SetMetadata(m map[string]any) {
	e.Metadata_ = m
}

type Command interface{}

type Rejection struct {
	Msg string `json:"msg"`
}

func (r *Rejection) Error() string {
	return r.Msg
}

type AlreadyExists struct {
	Rejection
}

type EvDirectMessage struct {
	EventBase
	TargetWorkflowID   string `json:"target_workflow_id"`
	TargetWorkflowType string `json:"target_workflow_type"`
}

func (e *EvDirectMessage) GetType() string { return "direct_message" }

type EvDelayComplete struct {
	EventBase
	Type    string    `json:"type"` // "delay_complete"
	DelayID string    `json:"delay_id"`
	At      time.Time `json:"at"`
	NextCmd Command   `json:"next_cmd"`
}

func (e *EvDelayComplete) GetType() string { return "delay_complete" }

type EvDelay struct {
	EventBase
	ID             string    `json:"id"`
	DelayUntil     time.Time `json:"delay_until"`
	NextCmd        Command   `json:"next_cmd"`
	CronExpression string    `json:"cron_expression,omitempty"`
	Timezone       string    `json:"timezone,omitempty"`
}

func (e *EvDelay) GetType() string { return "delay" }

type EvCancelSchedule struct {
	EventBase
	Type    string `json:"type"` // "cancel_schedule"
	DelayID string `json:"delay_id"`
}

func (e *EvCancelSchedule) GetType() string { return "cancel_schedule" }

type EvActionCancel struct {
	EventBase
	Type         string `json:"type"` // "action_cancel"
	EventNumbers []int  `json:"event_numbers,omitempty"`
}

func (e *EvActionCancel) GetType() string { return "action_cancel" }

type EvSubscriptionAdded struct {
	EventBase
	Type string `json:"type"` // "subscription_added"
	Sub  Sub    `json:"sub"`
}

func (e *EvSubscriptionAdded) GetType() string { return "subscription_added" }

type EvSubscriptionRemoved struct {
	EventBase
	Type string `json:"type"` // "subscription_removed"
	Sub  Sub    `json:"sub"`
}

func (e *EvSubscriptionRemoved) GetType() string { return "subscription_removed" }

type EvExternalSubscriptionAdded struct {
	EventBase
	Type string      `json:"type"` // "external_subscription_added"
	Sub  ExternalSub `json:"sub"`
}

func (e *EvExternalSubscriptionAdded) GetType() string { return "external_subscription_added" }

type EvExternalSubscriptionRemoved struct {
	EventBase
	Type  string `json:"type"` // "external_subscription_removed"
	Topic string `json:"topic"`
}

func (e *EvExternalSubscriptionRemoved) GetType() string { return "external_subscription_removed" }

type EvScheduleAdded struct {
	EventBase
	Type     string   `json:"type"` // "schedule_added"
	Schedule Schedule `json:"schedule"`
}

func (e *EvScheduleAdded) GetType() string { return "schedule_added" }

type EvScheduleRemoved struct {
	EventBase
	Type    string `json:"type"` // "schedule_removed"
	DelayID string `json:"delay_id"`
}

func (e *EvScheduleRemoved) GetType() string { return "schedule_removed" }

type EvSystemPause struct {
	EventBase
	Type   string `json:"type"` // "system_pause"
	Reason string `json:"reason"`
}

func (e *EvSystemPause) GetType() string { return "system_pause" }

type EvSystemResume struct {
	EventBase
	Type string `json:"type"` // "system_resume"
}

func (e *EvSystemResume) GetType() string { return "system_resume" }

type EvSystemCancel struct {
	EventBase
	Type   string `json:"type"` // "system_cancel"
	Reason string `json:"reason"`
}

func (e *EvSystemCancel) GetType() string { return "system_cancel" }

type EvContinueAsNew struct {
	EventBase
	Type            string `json:"type"` // "system_continue_as_new"
	Reason          string `json:"reason"`
	NewWorkflowType string `json:"new_workflow_type,omitempty"`
}

func (e *EvContinueAsNew) GetType() string { return "system_continue_as_new" }

type ActionContext struct {
	WorkflowID  string         `json:"workflow_id"`
	EventNumber int            `json:"event_number"`
	Checkpoint  map[string]any `json:"checkpoint"`
	RetryCount  int            `json:"retry_count"`
	RetryPolicy RetryPolicy    `json:"retry_policy"`
}

type CheckpointYield struct {
	Data    map[string]any `json:"data"`
	SaveNow bool           `json:"save_now"`
}

type ActionTimeout struct {
	Seconds float64 `json:"seconds"`
}

type ConsumedEvent struct {
	GlobalID     int64          `json:"global_id"`
	WorkflowID   string         `json:"workflow_id"`
	WorkflowType string         `json:"workflow_type"`
	EventNo      int64          `json:"event_no"`
	EventType    string         `json:"event_type"`
	Event        Event          `json:"event"`
	At           time.Time      `json:"at"`
	Metadata_    map[string]any `json:"metadata"`
}

func (e *ConsumedEvent) GetMetadata() map[string]any {
	if e.Metadata_ == nil {
		e.Metadata_ = make(map[string]any)
	}
	return e.Metadata_
}

type Workflow interface {
	Name() string
	SchemaVersion() int
	Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any
	Decide(state State, cmd Command) ([]Event, *Rejection)
	Evolve(state State, event Event) State
	EventToCmd(e Event) Command
	IsFinalEvent(e Event) bool
}

type State interface {
	GetSubscriptions() []Sub
	GetExternalSubscriptions() []ExternalSub
	GetLifecycle() LifecycleState
	GetSchedules() []Schedule
	Copy() State
}

type Adapter interface {
	ActOn(ctx context.Context, event *ConsumedEvent, context *ActionContext) (<-chan ActionYield, error)
	ToBeActOn(event *ConsumedEvent) bool
	SyncDB(ctx context.Context, workflowID string, oldState, newState State, events []Event) error
}

type ActionYield interface {
	IsCommand() bool
	GetCommand() Command
	IsCheckpoint() bool
	GetCheckpoint() *CheckpointYield
	IsTimeout() bool
	GetTimeout() *ActionTimeout
}

type CommandYield struct{ Cmd Command }

func (y CommandYield) IsCommand() bool                 { return true }
func (y CommandYield) GetCommand() Command             { return y.Cmd }
func (y CommandYield) IsCheckpoint() bool              { return false }
func (y CommandYield) GetCheckpoint() *CheckpointYield { return nil }
func (y CommandYield) IsTimeout() bool                 { return false }
func (y CommandYield) GetTimeout() *ActionTimeout      { return nil }

type CheckpointYieldWrapper struct{ CP *CheckpointYield }

func (y CheckpointYieldWrapper) IsCommand() bool                 { return false }
func (y CheckpointYieldWrapper) GetCommand() Command             { return nil }
func (y CheckpointYieldWrapper) IsCheckpoint() bool              { return true }
func (y CheckpointYieldWrapper) GetCheckpoint() *CheckpointYield { return y.CP }
func (y CheckpointYieldWrapper) IsTimeout() bool                 { return false }
func (y CheckpointYieldWrapper) GetTimeout() *ActionTimeout      { return nil }

type TimeoutYield struct{ T *ActionTimeout }

func (y TimeoutYield) IsCommand() bool                 { return false }
func (y TimeoutYield) GetCommand() Command             { return nil }
func (y TimeoutYield) IsCheckpoint() bool              { return false }
func (y TimeoutYield) GetCheckpoint() *CheckpointYield { return nil }
func (y TimeoutYield) IsTimeout() bool                 { return true }
func (y TimeoutYield) GetTimeout() *ActionTimeout      { return y.T }
