package model

import "context"

// =============================================================================
// Command
// =============================================================================

// Command is the interface for workflow commands.
// Commands express intent and are passed to Decide() which returns events.
// Commands must be deterministic and side-effect-free.
type Command interface {
	CommandType() string
}

// =============================================================================
// Rejection
// =============================================================================

// Rejection is returned by Decide() when a command should not produce events.
// The Msg field contains a human-readable reason.
type Rejection struct {
	Msg string `json:"msg"`
}

// Error implements the error interface.
func (r *Rejection) Error() string {
	return r.Msg
}

// AlreadyExists is a specific rejection returned by CreateNew() when
// a workflow with the given ID already exists (IntegrityError on insert).
type AlreadyExists struct {
	Rejection
}

// Error implements the error interface with a specific message.
func (a *AlreadyExists) Error() string {
	if a.Msg != "" {
		return a.Msg
	}
	return "workflow already exists"
}

// WorkflowNotFound is returned when attempting to operate on a non-existent workflow.
type WorkflowNotFound struct {
	ID           string
	WorkflowType string
}

// Error implements the error interface.
func (w *WorkflowNotFound) Error() string {
	return "workflow not found: " + w.ID + " (type: " + w.WorkflowType + ")"
}

// WorkflowPaused is returned when attempting to command a paused workflow.
type WorkflowPaused struct {
	Reason string
}

// Error implements the error interface.
func (w *WorkflowPaused) Error() string {
	msg := "workflow is paused"
	if w.Reason != "" {
		msg += ": " + w.Reason
	}
	return msg
}

// WorkflowCanceled is returned when attempting to command a cancelled workflow.
type WorkflowCanceled struct {
	Reason string
}

// Error implements the error interface.
func (w *WorkflowCanceled) Error() string {
	msg := "workflow is cancelled"
	if w.Reason != "" {
		msg += ": " + w.Reason
	}
	return msg
}

// =============================================================================
// Workflow Interface
// =============================================================================

// Workflow is the core interface that all workflow types must implement.
// It defines the decide/evolve pattern for event-sourced workflows.
//
// Type parameters are expressed through the concrete implementation:
//   - Events this workflow emits (via Decide return)
//   - Commands this workflow accepts (via Decide param)
//   - State type (must embed StateBase)
//   - External Events this workflow reacts to (via EventToCmd param)
type Workflow interface {
	// Name returns a static identifier for this workflow type.
	// Used as DB column value, NATS subject prefix, and reader name prefix.
	// Must be stable and unique across all workflow types.
	Name() string

	// SchemaVersion returns the current schema version for this workflow.
	// Override when evolving event schemas. Defaults to 1.
	SchemaVersion() int

	// Upcast transforms old event data to the current schema format.
	// Called during state loading when schema_version < current.
	// Receives raw JSON dict and must return updated dict matching current schema.
	Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any

	// Decide is a pure function that given current state (nil for new workflow)
	// and a command, returns events to append OR a rejection.
	// MUST be deterministic and side-effect-free.
	// Can return empty list (no-op, returns current state unchanged).
	Decide(state State, cmd Command) ([]Event, *Rejection)

	// Evolve is a pure state transition function.
	// Applies a single event to produce new state.
	// Called by EvolveAll AFTER EvolveSystem returns nil (i.e., not a system event).
	// The state argument may be nil for the first event of a new workflow.
	Evolve(state State, event Event) State

	// EventToCmd maps an external event to a command.
	// Called by the runner when routing events to workflows.
	// Return nil to ignore the event.
	EventToCmd(e Event) Command

	// IsFinalEvent returns true if the event represents workflow completion.
	// When the last event in a batch is final, the ephemeral storage entry
	// is REMOVED (not updated).
	// EXCEPTION: EvSystemCancel is NOT treated as final - state is kept for
	// lifecycle checks. This is handled by the repo, not this method.
	IsFinalEvent(e Event) bool
}

// =============================================================================
// EvolveSystem - System Event Handling
// =============================================================================

// EvolveSystem handles ALL system/sync events according to the priority order
// specified in the spec. Returns the new state if the event was handled,
// or nil if the event should be passed to the user's Evolve method.
//
// Priority order (first match wins):
//  1. EvSystemPause     → lifecycle = "paused"
//  2. EvSystemResume    → lifecycle = "active"
//  3. EvSystemCancel    → lifecycle = "cancelled", schedules = []
//  4. EvContinueAsNew   → return state unchanged (event log reset in repo)
//  5. EvSubscriptionAdded        → append to state.subscriptions
//  6. EvSubscriptionRemoved      → remove matching sub from state.subscriptions
//  7. EvExternalSubscriptionAdded → append to state.external_subscriptions
//  8. EvExternalSubscriptionRemoved → remove by topic from state.external_subscriptions
//  9. EvScheduleAdded             → upsert by schedule.id in state.schedules
//
// 10. EvScheduleRemoved          → remove by delay_id from state.schedules
// 11. EvDelay (with cron)        → upsert as Schedule in state.schedules
// 12. EvCancelSchedule           → remove by delay_id from state.schedules
// 13. If new_lifecycle was set   → update lifecycle
// 14. Return nil (user _evolve handles it)
// EvolveSystem handles ALL system/sync events according to the priority order
// specified in the spec. Returns the new state if the event was handled,
// or nil if the event should be passed to the user's Evolve method.
//
// Priority order (first match wins):
//  1. EvSystemPause     → lifecycle = "paused"
//  2. EvSystemResume    → lifecycle = "active"
//  3. EvSystemCancel    → lifecycle = "cancelled", schedules = []
//  4. EvContinueAsNew   → return state unchanged (event log reset in repo)
//  5. EvSubscriptionAdded        → append to state.subscriptions
//  6. EvSubscriptionRemoved      → remove matching sub from state.subscriptions
//  7. EvExternalSubscriptionAdded → append to state.external_subscriptions
//  8. EvExternalSubscriptionRemoved → remove by topic from state.external_subscriptions
//  9. EvScheduleAdded             → upsert by schedule.id in state.schedules
//
// 10. EvScheduleRemoved          → remove by delay_id from state.schedules
// 11. EvDelay (with cron)        → upsert as Schedule in state.schedules
// 12. EvCancelSchedule           → remove by delay_id from state.schedules
// 13. If new_lifecycle was set   → update lifecycle
// 14. Return nil (user _evolve handles it)
//
// For nil state, a fresh StateBase is returned with the appropriate field updated.
// For non-nil state, if the State implements SystemEvolver, the corresponding
// method is called to produce a new concrete state. If SystemEvolver is not
// implemented, nil is returned and the concrete Evolve() must handle the event.
func EvolveSystem(state State, event Event) State {
	// If state is non-nil and implements SystemEvolver, dispatch to it directly.
	// This is the primary path for concrete state types that properly implement
	// the SystemEvolver interface.
	if se, ok := state.(SystemEvolver); ok {
		switch e := event.(type) {
		case *EvSystemPause:
			return se.ApplyLifecycle(LifecyclePaused)
		case *EvSystemResume:
			return se.ApplyLifecycle(LifecycleActive)
		case *EvSystemCancel:
			return se.ApplyCancel()
		case *EvSubscriptionAdded:
			return se.ApplySubscriptionAdded(e.Sub)
		case *EvSubscriptionRemoved:
			return se.ApplySubscriptionRemoved(e.Sub)
		case *EvExternalSubscriptionAdded:
			return se.ApplyExternalSubscriptionAdded(e.Sub)
		case *EvExternalSubscriptionRemoved:
			return se.ApplyExternalSubscriptionRemoved(e.Topic)
		case *EvScheduleAdded:
			return se.ApplyScheduleUpsert(e.Schedule)
		case *EvScheduleRemoved:
			return se.ApplyScheduleRemove(e.DelayID)
		case *EvDelay:
			if e.IsCron() {
				sched := Schedule{
					ID:             e.ID,
					CronExpression: e.CronExpression,
					Timezone:       e.Timezone,
					NextCmd:        e.NextCmd,
				}
				return se.ApplyScheduleUpsert(sched)
			}
			// One-shot delays are NOT handled here - they go through DelayScheduler
			return nil
		case *EvCancelSchedule:
			return se.ApplyScheduleRemove(e.DelayID)
		}
		// Fall through to nil return for non-system events
		return nil
	}

	// Fallback path: state is nil OR does not implement SystemEvolver.
	// For nil state, construct fresh StateBase instances.
	// For non-nil state without SystemEvolver, return nil (concrete Evolve must handle).
	switch e := event.(type) {
	case *EvSystemPause:
		if state == nil {
			sb := NewStateBase()
			sb.Lifecycle = LifecyclePaused
			return sb
		}
		return nil

	case *EvSystemResume:
		if state == nil {
			sb := NewStateBase()
			sb.Lifecycle = LifecycleActive
			return sb
		}
		return nil

	case *EvSystemCancel:
		if state == nil {
			sb := NewStateBase()
			sb.Lifecycle = LifecycleCanceled
			sb.Schedules = make([]Schedule, 0)
			return sb
		}
		return nil

	case *EvContinueAsNew:
		// Return state unchanged - event log reset happens in repo
		return state

	case *EvSubscriptionAdded:
		if state == nil {
			sb := NewStateBase()
			sb.Subscriptions = append(sb.Subscriptions, e.Sub)
			return sb
		}
		return nil

	case *EvSubscriptionRemoved:
		if state == nil {
			return NewStateBase()
		}
		return nil

	case *EvExternalSubscriptionAdded:
		if state == nil {
			sb := NewStateBase()
			sb.ExternalSubscriptions = append(sb.ExternalSubscriptions, e.Sub)
			return sb
		}
		return nil

	case *EvExternalSubscriptionRemoved:
		if state == nil {
			return NewStateBase()
		}
		return nil

	case *EvScheduleAdded:
		if state == nil {
			sb := NewStateBase()
			sb.Schedules = append(sb.Schedules, e.Schedule)
			return sb
		}
		return nil

	case *EvScheduleRemoved:
		if state == nil {
			return NewStateBase()
		}
		return nil

	case *EvDelay:
		if e.IsCron() {
			if state == nil {
				sched := Schedule{
					ID:             e.ID,
					CronExpression: e.CronExpression,
					Timezone:       e.Timezone,
					NextCmd:        e.NextCmd,
				}
				sb := NewStateBase()
				sb.Schedules = append(sb.Schedules, sched)
				return sb
			}
		}
		// One-shot delays are NOT handled here - they go through DelayScheduler
		return nil

	case *EvCancelSchedule:
		if state == nil {
			return NewStateBase()
		}
		return nil
	}

	// Not a system/sync event - user Evolve handles it
	return nil
}

// =============================================================================
// EvolveAll - Fold Events Through Evolve
// =============================================================================

// EvolveAll folds a sequence of events through Evolve, one by one.
// For each event, it first tries EvolveSystem; if that returns nil,
// it calls the user's Evolve method.
func EvolveAll(wf Workflow, state State, events []Event) State {
	for _, event := range events {
		state = Evolve(wf, state, event)
	}
	return state
}

// Evolve applies a single event to state.
// First tries EvolveSystem for system/sync events.
// If EvolveSystem returns nil, calls the user's Evolve method.
func Evolve(wf Workflow, state State, event Event) State {
	if newState := EvolveSystem(state, event); newState != nil {
		return newState
	}
	return wf.Evolve(state, event)
}

// DecideAndEvolve calls Decide then EvolveAll on the result.
// Returns the new state and events, or a rejection.
func DecideAndEvolve(wf Workflow, state State, cmd Command) (State, []Event, *Rejection) {
	events, rejection := wf.Decide(state, cmd)
	if rejection != nil {
		return nil, nil, rejection
	}
	if len(events) == 0 {
		return state, nil, nil
	}
	newState := EvolveAll(wf, state, events)
	return newState, events, nil
}

// =============================================================================
// Action Types
// =============================================================================

// RetryPolicy defines how actions should be retried on failure.
type RetryPolicy struct {
	MaxRetries      int     `json:"max_retries"`
	BackoffStrategy string  `json:"backoff_strategy"` // "exponential" or "linear"
	BackoffFactor   float64 `json:"backoff_factor"`   // seconds (exp) or multiplier (linear)
	BackoffMax      string  `json:"backoff_max"`      // duration string, e.g., "60s"
	BackoffMin      string  `json:"backoff_min"`      // duration string, e.g., "1s"
	BackoffJitter   float64 `json:"backoff_jitter"`   // 0.0-1.0, applied as random(0, jitter * delay)
}

// DefaultRetryPolicy returns a RetryPolicy with sensible defaults.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:      3,
		BackoffStrategy: "exponential",
		BackoffFactor:   2.0,
		BackoffMax:      "60s",
		BackoffMin:      "1s",
		BackoffJitter:   0.5,
	}
}

// ActionContext is passed to ActOn on each execution (including retries).
// The Checkpoint is loaded from DB at start and saved at end.
// On retry, the checkpoint from the previous attempt is restored.
type ActionContext struct {
	WorkflowID  string         `json:"workflow_id"`
	EventNumber int            `json:"event_number"`
	Checkpoint  map[string]any `json:"checkpoint"`
	RetryCount  int            `json:"retry_count"`
	RetryPolicy RetryPolicy    `json:"retry_policy"`
}

// NewActionContext creates a new ActionContext with defaults.
func NewActionContext(workflowID string, eventNumber int) *ActionContext {
	return &ActionContext{
		WorkflowID:  workflowID,
		EventNumber: eventNumber,
		Checkpoint:  make(map[string]any),
		RetryPolicy: DefaultRetryPolicy(),
	}
}

// MergeCheckpoint merges data into the checkpoint.
func (ac *ActionContext) MergeCheckpoint(data map[string]any) {
	if ac.Checkpoint == nil {
		ac.Checkpoint = make(map[string]any)
	}
	for k, v := range data {
		ac.Checkpoint[k] = v
	}
}

// =============================================================================
// Action Yield Types
// =============================================================================

// ActionYield is the interface for values yielded by an action's ActOn method.
// The executor inspects the type to determine how to handle each yield.
type ActionYield interface {
	actionYieldType() int
}

const (
	actionYieldTypeCommand    = iota
	actionYieldTypeCheckpoint = iota
	actionYieldTypeTimeout    = iota
)

// CommandYield yields a command to be processed via repo.ProcessCommand().
type CommandYield struct {
	Cmd Command
}

func (y CommandYield) actionYieldType() int { return actionYieldTypeCommand }

// GetCommand returns the command.
func (y CommandYield) GetCommand() Command { return y.Cmd }

// CheckpointYield yields checkpoint data to be saved.
// If SaveNow is true, the checkpoint is persisted immediately.
// Otherwise, it's merged and persisted at the end of the action.
type CheckpointYield struct {
	Data    map[string]any
	SaveNow bool
}

func (y CheckpointYield) actionYieldType() int { return actionYieldTypeCheckpoint }

// GetCheckpoint returns the checkpoint yield data.
func (y CheckpointYield) GetCheckpoint() *CheckpointYield { return &y }

// ActionTimeout yields a timeout to be applied to all subsequent yields.
// The executor wraps ALL SUBSEQUENT yields in the appropriate timeout.
// If the remainder doesn't complete in time, the action retries.
type ActionTimeout struct {
	Seconds float64
}

func (y ActionTimeout) actionYieldType() int { return actionYieldTypeTimeout }

// GetTimeout returns the timeout value.
func (y ActionTimeout) GetTimeout() *ActionTimeout { return &y }

// =============================================================================
// Adapter Interface
// =============================================================================

// Adapter defines the interface for workflow side effects.
// Implementations provide ActOn for executing actions in response to events.
type Adapter interface {
	// ActOn executes side effects for the given event.
	// Returns a channel of ActionYield values (commands, checkpoints, timeouts).
	// The channel is closed when the action completes.
	// Returns an error if the action cannot be started.
	ActOn(ctx context.Context, event *ConsumedEvent, actionCtx *ActionContext) (<-chan ActionYield, error)

	// ToBeActOn returns true if this adapter should handle the given event.
	ToBeActOn(event *ConsumedEvent) bool

	// SyncDB is called in the same transaction as event insertion.
	// Used for denormalized DB updates that must be consistent with events.
	// Optional - return nil if no sync DB updates needed.
	SyncDB(ctx context.Context, workflowID string, oldState, newState State, events []Event) error
}

// =============================================================================
// ConsumedEvent (for Runner → Adapter)
// =============================================================================

// ConsumedEvent represents an event consumed from the stream.
// Used to pass events to adapters for side effect processing.
// The Event field is lazily deserialized - raw JSON is stored until first access.
type ConsumedEvent struct {
	GlobalID     int64
	WorkflowID   string
	WorkflowType string
	EventNo      int64
	EventType    string
	Event        Event          // Lazily deserialized
	At           string         // ISO8601 timestamp
	Metadata     map[string]any // From metadata column (includes workflow_tags)
}

// GetEvent returns the deserialized event, deserializing from raw if needed.
// This method handles lazy deserialization.
func (e *ConsumedEvent) GetEvent() Event {
	return e.Event
}

// GetEventTags returns tags from the event metadata.
func (e *ConsumedEvent) GetEventTags() []string {
	if e.Metadata == nil {
		return nil
	}
	tags, ok := e.Metadata["tags"]
	if !ok {
		return nil
	}
	switch t := tags.(type) {
	case []string:
		return t
	case []any:
		result := make([]string, len(t))
		for i, v := range t {
			result[i], _ = v.(string)
		}
		return result
	}
	return nil
}

// GetWorkflowTags returns workflow tags from the event metadata.
// These are injected at event creation time by the repo.
func (e *ConsumedEvent) GetWorkflowTags() []string {
	if e.Metadata == nil {
		return nil
	}
	tags, ok := e.Metadata["workflow_tags"]
	if !ok {
		return nil
	}
	switch t := tags.(type) {
	case []string:
		return t
	case []any:
		result := make([]string, len(t))
		for i, v := range t {
			result[i], _ = v.(string)
		}
		return result
	}
	return nil
}

// =============================================================================
// StoredState
// =============================================================================

// StoredState represents a workflow's state at a specific version.
// Used as the cache value in ephemeral storage.
type StoredState struct {
	ID      string
	Version int64
	State   State
}
