package model

// LifecycleState represents the workflow lifecycle status.
type LifecycleState string

const (
	LifecycleActive   LifecycleState = "active"
	LifecyclePaused   LifecycleState = "paused"
	LifecycleCanceled LifecycleState = "cancelled"
)

// =============================================================================
// Sub - Internal Subscription
// =============================================================================

// Sub represents an internal cross-workflow subscription.
// Matching logic:
//   - WorkflowID: exact match or "*" for any workflow
//   - EventType: exact match or "*" for any event type
//   - Tags (OR): if non-empty, at least ONE tag must be in event_tags ∪ workflow_tags
//   - TagsAll (AND): if non-empty, ALL tags must be in event_tags ∪ workflow_tags
//   - Both Tags and TagsAll can be empty (no tag filtering)
//   - Both can be specified simultaneously (combined with AND between groups)
type Sub struct {
	EventType  string   `json:"event_type"`
	WorkflowID string   `json:"workflow_id"`
	Tags       []string `json:"tags,omitempty"`
	TagsAll    []string `json:"tags_all,omitempty"`
}

// MatchesTags checks if this subscription matches the given event and workflow tags.
// Implements the OR/AND matching logic per the spec.
func (s *Sub) MatchesTags(eventTags, workflowTags []string) bool {
	// Build unified tag set from event and workflow tags
	allTags := make(map[string]struct{}, len(eventTags)+len(workflowTags))
	for _, t := range eventTags {
		allTags[t] = struct{}{}
	}
	for _, t := range workflowTags {
		allTags[t] = struct{}{}
	}

	// Tags: OR logic - at least ONE must match
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

	// TagsAll: AND logic - ALL must match
	if len(s.TagsAll) > 0 {
		for _, tag := range s.TagsAll {
			if _, ok := allTags[tag]; !ok {
				return false
			}
		}
	}

	return true
}

// MatchesWorkflowAndEvent checks if this subscription matches the given workflow ID and event type.
// Supports wildcard "*" for both fields.
func (s *Sub) MatchesWorkflowAndEvent(workflowID, eventType string) bool {
	if s.WorkflowID != "*" && s.WorkflowID != workflowID {
		return false
	}
	if s.EventType != "*" && s.EventType != eventType {
		return false
	}
	return true
}

// Equals returns true if all four fields match exactly.
// Used for subscription removal matching.
func (s *Sub) Equals(other Sub) bool {
	if s.WorkflowID != other.WorkflowID {
		return false
	}
	if s.EventType != other.EventType {
		return false
	}
	if !stringSlicesEqual(s.Tags, other.Tags) {
		return false
	}
	if !stringSlicesEqual(s.TagsAll, other.TagsAll) {
		return false
	}
	return true
}

// Copy returns a deep copy of the Sub.
func (s *Sub) Copy() Sub {
	return Sub{
		EventType:  s.EventType,
		WorkflowID: s.WorkflowID,
		Tags:       append([]string(nil), s.Tags...),
		TagsAll:    append([]string(nil), s.TagsAll...),
	}
}

// =============================================================================
// ExternalSub - External Subscription
// =============================================================================

// ExternalSub represents a subscription to an external NATS topic.
type ExternalSub struct {
	Topic string `json:"topic"`
}

// Copy returns a deep copy of the ExternalSub.
func (s *ExternalSub) Copy() ExternalSub {
	return ExternalSub{Topic: s.Topic}
}

// =============================================================================
// Schedule - Cron Schedule in State
// =============================================================================

// Schedule represents a cron-based delay schedule stored in workflow state.
// This is the source of truth; synced to delay_schedule table by the repo.
type Schedule struct {
	ID             string  `json:"id"`
	CronExpression string  `json:"cron_expression"`
	Timezone       string  `json:"timezone,omitempty"`
	NextCmd        Command `json:"next_cmd"`
}

// Copy returns a deep copy of the Schedule.
// Note: NextCmd is not deep-copied as commands are typically immutable.
func (s *Schedule) Copy() Schedule {
	return Schedule{
		ID:             s.ID,
		CronExpression: s.CronExpression,
		Timezone:       s.Timezone,
		NextCmd:        s.NextCmd,
	}
}

// =============================================================================
// StateBase - Base State for All Workflows
// =============================================================================

// StateBase contains fields that EVERY workflow state MUST embed.
// The Lifecycle field is managed by system events but can be read by user code.
// Subscriptions, ExternalSubscriptions, and Schedules are managed by sync events
// in _evolve_system().
type StateBase struct {
	Subscriptions         []Sub          `json:"subscriptions"`
	ExternalSubscriptions []ExternalSub  `json:"external_subscriptions,omitempty"`
	Lifecycle             LifecycleState `json:"lifecycle"`
	Schedules             []Schedule     `json:"schedules,omitempty"`
}

// NewStateBase creates a new StateBase with default values.
func NewStateBase() *StateBase {
	return &StateBase{
		Subscriptions:         make([]Sub, 0),
		ExternalSubscriptions: make([]ExternalSub, 0),
		Lifecycle:             LifecycleActive,
		Schedules:             make([]Schedule, 0),
	}
}

// GetSubscriptions returns the subscriptions slice.
func (s *StateBase) GetSubscriptions() []Sub {
	if s == nil {
		return nil
	}
	return s.Subscriptions
}

// GetExternalSubscriptions returns the external subscriptions slice.
func (s *StateBase) GetExternalSubscriptions() []ExternalSub {
	if s == nil {
		return nil
	}
	return s.ExternalSubscriptions
}

// GetLifecycle returns the current lifecycle state.
func (s *StateBase) GetLifecycle() LifecycleState {
	if s == nil {
		return LifecycleActive
	}
	return s.Lifecycle
}

// GetSchedules returns the schedules slice.
func (s *StateBase) GetSchedules() []Schedule {
	if s == nil {
		return nil
	}
	return s.Schedules
}

// Copy returns a deep copy of the StateBase.
func (s *StateBase) Copy() State {
	if s == nil {
		return NewStateBase()
	}
	sb := &StateBase{
		Lifecycle: s.Lifecycle,
	}
	if s.Subscriptions != nil {
		sb.Subscriptions = make([]Sub, len(s.Subscriptions))
		for i, sub := range s.Subscriptions {
			sb.Subscriptions[i] = sub.Copy()
		}
	} else {
		sb.Subscriptions = make([]Sub, 0)
	}
	if s.ExternalSubscriptions != nil {
		sb.ExternalSubscriptions = make([]ExternalSub, len(s.ExternalSubscriptions))
		for i, ext := range s.ExternalSubscriptions {
			sb.ExternalSubscriptions[i] = ext.Copy()
		}
	} else {
		sb.ExternalSubscriptions = make([]ExternalSub, 0)
	}
	if s.Schedules != nil {
		sb.Schedules = make([]Schedule, len(s.Schedules))
		for i, sched := range s.Schedules {
			sb.Schedules[i] = sched.Copy()
		}
	} else {
		sb.Schedules = make([]Schedule, 0)
	}
	return sb
}

// =============================================================================
// State Interface
// =============================================================================

// State is the interface that all workflow state types must implement.
// Every workflow state MUST embed StateBase and implement Copy() to return
// a proper copy of the full state (including any user-defined fields).
type State interface {
	GetSubscriptions() []Sub
	GetExternalSubscriptions() []ExternalSub
	GetLifecycle() LifecycleState
	GetSchedules() []Schedule
	Copy() State
}

// SystemEvolver is an optional interface that State implementations SHOULD implement
// to properly handle system and sync event state mutations during EvolveSystem.
//
// PROBLEM: Go's State is an interface (unlike Python's generic classmethod), so
// EvolveSystem cannot construct a new concrete state via model_copy(update={...}).
// The apply*() helper functions return nil for non-nil state, causing Evolve() to
// fall through to the user's Evolve() method with system events.
//
// SOLUTION: If a State implements SystemEvolver, EvolveSystem calls the appropriate
// method instead of returning nil. If not implemented, the existing fallback behavior
// applies (concrete Evolve must handle system events).
//
// All concrete workflow state types SHOULD implement this interface to ensure correct
// state reconstruction during event replay (loadStateTx → EvolveAll → Evolve).
//
// Canonical implementation pattern (using the StateBase mutation helpers):
//
//	func (s *MyState) ApplyLifecycle(lifecycle model.LifecycleState) model.State {
//	    result := s.Copy().(*MyState)
//	    result.SetLifecycle(lifecycle)
//	    return result
//	}
//
//	func (s *MyState) ApplySubscriptionAdded(sub model.Sub) model.State {
//	    result := s.Copy().(*MyState)
//	    result.AddSubscription(sub)
//	    return result
//	}
type SystemEvolver interface {
	State
	ApplyLifecycle(lifecycle LifecycleState) State
	ApplyCancel() State
	ApplySubscriptionAdded(sub Sub) State
	ApplySubscriptionRemoved(sub Sub) State
	ApplyExternalSubscriptionAdded(sub ExternalSub) State
	ApplyExternalSubscriptionRemoved(topic string) State
	ApplyScheduleUpsert(schedule Schedule) State
	ApplyScheduleRemove(delayID string) State
}

// =============================================================================
// Helper Functions
// =============================================================================

// stringSlicesEqual compares two string slices for equality.
// Nil and empty slices are considered equal.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if (a == nil) != (b == nil) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// FindSubIndex finds the index of a matching subscription in the slice.
// Returns -1 if not found. Uses Equals for exact matching.
func FindSubIndex(subs []Sub, target Sub) int {
	for i, s := range subs {
		if s.Equals(target) {
			return i
		}
	}
	return -1
}

// FindScheduleIndex finds the index of a schedule by ID.
// Returns -1 if not found.
func FindScheduleIndex(schedules []Schedule, id string) int {
	for i, s := range schedules {
		if s.ID == id {
			return i
		}
	}
	return -1
}

// FindExternalSubIndex finds the index of an external subscription by topic.
// Returns -1 if not found.
func FindExternalSubIndex(subs []ExternalSub, topic string) int {
	for i, s := range subs {
		if s.Topic == topic {
			return i
		}
	}
	return -1
}

// RemoveSub removes a subscription at the given index, preserving order.
func RemoveSub(subs []Sub, index int) []Sub {
	if index < 0 || index >= len(subs) {
		return subs
	}
	return append(subs[:index], subs[index+1:]...)
}

// RemoveSchedule removes a schedule at the given index, preserving order.
func RemoveSchedule(schedules []Schedule, index int) []Schedule {
	if index < 0 || index >= len(schedules) {
		return schedules
	}
	return append(schedules[:index], schedules[index+1:]...)
}

// RemoveExternalSub removes an external subscription at the given index, preserving order.
func RemoveExternalSub(subs []ExternalSub, index int) []ExternalSub {
	if index < 0 || index >= len(subs) {
		return subs
	}
	return append(subs[:index], subs[index+1:]...)
}

// =============================================================================
// StateBase Mutation Helpers
//
// These methods mutate a StateBase IN PLACE. They are designed to be called on a
// COPY of the concrete state (after Copy()) when implementing the SystemEvolver
// interface. Never call these on the original state.
// =============================================================================

// SetLifecycle sets the lifecycle field.
func (sb *StateBase) SetLifecycle(lifecycle LifecycleState) {
	sb.Lifecycle = lifecycle
}

// SetCanceled sets lifecycle to cancelled and clears all schedules.
func (sb *StateBase) SetCanceled() {
	sb.Lifecycle = LifecycleCanceled
	sb.Schedules = make([]Schedule, 0)
}

// AddSubscription appends a subscription to the subscriptions slice.
func (sb *StateBase) AddSubscription(sub Sub) {
	sb.Subscriptions = append(sb.Subscriptions, sub)
}

// RemoveSubscription removes the first subscription matching sub (using Equals).
func (sb *StateBase) RemoveSubscription(sub Sub) {
	for i, s := range sb.Subscriptions {
		if s.Equals(sub) {
			sb.Subscriptions = append(sb.Subscriptions[:i], sb.Subscriptions[i+1:]...)
			return
		}
	}
}

// AddExternalSubscription appends an external subscription.
func (sb *StateBase) AddExternalSubscription(sub ExternalSub) {
	sb.ExternalSubscriptions = append(sb.ExternalSubscriptions, sub)
}

// RemoveExternalSubscription removes the first external subscription with the given topic.
func (sb *StateBase) RemoveExternalSubscription(topic string) {
	for i, s := range sb.ExternalSubscriptions {
		if s.Topic == topic {
			sb.ExternalSubscriptions = append(sb.ExternalSubscriptions[:i], sb.ExternalSubscriptions[i+1:]...)
			return
		}
	}
}

// UpsertSchedule adds a schedule if its ID doesn't exist, or updates it if it does.
func (sb *StateBase) UpsertSchedule(schedule Schedule) {
	idx := FindScheduleIndex(sb.Schedules, schedule.ID)
	if idx >= 0 {
		sb.Schedules[idx] = schedule
	} else {
		sb.Schedules = append(sb.Schedules, schedule)
	}
}

// RemoveSchedule removes the first schedule with the given delayID.
func (sb *StateBase) RemoveSchedule(delayID string) {
	idx := FindScheduleIndex(sb.Schedules, delayID)
	if idx >= 0 {
		sb.Schedules = append(sb.Schedules[:idx], sb.Schedules[idx+1:]...)
	}
}
