package testing

import (
	"fmt"
	"sort"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

// PendingDelay represents a delay that is waiting to fire in the test harness.
type PendingDelay struct {
	WorkflowID     string
	DelayID        string
	FireAt         time.Time
	NextCmd        model.Command
	CronExpression string // empty for one-shot delays
}

// WorkflowTestHarness provides an in-memory test harness for workflows.
// It runs decide→evolve without any database, NATS, or side effects.
type WorkflowTestHarness struct {
	workflow     model.Workflow
	states       map[string]model.State
	versions     map[string]int64
	delays       map[string][]PendingDelay
	simulatedNow time.Time
}

// NewWorkflowTestHarness creates a new in-memory test harness for the given workflow.
func NewWorkflowTestHarness(workflow model.Workflow) *WorkflowTestHarness {
	return &WorkflowTestHarness{
		workflow:     workflow,
		states:       make(map[string]model.State),
		versions:     make(map[string]int64),
		delays:       make(map[string][]PendingDelay),
		simulatedNow: time.Now().UTC(),
	}
}

// CreateNew creates a new workflow instance with the given ID and initial command.
// Returns the initial state and events, or AlreadyExists if the ID is already taken.
func (h *WorkflowTestHarness) CreateNew(workflowID string, cmd model.Command, tags []string) (model.State, []model.Event, error) {
	if _, exists := h.states[workflowID]; exists {
		return nil, nil, &model.AlreadyExists{}
	}

	events, rejection := h.workflow.Decide(nil, cmd)
	if rejection != nil {
		return nil, nil, rejection
	}

	// Record workflow existence even with no events
	if len(events) == 0 {
		h.states[workflowID] = nil
		h.versions[workflowID] = 0
		return nil, nil, nil
	}

	// Inject workflow tags into event metadata (mirrors repo behavior)
	if len(tags) > 0 {
		for _, ev := range events {
			model.SetWorkflowTagsInMetadata(ev, tags)
		}
	}

	state := model.EvolveAll(h.workflow, nil, events)
	h.states[workflowID] = state
	h.versions[workflowID] = int64(len(events))

	h.trackDelays(workflowID, events)

	return state, events, nil
}

// SendCommand sends a command to an existing workflow, running decide→evolve.
// Returns the new state and produced events.
// Returns WorkflowNotFound, WorkflowPaused, or WorkflowCanceled as appropriate.
func (h *WorkflowTestHarness) SendCommand(workflowID string, cmd model.Command) (model.State, []model.Event, error) {
	state, exists := h.states[workflowID]
	if !exists {
		return nil, nil, &model.WorkflowNotFound{
			ID:           workflowID,
			WorkflowType: h.workflow.Name(),
		}
	}

	if state != nil {
		switch state.GetLifecycle() {
		case model.LifecyclePaused:
			return nil, nil, &model.WorkflowPaused{}
		case model.LifecycleCanceled:
			return nil, nil, &model.WorkflowCanceled{}
		}
	}

	events, rejection := h.workflow.Decide(state, cmd)
	if rejection != nil {
		return state, nil, rejection
	}

	if len(events) == 0 {
		return state, nil, nil
	}

	newState := model.EvolveAll(h.workflow, state, events)
	h.states[workflowID] = newState
	h.versions[workflowID] += int64(len(events))

	h.trackDelays(workflowID, events)

	return newState, events, nil
}

// Simulate runs Decide without mutating any state.
// Returns the events that would be produced and any rejection.
func (h *WorkflowTestHarness) Simulate(workflowID string, cmd model.Command) ([]model.Event, *model.Rejection) {
	state := h.states[workflowID]
	events, rejection := h.workflow.Decide(state, cmd)
	return events, rejection
}

// AdvanceTime advances the simulated clock by delta and fires all pending delays
// whose FireAt <= simulated_now + delta. Delays are processed in chronological order.
// For cron delays, reschedules to the next fire time after the current simulated time.
// Returns all events produced by fired delays.
func (h *WorkflowTestHarness) AdvanceTime(delta time.Duration) ([]model.Event, error) {
	h.simulatedNow = h.simulatedNow.Add(delta)
	cutoff := h.simulatedNow

	// Collect all fireable delays across all workflows
	var toFire []PendingDelay
	for wfID, delays := range h.delays {
		var keep []PendingDelay
		for _, d := range delays {
			if !d.FireAt.After(cutoff) {
				toFire = append(toFire, d)
			} else {
				keep = append(keep, d)
			}
		}
		if len(keep) == 0 {
			delete(h.delays, wfID)
		} else {
			h.delays[wfID] = keep
		}
	}

	// Sort chronologically for deterministic processing
	sort.Slice(toFire, func(i, j int) bool {
		if toFire[i].FireAt.Equal(toFire[j].FireAt) {
			return toFire[i].DelayID < toFire[j].DelayID
		}
		return toFire[i].FireAt.Before(toFire[j].FireAt)
	})

	var allEvents []model.Event
	for _, pd := range toFire {
		events, err := h.fireDelay(pd)
		if err != nil {
			return allEvents, err
		}
		allEvents = append(allEvents, events...)
	}

	return allEvents, nil
}

// AssertSubscriptions checks that the workflow's current subscriptions match expected.
// Comparison is order-independent using Sub.Equals.
func (h *WorkflowTestHarness) AssertSubscriptions(workflowID string, expected []model.Sub) error {
	state, exists := h.states[workflowID]
	if !exists {
		return &model.WorkflowNotFound{
			ID:           workflowID,
			WorkflowType: h.workflow.Name(),
		}
	}

	actual := state.GetSubscriptions()

	if len(actual) != len(expected) {
		return fmt.Errorf("subscription count mismatch: got %d, want %d\n  actual:   %+v\n  expected: %+v",
			len(actual), len(expected), actual, expected)
	}

	// Order-independent matching using Equals
	matched := make([]bool, len(expected))
	for _, a := range actual {
		found := false
		for i, e := range expected {
			if !matched[i] && a.Equals(e) {
				matched[i] = true
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unexpected subscription not found in expected: %+v\n  all actual:   %+v\n  all expected: %+v",
				a, actual, expected)
		}
	}

	return nil
}

// GetState returns the current state of the workflow, or nil if it doesn't exist.
func (h *WorkflowTestHarness) GetState(workflowID string) model.State {
	return h.states[workflowID]
}

// WorkflowIDs returns all known workflow IDs in sorted order.
func (h *WorkflowTestHarness) WorkflowIDs() []string {
	ids := make([]string, 0, len(h.states))
	for id := range h.states {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// PendingDelays returns all pending delays across all workflows, sorted by FireAt.
func (h *WorkflowTestHarness) PendingDelays() []PendingDelay {
	var all []PendingDelay
	for _, delays := range h.delays {
		all = append(all, delays...)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].FireAt.Equal(all[j].FireAt) {
			return all[i].DelayID < all[j].DelayID
		}
		return all[i].FireAt.Before(all[j].FireAt)
	})
	return all
}

// SetSimulatedNow sets the simulated clock to a specific time.
// Use this at the start of tests for deterministic time-dependent behavior.
func (h *WorkflowTestHarness) SetSimulatedNow(t time.Time) {
	h.simulatedNow = t.UTC()
}

// SimulatedNow returns the current simulated time.
func (h *WorkflowTestHarness) SimulatedNow() time.Time {
	return h.simulatedNow
}

// fireDelay fires a single pending delay by constructing an EvDelayComplete,
// routing it through EventToCmd → Decide → Evolve.
// For cron delays, reschedules after firing unless the delay was cancelled.
func (h *WorkflowTestHarness) fireDelay(pd PendingDelay) ([]model.Event, error) {
	state, exists := h.states[pd.WorkflowID]
	if !exists {
		return nil, nil
	}

	// Skip cancelled workflows
	if state != nil && state.GetLifecycle() == model.LifecycleCanceled {
		return nil, nil
	}

	evDelayComplete := &model.EvDelayComplete{
		DelayID: pd.DelayID,
		At:      pd.FireAt,
		NextCmd: pd.NextCmd,
	}

	cmd := h.workflow.EventToCmd(evDelayComplete)
	if cmd == nil {
		return nil, nil
	}

	events, rejection := h.workflow.Decide(state, cmd)
	if rejection != nil {
		return nil, fmt.Errorf("delay %s fire rejected for workflow %s: %s",
			pd.DelayID, pd.WorkflowID, rejection.Msg)
	}

	if len(events) == 0 {
		return nil, nil
	}

	newState := model.EvolveAll(h.workflow, state, events)
	h.states[pd.WorkflowID] = newState
	h.versions[pd.WorkflowID] += int64(len(events))

	// Track new delays and handle cancellations from the produced events
	h.trackDelays(pd.WorkflowID, events)

	// Reschedule cron delays unless they were cancelled by the events
	if pd.CronExpression != "" {
		cancelled := false
		for _, ev := range events {
			switch e := ev.(type) {
			case *model.EvCancelSchedule:
				if e.DelayID == pd.DelayID {
					cancelled = true
				}
			case *model.EvScheduleRemoved:
				if e.DelayID == pd.DelayID {
					cancelled = true
				}
			}
			if cancelled {
				break
			}
		}
		if cancelled {
			return events, nil
		}

		nextFire, err := computeNextCronFire(pd.CronExpression, "", h.simulatedNow)
		if err != nil {
			return events, fmt.Errorf("failed to reschedule cron delay %s: %w", pd.DelayID, err)
		}

		newPD := PendingDelay{
			WorkflowID:     pd.WorkflowID,
			DelayID:        pd.DelayID,
			FireAt:         nextFire,
			NextCmd:        pd.NextCmd,
			CronExpression: pd.CronExpression,
		}
		h.delays[pd.WorkflowID] = append(h.delays[pd.WorkflowID], newPD)
	}

	return events, nil
}

// trackDelays extracts EvDelay events to track as pending, and removes
// delays that were cancelled or removed by EvCancelSchedule/EvScheduleRemoved events.
func (h *WorkflowTestHarness) trackDelays(workflowID string, events []model.Event) {
	for _, ev := range events {
		// Handle delay cancellation/removal first
		switch e := ev.(type) {
		case *model.EvCancelSchedule:
			h.removePendingDelay(workflowID, e.DelayID)
			continue
		case *model.EvScheduleRemoved:
			h.removePendingDelay(workflowID, e.DelayID)
			continue
		}

		delayEv, ok := ev.(*model.EvDelay)
		if !ok {
			continue
		}

		pd := PendingDelay{
			WorkflowID: workflowID,
			DelayID:    delayEv.ID,
			NextCmd:    delayEv.NextCmd,
		}

		if delayEv.IsCron() {
			pd.CronExpression = delayEv.CronExpression
			fireAt, err := computeNextCronFire(delayEv.CronExpression, delayEv.Timezone, h.simulatedNow)
			if err != nil {
				// Invalid cron expression — skip rather than panic
				continue
			}
			pd.FireAt = fireAt
		} else {
			pd.FireAt = delayEv.DelayUntil
		}

		// Replace semantics: if a delay with the same ID already exists, update it
		existing := h.delays[workflowID]
		replaced := false
		for i, d := range existing {
			if d.DelayID == delayEv.ID {
				existing[i] = pd
				replaced = true
				break
			}
		}
		if !replaced {
			h.delays[workflowID] = append(existing, pd)
		}
	}
}

// removePendingDelay removes a pending delay by ID for a given workflow.
func (h *WorkflowTestHarness) removePendingDelay(workflowID, delayID string) {
	existing := h.delays[workflowID]
	keep := make([]PendingDelay, 0, len(existing))
	for _, d := range existing {
		if d.DelayID != delayID {
			keep = append(keep, d)
		}
	}
	if len(keep) == 0 {
		delete(h.delays, workflowID)
	} else {
		h.delays[workflowID] = keep
	}
}

// computeNextCronFire computes the next fire time for a cron expression starting from `from`.
// Uses robfig/cron/v3 with standard 5-field parsing (minute, hour, dom, month, dow).
func computeNextCronFire(cronExpression, timezoneName string, from time.Time) (time.Time, error) {
	var loc *time.Location
	if timezoneName != "" {
		var err error
		loc, err = time.LoadLocation(timezoneName)
		if err != nil {
			loc = time.UTC
		}
	} else {
		loc = time.UTC
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(cronExpression)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", cronExpression, err)
	}

	now := from.In(loc)
	return schedule.Next(now).In(loc), nil
}
