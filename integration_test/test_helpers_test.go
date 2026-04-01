//go:build realdeps

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/repo"
)

// =============================================================================
// Test Workflow Types
// =============================================================================

type testWorkflow struct {
	name string
}

type testState struct {
	model.StateBase
	Counter int `json:"counter"`
}

func (s *testState) Copy() model.State {
	return &testState{
		StateBase: *s.StateBase.Copy().(*model.StateBase),
		Counter:   s.Counter,
	}
}

// Events

type testIncremented struct {
	model.EventBase
	Amount int `json:"amount"`
}

func (e *testIncremented) Type() string { return "test_incremented" }

type testSubAdded struct {
	model.EventBase
	Sub model.Sub `json:"sub"`
}

func (e *testSubAdded) Type() string { return "test_sub_added" }

type testFinalEvent struct {
	model.EventBase
}

func (e *testFinalEvent) Type() string { return "test_final" }

// Commands

type testIncrementCmd struct {
	Amount int
}

func (c *testIncrementCmd) CommandType() string { return "increment" }

type testNoOpCmd struct{}

func (c *testNoOpCmd) CommandType() string { return "noop" }

type testAddSubCmd struct {
	Sub model.Sub
}

func (c *testAddSubCmd) CommandType() string { return "add_sub" }

type testFinalCmd struct{}

func (c *testFinalCmd) CommandType() string { return "finalize" }

// Workflow implementation

func (w *testWorkflow) Name() string       { return w.name }
func (w *testWorkflow) SchemaVersion() int { return 1 }
func (w *testWorkflow) Upcast(et string, sv int, rd map[string]any) map[string]any {
	return rd
}

func (w *testWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	switch c := cmd.(type) {
	case *testIncrementCmd:
		if c.Amount <= 0 {
			return nil, &model.Rejection{Msg: "amount must be positive"}
		}
		return []model.Event{&testIncremented{Amount: c.Amount}}, nil
	case *testNoOpCmd:
		return nil, nil
	case *testAddSubCmd:
		return []model.Event{
			&testSubAdded{Sub: c.Sub},
			&model.EvSubscriptionAdded{Sub: c.Sub},
		}, nil
	case *testFinalCmd:
		return []model.Event{&testFinalEvent{}}, nil
	}
	return nil, &model.Rejection{Msg: "unknown command"}
}

func (w *testWorkflow) Evolve(state model.State, event model.Event) model.State {
	// When EvolveSystem handled a nil-state system event, it returns *StateBase.
	// Wrap it in *testState so subsequent user events see the correct concrete type.
	if sb, ok := state.(*model.StateBase); ok {
		state = &testState{StateBase: *sb}
	}

	ts, ok := state.(*testState)
	if !ok {
		ts = &testState{StateBase: *model.NewStateBase()}
	}
	result := ts.Copy().(*testState)

	switch e := event.(type) {
	case *testIncremented:
		result.Counter += e.Amount
	case *testFinalEvent:
		// No state change needed; IsFinalEvent handles cache removal.
	case *testSubAdded:
		// User event — no StateBase side effects.
	}

	return result
}

// SystemEvolver implementation for testState.
// Uses StateBase mutation helpers on a Copy(), which is the canonical pattern.
// These methods are called by EvolveSystem when state is non-nil.

func (s *testState) ApplyLifecycle(lifecycle model.LifecycleState) model.State {
	result := s.Copy().(*testState)
	result.StateBase.SetLifecycle(lifecycle)
	return result
}

func (s *testState) ApplyCancel() model.State {
	result := s.Copy().(*testState)
	result.StateBase.SetCanceled()
	return result
}

func (s *testState) ApplySubscriptionAdded(sub model.Sub) model.State {
	result := s.Copy().(*testState)
	result.StateBase.AddSubscription(sub)
	return result
}

func (s *testState) ApplySubscriptionRemoved(sub model.Sub) model.State {
	result := s.Copy().(*testState)
	result.StateBase.RemoveSubscription(sub)
	return result
}

func (s *testState) ApplyExternalSubscriptionAdded(sub model.ExternalSub) model.State {
	result := s.Copy().(*testState)
	result.StateBase.AddExternalSubscription(sub)
	return result
}

func (s *testState) ApplyExternalSubscriptionRemoved(topic string) model.State {
	result := s.Copy().(*testState)
	result.StateBase.RemoveExternalSubscription(topic)
	return result
}

func (s *testState) ApplyScheduleUpsert(schedule model.Schedule) model.State {
	result := s.Copy().(*testState)
	result.StateBase.UpsertSchedule(schedule)
	return result
}

func (s *testState) ApplyScheduleRemove(delayID string) model.State {
	result := s.Copy().(*testState)
	result.StateBase.RemoveSchedule(delayID)
	return result
}

func (w *testWorkflow) EventToCmd(e model.Event) model.Command { return nil }

func (w *testWorkflow) IsFinalEvent(e model.Event) bool {
	_, ok := e.(*testFinalEvent)
	return ok
}

// =============================================================================
// Event Parser
// =============================================================================

func testEventParser(eventType string, raw json.RawMessage) (model.Event, error) {
	switch eventType {
	case "test_incremented":
		var e testIncremented
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "test_sub_added":
		var e testSubAdded
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "test_final":
		return &testFinalEvent{}, nil
	case "subscription_added":
		var e model.EvSubscriptionAdded
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "subscription_removed":
		var e model.EvSubscriptionRemoved
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "system_pause":
		var e model.EvSystemPause
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "system_resume":
		var e model.EvSystemResume
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "system_cancel":
		var e model.EvSystemCancel
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "system_continue_as_new":
		var e model.EvContinueAsNew
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil

	// Delay events
	case "delay":
		var e model.EvDelay
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "delay_complete":
		var e model.EvDelayComplete
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil

	// Schedule sync events
	case "schedule_added":
		var e model.EvScheduleAdded
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "schedule_removed":
		var e model.EvScheduleRemoved
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "cancel_schedule":
		var e model.EvCancelSchedule
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil

	// External subscription sync events
	case "external_subscription_added":
		var e model.EvExternalSubscriptionAdded
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "external_subscription_removed":
		var e model.EvExternalSubscriptionRemoved
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil

	// Action sync events
	case "action_cancel":
		var e model.EvActionCancel
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return &e, nil
	}
	return nil, fmt.Errorf("unknown event type: %s", eventType)
}

// =============================================================================
// Table Cleanup
// =============================================================================

// CleanTables truncates all workflow tables. Call this in t.Cleanup()
// or at the start of each test for isolation.
func CleanTables(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tables := []string{
		"outbox",
		"workflow_activities",
		"workflow_search_attributes",
		"delay_schedules",
		"external_subscriptions",
		"subscriptions",
		"workflow_metadata",
		"snapshots",
		"stored_events",
		"offsets",
		"scaling_operations",
	}

	for _, table := range tables {
		_, _ = GetTestPool(t).Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table))
	}
}

// CleanWorkflow removes all data for a specific workflow ID.
func CleanWorkflow(t *testing.T, workflowID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool := GetTestPool(t)

	tables := []string{
		"outbox",
		"workflow_activities",
		"workflow_search_attributes",
		"delay_schedules",
		"external_subscriptions",
		"subscriptions",
		"workflow_metadata",
		"snapshots",
		"stored_events",
	}

	for _, table := range tables {
		_, _ = pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE workflow_id = $1", table), workflowID)
	}
}

// =============================================================================
// Pool Access
// =============================================================================

// GetTestPool returns the shared test database pool.
// The pool is created once in TestMain and reused across tests.
func GetTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	testPoolMu.Lock()
	defer testPoolMu.Unlock()
	if testPool == nil {
		t.Fatal("test pool not initialized - TestMain may not have run")
	}
	return testPool
}

// =============================================================================
// Repo Helpers
// =============================================================================

// NewTestRepo creates a repo.Repo with sensible defaults for testing.
func NewTestRepo(t *testing.T, workflow model.Workflow, opts ...repo.RepoOption) *repo.Repo {
	t.Helper()
	pool := GetTestPool(t)
	es := repo.NewInProcessEuphemeralStorage(1000)

	defaultOpts := []repo.RepoOption{
		repo.WithEventParser(testEventParser),
	}
	defaultOpts = append(defaultOpts, opts...)

	return repo.NewRepo(pool, workflow.Name(), workflow, es, defaultOpts...)
}

// NewTestRepoWithSnapshots creates a repo with snapshotting enabled.
func NewTestRepoWithSnapshots(t *testing.T, workflow model.Workflow, interval int, opts ...repo.RepoOption) *repo.Repo {
	t.Helper()
	opts = append([]repo.RepoOption{repo.WithSnapshotInterval(interval)}, opts...)
	return NewTestRepo(t, workflow, opts...)
}

// NewTestRepoWithCache returns both the repo and its ephemeral storage for cache inspection.
func NewTestRepoWithCache(t *testing.T, workflow model.Workflow, opts ...repo.RepoOption) (*repo.Repo, repo.EphemeralStorage) {
	t.Helper()
	pool := GetTestPool(t)
	es := repo.NewInProcessEuphemeralStorage(1000)

	defaultOpts := []repo.RepoOption{
		repo.WithEventParser(testEventParser),
	}
	defaultOpts = append(defaultOpts, opts...)

	r := repo.NewRepo(pool, workflow.Name(), workflow, es, defaultOpts...)
	return r, es
}

// =============================================================================
// Workflow ID Generation
// =============================================================================

// UniqueID generates a unique workflow ID for testing.
func UniqueID(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), os.Getpid())
}

// =============================================================================
// Database Query Helpers
// =============================================================================

// CountEvents returns the number of events for a workflow.
func CountEvents(t *testing.T, workflowID string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err := GetTestPool(t).QueryRow(ctx,
		"SELECT COUNT(*) FROM stored_events WHERE workflow_id = $1",
		workflowID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count events: %v", err)
	}
	return count
}

// GetEventVersions returns all event versions for a workflow.
func GetEventVersions(t *testing.T, workflowID string) []int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := GetTestPool(t).Query(ctx,
		"SELECT workflow_version FROM stored_events WHERE workflow_id = $1 ORDER BY workflow_version",
		workflowID,
	)
	if err != nil {
		t.Fatalf("failed to query event versions: %v", err)
	}
	defer rows.Close()

	var versions []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("failed to scan event version: %v", err)
		}
		versions = append(versions, v)
	}
	return versions
}

// GetEventTypes returns all event types for a workflow, ordered by version.
func GetEventTypes(t *testing.T, workflowID string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := GetTestPool(t).Query(ctx,
		"SELECT event_type FROM stored_events WHERE workflow_id = $1 ORDER BY workflow_version",
		workflowID,
	)
	if err != nil {
		t.Fatalf("failed to query event types: %v", err)
	}
	defer rows.Close()

	var types []string
	for rows.Next() {
		var et string
		if err := rows.Scan(&et); err != nil {
			t.Fatalf("failed to scan event type: %v", err)
		}
		types = append(types, et)
	}
	return types
}

// GetSnapshotVersion returns the version of the snapshot for a workflow.
// Returns 0 if no snapshot exists.
func GetSnapshotVersion(t *testing.T, workflowID string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var version int64
	err := GetTestPool(t).QueryRow(ctx,
		"SELECT version FROM snapshots WHERE workflow_id = $1",
		workflowID,
	).Scan(&version)
	if err != nil {
		return 0
	}
	return version
}

// GetSnapshotState returns the raw JSON state from the snapshot table.
func GetSnapshotState(t *testing.T, workflowID string) json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var state json.RawMessage
	err := GetTestPool(t).QueryRow(ctx,
		"SELECT state FROM snapshots WHERE workflow_id = $1",
		workflowID,
	).Scan(&state)
	if err != nil {
		return nil
	}
	return state
}

// GetWorkflowTags returns the tags for a workflow from the metadata table.
func GetWorkflowTags(t *testing.T, workflowID string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var tags []string
	err := GetTestPool(t).QueryRow(ctx,
		"SELECT tags FROM workflow_metadata WHERE workflow_id = $1",
		workflowID,
	).Scan(&tags)
	if err != nil {
		return nil
	}
	return tags
}

// CountSubscriptions returns the number of subscriptions for a workflow.
func CountSubscriptions(t *testing.T, workflowID string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err := GetTestPool(t).QueryRow(ctx,
		"SELECT COUNT(*) FROM subscriptions WHERE workflow_id = $1",
		workflowID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count subscriptions: %v", err)
	}
	return count
}

// CountDelaySchedules returns the number of delay schedules for a workflow.
func CountDelaySchedules(t *testing.T, workflowID string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err := GetTestPool(t).QueryRow(ctx,
		"SELECT COUNT(*) FROM delay_schedules WHERE workflow_id = $1",
		workflowID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count delay schedules: %v", err)
	}
	return count
}

// WorkflowExists checks if a workflow has any events in the database.
func WorkflowExists(t *testing.T, workflowID string) bool {
	t.Helper()
	return CountEvents(t, workflowID) > 0
}

// IsWorkflowInCache checks if a workflow is present in the ephemeral storage.
func IsWorkflowInCache(t *testing.T, es repo.EphemeralStorage, workflowID string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	state, err := es.GetState(ctx, workflowID)
	if err != nil {
		t.Fatalf("failed to check cache: %v", err)
	}
	return state != nil
}

// GetCachedVersion returns the cached version for a workflow, or -1 if not cached.
func GetCachedVersion(t *testing.T, es repo.EphemeralStorage, workflowID string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	state, err := es.GetState(ctx, workflowID)
	if err != nil {
		t.Fatalf("failed to get cached state: %v", err)
	}
	if state == nil {
		return -1
	}
	return state.Version
}
