//go:build realdeps

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/repo"
)

// unknownCmd is a command that no workflow handles, used to trigger rejections.
type unknownCmd struct{}

func (c *unknownCmd) CommandType() string { return "unknown_command" }

// =============================================================================
// CreateNew Tests
// =============================================================================

func TestCreateNew(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "create")

	ctx := context.Background()
	ss, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	if ss == nil {
		t.Fatal("expected non-nil StoredState")
	}
	if ss.ID != id {
		t.Errorf("expected ID %q, got %q", id, ss.ID)
	}
	if ss.Version != 1 {
		t.Errorf("expected version 1, got %d", ss.Version)
	}

	if !WorkflowExists(t, id) {
		t.Error("expected WorkflowExists to return true")
	}

	if n := CountEvents(t, id); n != 1 {
		t.Errorf("expected 1 event, got %d", n)
	}

	versions := GetEventVersions(t, id)
	if len(versions) != 1 || versions[0] != 1 {
		t.Errorf("expected versions [1], got %v", versions)
	}
}

func TestCreateNew_AlreadyExists(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "dup")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("first CreateNew failed: %v", err)
	}

	_, err = r.CreateNew(ctx, &testIncrementCmd{Amount: 3}, id, nil)
	if err == nil {
		t.Fatal("expected error for duplicate creation")
	}

	var ae *model.AlreadyExists
	if !errors.As(err, &ae) {
		t.Errorf("expected AlreadyExists error, got %T: %v", err, err)
	}
}

func TestCreateNew_WithTags(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "tags")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, []string{"tag1", "tag2"})
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	tags := GetWorkflowTags(t, id)
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(tags))
	}

	found := make(map[string]bool)
	for _, tag := range tags {
		found[tag] = true
	}
	if !found["tag1"] || !found["tag2"] {
		t.Errorf("expected tags [tag1, tag2], got %v", tags)
	}
}

func TestCreateNew_WithSyncDBHandler(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	var called bool
	handler := func(ctx context.Context, tx pgx.Tx, workflowID string, oldState, newState model.State, events []model.Event) error {
		called = true
		return nil
	}

	r := NewTestRepo(t, &testWorkflow{name: "test"}, repo.WithSyncDBHandler(handler))
	id := UniqueID(t, "synchandler")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	if !called {
		t.Error("expected SyncDBHandler to be called")
	}
}

// =============================================================================
// ProcessCommand Tests
// =============================================================================

func TestProcessCommand(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "process")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	ss, events, rej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 3})
	if rej != nil {
		t.Fatalf("ProcessCommand rejected: %v", rej)
	}
	if ss == nil {
		t.Fatal("expected non-nil StoredState")
	}
	if ss.Version != 2 {
		t.Errorf("expected version 2, got %d", ss.Version)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ts, ok := ss.State.(*testState)
	if !ok {
		t.Fatal("expected *testState")
	}
	if ts.Counter != 8 {
		t.Errorf("expected counter 8, got %d", ts.Counter)
	}
}

func TestProcessCommand_EmptyDecide(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "noop")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	ss, events, rej := r.ProcessCommand(ctx, id, &testNoOpCmd{})
	if rej != nil {
		t.Fatalf("ProcessCommand rejected: %v", rej)
	}
	if events != nil {
		t.Errorf("expected nil events, got %v", events)
	}

	if n := CountEvents(t, id); n != 1 {
		t.Errorf("expected 1 event (unchanged), got %d", n)
	}

	ts, ok := ss.State.(*testState)
	if !ok {
		t.Fatal("expected *testState")
	}
	if ts.Counter != 5 {
		t.Errorf("expected counter 5 (unchanged), got %d", ts.Counter)
	}
}

func TestProcessCommand_Rejection(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "reject")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	ss, events, rej := r.ProcessCommand(ctx, id, &unknownCmd{})
	if ss != nil {
		t.Error("expected nil StoredState on rejection")
	}
	if events != nil {
		t.Error("expected nil events on rejection")
	}
	if rej == nil {
		t.Fatal("expected rejection")
	}
	if rej.Msg != "unknown command" {
		t.Errorf("expected rejection message %q, got %q", "unknown command", rej.Msg)
	}
}

func TestProcessCommand_ConcurrentCommands(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "concurrent")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, rej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 1})
			if rej != nil {
				errCh <- fmt.Errorf("concurrent ProcessCommand rejected: %v", rej)
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	if n := CountEvents(t, id); n != 11 {
		t.Errorf("expected 11 events (1 create + 10 increments), got %d", n)
	}
}

func TestProcessCommand_PausedWorkflow(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "paused")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	_, rej := r.PauseWorkflow(ctx, id, "test pause")
	if rej != nil {
		t.Fatalf("PauseWorkflow failed: %v", rej)
	}

	_, _, cmdRej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 3})
	if cmdRej == nil {
		t.Fatal("expected rejection when processing command on paused workflow")
	}
	expected := (&model.WorkflowPaused{}).Error()
	if cmdRej.Msg != expected {
		t.Errorf("expected rejection message %q, got %q", expected, cmdRej.Msg)
	}
}

func TestProcessCommand_CancelledWorkflow(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "cancelled")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	_, rej := r.CancelWorkflow(ctx, id, "test cancel")
	if rej != nil {
		t.Fatalf("CancelWorkflow failed: %v", rej)
	}

	_, _, cmdRej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 3})
	if cmdRej == nil {
		t.Fatal("expected rejection when processing command on cancelled workflow")
	}
	expected := (&model.WorkflowCanceled{}).Error()
	if cmdRej.Msg != expected {
		t.Errorf("expected rejection message %q, got %q", expected, cmdRej.Msg)
	}
}

// =============================================================================
// Pause/Resume Tests
// =============================================================================

func TestPauseResume(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "pauseresume")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	// Pause
	ss, rej := r.PauseWorkflow(ctx, id, "test pause")
	if rej != nil {
		t.Fatalf("PauseWorkflow failed: %v", rej)
	}
	ts, ok := ss.State.(*testState)
	if !ok {
		t.Fatal("expected *testState")
	}
	if ts.Lifecycle != model.LifecyclePaused {
		t.Errorf("expected lifecycle paused, got %s", ts.Lifecycle)
	}

	// Resume
	ss, rej = r.ResumeWorkflow(ctx, id)
	if rej != nil {
		t.Fatalf("ResumeWorkflow failed: %v", rej)
	}
	ts, ok = ss.State.(*testState)
	if !ok {
		t.Fatal("expected *testState")
	}
	if ts.Lifecycle != model.LifecycleActive {
		t.Errorf("expected lifecycle active, got %s", ts.Lifecycle)
	}

	// Verify event types include system_pause and system_resume
	types := GetEventTypes(t, id)
	foundPause, foundResume := false, false
	for _, et := range types {
		if et == "system_pause" {
			foundPause = true
		}
		if et == "system_resume" {
			foundResume = true
		}
	}
	if !foundPause {
		t.Error("expected system_pause event")
	}
	if !foundResume {
		t.Error("expected system_resume event")
	}
}

func TestPauseWorkflow_AlreadyPaused(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "doublepause")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	_, rej := r.PauseWorkflow(ctx, id, "first pause")
	if rej != nil {
		t.Fatalf("first PauseWorkflow failed: %v", rej)
	}

	_, rej = r.PauseWorkflow(ctx, id, "second pause")
	if rej == nil {
		t.Fatal("expected rejection on second pause")
	}
	if rej.Msg != "workflow is already paused" {
		t.Errorf("expected rejection message %q, got %q", "workflow is already paused", rej.Msg)
	}
}

// =============================================================================
// Cancel Tests
// =============================================================================

func TestCancelWorkflow(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r, es := NewTestRepoWithCache(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "cancel")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	// Verify workflow is in cache after creation
	if !IsWorkflowInCache(t, es, id) {
		t.Error("expected workflow to be in cache after creation")
	}

	ss, rej := r.CancelWorkflow(ctx, id, "test cancel")
	if rej != nil {
		t.Fatalf("CancelWorkflow failed: %v", rej)
	}

	ts, ok := ss.State.(*testState)
	if !ok {
		t.Fatal("expected *testState")
	}
	if ts.Lifecycle != model.LifecycleCanceled {
		t.Errorf("expected lifecycle cancelled, got %s", ts.Lifecycle)
	}

	// Verify cache is evicted
	if IsWorkflowInCache(t, es, id) {
		t.Error("expected workflow to be evicted from cache after cancel")
	}

	// Verify delay schedules cleaned up
	if n := CountDelaySchedules(t, id); n != 0 {
		t.Errorf("expected 0 delay schedules, got %d", n)
	}
}

func TestCancelWorkflow_AlreadyCancelled(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "doublecancel")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	_, rej := r.CancelWorkflow(ctx, id, "first cancel")
	if rej != nil {
		t.Fatalf("first CancelWorkflow failed: %v", rej)
	}

	_, rej = r.CancelWorkflow(ctx, id, "second cancel")
	if rej == nil {
		t.Fatal("expected rejection on second cancel")
	}
	if rej.Msg != "workflow is already cancelled" {
		t.Errorf("expected rejection message %q, got %q", "workflow is already cancelled", rej.Msg)
	}
}

func TestCancelWorkflow_NotFinalEvent(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "cancelnotfinal")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	_, rej := r.CancelWorkflow(ctx, id, "test cancel")
	if rej != nil {
		t.Fatalf("CancelWorkflow failed: %v", rej)
	}

	// Cancel should NOT be treated as final — workflow still has events in DB
	if !WorkflowExists(t, id) {
		t.Error("expected WorkflowExists to return true after cancel")
	}

	// State should still be loadable via ReplayWorkflow
	ss, err := r.ReplayWorkflow(ctx, id, 1)
	if err != nil {
		t.Fatalf("ReplayWorkflow failed: %v", err)
	}
	if ss == nil {
		t.Fatal("expected non-nil StoredState from ReplayWorkflow")
	}
	ts, ok := ss.State.(*testState)
	if !ok {
		t.Fatal("expected *testState")
	}
	if ts.Counter != 5 {
		t.Errorf("expected counter 5, got %d", ts.Counter)
	}
}

// =============================================================================
// ContinueAsNew Tests
// =============================================================================

func TestContinueAsNew(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepoWithSnapshots(t, &testWorkflow{name: "test"}, 5)
	id := UniqueID(t, "continue")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	// Increment several times to accumulate state
	for i := 0; i < 3; i++ {
		_, _, rej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 1})
		if rej != nil {
			t.Fatalf("ProcessCommand %d failed: %v", i, rej)
		}
	}
	// At this point: version=4, counter=8

	// Call ContinueAsNew without newCmd
	ss, events, rej := r.ContinueAsNew(ctx, id, nil, "test reason", "test")
	if rej != nil {
		t.Fatalf("ContinueAsNew rejected: %v", rej)
	}
	if ss == nil {
		t.Fatal("expected non-nil StoredState")
	}
	if events != nil {
		t.Errorf("expected nil events, got %v", events)
	}

	// Version should reset to 1
	if ss.Version != 1 {
		t.Errorf("expected version 1 after ContinueAsNew, got %d", ss.Version)
	}

	// State should be preserved
	ts, ok := ss.State.(*testState)
	if !ok {
		t.Fatal("expected *testState")
	}
	if ts.Counter != 8 {
		t.Errorf("expected counter 8 (preserved), got %d", ts.Counter)
	}

	// Old events should be deleted, only EvContinueAsNew remains
	if n := CountEvents(t, id); n != 1 {
		t.Errorf("expected 1 event (EvContinueAsNew), got %d", n)
	}

	// Snapshot should exist at the pre-reset version
	snapVersion := GetSnapshotVersion(t, id)
	if snapVersion != 4 {
		t.Errorf("expected snapshot at version 4, got %d", snapVersion)
	}
}

func TestContinueAsNew_WithNewCommand(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepoWithSnapshots(t, &testWorkflow{name: "test"}, 5)
	id := UniqueID(t, "continuecmd")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	// Increment a couple times
	for i := 0; i < 2; i++ {
		_, _, rej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 1})
		if rej != nil {
			t.Fatalf("ProcessCommand %d failed: %v", i, rej)
		}
	}
	// At this point: version=3, counter=7

	// ContinueAsNew with a new command
	newCmd := &testIncrementCmd{Amount: 10}
	ss, events, rej := r.ContinueAsNew(ctx, id, newCmd, "test reason", "test")
	if rej != nil {
		t.Fatalf("ContinueAsNew rejected: %v", rej)
	}
	if ss == nil {
		t.Fatal("expected non-nil StoredState")
	}

	// Version should be 2 (EvContinueAsNew at v1, testIncremented at v2)
	if ss.Version != 2 {
		t.Errorf("expected version 2, got %d", ss.Version)
	}

	// State should include preserved state + new command's effect
	ts, ok := ss.State.(*testState)
	if !ok {
		t.Fatal("expected *testState")
	}
	if ts.Counter != 17 { // 7 (preserved) + 10 (new cmd)
		t.Errorf("expected counter 17, got %d", ts.Counter)
	}

	// Events should contain the new command's events
	if len(events) != 1 {
		t.Fatalf("expected 1 event from new command, got %d", len(events))
	}
	if _, ok := events[0].(*testIncremented); !ok {
		t.Error("expected testIncremented event")
	}
}

func TestContinueAsNew_WithoutSnapshots(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"}) // No snapshot interval
	id := UniqueID(t, "continue-nosnap")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	_, _, rej := r.ContinueAsNew(ctx, id, nil, "test reason", "test")
	if rej == nil {
		t.Fatal("expected rejection when ContinueAsNew without snapshotting")
	}
	if rej.Msg != "continue_as_new requires snapshotting to be enabled" {
		t.Errorf("expected rejection about snapshotting, got %q", rej.Msg)
	}
}

// =============================================================================
// Snapshot Tests
// =============================================================================

func TestSnapshots(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepoWithSnapshots(t, &testWorkflow{name: "test"}, 3)
	id := UniqueID(t, "snapshots")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}
	// version=1, no snapshot (1 % 3 != 0)

	// Process 5 increment commands
	for i := 0; i < 5; i++ {
		_, _, rej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 1})
		if rej != nil {
			t.Fatalf("ProcessCommand %d failed: %v", i, rej)
		}
	}
	// versions: 1, 2, 3, 4, 5, 6
	// snapshots at: 3, 6

	snapVersion := GetSnapshotVersion(t, id)
	if snapVersion != 6 {
		t.Errorf("expected snapshot at version 6, got %d", snapVersion)
	}

	// Verify snapshot state
	snapState := GetSnapshotState(t, id)
	if snapState == nil {
		t.Fatal("expected non-nil snapshot state")
	}
	var stateMap map[string]any
	if err := json.Unmarshal(snapState, &stateMap); err != nil {
		t.Fatalf("failed to unmarshal snapshot state: %v", err)
	}
	counter, ok := stateMap["counter"].(float64)
	if !ok {
		t.Fatal("expected counter in snapshot state")
	}
	if int(counter) != 10 { // 5 + 5*1
		t.Errorf("expected counter 10 in snapshot, got %d", int(counter))
	}
}

func TestSnapshot_AfterCancel(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepoWithSnapshots(t, &testWorkflow{name: "test"}, 2)
	id := UniqueID(t, "snapcancel")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}
	// version=1, no snapshot (1 % 2 != 0)

	_, rej := r.CancelWorkflow(ctx, id, "test cancel")
	if rej != nil {
		t.Fatalf("CancelWorkflow failed: %v", rej)
	}
	// version=2, snapshot should be created (2 % 2 == 0)

	snapVersion := GetSnapshotVersion(t, id)
	if snapVersion != 2 {
		t.Errorf("expected snapshot at version 2 after cancel, got %d", snapVersion)
	}
}

// =============================================================================
// Subscription Tests
// =============================================================================

func TestSubscriptionAdded(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "subadd")

	ctx := context.Background()
	sub := model.Sub{
		EventType:  "some_event",
		WorkflowID: "*",
		Tags:       []string{"tag1"},
	}
	_, err := r.CreateNew(ctx, &testAddSubCmd{Sub: sub}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	if n := CountSubscriptions(t, id); n != 1 {
		t.Errorf("expected 1 subscription, got %d", n)
	}
}

func TestSubscriptionLifecycle(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "sublifecycle")

	ctx := context.Background()
	sub := model.Sub{
		EventType:  "some_event",
		WorkflowID: "*",
	}
	_, err := r.CreateNew(ctx, &testAddSubCmd{Sub: sub}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	// Process another command — subscriptions should persist
	_, _, rej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 3})
	if rej != nil {
		t.Fatalf("ProcessCommand failed: %v", rej)
	}

	if n := CountSubscriptions(t, id); n != 1 {
		t.Errorf("expected 1 subscription to persist, got %d", n)
	}
}

// =============================================================================
// FinalEvent / Cache Removal Tests
// =============================================================================

func TestFinalEvent_RemovesFromCache(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r, es := NewTestRepoWithCache(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "finalcache")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	// Verify in cache after creation
	if !IsWorkflowInCache(t, es, id) {
		t.Error("expected workflow to be in cache after creation")
	}

	// Process final command
	_, _, rej := r.ProcessCommand(ctx, id, &testFinalCmd{})
	if rej != nil {
		t.Fatalf("ProcessCommand failed: %v", rej)
	}

	// Verify NOT in cache after final event
	if IsWorkflowInCache(t, es, id) {
		t.Error("expected workflow to be removed from cache after final event")
	}
}

func TestCancelNotFinal(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r, es := NewTestRepoWithCache(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "cancelnotfinal2")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	_, rej := r.CancelWorkflow(ctx, id, "test cancel")
	if rej != nil {
		t.Fatalf("CancelWorkflow failed: %v", rej)
	}

	// Workflow should still exist in DB
	if !WorkflowExists(t, id) {
		t.Error("expected WorkflowExists to return true after cancel")
	}

	// Should NOT be in cache (cancel always evicts)
	if IsWorkflowInCache(t, es, id) {
		t.Error("expected workflow to NOT be in cache after cancel")
	}
}

// =============================================================================
// WorkflowNotFound Tests
// =============================================================================

func TestProcessCommand_NotFound(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})

	ctx := context.Background()
	ss, events, rej := r.ProcessCommand(ctx, "nonexistent-workflow-id", &testIncrementCmd{Amount: 5})
	if ss != nil {
		t.Error("expected nil StoredState")
	}
	if events != nil {
		t.Error("expected nil events")
	}
	if rej == nil {
		t.Fatal("expected rejection")
	}
	// The rejection message should contain "workflow not found"
	if !strings.Contains(rej.Msg, "workflow not found") {
		t.Errorf("expected rejection to mention 'workflow not found', got %q", rej.Msg)
	}
}

// =============================================================================
// ReplayWorkflow Tests
// =============================================================================

func TestReplayWorkflow(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r := NewTestRepo(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "replay")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}
	// version=1, counter=5

	// Increment 3 times
	for i := 0; i < 3; i++ {
		_, _, rej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 3})
		if rej != nil {
			t.Fatalf("ProcessCommand %d failed: %v", i, rej)
		}
	}
	// version=4, counter=14

	// Replay from version 2
	ss, err := r.ReplayWorkflow(ctx, id, 2)
	if err != nil {
		t.Fatalf("ReplayWorkflow failed: %v", err)
	}
	if ss == nil {
		t.Fatal("expected non-nil StoredState")
	}

	// Should replay events from version 2, 3, 4
	if ss.Version != 4 {
		t.Errorf("expected version 4, got %d", ss.Version)
	}

	ts, ok := ss.State.(*testState)
	if !ok {
		t.Fatal("expected *testState")
	}
	// Base state at version 1: counter=5
	// Events from v2, v3, v4: +3, +3, +3
	// Expected: counter=14
	if ts.Counter != 14 {
		t.Errorf("expected counter 14, got %d", ts.Counter)
	}
}

// =============================================================================
// EphemeralStorage / Caching Tests
// =============================================================================

func TestCacheHit(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r, es := NewTestRepoWithCache(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "cachehit")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	// Verify in cache
	if !IsWorkflowInCache(t, es, id) {
		t.Fatal("expected workflow to be in cache after creation")
	}
	if v := GetCachedVersion(t, es, id); v != 1 {
		t.Errorf("expected cached version 1, got %d", v)
	}

	// Process command
	_, _, rej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 3})
	if rej != nil {
		t.Fatalf("ProcessCommand failed: %v", rej)
	}

	// Verify cache updated
	if !IsWorkflowInCache(t, es, id) {
		t.Fatal("expected workflow to still be in cache")
	}
	if v := GetCachedVersion(t, es, id); v != 2 {
		t.Errorf("expected cached version 2, got %d", v)
	}
}

func TestCacheStaleDetected(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	r, es := NewTestRepoWithCache(t, &testWorkflow{name: "test"})
	id := UniqueID(t, "stalecache")

	ctx := context.Background()
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew failed: %v", err)
	}

	// Verify cache has version 1
	if v := GetCachedVersion(t, es, id); v != 1 {
		t.Fatalf("expected cached version 1, got %d", v)
	}

	// Simulate another writer: directly insert an event into DB at version 2
	pool := GetTestPool(t)
	eventJSON, _ := json.Marshal(&testIncremented{Amount: 3})
	_, err = pool.Exec(ctx,
		`INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
		 VALUES ($1, $2, NULL, $3, $4, $5, $6, NOW(), '{}', false)`,
		id, 2, "test_incremented", "test", 1, eventJSON,
	)
	if err != nil {
		t.Fatalf("failed to insert event directly: %v", err)
	}

	// Now ProcessCommand should detect stale cache (DB version 2 > cache version 1)
	// and reload from DB. After reload: state at version 2 = counter 5+3=8.
	// Then process increment by 1: counter = 9, version = 3.
	ss, _, rej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 1})
	if rej != nil {
		t.Fatalf("ProcessCommand failed: %v", rej)
	}
	if ss == nil {
		t.Fatal("expected non-nil StoredState")
	}

	if ss.Version != 3 {
		t.Errorf("expected version 3, got %d", ss.Version)
	}

	ts, ok := ss.State.(*testState)
	if !ok {
		t.Fatal("expected *testState")
	}
	if ts.Counter != 9 {
		t.Errorf("expected counter 9, got %d", ts.Counter)
	}
}
