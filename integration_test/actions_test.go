//go:build realdeps

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/actions"
	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/repo"
)

// testActionAdapter implements model.Adapter for action executor tests.
type testActionAdapter struct {
	actOn func(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error)
	toBe  func(event *model.ConsumedEvent) bool
}

func (a *testActionAdapter) ActOn(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
	if a.actOn != nil {
		return a.actOn(ctx, event, ac)
	}
	ch := make(chan model.ActionYield)
	close(ch)
	return ch, nil
}

func (a *testActionAdapter) ToBeActOn(event *model.ConsumedEvent) bool {
	if a.toBe != nil {
		return a.toBe(event)
	}
	return true
}

func (a *testActionAdapter) SyncDB(ctx context.Context, workflowID string, oldState, newState model.State, events []model.Event) error {
	return nil
}

func getActivityStatus(t *testing.T, workflowID string, eventNo int) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var st string
	err := GetTestPool(t).QueryRow(ctx,
		`SELECT status FROM workflow_activities WHERE workflow_id = $1 AND event_number = $2`,
		workflowID, eventNo,
	).Scan(&st)
	if err != nil {
		t.Fatalf("query activity: %v", err)
	}
	return st
}

func getActivityRetryCount(t *testing.T, workflowID string, eventNo int) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	err := GetTestPool(t).QueryRow(ctx,
		`SELECT retry_count FROM workflow_activities WHERE workflow_id = $1 AND event_number = $2`,
		workflowID, eventNo,
	).Scan(&n)
	if err != nil {
		t.Fatalf("query retry_count: %v", err)
	}
	return n
}

func getActivityCheckpointJSON(t *testing.T, workflowID string, eventNo int) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var raw []byte
	err := GetTestPool(t).QueryRow(ctx,
		`SELECT checkpoint FROM workflow_activities WHERE workflow_id = $1 AND event_number = $2`,
		workflowID, eventNo,
	).Scan(&raw)
	if err != nil {
		t.Fatalf("query checkpoint: %v", err)
	}
	return raw
}

func waitActivityStatus(t *testing.T, workflowID string, eventNo int, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st := getActivityStatus(t, workflowID, eventNo)
		if st == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("activity status want %q, got %q", want, getActivityStatus(t, workflowID, eventNo))
}

func newActionTestRepo(t *testing.T, name string) *repo.Repo {
	t.Helper()
	return NewTestRepo(t, &testWorkflow{name: name})
}

func TestActionExecutor_BasicExecution(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "act_" + UniqueID(t, "basic")
	r := newActionTestRepo(t, wfName)
	ctx := context.Background()
	wid := UniqueID(t, "ab")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 2}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	adapter := &testActionAdapter{
		toBe: func(e *model.ConsumedEvent) bool { return e.EventType == "test_incremented" },
		actOn: func(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
			ch := make(chan model.ActionYield, 1)
			ch <- model.CommandYield{Cmd: &testIncrementCmd{Amount: 1}}
			close(ch)
			return ch, nil
		},
	}
	ex := actions.NewActionExecutor(GetTestPool(t), adapter, r, actions.ExecutorOptions{MaxRetries: 3})
	defer ex.Stop()

	ev := &model.ConsumedEvent{
		WorkflowID:   wid,
		WorkflowType: wfName,
		EventNo:      1,
		EventType:    "test_incremented",
		Event:        &testIncremented{Amount: 2},
	}
	if err := ex.ExecuteAction(ev); err != nil {
		t.Fatalf("ExecuteAction: %v", err)
	}
	waitActivityStatus(t, wid, 1, actions.ActivityStatusCompleted, 3*time.Second)
	if v := CountEvents(t, wid); v != 2 {
		t.Errorf("expected 2 events, got %d", v)
	}
}

func TestActionExecutor_Checkpoint(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "act_" + UniqueID(t, "ckpt")
	r := newActionTestRepo(t, wfName)
	ctx := context.Background()
	wid := UniqueID(t, "ck")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	adapter := &testActionAdapter{
		toBe: func(e *model.ConsumedEvent) bool { return true },
		actOn: func(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
			ch := make(chan model.ActionYield, 1)
			ch <- model.CheckpointYield{Data: map[string]any{"k": "v"}, SaveNow: true}
			close(ch)
			return ch, nil
		},
	}
	ex := actions.NewActionExecutor(GetTestPool(t), adapter, r, actions.ExecutorOptions{})
	defer ex.Stop()

	ev := &model.ConsumedEvent{WorkflowID: wid, WorkflowType: wfName, EventNo: 1, EventType: "test_incremented", Event: &testIncremented{Amount: 1}}
	if err := ex.ExecuteAction(ev); err != nil {
		t.Fatalf("ExecuteAction: %v", err)
	}
	waitActivityStatus(t, wid, 1, actions.ActivityStatusCompleted, 3*time.Second)
	raw := getActivityCheckpointJSON(t, wid, 1)
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("checkpoint json: %v", err)
	}
	if m["k"] != "v" {
		t.Errorf("expected checkpoint k=v, got %v", m)
	}
}

func TestActionExecutor_Retry(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "act_" + UniqueID(t, "retry")
	r := newActionTestRepo(t, wfName)
	ctx := context.Background()
	wid := UniqueID(t, "ar")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	var calls int
	adapter := &testActionAdapter{
		toBe: func(e *model.ConsumedEvent) bool { return true },
		actOn: func(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
			ch := make(chan model.ActionYield, 1)
			calls++
			if calls == 1 {
				ch <- model.CommandYield{Cmd: &unknownCmd{}}
			} else {
				ch <- model.CommandYield{Cmd: &testNoOpCmd{}}
			}
			close(ch)
			return ch, nil
		},
	}
	ex := actions.NewActionExecutor(GetTestPool(t), adapter, r, actions.ExecutorOptions{
		MaxRetries: 5,
	})
	defer ex.Stop()

	ev := &model.ConsumedEvent{WorkflowID: wid, WorkflowType: wfName, EventNo: 1, EventType: "test_incremented", Event: &testIncremented{Amount: 1}}
	if err := ex.ExecuteAction(ev); err != nil {
		t.Fatalf("ExecuteAction: %v", err)
	}
	waitActivityStatus(t, wid, 1, actions.ActivityStatusCompleted, 5*time.Second)
	if calls < 2 {
		t.Errorf("expected adapter called at least twice, got %d", calls)
	}
	if getActivityRetryCount(t, wid, 1) < 1 {
		t.Errorf("expected retry_count >= 1, got %d", getActivityRetryCount(t, wid, 1))
	}
}

func TestActionExecutor_MaxRetries(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "act_" + UniqueID(t, "maxr")
	r := newActionTestRepo(t, wfName)
	ctx := context.Background()
	wid := UniqueID(t, "am")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	adapter := &testActionAdapter{
		toBe: func(e *model.ConsumedEvent) bool { return true },
		actOn: func(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
			ch := make(chan model.ActionYield, 1)
			ch <- model.CommandYield{Cmd: &unknownCmd{}}
			close(ch)
			return ch, nil
		},
	}
	ex := actions.NewActionExecutor(GetTestPool(t), adapter, r, actions.ExecutorOptions{MaxRetries: 2})
	defer ex.Stop()

	ev := &model.ConsumedEvent{WorkflowID: wid, WorkflowType: wfName, EventNo: 1, EventType: "test_incremented", Event: &testIncremented{Amount: 1}}
	if err := ex.ExecuteAction(ev); err != nil {
		t.Fatalf("ExecuteAction: %v", err)
	}
	waitActivityStatus(t, wid, 1, actions.ActivityStatusFailed, 5*time.Second)
}

func TestActionExecutor_AlreadyRunning(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "act_" + UniqueID(t, "run")
	r := newActionTestRepo(t, wfName)
	ctx := context.Background()
	wid := UniqueID(t, "al")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	unblock := make(chan struct{})
	adapter := &testActionAdapter{
		toBe: func(e *model.ConsumedEvent) bool { return true },
		actOn: func(actCtx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
			ch := make(chan model.ActionYield)
			go func() {
				<-unblock
				close(ch)
			}()
			return ch, nil
		},
	}
	ex := actions.NewActionExecutor(GetTestPool(t), adapter, r, actions.ExecutorOptions{})
	defer ex.Stop()

	ev := &model.ConsumedEvent{WorkflowID: wid, WorkflowType: wfName, EventNo: 1, EventType: "test_incremented", Event: &testIncremented{Amount: 1}}
	if err := ex.ExecuteAction(ev); err != nil {
		t.Fatalf("first ExecuteAction: %v", err)
	}
	err = ex.ExecuteAction(ev)
	if !errors.Is(err, actions.ErrAlreadyRunning) {
		t.Fatalf("second ExecuteAction: want ErrAlreadyRunning, got %v", err)
	}
	close(unblock)
}

func TestActionExecutor_CancelPendingViaAPI(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "act_" + UniqueID(t, "can")
	r := newActionTestRepo(t, wfName)
	adapter := &testActionAdapter{}
	ex := actions.NewActionExecutor(GetTestPool(t), adapter, r, actions.ExecutorOptions{})
	defer ex.Stop()

	wid := UniqueID(t, "cp")
	ctx := context.Background()
	_, err := GetTestPool(t).Exec(ctx,
		`INSERT INTO workflow_activities (workflow_id, event_number, workflow_type, event_type, status, retry_count, max_retries, checkpoint, retry_policy)
		 VALUES ($1, $2, $3, $4, $5, 0, 3, '{}', '{}')`,
		wid, 1, wfName, "test_incremented", actions.ActivityStatusPending,
	)
	if err != nil {
		t.Fatalf("insert activity: %v", err)
	}

	ex.CancelWorkflowActions(wid, nil)
	waitActivityStatus(t, wid, 1, actions.ActivityStatusCancelled, 2*time.Second)
}
