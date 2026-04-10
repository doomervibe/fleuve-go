//go:build realdeps

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/runner"
)

func countWorkflowEventsOfType(t *testing.T, workflowID, eventType string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	err := GetTestPool(t).QueryRow(ctx,
		`SELECT COUNT(*) FROM stored_events WHERE workflow_id = $1 AND event_type = $2`,
		workflowID, eventType,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}
	return n
}

func TestSubscriptionHorizon_BackfillAndForward(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })

	wfName := "sh_" + UniqueID(t, "wf")
	wf := &testWorkflow{
		name: wfName,
		eventToCmd: func(e model.Event) model.Command {
			if e, ok := e.(*testIncremented); ok {
				return &testIncrementCmd{Amount: e.Amount}
			}
			return nil
		},
	}
	r := NewTestRepo(t, wf)
	ctx := context.Background()

	emitID := UniqueID(t, "emit")
	subID := UniqueID(t, "sub")

	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, emitID, nil)
	if err != nil {
		t.Fatalf("CreateNew emitter: %v", err)
	}
	_, _, rej := r.ProcessCommand(ctx, emitID, &testIncrementCmd{Amount: 2})
	if rej != nil {
		t.Fatalf("ProcessCommand emitter: %v", rej)
	}

	h := int64(1)
	_, err = r.CreateNew(ctx, &testAddSubCmd{Sub: model.Sub{
		WorkflowID: emitID, EventType: "test_incremented", AfterEmitterEventNo: &h,
	}}, subID, nil)
	if err != nil {
		t.Fatalf("CreateNew subscriber: %v", err)
	}

	_, _, rej = r.ProcessCommand(ctx, emitID, &testIncrementCmd{Amount: 3})
	if rej != nil {
		t.Fatalf("ProcessCommand emitter v3: %v", rej)
	}

	rctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := runner.Config{
		Pool:         GetTestPool(t),
		WorkflowType: wfName,
		Workflow:     wf,
		Repo:         r,
		EventParser:  testEventParser,
		ReaderName:   "sh_runner_" + wfName,
	}
	rn := runner.New(cfg)
	go func() { _ = rn.Start(rctx) }()
	defer rn.Stop()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if countWorkflowEventsOfType(t, subID, "test_incremented") >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := countWorkflowEventsOfType(t, subID, "test_incremented"); n != 2 {
		t.Fatalf("subscriber test_incremented events: got %d, want 2 (backfill v2 + forward v3)", n)
	}
}
