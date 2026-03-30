package actions

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

type yieldRepo struct {
	mu sync.Mutex
	n  int
	wg sync.WaitGroup
}

func (r *yieldRepo) ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection) {
	r.mu.Lock()
	r.n++
	r.mu.Unlock()
	r.wg.Done()
	return &model.StoredState{ID: id}, nil, nil
}

type yieldAdapter struct {
	yields []model.ActionYield
}

func (a *yieldAdapter) ActOn(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
	ch := make(chan model.ActionYield, len(a.yields))
	for _, y := range a.yields {
		ch <- y
	}
	close(ch)
	return ch, nil
}

func (a *yieldAdapter) ToBeActOn(event *model.ConsumedEvent) bool { return true }

func (a *yieldAdapter) SyncDB(ctx context.Context, workflowID string, oldState, newState model.State, events []model.Event) error {
	return nil
}

func TestActionExecutorCommandYieldCallsRepo(t *testing.T) {
	repo := &yieldRepo{}
	repo.wg.Add(1)
	adp := &yieldAdapter{yields: []model.ActionYield{model.CommandYield{Cmd: struct{ K string }{"x"}}}}
	ex := NewActionExecutor(adp, repo)

	ctx := context.Background()
	if err := ex.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ex.Stop() }()

	ev := &model.ConsumedEvent{WorkflowID: "w1", EventNo: 7}
	if err := ex.ExecuteAction(ctx, ev); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		repo.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for ProcessCommand")
	}
	repo.mu.Lock()
	n := repo.n
	repo.mu.Unlock()
	if n != 1 {
		t.Fatalf("ProcessCommand calls: %d", n)
	}
}

type countingErrAdapter struct {
	actOnCalls atomic.Int32
}

func (a *countingErrAdapter) ActOn(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
	a.actOnCalls.Add(1)
	return nil, errors.New("acton fails")
}

func (a *countingErrAdapter) ToBeActOn(event *model.ConsumedEvent) bool { return true }

func (a *countingErrAdapter) SyncDB(ctx context.Context, workflowID string, oldState, newState model.State, events []model.Event) error {
	return nil
}

func TestActionExecutorWithMaxRetriesZeroSingleAttempt(t *testing.T) {
	adp := &countingErrAdapter{}
	var wg sync.WaitGroup
	wg.Add(1)
	var gotWf string
	var gotEv int64
	ex := NewActionExecutor(adp, &yieldRepo{},
		WithMaxRetries(0),
		WithOnActionFailed(func(workflowID string, eventNumber int64, err error) {
			gotWf = workflowID
			gotEv = eventNumber
			wg.Done()
		}),
	)

	ctx := context.Background()
	if err := ex.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ex.Stop() }()

	if err := ex.ExecuteAction(ctx, &model.ConsumedEvent{WorkflowID: "wf1", EventNo: 42}); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for onActionFailed")
	}

	if adp.actOnCalls.Load() != 1 {
		t.Fatalf("ActOn calls: %d, want 1", adp.actOnCalls.Load())
	}
	if gotWf != "wf1" || gotEv != 42 {
		t.Fatalf("onActionFailed workflow/event: %q %d, want wf1 42", gotWf, gotEv)
	}
}

func TestActionExecutorToBeActOnDelegates(t *testing.T) {
	adp := &yieldAdapter{}
	ex := NewActionExecutor(adp, &yieldRepo{})
	if !ex.ToBeActOn(&model.ConsumedEvent{}) {
		t.Fatal("expected true")
	}
}

func TestActionExecutorCancelWorkflowActionsNoPanic(t *testing.T) {
	ex := NewActionExecutor(&yieldAdapter{}, &yieldRepo{})
	ex.CancelWorkflowActions("wf", nil)
	ex.CancelWorkflowActions("wf", []int64{1, 2})
}

type checkpointAdapter struct{}

func (a *checkpointAdapter) ActOn(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
	ch := make(chan model.ActionYield, 2)
	ch <- model.CommandYield{Cmd: struct{ K string }{"c1"}}
	ch <- model.CheckpointYieldWrapper{CP: &model.CheckpointYield{Data: map[string]any{"k": "v"}, SaveNow: true}}
	close(ch)
	return ch, nil
}

func (a *checkpointAdapter) ToBeActOn(event *model.ConsumedEvent) bool { return true }

func (a *checkpointAdapter) SyncDB(ctx context.Context, workflowID string, oldState, newState model.State, events []model.Event) error {
	return nil
}

func TestActionExecutorCheckpointYield(t *testing.T) {
	repo := &yieldRepo{}
	repo.wg.Add(1)
	ex := NewActionExecutor(&checkpointAdapter{}, repo)
	ctx := context.Background()
	if err := ex.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ex.Stop() }()

	if err := ex.ExecuteAction(ctx, &model.ConsumedEvent{WorkflowID: "w1", EventNo: 1}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		repo.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}
	repo.mu.Lock()
	n := repo.n
	repo.mu.Unlock()
	if n != 1 {
		t.Fatalf("ProcessCommand calls: %d", n)
	}
}
