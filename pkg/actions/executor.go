package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fleuve/fleuve-go/pkg/model"
	"github.com/fleuve/fleuve-go/pkg/postgres"
)

type ActionStatus string

const (
	StatusPending   ActionStatus = "pending"
	StatusRunning   ActionStatus = "running"
	StatusCompleted ActionStatus = "completed"
	StatusFailed    ActionStatus = "failed"
	StatusRetrying  ActionStatus = "retrying"
	StatusCancelled ActionStatus = "cancelled"
)

type OnActionFailed func(workflowID string, eventNumber int64, err error)

type ActionExecutor struct {
	adapter                   model.Adapter
	repo                      Repository
	dbActivityModel           string
	dbEventModel              string
	maxRetries                int
	recoveryInterval          time.Duration
	actionTimeout             time.Duration
	onActionFailed            OnActionFailed
	runnerName                string
	maxConcurrentActions      int
	maxConcurrentActionsPerWf int
	runningActions            map[string]map[int64]context.CancelFunc
	mu                        sync.RWMutex
	ctx                       context.Context
	cancel                    context.CancelFunc
	wg                        sync.WaitGroup
	globalSem                 chan struct{}
	activityPool              *pgxpool.Pool
	recoveryWorkflow          model.Workflow
}

type Repository interface {
	ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection)
}

type ActionExecutorOption func(*ActionExecutor)

func WithMaxRetries(n int) ActionExecutorOption {
	return func(e *ActionExecutor) { e.maxRetries = n }
}

func WithRecoveryInterval(d time.Duration) ActionExecutorOption {
	return func(e *ActionExecutor) { e.recoveryInterval = d }
}

func WithActionTimeout(d time.Duration) ActionExecutorOption {
	return func(e *ActionExecutor) { e.actionTimeout = d }
}

func WithOnActionFailed(fn OnActionFailed) ActionExecutorOption {
	return func(e *ActionExecutor) { e.onActionFailed = fn }
}

func WithRunnerName(name string) ActionExecutorOption {
	return func(e *ActionExecutor) { e.runnerName = name }
}

func WithMaxConcurrentActions(n int) ActionExecutorOption {
	return func(e *ActionExecutor) { e.maxConcurrentActions = n }
}

func WithMaxConcurrentActionsPerWorkflow(n int) ActionExecutorOption {
	return func(e *ActionExecutor) { e.maxConcurrentActionsPerWf = n }
}

func NewActionExecutor(adapter model.Adapter, repo Repository, opts ...ActionExecutorOption) *ActionExecutor {
	e := &ActionExecutor{
		adapter:                   adapter,
		repo:                      repo,
		maxRetries:                3,
		recoveryInterval:          30 * time.Second,
		runningActions:            make(map[string]map[int64]context.CancelFunc),
		maxConcurrentActions:      0,
		maxConcurrentActionsPerWf: 0,
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.maxConcurrentActions > 0 {
		e.globalSem = make(chan struct{}, e.maxConcurrentActions)
	}
	return e
}

func (e *ActionExecutor) Start(ctx context.Context) error {
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.wg.Add(1)
	go e.recoveryLoop()
	return nil
}

func (e *ActionExecutor) Stop() error {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	return nil
}

func (e *ActionExecutor) ToBeActOn(event *model.ConsumedEvent) bool {
	return e.adapter.ToBeActOn(event)
}

func (e *ActionExecutor) ExecuteAction(ctx context.Context, event *model.ConsumedEvent) error {
	e.mu.Lock()
	if _, ok := e.runningActions[event.WorkflowID]; !ok {
		e.runningActions[event.WorkflowID] = make(map[int64]context.CancelFunc)
	}
	if _, running := e.runningActions[event.WorkflowID][event.EventNo]; running {
		e.mu.Unlock()
		return nil
	}

	actionCtx, cancel := context.WithCancel(e.ctx)
	e.runningActions[event.WorkflowID][event.EventNo] = cancel
	e.mu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() {
			e.mu.Lock()
			delete(e.runningActions[event.WorkflowID], event.EventNo)
			e.mu.Unlock()
		}()

		if e.globalSem != nil {
			select {
			case e.globalSem <- struct{}{}:
				defer func() { <-e.globalSem }()
			case <-actionCtx.Done():
				return
			}
		}

		e.runActionWithRetry(actionCtx, event)
	}()

	return nil
}

func (e *ActionExecutor) runActionWithRetry(ctx context.Context, event *model.ConsumedEvent) {
	retryCount := 0
	retryPolicy := model.DefaultRetryPolicy()
	retryPolicy.MaxRetries = e.maxRetries

	e.persistActivityRunning(ctx, event)

	for retryCount <= retryPolicy.MaxRetries {
		select {
		case <-ctx.Done():
			return
		default:
		}

		actionCtx := &model.ActionContext{
			WorkflowID:  event.WorkflowID,
			EventNumber: int(event.EventNo),
			Checkpoint:  make(map[string]any),
			RetryCount:  retryCount,
			RetryPolicy: retryPolicy,
		}

		yieldCh, err := e.adapter.ActOn(ctx, event, actionCtx)
		if err != nil {
			slog.Warn("action_acton_failed", "workflow_id", event.WorkflowID, "event_no", event.EventNo, "err", err)
			retryCount++
			e.persistActivityRetrying(ctx, event, retryCount)
			if retryCount <= retryPolicy.MaxRetries {
				delay := retryPolicy.CalculateDelay(retryCount)
				time.Sleep(delay)
			}
			continue
		}

		completed := true
		for y := range yieldCh {
			if y.IsCommand() {
				_, _, rej := e.repo.ProcessCommand(ctx, event.WorkflowID, y.GetCommand())
				if rej != nil {
					slog.Error("action_command_yield_rejected", "workflow_id", event.WorkflowID, "msg", rej.Msg)
				}
			} else if y.IsCheckpoint() {
				cp := y.GetCheckpoint()
				if cp != nil {
					for k, v := range cp.Data {
						actionCtx.Checkpoint[k] = v
					}
				}
			} else if y.IsTimeout() {
				t := y.GetTimeout()
				if t != nil {
					timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(t.Seconds*float64(time.Second)))
					_ = timeoutCtx
					defer cancel()
				}
			}
		}

		if completed {
			e.persistActivityCompleted(ctx, event)
			slog.Info("action_completed", "workflow_id", event.WorkflowID, "event_no", event.EventNo)
			return
		}

		retryCount++
		if retryCount <= retryPolicy.MaxRetries {
			delay := retryPolicy.CalculateDelay(retryCount)
			slog.Info("action_retry_after_yield", "workflow_id", event.WorkflowID, "event_no", event.EventNo, "delay", delay)
			time.Sleep(delay)
		}
	}

	e.persistActivityFailed(ctx, event)
	if e.onActionFailed != nil {
		e.onActionFailed(event.WorkflowID, event.EventNo, fmt.Errorf("max retries exceeded"))
	}
}

func (e *ActionExecutor) CancelWorkflowActions(workflowID string, eventNumbers []int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if eventNumbers == nil || len(eventNumbers) == 0 {
		if cancels, ok := e.runningActions[workflowID]; ok {
			for _, cancel := range cancels {
				cancel()
			}
		}
	} else {
		if cancels, ok := e.runningActions[workflowID]; ok {
			for _, evNo := range eventNumbers {
				if cancel, exists := cancels[evNo]; exists {
					cancel()
				}
			}
		}
	}
}

func (e *ActionExecutor) recoveryLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.recoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.recoverInterruptedActions()
		}
	}
}

func ActivityToJSON(a *postgres.Activity) ([]byte, error) {
	return json.Marshal(a)
}
