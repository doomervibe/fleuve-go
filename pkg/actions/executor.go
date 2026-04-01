package actions

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/repo"
)

// =============================================================================
// Activity Status Constants
// =============================================================================

const (
	ActivityStatusPending   = "PENDING"
	ActivityStatusRunning   = "RUNNING"
	ActivityStatusRetrying  = "RETRYING"
	ActivityStatusCompleted = "COMPLETED"
	ActivityStatusFailed    = "FAILED"
	ActivityStatusCancelled = "CANCELLED"
)

// =============================================================================
// Custom Errors
// =============================================================================

var (
	ErrActionCancelled = errors.New("action cancelled")
	ErrActionTimeout   = errors.New("action timed out")
	ErrAlreadyRunning  = errors.New("action already running")
	ErrActionCompleted = errors.New("action already completed")
)

// =============================================================================
// Options
// =============================================================================

// ActionFailedCallback is called when an action exhausts all retries.
type ActionFailedCallback func(ctx context.Context, workflowID string, eventNumber int, err error)

// ExecutorOptions configures the ActionExecutor.
type ExecutorOptions struct {
	// MaxRetries is the default maximum number of retries (default: 3).
	MaxRetries int

	// RecoveryInterval is how often to scan for stuck activities (default: 30s).
	RecoveryInterval time.Duration

	// GlobalActionTimeout is the maximum duration for any single action execution.
	// Zero means no global timeout.
	GlobalActionTimeout time.Duration

	// OnActionFailed is called when an action permanently fails after exhausting retries.
	OnActionFailed ActionFailedCallback

	// MaxConcurrentActions limits total concurrent actions across all workflows.
	// Zero means unlimited.
	MaxConcurrentActions int

	// MaxConcurrentActionsPerWorkflow limits concurrent actions per workflow.
	// Zero means unlimited.
	MaxConcurrentActionsPerWorkflow int

	// RunnerID identifies this executor instance for recovery ownership.
	RunnerID string
}

// applyDefaults fills in zero-value fields with sensible defaults.
func (o *ExecutorOptions) applyDefaults() {
	if o.MaxRetries == 0 {
		o.MaxRetries = 3
	}
	if o.RecoveryInterval == 0 {
		o.RecoveryInterval = 30 * time.Second
	}
}

// =============================================================================
// ActionExecutor
// =============================================================================

// ActionExecutor manages background task execution with retry and checkpoint support.
type ActionExecutor struct {
	pool    *pgxpool.Pool
	adapter model.Adapter
	repo    *repo.Repo
	opts    ExecutorOptions

	// _running_actions tracks in-flight actions keyed by "workflow_id:event_number".
	_runningActions   map[string]context.CancelFunc
	_runningActionsMu sync.Mutex

	// Global concurrency semaphore
	globalSem chan struct{}

	// Per-workflow concurrency semaphores
	workflowSems   map[string]chan struct{}
	workflowSemsMu sync.Mutex

	// Context for the executor lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewActionExecutor creates a new ActionExecutor.
func NewActionExecutor(
	pool *pgxpool.Pool,
	adapter model.Adapter,
	repo *repo.Repo,
	opts ExecutorOptions,
) *ActionExecutor {
	opts.applyDefaults()

	ctx, cancel := context.WithCancel(context.Background())

	ex := &ActionExecutor{
		pool:            pool,
		adapter:         adapter,
		repo:            repo,
		opts:            opts,
		_runningActions: make(map[string]context.CancelFunc),
		workflowSems:    make(map[string]chan struct{}),
		ctx:             ctx,
		cancel:          cancel,
	}

	// Initialize global semaphore if configured
	if opts.MaxConcurrentActions > 0 {
		ex.globalSem = make(chan struct{}, opts.MaxConcurrentActions)
	}

	return ex
}

// Start begins the recovery loop. Call Stop to shut down.
func (ex *ActionExecutor) Start() {
	ex.wg.Add(1)
	go ex.recoveryLoop()
}

// Stop gracefully shuts down the executor, waiting for in-flight actions.
func (ex *ActionExecutor) Stop() {
	ex.cancel()
	ex.wg.Wait()
}

// =============================================================================
// execute_action - Entry Point
// =============================================================================

// actionKey builds the dedup key for an action.
func actionKey(workflowID string, eventNumber int) string {
	return fmt.Sprintf("%s:%d", workflowID, eventNumber)
}

// ExecuteAction is the main entry point for executing an action.
// It is idempotent: calling it multiple times with the same event is safe.
func (ex *ActionExecutor) ExecuteAction(event *model.ConsumedEvent) error {
	key := actionKey(event.WorkflowID, int(event.EventNo))

	// Dedup: check if already running
	ex._runningActionsMu.Lock()
	if _, exists := ex._runningActions[key]; exists {
		ex._runningActionsMu.Unlock()
		return ErrAlreadyRunning
	}
	ex._runningActionsMu.Unlock()

	// Load activity from DB for idempotency check
	activity, err := ex.loadActivity(ex.ctx, event.WorkflowID, int(event.EventNo))
	if err != nil {
		return fmt.Errorf("failed to load activity: %w", err)
	}

	// If COMPLETED, return (idempotency)
	if activity != nil && activity.Status == ActivityStatusCompleted {
		return ErrActionCompleted
	}

	// Create activity record SYNCHRONOUSLY before spawning goroutine
	if activity == nil {
		retryPolicy := model.DefaultRetryPolicy()
		if ex.opts.MaxRetries > 0 {
			retryPolicy.MaxRetries = ex.opts.MaxRetries
		}

		wfType := event.WorkflowType
		if wfType == "" && ex.repo != nil {
			wfType = ex.repo.WorkflowType()
		}
		eventType := event.EventType
		if eventType == "" {
			if ev := event.GetEvent(); ev != nil {
				eventType = ev.Type()
			}
		}
		if err := ex.createActivity(ex.ctx, event.WorkflowID, int(event.EventNo), wfType, eventType, &retryPolicy); err != nil {
			return fmt.Errorf("failed to create activity: %w", err)
		}
	}

	// Create a cancellable context for this action
	actionCtx, actionCancel := context.WithCancel(ex.ctx)

	// Register in running actions with cancel func
	ex._runningActionsMu.Lock()
	// Double-check after acquiring lock
	if _, exists := ex._runningActions[key]; exists {
		ex._runningActionsMu.Unlock()
		actionCancel()
		return ErrAlreadyRunning
	}
	ex._runningActions[key] = actionCancel
	ex._runningActionsMu.Unlock()

	// Spawn goroutine for async execution
	ex.wg.Add(1)
	go func() {
		defer ex.wg.Done()
		defer actionCancel()

		// Always remove from running actions when done
		defer func() {
			ex._runningActionsMu.Lock()
			delete(ex._runningActions, key)
			ex._runningActionsMu.Unlock()
		}()

		ex._runActionWithRetry(actionCtx, event)
	}()

	return nil
}

// =============================================================================
// _run_action_with_retry - Core Retry Logic
// =============================================================================

func (ex *ActionExecutor) _runActionWithRetry(ctx context.Context, event *model.ConsumedEvent) {
	workflowID := event.WorkflowID
	eventNumber := int(event.EventNo)

	// Load activity for retry policy
	activity, err := ex.loadActivity(ctx, workflowID, eventNumber)
	if err != nil {
		ex.markFailed(ctx, workflowID, eventNumber, fmt.Errorf("failed to load activity: %w", err))
		return
	}
	if activity == nil {
		ex.markFailed(ctx, workflowID, eventNumber, fmt.Errorf("activity not found"))
		return
	}

	retryPolicy := activity.RetryPolicy
	maxRetries := retryPolicy.MaxRetries
	if ex.opts.MaxRetries > 0 && maxRetries > ex.opts.MaxRetries {
		maxRetries = ex.opts.MaxRetries
	}

	// Acquire global semaphore
	if ex.globalSem != nil {
		select {
		case ex.globalSem <- struct{}{}:
			defer func() { <-ex.globalSem }()
		case <-ctx.Done():
			_ = ex.updateStatus(ctx, workflowID, eventNumber, ActivityStatusCancelled, nil, "")
			return
		}
	}

	// Acquire per-workflow semaphore
	wfSem := ex.getWorkflowSemaphore(workflowID)
	if wfSem != nil {
		select {
		case wfSem <- struct{}{}:
			defer func() { <-wfSem }()
		case <-ctx.Done():
			_ = ex.updateStatus(ctx, workflowID, eventNumber, ActivityStatusCancelled, nil, "")
			return
		}
	}

	retryCount := activity.RetryCount
	var lastErr error

	for retryCount <= maxRetries {
		// Check for cancellation
		select {
		case <-ctx.Done():
			_ = ex.updateStatus(ctx, workflowID, eventNumber, ActivityStatusCancelled, nil, "")
			return
		default:
		}

		// Update status to RUNNING or RETRYING
		status := ActivityStatusRunning
		if retryCount > 0 {
			status = ActivityStatusRetrying
		}
		if err := ex.updateStatus(ctx, workflowID, eventNumber, status, nil, ""); err != nil {
			lastErr = fmt.Errorf("failed to update status: %w", err)
			break
		}

		// Build ActionContext from activity
		actionCtx := &model.ActionContext{
			WorkflowID:  workflowID,
			EventNumber: eventNumber,
			Checkpoint:  activity.Checkpoint,
			RetryCount:  retryCount,
			RetryPolicy: retryPolicy,
		}
		if actionCtx.Checkpoint == nil {
			actionCtx.Checkpoint = make(map[string]any)
		}

		// Execute the action
		execErr := ex.executeSingleAttempt(ctx, event, actionCtx)

		if execErr == nil {
			// SUCCESS: save checkpoint and mark COMPLETED
			_ = ex.saveCheckpoint(ctx, workflowID, eventNumber, actionCtx.Checkpoint)
			now := time.Now().UTC()
			_, _ = ex.pool.Exec(ctx,
				`UPDATE workflow_activities SET status = $1, finished_at = $2, last_attempt_at = $2, retry_count = $3
				 WHERE workflow_id = $4 AND event_number = $5`,
				ActivityStatusCompleted, now, retryCount, workflowID, eventNumber,
			)
			return
		}

		// Handle cancellation - don't retry
		if errors.Is(execErr, ErrActionCancelled) {
			_ = ex.updateStatus(ctx, workflowID, eventNumber, ActivityStatusCancelled, nil, "")
			return
		}

		// Timeout or other error - save checkpoint for retry
		_ = ex.saveCheckpoint(ctx, workflowID, eventNumber, actionCtx.Checkpoint)

		lastErr = execErr
		retryCount++

		if retryCount > maxRetries {
			break
		}

		// Sleep with backoff
		delay := ex.calculateBackoff(retryPolicy, retryCount)
		if delay > 0 {
			select {
			case <-time.After(delay):
				// Continue to next retry
			case <-ctx.Done():
				_ = ex.updateStatus(ctx, workflowID, eventNumber, ActivityStatusCancelled, nil, "")
				return
			}
		}
	}

	// Exhausted retries - mark FAILED permanently
	ex.markFailed(ctx, workflowID, eventNumber, lastErr)
}

// executeSingleAttempt runs a single execution attempt, consuming the ActOn channel.
func (ex *ActionExecutor) executeSingleAttempt(
	ctx context.Context,
	event *model.ConsumedEvent,
	actionCtx *model.ActionContext,
) error {
	// Apply global timeout if configured
	execCtx := ctx
	if ex.opts.GlobalActionTimeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, ex.opts.GlobalActionTimeout)
		defer cancel()
	}

	// Call adapter.ActOn
	yieldCh, err := ex.adapter.ActOn(execCtx, event, actionCtx)
	if err != nil {
		return err
	}

	// Consume the yield channel
	var currentTimeout *time.Timer
	defer func() {
		if currentTimeout != nil {
			currentTimeout.Stop()
		}
	}()

	for yield := range yieldCh {
		// Check for context cancellation
		select {
		case <-execCtx.Done():
			if errors.Is(execCtx.Err(), context.Canceled) {
				return ErrActionCancelled
			}
			return ErrActionTimeout
		default:
		}

		switch y := yield.(type) {
		case model.CommandYield:
			// Call repo.ProcessCommand
			_, _, rejection := ex.repo.ProcessCommand(execCtx, event.WorkflowID, y.Cmd)
			if rejection != nil {
				return fmt.Errorf("command rejected: %s", rejection.Msg)
			}

		case model.CheckpointYield:
			// Merge checkpoint data
			actionCtx.MergeCheckpoint(y.Data)
			if y.SaveNow {
				// Persist immediately
				if err := ex.saveCheckpoint(execCtx, event.WorkflowID, int(event.EventNo), actionCtx.Checkpoint); err != nil {
					return fmt.Errorf("failed to save checkpoint: %w", err)
				}
			}

		case model.ActionTimeout:
			// Set timeout for remainder of yields
			if currentTimeout != nil {
				currentTimeout.Stop()
			}
			timeoutDuration := time.Duration(y.Seconds * float64(time.Second))
			currentTimeout = time.NewTimer(timeoutDuration)

			// Continue consuming with timeout
			err := ex.consumeWithTimeout(execCtx, event, actionCtx, yieldCh, currentTimeout)
			if err != nil {
				return err
			}
			// If we get here, all remaining yields were consumed successfully
			return nil
		}
	}

	return nil
}

// consumeWithTimeout consumes remaining yields with a deadline.
func (ex *ActionExecutor) consumeWithTimeout(
	ctx context.Context,
	event *model.ConsumedEvent,
	actionCtx *model.ActionContext,
	yieldCh <-chan model.ActionYield,
	timer *time.Timer,
) error {
	for yield := range yieldCh {
		select {
		case <-timer.C:
			return ErrActionTimeout
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return ErrActionCancelled
			}
			return ErrActionTimeout
		default:
		}

		switch y := yield.(type) {
		case model.CommandYield:
			_, _, rejection := ex.repo.ProcessCommand(ctx, event.WorkflowID, y.Cmd)
			if rejection != nil {
				return fmt.Errorf("command rejected: %s", rejection.Msg)
			}

		case model.CheckpointYield:
			actionCtx.MergeCheckpoint(y.Data)
			if y.SaveNow {
				if err := ex.saveCheckpoint(ctx, event.WorkflowID, int(event.EventNo), actionCtx.Checkpoint); err != nil {
					return fmt.Errorf("failed to save checkpoint: %w", err)
				}
			}

		case model.ActionTimeout:
			// Nested timeout - reset the timer
			timer.Stop()
			timeoutDuration := time.Duration(y.Seconds * float64(time.Second))
			*timer = *time.NewTimer(timeoutDuration)
		}
	}
	return nil
}

// =============================================================================
// RetryPolicy Handling
// =============================================================================

// randUnitFloat01 returns a uniform float in [0, 1) using crypto/rand (for backoff jitter).
func randUnitFloat01() float64 {
	var buf [8]byte
	if _, err := crand.Read(buf[:]); err != nil {
		return 0
	}
	u := binary.BigEndian.Uint64(buf[:])
	return float64(u>>11) / float64(1<<53)
}

// calculateBackoff computes the delay before the next retry.
func (ex *ActionExecutor) calculateBackoff(policy model.RetryPolicy, retryCount int) time.Duration {
	backoffMin := ex.parseDuration(policy.BackoffMin, 1*time.Second)
	backoffMax := ex.parseDuration(policy.BackoffMax, 60*time.Second)

	var delay time.Duration

	switch policy.BackoffStrategy {
	case "linear":
		// Linear: min(backoff_min, backoff_factor * retry_count)
		linearDelay := time.Duration(policy.BackoffFactor * float64(retryCount) * float64(time.Second))
		delay = backoffMin
		if linearDelay < delay {
			delay = linearDelay
		}

	case "exponential":
		fallthrough
	default:
		// Exponential: min(backoff_min, min(backoff_factor^retry_count, backoff_max))
		expDelay := time.Duration(
			math.Pow(policy.BackoffFactor, float64(retryCount)) * float64(time.Second),
		)
		delay = backoffMin
		if expDelay < delay {
			delay = expDelay
		}
		if delay > backoffMax {
			delay = backoffMax
		}
	}

	// Apply jitter: add random(0, jitter * delay) using crypto/rand (not a secret, but unpredictable).
	if policy.BackoffJitter > 0 && delay > 0 {
		jitterRange := float64(delay) * policy.BackoffJitter
		jitter := time.Duration(randUnitFloat01() * jitterRange)
		delay += jitter
	}

	return delay
}

// parseDuration parses a duration string, returning the default if empty or invalid.
func (ex *ActionExecutor) parseDuration(s string, defaultVal time.Duration) time.Duration {
	if s == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return defaultVal
	}
	return d
}

// =============================================================================
// Per-Workflow Semaphore Management
// =============================================================================

// getWorkflowSemaphore returns (or creates) a semaphore for the given workflow.
func (ex *ActionExecutor) getWorkflowSemaphore(workflowID string) chan struct{} {
	if ex.opts.MaxConcurrentActionsPerWorkflow <= 0 {
		return nil
	}

	ex.workflowSemsMu.Lock()
	defer ex.workflowSemsMu.Unlock()

	sem, exists := ex.workflowSems[workflowID]
	if !exists {
		sem = make(chan struct{}, ex.opts.MaxConcurrentActionsPerWorkflow)
		ex.workflowSems[workflowID] = sem
	}
	return sem
}

// cleanupWorkflowSemaphore removes the semaphore for a workflow.
func (ex *ActionExecutor) cleanupWorkflowSemaphore(workflowID string) {
	ex.workflowSemsMu.Lock()
	defer ex.workflowSemsMu.Unlock()
	delete(ex.workflowSems, workflowID)
}

// =============================================================================
// Recovery Loop
// =============================================================================

func (ex *ActionExecutor) recoveryLoop() {
	defer ex.wg.Done()

	ticker := time.NewTicker(ex.opts.RecoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ex.ctx.Done():
			return
		case <-ticker.C:
			ex.recoverStuckActivities()
		}
	}
}

func (ex *ActionExecutor) recoverStuckActivities() {
	// Find activities with status RUNNING/RETRYING where last_attempt_at < 5 minutes ago OR null
	// These are considered "stuck" and should be retried
	cutoff := time.Now().UTC().Add(-5 * time.Minute)

	rows, err := ex.pool.Query(ex.ctx,
		`SELECT workflow_id, event_number, event_type, workflow_type, checkpoint, retry_count, retry_policy
		 FROM workflow_activities
		 WHERE status IN ($1, $2)
		   AND (last_attempt_at IS NULL OR last_attempt_at < $3)
		 FOR UPDATE SKIP LOCKED`,
		ActivityStatusRunning, ActivityStatusRetrying, cutoff,
	)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var workflowID string
		var eventNumber int
		var eventType string
		var workflowType string
		var checkpoint json.RawMessage
		var retryCount int
		var retryPolicyJSON json.RawMessage

		if err := rows.Scan(&workflowID, &eventNumber, &eventType, &workflowType, &checkpoint, &retryCount, &retryPolicyJSON); err != nil {
			continue
		}

		// Reconstruct ConsumedEvent from DB
		event := &model.ConsumedEvent{
			WorkflowID:   workflowID,
			EventNo:      int64(eventNumber),
			EventType:    eventType,
			WorkflowType: workflowType,
		}

		// Reset status to PENDING so execute_action can pick it up
		_, _ = ex.pool.Exec(ex.ctx,
			`UPDATE workflow_activities SET status = $1 WHERE workflow_id = $2 AND event_number = $3`,
			ActivityStatusPending, workflowID, eventNumber,
		)

		// Call execute_action - will retry from last checkpoint
		_ = ex.ExecuteAction(event)
	}
}

// =============================================================================
// cancel_workflow_actions
// =============================================================================

// CancelWorkflowActions cancels running actions for a workflow.
// If eventNumbers is nil, cancels ALL running tasks and marks ALL RUNNING/RETRYING/PENDING as CANCELLED.
// If specified, cancels only those tasks.
func (ex *ActionExecutor) CancelWorkflowActions(workflowID string, eventNumbers []int) {
	if eventNumbers == nil {
		// Cancel ALL running tasks for this workflow
		prefix := workflowID + ":"

		ex._runningActionsMu.Lock()
		for key, cancel := range ex._runningActions {
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				cancel()
			}
		}
		ex._runningActionsMu.Unlock()

		// Mark ALL RUNNING/RETRYING/PENDING as CANCELLED
		_, _ = ex.pool.Exec(ex.ctx,
			`UPDATE workflow_activities SET status = $1, finished_at = $2
			 WHERE workflow_id = $3 AND status IN ($4, $5, $6)`,
			ActivityStatusCancelled, time.Now().UTC(), workflowID,
			ActivityStatusRunning, ActivityStatusRetrying, ActivityStatusPending,
		)

		// Cleanup workflow semaphore
		ex.cleanupWorkflowSemaphore(workflowID)
	} else {
		// Cancel only specified event numbers
		for _, eventNo := range eventNumbers {
			key := actionKey(workflowID, eventNo)

			ex._runningActionsMu.Lock()
			if cancel, exists := ex._runningActions[key]; exists {
				cancel()
			}
			ex._runningActionsMu.Unlock()
		}

		// Mark specified activities as CANCELLED
		for _, eventNo := range eventNumbers {
			_, _ = ex.pool.Exec(ex.ctx,
				`UPDATE workflow_activities SET status = $1, finished_at = $2
				 WHERE workflow_id = $3 AND event_number = $4 AND status IN ($5, $6, $7)`,
				ActivityStatusCancelled, time.Now().UTC(), workflowID, eventNo,
				ActivityStatusRunning, ActivityStatusRetrying, ActivityStatusPending,
			)
		}
	}
}

// =============================================================================
// Activity DB Operations
// =============================================================================

// activityRecord represents a row in the workflow_activities table.
type activityRecord struct {
	WorkflowID    string            `json:"workflow_id"`
	EventNumber   int               `json:"event_number"`
	Status        string            `json:"status"`
	StartedAt     *time.Time        `json:"started_at,omitempty"`
	FinishedAt    *time.Time        `json:"finished_at,omitempty"`
	LastAttemptAt *time.Time        `json:"last_attempt_at,omitempty"`
	RetryCount    int               `json:"retry_count"`
	MaxRetries    int               `json:"max_retries"`
	Checkpoint    map[string]any    `json:"checkpoint,omitempty"`
	RetryPolicy   model.RetryPolicy `json:"retry_policy,omitempty"`
	ErrorMessage  string            `json:"error_message,omitempty"`
	ErrorType     string            `json:"error_type,omitempty"`
	RunnerID      string            `json:"runner_id,omitempty"`
}

// loadActivity loads an activity record from the database.
func (ex *ActionExecutor) loadActivity(ctx context.Context, workflowID string, eventNumber int) (*activityRecord, error) {
	var rec activityRecord
	var checkpointJSON json.RawMessage
	var retryPolicyJSON json.RawMessage
	var startedAt, finishedAt, lastAttemptAt *time.Time
	var errMsg, errType, runnerID sql.NullString

	err := ex.pool.QueryRow(ctx,
		`SELECT workflow_id, event_number, status, started_at, finished_at, last_attempt_at,
		        retry_count, max_retries, checkpoint, retry_policy, error_message, error_type, runner_id
		 FROM workflow_activities
		 WHERE workflow_id = $1 AND event_number = $2`,
		workflowID, eventNumber,
	).Scan(
		&rec.WorkflowID, &rec.EventNumber, &rec.Status,
		&startedAt, &finishedAt, &lastAttemptAt,
		&rec.RetryCount, &rec.MaxRetries, &checkpointJSON, &retryPolicyJSON,
		&errMsg, &errType, &runnerID,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rec.StartedAt = startedAt
	rec.FinishedAt = finishedAt
	rec.LastAttemptAt = lastAttemptAt
	if errMsg.Valid {
		rec.ErrorMessage = errMsg.String
	}
	if errType.Valid {
		rec.ErrorType = errType.String
	}
	if runnerID.Valid {
		rec.RunnerID = runnerID.String
	}

	if len(checkpointJSON) > 0 {
		if err := json.Unmarshal(checkpointJSON, &rec.Checkpoint); err != nil {
			return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
		}
	}
	if len(retryPolicyJSON) > 0 {
		if err := json.Unmarshal(retryPolicyJSON, &rec.RetryPolicy); err != nil {
			return nil, fmt.Errorf("failed to unmarshal retry_policy: %w", err)
		}
	}

	return &rec, nil
}

// createActivity creates a new PENDING activity record.
func (ex *ActionExecutor) createActivity(ctx context.Context, workflowID string, eventNumber int, workflowType, eventType string, retryPolicy *model.RetryPolicy) error {
	checkpointJSON, _ := json.Marshal(map[string]any{})
	retryPolicyJSON, _ := json.Marshal(retryPolicy)

	_, err := ex.pool.Exec(ctx,
		`INSERT INTO workflow_activities
		 (workflow_id, event_number, workflow_type, event_type, status, retry_count, max_retries, checkpoint, retry_policy, runner_id)
		 VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, $9)`,
		workflowID, eventNumber, workflowType, eventType, ActivityStatusPending,
		retryPolicy.MaxRetries, checkpointJSON, retryPolicyJSON, ex.opts.RunnerID,
	)
	return err
}

// updateStatus updates the status and optionally last_attempt_at of an activity.
func (ex *ActionExecutor) updateStatus(
	ctx context.Context,
	workflowID string,
	eventNumber int,
	status string,
	err error,
	errType string,
) error {
	now := time.Now().UTC()
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	if errType == "" && err != nil {
		errType = errorTypeName(err)
	}

	_, dbErr := ex.pool.Exec(ctx,
		`UPDATE workflow_activities SET status = $1, last_attempt_at = $2, error_message = $3, error_type = $4
		 WHERE workflow_id = $5 AND event_number = $6`,
		status, now, errMsg, errType, workflowID, eventNumber,
	)
	return dbErr
}

// saveCheckpoint persists the checkpoint data.
func (ex *ActionExecutor) saveCheckpoint(ctx context.Context, workflowID string, eventNumber int, checkpoint map[string]any) error {
	checkpointJSON, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	_, err = ex.pool.Exec(ctx,
		`UPDATE workflow_activities SET checkpoint = $1 WHERE workflow_id = $2 AND event_number = $3`,
		checkpointJSON, workflowID, eventNumber,
	)
	return err
}

// markFailed permanently marks an activity as FAILED and calls the callback.
func (ex *ActionExecutor) markFailed(ctx context.Context, workflowID string, eventNumber int, err error) {
	errMsg := ""
	errType := ""
	if err != nil {
		errMsg = err.Error()
		errType = errorTypeName(err)
	}
	now := time.Now().UTC()

	_, _ = ex.pool.Exec(ctx,
		`UPDATE workflow_activities
		 SET status = $1, finished_at = $2, last_attempt_at = $2, error_message = $3, error_type = $4
		 WHERE workflow_id = $5 AND event_number = $6`,
		ActivityStatusFailed, now, errMsg, errType, workflowID, eventNumber,
	)

	if ex.opts.OnActionFailed != nil && err != nil {
		ex.opts.OnActionFailed(ctx, workflowID, eventNumber, err)
	}
}

// errorTypeName returns a human-readable error type name.
func errorTypeName(err error) string {
	switch {
	case errors.Is(err, ErrActionCancelled):
		return "CancelledError"
	case errors.Is(err, ErrActionTimeout):
		return "TimeoutError"
	case errors.Is(err, context.Canceled):
		return "CancelledError"
	case errors.Is(err, context.DeadlineExceeded):
		return "TimeoutError"
	default:
		return "Exception"
	}
}
