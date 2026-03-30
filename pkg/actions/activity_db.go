package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/postgres"
)

// ErrActivityNotFound is returned when no activities row exists for retry.
var ErrActivityNotFound = errors.New("activity not found")

// ErrActivityNotFailed is returned when retry is requested for a non-failed activity.
var ErrActivityNotFailed = errors.New("activity not in failed state")

// WithActivityPersistence records rows in the activities table and enables periodic recovery
// for the given workflow implementation (same type string as stored_events.workflow_type).
func WithActivityPersistence(pool *pgxpool.Pool, wf model.Workflow) ActionExecutorOption {
	return func(e *ActionExecutor) {
		e.activityPool = pool
		e.recoveryWorkflow = wf
	}
}

func (e *ActionExecutor) runnerID() string {
	if e.runnerName != "" {
		return e.runnerName
	}
	h, _ := os.Hostname()
	return h
}

func (e *ActionExecutor) persistActivityRunning(ctx context.Context, event *model.ConsumedEvent) {
	if e.activityPool == nil {
		return
	}
	_, err := e.activityPool.Exec(ctx, `
		INSERT INTO activities (workflow_id, event_number, workflow_type, status, started_at, retry_count, max_retries, runner_id, last_attempt_at)
		VALUES ($1, $2, $3, 'running', NOW(), 0, $4, $5, NOW())
		ON CONFLICT (workflow_id, event_number) DO UPDATE SET
			status = 'running',
			last_attempt_at = NOW(),
			runner_id = EXCLUDED.runner_id,
			workflow_type = EXCLUDED.workflow_type,
			max_retries = EXCLUDED.max_retries
	`, event.WorkflowID, event.EventNo, event.WorkflowType, e.maxRetries, e.runnerID())
	if err != nil {
		slog.Error("activity_persist_running", "workflow_id", event.WorkflowID, "event_no", event.EventNo, "err", err)
	}
}

func (e *ActionExecutor) persistActivityRetrying(ctx context.Context, event *model.ConsumedEvent, retryCount int) {
	if e.activityPool == nil {
		return
	}
	_, err := e.activityPool.Exec(ctx, `
		UPDATE activities SET status = 'retrying', retry_count = $3, last_attempt_at = NOW(), error_message = $4
		WHERE workflow_id = $1 AND event_number = $2
	`, event.WorkflowID, event.EventNo, retryCount, fmt.Sprintf("act_on_failed_attempt_%d", retryCount))
	if err != nil {
		slog.Error("activity_persist_retrying", "workflow_id", event.WorkflowID, "event_no", event.EventNo, "err", err)
	}
}

func (e *ActionExecutor) persistActivityCompleted(ctx context.Context, event *model.ConsumedEvent) {
	if e.activityPool == nil {
		return
	}
	_, err := e.activityPool.Exec(ctx, `
		UPDATE activities SET status = 'completed', finished_at = NOW(), error_message = NULL
		WHERE workflow_id = $1 AND event_number = $2
	`, event.WorkflowID, event.EventNo)
	if err != nil {
		slog.Error("activity_persist_completed", "workflow_id", event.WorkflowID, "event_no", event.EventNo, "err", err)
	}
}

func (e *ActionExecutor) persistActivityFailed(ctx context.Context, event *model.ConsumedEvent) {
	if e.activityPool == nil {
		return
	}
	_, err := e.activityPool.Exec(ctx, `
		UPDATE activities SET status = 'failed', finished_at = NOW(), error_message = 'max_retries_exceeded'
		WHERE workflow_id = $1 AND event_number = $2
	`, event.WorkflowID, event.EventNo)
	if err != nil {
		slog.Error("activity_persist_failed", "workflow_id", event.WorkflowID, "event_no", event.EventNo, "err", err)
	}
}

func (e *ActionExecutor) recoverInterruptedActions() {
	if e.activityPool == nil || e.recoveryWorkflow == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	wtype := e.recoveryWorkflow.Name()
	rows, err := e.activityPool.Query(ctx, `
		SELECT workflow_id, event_number FROM activities
		WHERE workflow_type = $1 AND status IN ('pending', 'running', 'retrying')
		LIMIT 50
	`, wtype)
	if err != nil {
		slog.Error("activity_recovery_query", "err", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var wfID string
		var evNo int64
		if err := rows.Scan(&wfID, &evNo); err != nil {
			slog.Error("activity_recovery_scan", "err", err)
			return
		}
		me, err := e.loadConsumedEventFromStore(ctx, wtype, wfID, evNo, e.recoveryWorkflow)
		if err != nil {
			slog.Warn("activity_recovery_load_event", "workflow_id", wfID, "event_no", evNo, "err", err)
			continue
		}
		e.ExecuteAction(ctx, me)
	}
}

// RequeueFailedAction reloads a stored event and schedules activity execution (HTTP retry endpoint).
func (e *ActionExecutor) RequeueFailedAction(ctx context.Context, workflowType, workflowID string, eventVersion int64, wf model.Workflow) error {
	if e.activityPool == nil {
		return fmt.Errorf("activity persistence not configured")
	}
	if wf == nil {
		return fmt.Errorf("workflow model is required")
	}

	var status string
	err := e.activityPool.QueryRow(ctx, `
		SELECT status FROM activities WHERE workflow_id = $1 AND event_number = $2 AND workflow_type = $3
	`, workflowID, eventVersion, workflowType).Scan(&status)
	if err != nil {
		if err == pgx.ErrNoRows {
			return ErrActivityNotFound
		}
		return err
	}
	if status != postgres.ActionStatusFailed {
		return fmt.Errorf("%w: got %s", ErrActivityNotFailed, status)
	}

	me, err := e.loadConsumedEventFromStore(ctx, workflowType, workflowID, eventVersion, wf)
	if err != nil {
		return err
	}

	_, err = e.activityPool.Exec(ctx, `
		UPDATE activities SET status = 'pending', retry_count = 0, error_message = NULL, finished_at = NULL, last_attempt_at = NOW()
		WHERE workflow_id = $1 AND event_number = $2 AND workflow_type = $3
	`, workflowID, eventVersion, workflowType)
	if err != nil {
		return err
	}

	return e.ExecuteAction(ctx, me)
}

func (e *ActionExecutor) loadConsumedEventFromStore(ctx context.Context, workflowType, workflowID string, version int64, wf model.Workflow) (*model.ConsumedEvent, error) {
	var eventType string
	var schemaVersion int32
	var body, meta []byte
	var globalID int64
	var at time.Time

	err := e.activityPool.QueryRow(ctx, `
		SELECT global_id, event_type, schema_version, body, at, metadata
		FROM stored_events
		WHERE workflow_id = $1 AND workflow_version = $2 AND workflow_type = $3
	`, workflowID, version, workflowType).Scan(&globalID, &eventType, &schemaVersion, &body, &at, &meta)
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	type decoder interface {
		DecodeEvent(eventType string, schemaVersion int, raw map[string]any) (model.Event, error)
	}
	dec, ok := wf.(decoder)
	if !ok {
		return nil, fmt.Errorf("workflow %T does not implement DecodeEvent", wf)
	}
	ev, err := dec.DecodeEvent(eventType, int(schemaVersion), raw)
	if err != nil {
		return nil, err
	}

	var metaMap map[string]any
	if len(meta) > 0 {
		_ = json.Unmarshal(meta, &metaMap)
	}
	if metaMap == nil {
		metaMap = make(map[string]any)
	}

	return &model.ConsumedEvent{
		GlobalID:     globalID,
		WorkflowID:   workflowID,
		WorkflowType: workflowType,
		EventNo:      version,
		EventType:    eventType,
		Event:        ev,
		At:           at,
		Metadata_:    metaMap,
	}, nil
}
