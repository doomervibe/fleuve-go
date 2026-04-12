package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

// SyncDBHandler is called in the same transaction as event insertion.
// Used for denormalized DB updates that must be consistent with events.
type SyncDBHandler func(ctx context.Context, tx pgx.Tx, workflowID string, oldState, newState model.State, events []model.Event) error

// EventParser deserializes raw JSON into an Event based on event type.
type EventParser func(eventType string, raw json.RawMessage) (model.Event, error)

// Repo is the pgx-backed workflow repository implementing event sourcing.
// It provides transactional command processing with the Outbox pattern.
type Repo struct {
	pool               *pgxpool.Pool
	workflowType       string
	workflow           model.Workflow
	es                 EphemeralStorage
	eventParser        EventParser
	syncDBHandler      SyncDBHandler
	snapshotInterval   int
	snapshotTable      string
	eventsTable        string
	subscriptionsTable string
	externalSubsTable  string
	delayScheduleTable string
	workflowMetaTable  string
	namespace          *string
	trustCache         bool
}

// RepoOption is a functional option for Repo configuration.
type RepoOption func(*Repo)

// WithNamespace sets the namespace for multi-tenant filtering.
func WithNamespace(ns string) RepoOption {
	return func(r *Repo) { r.namespace = &ns }
}

// WithSnapshotInterval enables snapshotting at the given interval.
// 0 = disabled.
func WithSnapshotInterval(interval int) RepoOption {
	return func(r *Repo) { r.snapshotInterval = interval }
}

// WithSnapshotTable sets the snapshot table name.
func WithSnapshotTable(table string) RepoOption {
	return func(r *Repo) { r.snapshotTable = table }
}

// WithEventsTable sets the events table name.
func WithEventsTable(table string) RepoOption {
	return func(r *Repo) { r.eventsTable = table }
}

// WithSyncDBHandler sets the sync DB handler for denormalized updates.
func WithSyncDBHandler(handler SyncDBHandler) RepoOption {
	return func(r *Repo) { r.syncDBHandler = handler }
}

// WithTrustCache disables DB version check on cache hit.
// Safe ONLY when this runner is the sole writer for its partition.
func WithTrustCache(trust bool) RepoOption {
	return func(r *Repo) { r.trustCache = trust }
}

// WithEventParser sets the event deserialization function.
func WithEventParser(parser EventParser) RepoOption {
	return func(r *Repo) { r.eventParser = parser }
}

// WithSubscriptionsTable sets the subscriptions table name.
func WithSubscriptionsTable(table string) RepoOption {
	return func(r *Repo) { r.subscriptionsTable = table }
}

// WithExternalSubscriptionsTable sets the external_subscriptions table name.
func WithExternalSubscriptionsTable(table string) RepoOption {
	return func(r *Repo) { r.externalSubsTable = table }
}

// WithDelayScheduleTable sets the delay_schedule table name.
func WithDelayScheduleTable(table string) RepoOption {
	return func(r *Repo) { r.delayScheduleTable = table }
}

// WithWorkflowMetaTable sets the workflow_metadata table name.
func WithWorkflowMetaTable(table string) RepoOption {
	return func(r *Repo) { r.workflowMetaTable = table }
}

// NewRepo creates a new workflow repository.
func NewRepo(
	pool *pgxpool.Pool,
	workflowType string,
	workflow model.Workflow,
	es EphemeralStorage,
	opts ...RepoOption,
) *Repo {
	r := &Repo{
		pool:               pool,
		workflowType:       workflowType,
		workflow:           workflow,
		es:                 es,
		eventsTable:        "stored_events",
		snapshotTable:      "snapshots",
		subscriptionsTable: "subscriptions",
		externalSubsTable:  "external_subscriptions",
		delayScheduleTable: "delay_schedules",
		workflowMetaTable:  "workflow_metadata",
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// WorkflowType returns the workflow type name this repository was constructed with.
func (r *Repo) WorkflowType() string {
	return r.workflowType
}

// ProcessCommand processes a command for an existing workflow.
// This is THE core method implementing event sourcing with optimistic concurrency.
//
// Full flow:
//  1. Acquire row-level lock on version=1 event for this workflow
//  2. Load current state (cache or DB)
//  3. Lifecycle check (paused/cancelled → rejection)
//  4. Call decide() → get events
//  5. Call evolve_() to compute new state
//  6. Handle sync events (subscriptions, schedules, etc.)
//  7. Call sync_db handler if configured
//  8. Inject workflow tags into event metadata
//  9. INSERT all events
//  10. Maybe snapshot
//  11. Update ephemeral cache
//
// Retries on IntegrityError (concurrent command from another process).
func (r *Repo) ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection) {
	const maxRetries = 100

	for retry := 0; retry < maxRetries; retry++ {
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to begin transaction: %v", err)}
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// Step 1: Acquire row-level lock on version=1 event
		var lockKey int64
		err = tx.QueryRow(ctx,
			fmt.Sprintf("SELECT global_id FROM %s WHERE workflow_id = $1 AND workflow_version = 1 FOR UPDATE", r.eventsTable),
			id,
		).Scan(&lockKey)
		if err == pgx.ErrNoRows {
			return nil, nil, &model.Rejection{Msg: (&model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}).Error()}
		}
		if err != nil {
			return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to acquire lock: %v", err)}
		}

		// Step 2: Load current state
		state, err := r.getCurrentStateTx(ctx, tx, id, false)
		if err != nil {
			return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to load state: %v", err)}
		}
		if state == nil || state.State == nil {
			return nil, nil, &model.Rejection{Msg: (&model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}).Error()}
		}

		// Step 3: Lifecycle check
		lifecycle := state.State.GetLifecycle()
		if lifecycle == model.LifecyclePaused {
			return nil, nil, &model.Rejection{Msg: (&model.WorkflowPaused{}).Error()}
		}
		if lifecycle == model.LifecycleCanceled {
			return nil, nil, &model.Rejection{Msg: (&model.WorkflowCanceled{}).Error()}
		}

		// Step 4: Call decide()
		events, rejection := r.workflow.Decide(state.State, cmd)
		if rejection != nil {
			return nil, nil, rejection
		}
		if len(events) == 0 {
			// No-op - return current state unchanged
			return state, nil, nil
		}

		// Step 5: Evolve to compute new state
		newState := model.EvolveAll(r.workflow, state.State, events)
		newVersion := state.Version + int64(len(events))

		// Step 6: Handle sync events BEFORE inserting events
		if err := r.handleSyncEventsTx(ctx, tx, id, state.Version, events); err != nil {
			return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to handle sync events: %v", err)}
		}

		// Step 7: Call sync_db handler if configured
		if r.syncDBHandler != nil {
			if err := r.syncDBHandler(ctx, tx, id, state.State, newState, events); err != nil {
				return nil, nil, &model.Rejection{Msg: fmt.Sprintf("sync_db handler failed: %v", err)}
			}
		}

		// Step 8: Inject workflow tags into event metadata
		wfTags, _ := r.loadWorkflowTagsTx(ctx, tx, id)
		for _, e := range events {
			model.SetWorkflowTagsInMetadata(e, wfTags)
		}

		// Step 9: INSERT all events
		for i, e := range events {
			eventVersion := state.Version + int64(i) + 1
			if err := r.insertEventTx(ctx, tx, id, eventVersion, e); err != nil {
				if isUniqueViolation(err) {
					// Concurrent command - retry
					break
				}
				return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to insert event: %v", err)}
			}
			// Check if we broke out due to unique violation
			if i < len(events)-1 {
				continue // Will be caught by the next iteration
			}
		}

		// Verify all events were inserted by checking the last one
		var lastInsertedVersion int64
		err = tx.QueryRow(ctx,
			fmt.Sprintf("SELECT workflow_version FROM %s WHERE workflow_id = $1 ORDER BY workflow_version DESC LIMIT 1", r.eventsTable),
			id,
		).Scan(&lastInsertedVersion)
		if err != nil {
			if isUniqueViolation(err) || err == pgx.ErrNoRows {
				continue // Retry
			}
			return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to verify insert: %v", err)}
		}
		if lastInsertedVersion < newVersion {
			continue // Some events weren't inserted - retry
		}

		// Step 9.5: Record global_id for horizon subscriptions to prevent double-delivery
		if err := r.updateSubscriptionAddedGlobalIDsTx(ctx, tx, id, state.Version, events); err != nil {
			return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to update subscription global IDs: %v", err)}
		}

		// Step 10: Maybe snapshot
		if err := r.maybeSnapshotTx(ctx, tx, id, newState, newVersion); err != nil {
			return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to snapshot: %v", err)}
		}

		// Commit
		if err := tx.Commit(ctx); err != nil {
			if isUniqueViolation(err) {
				continue // Retry
			}
			return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to commit: %v", err)}
		}

		// Step 11: Update ephemeral cache
		newSS := &model.StoredState{ID: id, Version: newVersion, State: newState}

		// Check if final event (but NOT EvSystemCancel)
		lastEvent := events[len(events)-1]
		if model.IsTerminalState(r.workflow, lastEvent) {
			_ = r.es.RemoveState(ctx, id)
		} else {
			_ = r.es.PutState(ctx, newSS)
		}

		return newSS, events, nil
	}

	return nil, nil, &model.Rejection{Msg: "max retries exceeded for concurrent command processing"}
}

// CreateNew creates a new workflow with the given ID and initial command.
// No SELECT FOR UPDATE lock is needed since the workflow doesn't exist yet.
// Uses IntegrityError on event INSERT as the concurrency guard.
func (r *Repo) CreateNew(ctx context.Context, cmd model.Command, id string, tags []string) (*model.StoredState, error) {
	// Step 1: Call decide with nil state
	events, rejection := r.workflow.Decide(nil, cmd)
	if rejection != nil {
		return nil, rejection
	}
	if len(events) == 0 {
		return nil, &model.Rejection{Msg: "cannot create workflow with no events"}
	}

	// Step 2: Evolve to compute initial state
	state := model.EvolveAll(r.workflow, nil, events)

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Step 3: Insert workflow metadata if tags provided
	if len(tags) > 0 {
		_, err := tx.Exec(ctx,
			fmt.Sprintf(`INSERT INTO %s (workflow_id, workflow_type, tags, created_at)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (workflow_id) DO UPDATE SET tags = EXCLUDED.tags`, r.workflowMetaTable),
			id, r.workflowType, tags, time.Now().UTC(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to insert workflow metadata: %w", err)
		}
	}

	// Step 4: Handle sync events with base_version=0
	if err := r.handleSyncEventsTx(ctx, tx, id, 0, events); err != nil {
		return nil, fmt.Errorf("failed to handle sync events: %w", err)
	}

	// Step 5: Call sync_db handler if configured
	if r.syncDBHandler != nil {
		if err := r.syncDBHandler(ctx, tx, id, nil, state, events); err != nil {
			return nil, fmt.Errorf("sync_db handler failed: %w", err)
		}
	}

	// Step 6: Inject workflow tags and insert events
	for i, e := range events {
		model.SetWorkflowTagsInMetadata(e, tags)
		if err := r.insertEventTx(ctx, tx, id, int64(i+1), e); err != nil {
			if isUniqueViolation(err) {
				return nil, &model.AlreadyExists{}
			}
			return nil, fmt.Errorf("failed to insert event at version %d: %w", i+1, err)
		}
	}

	// Step 6.5: Record global_id for horizon subscriptions to prevent double-delivery
	if err := r.updateSubscriptionAddedGlobalIDsTx(ctx, tx, id, 0, events); err != nil {
		return nil, fmt.Errorf("failed to update subscription global IDs: %w", err)
	}

	// Step 7: Maybe snapshot
	newVersion := int64(len(events))
	if err := r.maybeSnapshotTx(ctx, tx, id, state, newVersion); err != nil {
		return nil, fmt.Errorf("failed to snapshot: %w", err)
	}

	// Commit
	if err := tx.Commit(ctx); err != nil {
		if isUniqueViolation(err) {
			return nil, &model.AlreadyExists{}
		}
		return nil, fmt.Errorf("failed to commit: %w", err)
	}

	// Update ephemeral cache
	ss := &model.StoredState{ID: id, Version: newVersion, State: state}
	lastEvent := events[len(events)-1]
	if !model.IsTerminalState(r.workflow, lastEvent) {
		_ = r.es.PutState(ctx, ss)
	}

	return ss, nil
}

// PauseWorkflow pauses a workflow, preventing command processing.
func (r *Repo) PauseWorkflow(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to begin transaction: %v", err)}
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, err := r.loadStateTx(ctx, tx, id, nil)
	if err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to load state: %v", err)}
	}
	if state == nil || state.State == nil {
		return nil, &model.Rejection{Msg: (&model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}).Error()}
	}

	lifecycle := state.State.GetLifecycle()
	if lifecycle == model.LifecyclePaused {
		return nil, &model.Rejection{Msg: "workflow is already paused"}
	}
	if lifecycle == model.LifecycleCanceled {
		return nil, &model.Rejection{Msg: "workflow is cancelled"}
	}

	event := &model.EvSystemPause{Reason: reason}
	newState := model.Evolve(r.workflow, state.State, event)
	newVersion := state.Version + 1

	if err := r.insertEventTx(ctx, tx, id, newVersion, event); err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to insert event: %v", err)}
	}

	if err := r.maybeSnapshotTx(ctx, tx, id, newState, newVersion); err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to snapshot: %v", err)}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to commit: %v", err)}
	}

	newSS := &model.StoredState{ID: id, Version: newVersion, State: newState}
	_ = r.es.PutState(ctx, newSS)
	return newSS, nil
}

// ResumeWorkflow resumes a paused workflow.
func (r *Repo) ResumeWorkflow(ctx context.Context, id string) (*model.StoredState, *model.Rejection) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to begin transaction: %v", err)}
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, err := r.loadStateTx(ctx, tx, id, nil)
	if err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to load state: %v", err)}
	}
	if state == nil || state.State == nil {
		return nil, &model.Rejection{Msg: (&model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}).Error()}
	}

	lifecycle := state.State.GetLifecycle()
	if lifecycle != model.LifecyclePaused {
		return nil, &model.Rejection{Msg: "workflow is not paused"}
	}

	event := &model.EvSystemResume{}
	newState := model.Evolve(r.workflow, state.State, event)
	newVersion := state.Version + 1

	if err := r.insertEventTx(ctx, tx, id, newVersion, event); err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to insert event: %v", err)}
	}

	if err := r.maybeSnapshotTx(ctx, tx, id, newState, newVersion); err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to snapshot: %v", err)}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to commit: %v", err)}
	}

	newSS := &model.StoredState{ID: id, Version: newVersion, State: newState}
	_ = r.es.PutState(ctx, newSS)
	return newSS, nil
}

// CancelWorkflow cancels a workflow and cleans up associated resources.
func (r *Repo) CancelWorkflow(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to begin transaction: %v", err)}
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, err := r.loadStateTx(ctx, tx, id, nil)
	if err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to load state: %v", err)}
	}
	if state == nil || state.State == nil {
		return nil, &model.Rejection{Msg: (&model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}).Error()}
	}

	lifecycle := state.State.GetLifecycle()
	if lifecycle == model.LifecycleCanceled {
		return nil, &model.Rejection{Msg: "workflow is already cancelled"}
	}

	// Delete all delay schedules for this workflow
	_, err = tx.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE workflow_id = $1", r.delayScheduleTable),
		id,
	)
	if err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to delete delay schedules: %v", err)}
	}

	event := &model.EvSystemCancel{Reason: reason}
	newState := model.Evolve(r.workflow, state.State, event)
	newVersion := state.Version + 1

	if err := r.insertEventTx(ctx, tx, id, newVersion, event); err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to insert event: %v", err)}
	}

	if err := r.maybeSnapshotTx(ctx, tx, id, newState, newVersion); err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to snapshot: %v", err)}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, &model.Rejection{Msg: fmt.Sprintf("failed to commit: %v", err)}
	}

	// ALWAYS remove from cache on cancel
	_ = r.es.RemoveState(ctx, id)

	newSS := &model.StoredState{ID: id, Version: newVersion, State: newState}
	return newSS, nil
}

// ContinueAsNew resets event history while preserving state.
// Used for long-running workflows to reduce storage.
// Requires snapshotting to be enabled.
func (r *Repo) ContinueAsNew(ctx context.Context, id string, newCmd model.Command, reason string, newWorkflowType string) (*model.StoredState, []model.Event, *model.Rejection) {
	if r.snapshotInterval <= 0 {
		return nil, nil, &model.Rejection{Msg: "continue_as_new requires snapshotting to be enabled"}
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to begin transaction: %v", err)}
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Load current state
	state, err := r.loadStateTx(ctx, tx, id, nil)
	if err != nil {
		return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to load state: %v", err)}
	}
	if state == nil || state.State == nil {
		return nil, nil, &model.Rejection{Msg: (&model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}).Error()}
	}

	// Force UPSERT snapshot at current version
	if err := r.upsertSnapshotTx(ctx, tx, id, state.State, state.Version); err != nil {
		return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to force snapshot: %v", err)}
	}

	// Delete all events for this workflow
	_, err = tx.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE workflow_id = $1", r.eventsTable),
		id,
	)
	if err != nil {
		return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to delete events: %v", err)}
	}

	// Insert EvContinueAsNew marker event at version=1
	event := &model.EvContinueAsNew{Reason: reason, NewWorkflowType: newWorkflowType}
	if err := r.insertEventTx(ctx, tx, id, 1, event); err != nil {
		return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to insert marker event: %v", err)}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, &model.Rejection{Msg: fmt.Sprintf("failed to commit: %v", err)}
	}

	// Update ephemeral cache (version=1, state preserved)
	newSS := &model.StoredState{ID: id, Version: 1, State: state.State}
	_ = r.es.PutState(ctx, newSS)

	// If new_cmd provided, process it against preserved state
	if newCmd != nil {
		return r.ProcessCommand(ctx, id, newCmd)
	}

	return newSS, nil, nil
}

// ReplayWorkflow replays events from a specific version to rebuild state.
// Used for debugging or after data correction.
func (r *Repo) ReplayWorkflow(ctx context.Context, id string, fromVersion int64) (*model.StoredState, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Load base state at fromVersion-1
	var baseState model.State
	if fromVersion > 1 {
		atVersion := fromVersion - 1
		baseSS, err := r.loadStateTx(ctx, tx, id, &atVersion)
		if err != nil {
			return nil, fmt.Errorf("failed to load base state: %w", err)
		}
		if baseSS != nil {
			baseState = baseSS.State
		}
	}

	// Load events from fromVersion to HEAD (afterVersion is exclusive, so subtract 1)
	events, err := r.loadEventsTx(ctx, tx, id, fromVersion-1, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load events: %w", err)
	}

	if len(events) == 0 {
		return nil, fmt.Errorf("no events found from version %d", fromVersion)
	}

	// Evolve through events
	state := model.EvolveAll(r.workflow, baseState, events)
	newVersion := fromVersion + int64(len(events)) - 1

	// Maybe snapshot
	if err := r.maybeSnapshotTx(ctx, tx, id, state, newVersion); err != nil {
		return nil, fmt.Errorf("failed to snapshot: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}

	// Update ephemeral cache
	ss := &model.StoredState{ID: id, Version: newVersion, State: state}
	_ = r.es.PutState(ctx, ss)

	return ss, nil
}

// GetWorkflowTags loads the tags for a workflow from the metadata table.
func (r *Repo) GetWorkflowTags(ctx context.Context, workflowID string) ([]string, error) {
	var tags []string
	err := r.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT tags FROM %s WHERE workflow_id = $1", r.workflowMetaTable),
		workflowID,
	).Scan(&tags)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return tags, err
}

// GetState returns the current workflow state and its version.
//
// Loading order: ephemeral cache first; the cached StoredState.Version is
// always compared to the current MAX(workflow_version) in stored_events for
// this workflow. If the DB is ahead of the cache, state is rebuilt from
// snapshot + events. If there is no cache entry, state is loaded from the DB
// only.
//
// Returns (nil, nil) when the workflow has no events and no snapshot, or when
// replay ends in a terminal state that yields no readable aggregate state (same
// rules as loadStateTx).
//
// GetState does not write to the event log and does not update the ephemeral
// cache (read-only).
func (r *Repo) GetState(ctx context.Context, id string) (*model.StoredState, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ss, err := r.getCurrentStateTx(ctx, tx, id, true)
	if err != nil {
		return nil, err
	}
	return ss, nil
}

// =============================================================================
// Internal Methods
// =============================================================================

// Pool returns the underlying PostgreSQL connection pool.
func (r *Repo) Pool() *pgxpool.Pool {
	return r.pool
}

// EventParser returns the event parser function.
func (r *Repo) EventParser() EventParser {
	return r.eventParser
}

// EventsTable returns the configured stored_events table name. This is
// exposed so callers (notably the action executor's recovery scanner)
// can load raw event rows for replay without hardcoding the table name.
func (r *Repo) EventsTable() string {
	return r.eventsTable
}

// maxWorkflowVersionTx returns the highest workflow_version for id in stored_events, or 0 if none.
func (r *Repo) maxWorkflowVersionTx(ctx context.Context, tx pgx.Tx, id string) (int64, error) {
	var dbVersion int64
	err := tx.QueryRow(ctx,
		fmt.Sprintf("SELECT COALESCE(MAX(workflow_version), 0) FROM %s WHERE workflow_id = $1", r.eventsTable),
		id,
	).Scan(&dbVersion)
	if err != nil {
		return 0, err
	}
	return dbVersion, nil
}

// getCurrentStateTx loads the current state within a transaction.
// Tries ephemeral cache first, then falls back to DB.
//
// If forceDBVersionCheck is true, a cache hit is accepted only when
// cached.Version equals the current MAX(workflow_version) in the DB; otherwise
// state is reloaded from snapshot + events. ProcessCommand passes false here and
// relies on trustCache: when trustCache is false, the same check runs; when
// trustCache is true, the cache is trusted without a DB read.
func (r *Repo) getCurrentStateTx(ctx context.Context, tx pgx.Tx, id string, forceDBVersionCheck bool) (*model.StoredState, error) {
	cached, err := r.es.GetState(ctx, id)
	if err != nil {
		return nil, err
	}
	if cached != nil {
		if forceDBVersionCheck || !r.trustCache {
			dbVersion, err := r.maxWorkflowVersionTx(ctx, tx, id)
			if err != nil {
				return nil, err
			}
			if dbVersion != cached.Version {
				return r.loadStateTx(ctx, tx, id, nil)
			}
		}
		return cached, nil
	}

	return r.loadStateTx(ctx, tx, id, nil)
}

// loadStateTx reconstructs state from snapshot + events.
// If atVersion is non-nil, loads state at that specific version.
func (r *Repo) loadStateTx(ctx context.Context, tx pgx.Tx, id string, atVersion *int64) (*model.StoredState, error) {
	var baseState model.State
	var baseVersion int64

	// Try to load snapshot
	if r.snapshotInterval > 0 {
		var snapState json.RawMessage
		var snapVersion int64
		query := fmt.Sprintf(
			"SELECT version, state FROM %s WHERE workflow_id = $1",
			r.snapshotTable,
		)
		args := []any{id}

		if atVersion != nil {
			query += " AND version <= $2"
			args = append(args, *atVersion)
		}

		err := tx.QueryRow(ctx, query, args...).Scan(&snapVersion, &snapState)
		if err == nil {
			baseState, err = r.parseState(snapState)
			if err == nil {
				baseVersion = snapVersion
			}
		}
	}

	// Load events strictly after snapshot version (baseVersion is 0 when no snapshot).
	events, err := r.loadEventsTx(ctx, tx, id, baseVersion, atVersion)
	if err != nil {
		return nil, err
	}

	// No events and no snapshot - workflow doesn't exist
	if len(events) == 0 && baseState == nil {
		return nil, nil
	}

	// Evolve through events
	var state model.State
	if len(events) > 0 {
		state = model.EvolveAll(r.workflow, baseState, events)
		lastEvent := events[len(events)-1]

		// Check if workflow is terminal (except cancel)
		if model.IsTerminalState(r.workflow, lastEvent) {
			return nil, nil
		}
	} else {
		state = baseState
	}

	// Get final version
	var version int64
	if len(events) > 0 {
		version = baseVersion + int64(len(events))
	} else {
		version = baseVersion
	}

	return &model.StoredState{ID: id, Version: version, State: state}, nil
}

// loadEventsTx loads events from the database within a transaction.
func (r *Repo) loadEventsTx(ctx context.Context, tx pgx.Tx, id string, afterVersion int64, atVersion *int64) ([]model.Event, error) {
	query := fmt.Sprintf(
		"SELECT workflow_version, event_type, body, schema_version FROM %s WHERE workflow_id = $1 AND workflow_version > $2",
		r.eventsTable,
	)
	args := []any{id, afterVersion}

	if atVersion != nil {
		query += " AND workflow_version <= $3"
		args = append(args, *atVersion)
	}

	query += " ORDER BY workflow_version"

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]model.Event, 0)
	for rows.Next() {
		var version int64
		var eventType string
		var body json.RawMessage
		var schemaVersion int

		if err := rows.Scan(&version, &eventType, &body, &schemaVersion); err != nil {
			return nil, err
		}

		if r.eventParser == nil {
			return nil, fmt.Errorf("event parser is required but not configured for workflow type %q", r.workflowType)
		}

		// Check if upcasting is needed
		if schemaVersion < r.workflow.SchemaVersion() {
			var rawData map[string]any
			if err := json.Unmarshal(body, &rawData); err != nil {
				return nil, fmt.Errorf("failed to unmarshal raw data for upcast: %w", err)
			}
			rawData = r.workflow.Upcast(eventType, schemaVersion, rawData)
			if rawData != nil {
				body, _ = json.Marshal(rawData)
			}
		}
		event, err := r.eventParser(eventType, body)
		if err != nil {
			return nil, fmt.Errorf("failed to parse event type %s: %w", eventType, err)
		}
		events = append(events, event)
	}

	return events, rows.Err()
}

// insertEventTx inserts a single event into the events table.
func (r *Repo) insertEventTx(ctx context.Context, tx pgx.Tx, id string, version int64, event model.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	var metadata map[string]any
	if getter, ok := event.(interface{ GetMetadata() map[string]any }); ok {
		metadata = getter.GetMetadata()
	}
	metaBytes, _ := json.Marshal(metadata)

	_, err = tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)`, r.eventsTable),
		id, version, r.namespace, event.Type(), r.workflowType, r.workflow.SchemaVersion(),
		body, time.Now().UTC(), metaBytes,
	)
	return err
}

// handleSyncEventsTx processes sync events (subscriptions, schedules, etc.).
// Called BEFORE inserting events into the event table.
func (r *Repo) handleSyncEventsTx(ctx context.Context, tx pgx.Tx, id string, baseVersion int64, events []model.Event) error {
	for _, event := range events {
		switch e := event.(type) {
		case *model.EvSubscriptionAdded:
			if err := r.insertSubscriptionTx(ctx, tx, id, e.Sub); err != nil {
				return err
			}
		case *model.EvSubscriptionRemoved:
			if err := r.deleteSubscriptionTx(ctx, tx, id, e.Sub); err != nil {
				return err
			}
		case *model.EvExternalSubscriptionAdded:
			if err := r.insertExternalSubscriptionTx(ctx, tx, id, e.Sub); err != nil {
				return err
			}
		case *model.EvExternalSubscriptionRemoved:
			if err := r.deleteExternalSubscriptionTx(ctx, tx, id, e.Topic); err != nil {
				return err
			}
		case *model.EvScheduleAdded:
			if err := r.insertDelayScheduleTx(ctx, tx, id, e.Schedule); err != nil {
				return err
			}
		case *model.EvScheduleRemoved:
			if err := r.deleteDelayScheduleTx(ctx, tx, id, e.DelayID); err != nil {
				return err
			}
		case *model.EvDelay:
			if e.IsCron() {
				// Cron delay: store in delay_schedules so the scheduler can compute
				// the first fire time from the cron expression (delay_until = now as
				// a sentinel; scheduler overwrites on first tick).
				sched := model.Schedule{
					ID:             e.ID,
					CronExpression: e.CronExpression,
					Timezone:       e.Timezone,
					NextCmd:        e.NextCmd,
				}
				if err := r.insertDelayScheduleTx(ctx, tx, id, sched); err != nil {
					return err
				}
			} else {
				// One-shot delay: insert with the actual fire time from the event.
				// Python handles this in SideEffects.maybe_act_on via DelayScheduler;
				// doing it here (same tx) is cleaner and avoids a separate DB round-trip.
				if err := r.insertOneShotDelayTx(ctx, tx, id, e); err != nil {
					return err
				}
			}
		case *model.EvCancelSchedule:
			if err := r.deleteDelayScheduleTx(ctx, tx, id, e.DelayID); err != nil {
				return err
			}
		case *model.EvSystemCancel:
			// Delete all delay schedules for this workflow
			_, err := tx.Exec(ctx,
				fmt.Sprintf("DELETE FROM %s WHERE workflow_id = $1", r.delayScheduleTable),
				id,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// insertSubscriptionTx inserts a subscription into the subscriptions table.
func (r *Repo) insertSubscriptionTx(ctx context.Context, tx pgx.Tx, workflowID string, sub model.Sub) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (workflow_id, subscribed_to_workflow, subscribed_to_event_type, workflow_type, tags, tags_all, namespace, after_emitter_event_no)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (workflow_id, subscribed_to_workflow, subscribed_to_event_type) DO UPDATE SET
				workflow_type = EXCLUDED.workflow_type,
				tags = EXCLUDED.tags,
				tags_all = EXCLUDED.tags_all,
				namespace = EXCLUDED.namespace,
				after_emitter_event_no = EXCLUDED.after_emitter_event_no`,
			r.subscriptionsTable),
		workflowID, sub.WorkflowID, sub.EventType, r.workflowType, sub.Tags, sub.TagsAll, r.namespace, sub.AfterEmitterEventNo,
	)
	return err
}

// updateSubscriptionAddedGlobalIDsTx sets subscription_added_global_id for any
// EvSubscriptionAdded events with a horizon, matching the global_id just assigned
// by the DB to those events. Must be called inside the same transaction, after
// the events have been inserted into stored_events.
func (r *Repo) updateSubscriptionAddedGlobalIDsTx(ctx context.Context, tx pgx.Tx, workflowID string, baseVersion int64, events []model.Event) error {
	for i, event := range events {
		e, ok := event.(*model.EvSubscriptionAdded)
		if !ok || e.Sub.AfterEmitterEventNo == nil {
			continue
		}
		version := baseVersion + int64(i) + 1
		tag, err := tx.Exec(ctx,
			fmt.Sprintf(`UPDATE %s
				SET subscription_added_global_id = (
					SELECT global_id FROM %s WHERE workflow_id = $1 AND workflow_version = $2
				)
				WHERE workflow_id = $1
				  AND subscribed_to_workflow = $3
				  AND subscribed_to_event_type = $4`,
				r.subscriptionsTable, r.eventsTable),
			workflowID, version, e.Sub.WorkflowID, e.Sub.EventType,
		)
		if err != nil {
			return fmt.Errorf("updateSubscriptionAddedGlobalIDsTx: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("updateSubscriptionAddedGlobalIDsTx: no subscription row found for workflow %s -> %s/%s", workflowID, e.Sub.WorkflowID, e.Sub.EventType)
		}
	}
	return nil
}

// deleteSubscriptionTx deletes a subscription from the subscriptions table.
// Matches on the unique key (workflow_id, subscribed_to_workflow, subscribed_to_event_type),
// which is the same key used by insertSubscriptionTx's ON CONFLICT clause.
func (r *Repo) deleteSubscriptionTx(ctx context.Context, tx pgx.Tx, workflowID string, sub model.Sub) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE workflow_id = $1 AND subscribed_to_workflow = $2 AND subscribed_to_event_type = $3`,
			r.subscriptionsTable),
		workflowID, sub.WorkflowID, sub.EventType,
	)
	return err
}

// insertExternalSubscriptionTx inserts an external subscription.
func (r *Repo) insertExternalSubscriptionTx(ctx context.Context, tx pgx.Tx, workflowID string, sub model.ExternalSub) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (workflow_id, topic, workflow_type) VALUES ($1, $2, $3)
			ON CONFLICT (workflow_id, topic) DO NOTHING`,
			r.externalSubsTable),
		workflowID, sub.Topic, r.workflowType,
	)
	return err
}

// deleteExternalSubscriptionTx deletes an external subscription.
func (r *Repo) deleteExternalSubscriptionTx(ctx context.Context, tx pgx.Tx, workflowID, topic string) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE workflow_id = $1 AND topic = $2`,
			r.externalSubsTable),
		workflowID, topic,
	)
	return err
}

// insertDelayScheduleTx inserts or updates a delay schedule.
func (r *Repo) insertDelayScheduleTx(ctx context.Context, tx pgx.Tx, workflowID string, sched model.Schedule) error {
	nextCmdBytes, _ := json.Marshal(sched.NextCmd)
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (workflow_id, delay_id, workflow_type, delay_until, cron_expression, timezone, next_command, event_version, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (workflow_id, delay_id) DO UPDATE SET
				delay_until = EXCLUDED.delay_until,
				cron_expression = EXCLUDED.cron_expression,
				timezone = EXCLUDED.timezone,
				next_command = EXCLUDED.next_command,
				event_version = EXCLUDED.event_version`,
			r.delayScheduleTable),
		workflowID, sched.ID, r.workflowType, time.Now().UTC(), sched.CronExpression, sched.Timezone, nextCmdBytes, int64(0), time.Now().UTC(),
	)
	return err
}

// insertOneShotDelayTx inserts a one-shot delay schedule using the fire time from
// the EvDelay event.  Unlike insertDelayScheduleTx (which uses time.Now() as a
// sentinel for cron schedules), this sets delay_until to e.DelayUntil so the
// DelayScheduler fires at the correct wall-clock time.
func (r *Repo) insertOneShotDelayTx(ctx context.Context, tx pgx.Tx, workflowID string, e *model.EvDelay) error {
	nextCmdBytes, _ := json.Marshal(e.NextCmd)
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (workflow_id, delay_id, workflow_type, delay_until, cron_expression, timezone, next_command, event_version, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (workflow_id, delay_id) DO UPDATE SET
				delay_until    = EXCLUDED.delay_until,
				cron_expression = EXCLUDED.cron_expression,
				timezone       = EXCLUDED.timezone,
				next_command   = EXCLUDED.next_command,
				event_version  = EXCLUDED.event_version`,
			r.delayScheduleTable),
		workflowID, e.ID, r.workflowType, e.DelayUntil.UTC(), "", "", nextCmdBytes, int64(0), time.Now().UTC(),
	)
	return err
}

// deleteDelayScheduleTx deletes a delay schedule.
func (r *Repo) deleteDelayScheduleTx(ctx context.Context, tx pgx.Tx, workflowID, delayID string) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE workflow_id = $1 AND delay_id = $2`,
			r.delayScheduleTable),
		workflowID, delayID,
	)
	return err
}

// maybeSnapshotTx creates a snapshot if the version matches the interval.
func (r *Repo) maybeSnapshotTx(ctx context.Context, tx pgx.Tx, id string, state model.State, version int64) error {
	if r.snapshotInterval <= 0 || version%int64(r.snapshotInterval) != 0 {
		return nil
	}
	return r.upsertSnapshotTx(ctx, tx, id, state, version)
}

// upsertSnapshotTx creates or updates a snapshot.
func (r *Repo) upsertSnapshotTx(ctx context.Context, tx pgx.Tx, id string, state model.State, version int64) error {
	stateBytes, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	_, err = tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (workflow_id, workflow_type, version, state, created_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (workflow_id) DO UPDATE SET version = $3, state = $4, created_at = $5`,
			r.snapshotTable),
		id, r.workflowType, version, stateBytes, time.Now().UTC(),
	)
	return err
}

// loadWorkflowTagsTx loads workflow tags within a transaction.
func (r *Repo) loadWorkflowTagsTx(ctx context.Context, tx pgx.Tx, id string) ([]string, error) {
	var tags []string
	err := tx.QueryRow(ctx,
		fmt.Sprintf("SELECT tags FROM %s WHERE workflow_id = $1", r.workflowMetaTable),
		id,
	).Scan(&tags)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return tags, err
}

// parseState deserializes JSON into a State.
// This is a placeholder - concrete implementations should provide proper parsing.
func (r *Repo) parseState(data json.RawMessage) (model.State, error) {
	// The concrete workflow should provide a state parser
	// For now, return a basic StateBase
	var state model.StateBase
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// isUniqueViolation checks if an error is a PostgreSQL unique constraint violation.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "23505"
	}
	return false
}
