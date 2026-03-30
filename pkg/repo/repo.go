package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/doomervibe/fleuve-go/pkg/actions"
	"github.com/doomervibe/fleuve-go/pkg/delay"
	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/postgres"
)

type SyncDBHandler func(ctx context.Context, tx *sql.Tx, workflowID string, oldState, newState model.State, events []model.Event) error

// Repo is the database/sql-backed workflow repository (distinct from PGXRepo).
type Repo struct {
	db                   *sql.DB
	workflowType         string
	workflow             model.Workflow
	es                   EphemeralStorage
	dbSubModel           string
	dbWorkflowMetadata   string
	dbExternalSubModel   string
	syncDBHandler        SyncDBHandler
	adapter              model.Adapter
	dbSnapshotModel      string
	snapshotInterval     int
	dbDelayScheduleModel string
	dbSearchAttributes   string
	namespace            *string
}

type RepoOption func(*Repo)

func WithNamespace(ns string) RepoOption {
	return func(r *Repo) { r.namespace = &ns }
}

func WithSnapshotInterval(interval int) RepoOption {
	return func(r *Repo) { r.snapshotInterval = interval }
}

func WithSyncDBHandler(handler SyncDBHandler) RepoOption {
	return func(r *Repo) { r.syncDBHandler = handler }
}

func NewRepo(
	db *sql.DB,
	workflowType string,
	workflow model.Workflow,
	es EphemeralStorage,
	opts ...RepoOption,
) *Repo {
	r := &Repo{
		db:           db,
		workflowType: workflowType,
		workflow:     workflow,
		es:           es,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Repo) CreateNew(ctx context.Context, cmd model.Command, id string, tags []string) (*model.StoredState, error) {
	events, rejection := r.workflow.Decide(nil, cmd)
	if rejection != nil {
		return nil, rejection
	}
	if len(events) == 0 {
		return nil, &model.Rejection{Msg: "Cannot create workflow with no events"}
	}

	state := r.workflow.Evolve(nil, events[0])
	for _, e := range events[1:] {
		state = r.workflow.Evolve(state, e)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for i, e := range events {
		if len(tags) > 0 {
			mergeWorkflowTagsIntoMetadata(e, tags)
		}
		bodyBytes, _ := json.Marshal(e)
		metaBytes, _ := json.Marshal(e.GetMetadata())
		_, err := tx.ExecContext(ctx, `
			INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
		`, id, i+1, r.namespaceArg(), e.GetType(), r.workflowType, r.workflow.SchemaVersion(), bodyBytes, time.Now(), metaBytes)
		if isUniqueViolation(err) {
			return nil, &model.AlreadyExists{Rejection: model.Rejection{Msg: "workflow already exists"}}
		}
		if err != nil {
			return nil, err
		}
		if err := r.handleSyncEventTx(ctx, tx, id, int64(i+1), e); err != nil {
			return nil, err
		}
	}

	if len(tags) > 0 && r.dbWorkflowMetadata != "" {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO workflow_metadata (workflow_id, workflow_type, tags, created_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (workflow_id) DO UPDATE SET tags = EXCLUDED.tags
		`, id, r.workflowType, pq.Array(tags), time.Now())
		if err != nil {
			return nil, err
		}
	}

	if r.syncDBHandler != nil {
		if err := r.syncDBHandler(ctx, tx, id, nil, state, events); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	ss := &model.StoredState{ID: id, Version: int64(len(events)), State: state}
	if !r.workflow.IsFinalEvent(events[len(events)-1]) {
		_ = r.es.PutState(ctx, ss)
	}
	r.maybeSaveSnapshot(ctx, id, ss.Version, ss.State)
	return ss, nil
}

func (r *Repo) ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection) {
	for {
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, nil, &model.Rejection{Msg: err.Error()}
		}

		var lockKey int64
		err = tx.QueryRowContext(ctx, `
			SELECT global_id FROM stored_events WHERE workflow_id = $1 AND workflow_version = 1 FOR UPDATE
		`, id).Scan(&lockKey)
		if err == sql.ErrNoRows {
			_ = tx.Rollback()
			return nil, nil, &model.Rejection{Msg: (&model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}).Error()}
		}
		if err != nil {
			_ = tx.Rollback()
			return nil, nil, &model.Rejection{Msg: err.Error()}
		}

		state, err := r.loadStateTx(ctx, tx, id, nil)
		if err != nil {
			_ = tx.Rollback()
			return nil, nil, &model.Rejection{Msg: err.Error()}
		}
		if state == nil {
			_ = tx.Rollback()
			return nil, nil, &model.Rejection{Msg: (&model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}).Error()}
		}
		if state.State == nil {
			_ = tx.Rollback()
			return nil, nil, &model.Rejection{Msg: "Workflow has completed"}
		}

		lifecycle := model.LifecycleActive
		if s, ok := state.State.(interface{ GetLifecycle() model.LifecycleState }); ok {
			lifecycle = s.GetLifecycle()
		}
		if lifecycle == model.LifecyclePaused {
			_ = tx.Rollback()
			return nil, nil, &model.Rejection{Msg: "Workflow is paused"}
		}
		if lifecycle == model.LifecycleCanceled {
			_ = tx.Rollback()
			return nil, nil, &model.Rejection{Msg: "Workflow is cancelled"}
		}

		events, rejection := r.workflow.Decide(state.State, cmd)
		if rejection != nil {
			_ = tx.Rollback()
			return nil, nil, rejection
		}
		if len(events) == 0 {
			_ = tx.Rollback()
			return state, nil, nil
		}

		newState := state.State
		for _, e := range events {
			newState = r.workflow.Evolve(newState, e)
		}

		wfTags := r.loadWorkflowTagsTx(ctx, tx, id)
		for i, e := range events {
			mergeWorkflowTagsIntoMetadata(e, wfTags)
			bodyBytes, _ := json.Marshal(e)
			metaBytes, _ := json.Marshal(e.GetMetadata())
			nextVer := state.Version + int64(i) + 1
			_, err := tx.ExecContext(ctx, `
				INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
			`, id, nextVer, r.namespaceArg(), e.GetType(), r.workflowType, r.workflow.SchemaVersion(), bodyBytes, time.Now(), metaBytes)
			if isUniqueViolation(err) {
				_ = tx.Rollback()
				continue
			}
			if err != nil {
				_ = tx.Rollback()
				return nil, nil, &model.Rejection{Msg: err.Error()}
			}
			if err := r.handleSyncEventTx(ctx, tx, id, nextVer, e); err != nil {
				_ = tx.Rollback()
				return nil, nil, &model.Rejection{Msg: err.Error()}
			}
		}

		if r.syncDBHandler != nil {
			if err := r.syncDBHandler(ctx, tx, id, state.State, newState, events); err != nil {
				_ = tx.Rollback()
				return nil, nil, &model.Rejection{Msg: err.Error()}
			}
		}

		if err := tx.Commit(); err != nil {
			if isUniqueViolation(err) {
				continue
			}
			return nil, nil, &model.Rejection{Msg: err.Error()}
		}

		newVersion := state.Version + int64(len(events))
		newSS := &model.StoredState{ID: id, Version: newVersion, State: newState}

		if r.workflow.IsFinalEvent(events[len(events)-1]) {
			_ = r.es.RemoveState(ctx, id)
		} else {
			_ = r.es.PutState(ctx, newSS)
		}

		r.maybeSaveSnapshot(ctx, id, newVersion, newState)
		return newSS, events, nil
	}
}

func (r *Repo) PauseWorkflow(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection) {
	state, err := r.GetCurrentState(ctx, id)
	if err != nil {
		return nil, &model.Rejection{Msg: err.Error()}
	}

	lifecycle := model.LifecycleActive
	if s, ok := state.State.(interface{ GetLifecycle() model.LifecycleState }); ok {
		lifecycle = s.GetLifecycle()
	}
	if lifecycle == model.LifecyclePaused {
		return nil, &model.Rejection{Msg: "Workflow is already paused"}
	}
	if lifecycle == model.LifecycleCanceled {
		return nil, &model.Rejection{Msg: "Workflow is cancelled"}
	}

	ev := &model.EvSystemPause{Reason: reason}
	bodyBytes, _ := json.Marshal(ev)
	metaBytes, _ := json.Marshal(ev.GetMetadata())

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
	`, id, state.Version+1, r.namespaceArg(), ev.GetType(), r.workflowType, r.workflow.SchemaVersion(), bodyBytes, time.Now(), metaBytes)
	if err != nil {
		return nil, &model.Rejection{Msg: err.Error()}
	}

	newSS := &model.StoredState{ID: id, Version: state.Version + 1, State: state.State}
	_ = r.es.PutState(ctx, newSS)
	return newSS, nil
}

func (r *Repo) ResumeWorkflow(ctx context.Context, id string) (*model.StoredState, *model.Rejection) {
	state, err := r.GetCurrentState(ctx, id)
	if err != nil {
		return nil, &model.Rejection{Msg: err.Error()}
	}

	lifecycle := model.LifecycleActive
	if s, ok := state.State.(interface{ GetLifecycle() model.LifecycleState }); ok {
		lifecycle = s.GetLifecycle()
	}
	if lifecycle != model.LifecyclePaused {
		return nil, &model.Rejection{Msg: "Workflow is not paused"}
	}

	ev := &model.EvSystemResume{}
	bodyBytes, _ := json.Marshal(ev)
	metaBytes, _ := json.Marshal(ev.GetMetadata())

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
	`, id, state.Version+1, r.namespaceArg(), ev.GetType(), r.workflowType, r.workflow.SchemaVersion(), bodyBytes, time.Now(), metaBytes)
	if err != nil {
		return nil, &model.Rejection{Msg: err.Error()}
	}

	newSS := &model.StoredState{ID: id, Version: state.Version + 1, State: state.State}
	_ = r.es.PutState(ctx, newSS)
	return newSS, nil
}

func (r *Repo) CancelWorkflow(ctx context.Context, id string, reason string, actionExecutor *actions.ActionExecutor) (*model.StoredState, *model.Rejection) {
	state, err := r.GetCurrentState(ctx, id)
	if err != nil {
		return nil, &model.Rejection{Msg: err.Error()}
	}

	lifecycle := model.LifecycleActive
	if s, ok := state.State.(interface{ GetLifecycle() model.LifecycleState }); ok {
		lifecycle = s.GetLifecycle()
	}
	if lifecycle == model.LifecycleCanceled {
		return nil, &model.Rejection{Msg: "Workflow is already cancelled"}
	}

	if actionExecutor != nil {
		actionExecutor.CancelWorkflowActions(id, nil)
	}

	_, _ = r.db.ExecContext(ctx, `DELETE FROM delay_schedules WHERE workflow_id = $1`, id)

	ev := &model.EvSystemCancel{Reason: reason}
	bodyBytes, _ := json.Marshal(ev)
	metaBytes, _ := json.Marshal(ev.GetMetadata())

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
	`, id, state.Version+1, r.namespaceArg(), ev.GetType(), r.workflowType, r.workflow.SchemaVersion(), bodyBytes, time.Now(), metaBytes)
	if err != nil {
		return nil, &model.Rejection{Msg: err.Error()}
	}

	newSS := &model.StoredState{ID: id, Version: state.Version + 1, State: state.State}
	_ = r.es.RemoveState(ctx, id)
	return newSS, nil
}

func (r *Repo) GetWorkflowTags(ctx context.Context, workflowID string) ([]string, error) {
	if r.dbWorkflowMetadata == "" {
		return nil, nil
	}

	var tags pq.StringArray
	err := r.db.QueryRowContext(ctx, `SELECT tags FROM workflow_metadata WHERE workflow_id = $1`, workflowID).Scan(&tags)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []string(tags), nil
}

func (r *Repo) GetCurrentState(ctx context.Context, id string) (*model.StoredState, error) {
	cached, err := r.es.GetState(ctx, id)
	if err == nil && cached != nil {
		var lastVersion int64
		err := r.db.QueryRowContext(ctx, `
			SELECT workflow_version FROM stored_events WHERE workflow_id = $1 ORDER BY workflow_version DESC LIMIT 1
		`, id).Scan(&lastVersion)
		if err == nil && cached.Version == lastVersion {
			return cached, nil
		}
	}

	state, err := r.LoadState(ctx, id, nil)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, &model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}
	}
	if state.State != nil {
		_ = r.es.PutState(ctx, state)
	} else {
		_ = r.es.RemoveState(ctx, id)
	}
	return state, nil
}

func (r *Repo) loadStateTx(ctx context.Context, tx *sql.Tx, id string, atVersion *int64) (*model.StoredState, error) {
	var baseState model.State
	baseVersion := int64(0)

	if r.dbSnapshotModel != "" {
		var snapshotState []byte
		var snapshotVersion int64
		err := tx.QueryRowContext(ctx, `
			SELECT version, state FROM snapshots WHERE workflow_id = $1
		`, id).Scan(&snapshotVersion, &snapshotState)
		if err == nil && (atVersion == nil || snapshotVersion <= *atVersion) {
			baseVersion = snapshotVersion
		}
	}

	query := `
		SELECT body, workflow_version, schema_version, event_type FROM stored_events
		WHERE workflow_id = $1 AND workflow_version > $2
	`
	args := []interface{}{id, baseVersion}
	if atVersion != nil {
		query += " AND workflow_version <= $3"
		args = append(args, *atVersion)
	}
	query += " ORDER BY workflow_version ASC"

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return r.replayStoredEventRows(rows, id, baseVersion, baseState)
}

func (r *Repo) LoadState(ctx context.Context, id string, atVersion *int64) (*model.StoredState, error) {
	var baseState model.State
	baseVersion := int64(0)

	if r.dbSnapshotModel != "" {
		var snapshotState []byte
		var snapshotVersion int64
		err := r.db.QueryRowContext(ctx, `
			SELECT version, state FROM snapshots WHERE workflow_id = $1
		`, id).Scan(&snapshotVersion, &snapshotState)
		if err == nil && (atVersion == nil || snapshotVersion <= *atVersion) {
			baseVersion = snapshotVersion
		}
	}

	query := `
		SELECT body, workflow_version, schema_version, event_type FROM stored_events
		WHERE workflow_id = $1 AND workflow_version > $2
	`
	args := []interface{}{id, baseVersion}
	if atVersion != nil {
		query += " AND workflow_version <= $3"
		args = append(args, *atVersion)
	}
	query += " ORDER BY workflow_version ASC"

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return r.replayStoredEventRows(rows, id, baseVersion, baseState)
}

func (r *Repo) replayStoredEventRows(rows *sql.Rows, workflowID string, baseVersion int64, baseState model.State) (*model.StoredState, error) {
	defer rows.Close()

	var events []model.Event
	lastVersion := baseVersion

	for rows.Next() {
		var bodyBytes []byte
		var version int64
		var schemaVersion int
		var eventType string
		if err := rows.Scan(&bodyBytes, &version, &schemaVersion, &eventType); err != nil {
			continue
		}
		lastVersion = version

		var raw map[string]any
		if err := json.Unmarshal(bodyBytes, &raw); err != nil {
			continue
		}
		ev, err := model.DecodeReplayEvent(r.workflow, eventType, schemaVersion, raw)
		if err != nil || ev == nil {
			continue
		}
		events = append(events, ev)
	}

	if len(events) == 0 {
		if baseState == nil {
			return nil, nil
		}
		return &model.StoredState{ID: workflowID, Version: baseVersion, State: baseState}, nil
	}

	state := baseState
	for _, e := range events {
		state = r.workflow.Evolve(state, e)
	}

	last := events[len(events)-1]
	if r.workflow.IsFinalEvent(last) {
		if _, ok := last.(*model.EvSystemCancel); !ok {
			return &model.StoredState{ID: workflowID, Version: lastVersion, State: nil}, nil
		}
	}

	return &model.StoredState{ID: workflowID, Version: lastVersion, State: state}, nil
}

func (r *Repo) SetSearchAttributes(ctx context.Context, workflowID string, attrs map[string]interface{}) error {
	if r.dbSearchAttributes == "" {
		return fmt.Errorf("set_search_attributes requires db_search_attributes_model")
	}
	attrsBytes, _ := json.Marshal(attrs)
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO workflow_search_attributes (workflow_id, workflow_type, attributes, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (workflow_id) DO UPDATE SET attributes = workflow_search_attributes.attributes || EXCLUDED.attributes, updated_at = EXCLUDED.updated_at
	`, workflowID, r.workflowType, attrsBytes, time.Now())
	return err
}

func (r *Repo) SearchWorkflows(ctx context.Context, attrs map[string]interface{}, limit, offset int) ([]string, error) {
	if r.dbSearchAttributes == "" {
		return nil, fmt.Errorf("search_workflows requires db_search_attributes_model")
	}
	attrsBytes, _ := json.Marshal(attrs)
	rows, err := r.db.QueryContext(ctx, `
		SELECT workflow_id FROM workflow_search_attributes
		WHERE workflow_type = $1 AND attributes @> $2
		LIMIT $3 OFFSET $4
	`, r.workflowType, attrsBytes, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (r *Repo) RepublishEvents(ctx context.Context, workflowID string, minEventID, maxEventID *int64) (int, error) {
	query := `UPDATE stored_events SET pushed = false WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if workflowID != "" {
		query += fmt.Sprintf(" AND workflow_id = $%d", argIdx)
		args = append(args, workflowID)
		argIdx++
	}
	if minEventID != nil {
		query += fmt.Sprintf(" AND global_id >= $%d", argIdx)
		args = append(args, *minEventID)
		argIdx++
	}
	if maxEventID != nil {
		query += fmt.Sprintf(" AND global_id <= $%d", argIdx)
		args = append(args, *maxEventID)
		argIdx++
	}

	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

func NextCronFire(expr, tz string) *time.Time {
	return delay.NextCronFire(expr, tz)
}

func ActivityToJSON(a *postgres.Activity) ([]byte, error) {
	return json.Marshal(a)
}
