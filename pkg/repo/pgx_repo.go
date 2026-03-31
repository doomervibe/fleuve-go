package repo

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/actions"
	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/postgres"
)

// JetStreamCommittedPublisher pushes committed stored_events rows to JetStream for NATS-based runners.
type JetStreamCommittedPublisher interface {
	PublishWorkflowCommittedEvent(ctx context.Context, globalID int64, workflowID, workflowType string, eventNo int64, eventType string, body, metadata []byte, at time.Time) error
}

type PGXRepo struct {
	pool             *pgxpool.Pool
	workflowType     string
	workflow         model.Workflow
	es               EphemeralStorage
	snapshotInterval int
	namespace        *string
	adapter          model.Adapter
	jetStream        JetStreamCommittedPublisher
}

type jetStreamCommittedRow struct {
	globalID  int64
	eventNo   int64
	eventType string
	body      []byte
	meta      []byte
	at        time.Time
}

type PGXRepoOption func(*PGXRepo)

func WithPGXSnapshotInterval(n int) PGXRepoOption {
	return func(r *PGXRepo) { r.snapshotInterval = n }
}

func WithPGXNamespace(ns string) PGXRepoOption {
	return func(r *PGXRepo) { r.namespace = &ns }
}

// WithPGXTrustCache is deprecated: cache entries are always verified against DB head version.
func WithPGXTrustCache(_ bool) PGXRepoOption {
	return func(*PGXRepo) {}
}

func WithPGXAdapter(a model.Adapter) PGXRepoOption {
	return func(r *PGXRepo) { r.adapter = a }
}

// WithPGXJetStream publishes each committed event to JetStream after the DB transaction commits.
func WithPGXJetStream(p JetStreamCommittedPublisher) PGXRepoOption {
	return func(r *PGXRepo) { r.jetStream = p }
}

func NewPGXRepo(pool *pgxpool.Pool, workflowType string, workflow model.Workflow, es EphemeralStorage, opts ...PGXRepoOption) *PGXRepo {
	r := &PGXRepo{
		pool:         pool,
		workflowType: workflowType,
		workflow:     workflow,
		es:           es,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *PGXRepo) flushJetStreamAfterCommit(workflowID string, rows []jetStreamCommittedRow) {
	if r.jetStream == nil || len(rows) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, row := range rows {
		if err := r.jetStream.PublishWorkflowCommittedEvent(ctx, row.globalID, workflowID, r.workflowType, row.eventNo, row.eventType, row.body, row.meta, row.at); err != nil {
			log.Printf("jetstream publish global_id=%d workflow=%s: %v", row.globalID, workflowID, err)
		}
	}
}

func (r *PGXRepo) CreateNew(ctx context.Context, cmd model.Command, id string, tags []string) (*model.StoredState, error) {
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

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var jetRows []jetStreamCommittedRow
	for i, e := range events {
		if len(tags) > 0 {
			mergeWorkflowTagsIntoMetadata(e, tags)
		}
		bodyBytes, _ := json.Marshal(e)
		metaBytes, _ := json.Marshal(e.GetMetadata())
		at := time.Now()

		var gid int64
		var storedAt time.Time
		if err := tx.QueryRow(ctx, `
			INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
			RETURNING global_id, at
		`, id, i+1, r.namespaceArg(), e.GetType(), r.workflowType, r.workflow.SchemaVersion(), bodyBytes, at, metaBytes).Scan(&gid, &storedAt); err != nil {
			if isPGXUniqueViolation(err) {
				return nil, &model.AlreadyExists{Rejection: model.Rejection{Msg: "workflow already exists"}}
			}
			return nil, err
		}
		jetRows = append(jetRows, jetStreamCommittedRow{
			globalID: gid, eventNo: int64(i + 1), eventType: e.GetType(), body: bodyBytes, meta: metaBytes, at: storedAt,
		})
		if err := r.handleSyncEventPgx(ctx, tx, id, int64(i+1), e); err != nil {
			return nil, err
		}
	}

	if len(tags) > 0 {
		_, err := tx.Exec(ctx, `
			INSERT INTO workflow_metadata (workflow_id, workflow_type, tags, created_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (workflow_id) DO UPDATE SET tags = EXCLUDED.tags
		`, id, r.workflowType, tags, time.Now())
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	r.flushJetStreamAfterCommit(id, jetRows)

	ss := &model.StoredState{ID: id, Version: int64(len(events)), State: state}
	if !r.workflow.IsFinalEvent(events[len(events)-1]) {
		_ = r.es.PutState(ctx, ss)
	}
	r.maybeSaveSnapshotPgx(ctx, id, ss.Version, ss.State)
	return ss, nil
}

func (r *PGXRepo) ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection) {
	for {
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return nil, nil, &model.Rejection{Msg: err.Error()}
		}

		var lockKey int64
		err = tx.QueryRow(ctx, `
			SELECT global_id FROM stored_events WHERE workflow_id = $1 AND workflow_version = 1 FOR UPDATE
		`, id).Scan(&lockKey)
		if errors.Is(err, pgx.ErrNoRows) {
			_ = tx.Rollback(ctx)
			return nil, nil, &model.Rejection{Msg: (&model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}).Error()}
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, nil, &model.Rejection{Msg: err.Error()}
		}

		state, err := r.loadStatePgxTx(ctx, tx, id, nil)
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, nil, &model.Rejection{Msg: err.Error()}
		}
		if state == nil {
			_ = tx.Rollback(ctx)
			return nil, nil, &model.Rejection{Msg: (&model.WorkflowNotFound{ID: id, WorkflowType: r.workflowType}).Error()}
		}
		if state.State == nil {
			_ = tx.Rollback(ctx)
			return nil, nil, &model.Rejection{Msg: "Workflow has completed"}
		}

		lifecycle := model.LifecycleActive
		if s, ok := state.State.(interface{ GetLifecycle() model.LifecycleState }); ok {
			lifecycle = s.GetLifecycle()
		}
		if lifecycle == model.LifecyclePaused {
			_ = tx.Rollback(ctx)
			return nil, nil, &model.Rejection{Msg: "Workflow is paused"}
		}
		if lifecycle == model.LifecycleCanceled {
			_ = tx.Rollback(ctx)
			return nil, nil, &model.Rejection{Msg: "Workflow is cancelled"}
		}

		events, rejection := r.workflow.Decide(state.State, cmd)
		if rejection != nil {
			_ = tx.Rollback(ctx)
			return nil, nil, rejection
		}
		if len(events) == 0 {
			_ = tx.Rollback(ctx)
			return state, nil, nil
		}

		newState := state.State
		for _, e := range events {
			newState = r.workflow.Evolve(newState, e)
		}

		wfTags := r.loadWorkflowTagsPgxTx(ctx, tx, id)
		var jetRows []jetStreamCommittedRow
		for i, e := range events {
			mergeWorkflowTagsIntoMetadata(e, wfTags)
			bodyBytes, _ := json.Marshal(e)
			metaBytes, _ := json.Marshal(e.GetMetadata())
			nextVer := state.Version + int64(i) + 1
			at := time.Now()
			var gid int64
			var storedAt time.Time
			if err := tx.QueryRow(ctx, `
				INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
				RETURNING global_id, at
			`, id, nextVer, r.namespaceArg(), e.GetType(), r.workflowType, r.workflow.SchemaVersion(), bodyBytes, at, metaBytes).Scan(&gid, &storedAt); err != nil {
				if isPGXUniqueViolation(err) {
					_ = tx.Rollback(ctx)
					continue
				}
				_ = tx.Rollback(ctx)
				return nil, nil, &model.Rejection{Msg: err.Error()}
			}
			jetRows = append(jetRows, jetStreamCommittedRow{
				globalID: gid, eventNo: nextVer, eventType: e.GetType(), body: bodyBytes, meta: metaBytes, at: storedAt,
			})
			if err := r.handleSyncEventPgx(ctx, tx, id, nextVer, e); err != nil {
				_ = tx.Rollback(ctx)
				return nil, nil, &model.Rejection{Msg: err.Error()}
			}
		}

		if err := tx.Commit(ctx); err != nil {
			if isPGXUniqueViolation(err) {
				continue
			}
			return nil, nil, &model.Rejection{Msg: err.Error()}
		}

		r.flushJetStreamAfterCommit(id, jetRows)

		newVersion := state.Version + int64(len(events))
		newSS := &model.StoredState{ID: id, Version: newVersion, State: newState}

		if r.workflow.IsFinalEvent(events[len(events)-1]) {
			_ = r.es.RemoveState(ctx, id)
		} else {
			_ = r.es.PutState(ctx, newSS)
		}

		r.maybeSaveSnapshotPgx(ctx, id, newVersion, newState)
		return newSS, events, nil
	}
}

func (r *PGXRepo) PauseWorkflow(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection) {
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
	at := time.Now()
	var gid int64
	var storedAt time.Time
	if err := r.pool.QueryRow(ctx, `
		INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
		RETURNING global_id, at
	`, id, state.Version+1, r.namespaceArg(), ev.GetType(), r.workflowType, r.workflow.SchemaVersion(), bodyBytes, at, metaBytes).Scan(&gid, &storedAt); err != nil {
		return nil, &model.Rejection{Msg: err.Error()}
	}
	r.flushJetStreamAfterCommit(id, []jetStreamCommittedRow{{
		globalID: gid, eventNo: state.Version + 1, eventType: ev.GetType(), body: bodyBytes, meta: metaBytes, at: storedAt,
	}})

	newSS := &model.StoredState{ID: id, Version: state.Version + 1, State: state.State}
	_ = r.es.PutState(ctx, newSS)
	return newSS, nil
}

func (r *PGXRepo) ResumeWorkflow(ctx context.Context, id string) (*model.StoredState, *model.Rejection) {
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
	at := time.Now()
	var gid int64
	var storedAt time.Time
	if err := r.pool.QueryRow(ctx, `
		INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
		RETURNING global_id, at
	`, id, state.Version+1, r.namespaceArg(), ev.GetType(), r.workflowType, r.workflow.SchemaVersion(), bodyBytes, at, metaBytes).Scan(&gid, &storedAt); err != nil {
		return nil, &model.Rejection{Msg: err.Error()}
	}
	r.flushJetStreamAfterCommit(id, []jetStreamCommittedRow{{
		globalID: gid, eventNo: state.Version + 1, eventType: ev.GetType(), body: bodyBytes, meta: metaBytes, at: storedAt,
	}})

	newSS := &model.StoredState{ID: id, Version: state.Version + 1, State: state.State}
	_ = r.es.PutState(ctx, newSS)
	return newSS, nil
}

func (r *PGXRepo) CancelWorkflow(ctx context.Context, id string, reason string, actionExecutor *actions.ActionExecutor) (*model.StoredState, *model.Rejection) {
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

	_, _ = r.pool.Exec(ctx, `DELETE FROM delay_schedules WHERE workflow_id = $1`, id)

	ev := &model.EvSystemCancel{Reason: reason}
	bodyBytes, _ := json.Marshal(ev)
	metaBytes, _ := json.Marshal(ev.GetMetadata())
	at := time.Now()
	var gid int64
	var storedAt time.Time
	if err := r.pool.QueryRow(ctx, `
		INSERT INTO stored_events (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
		RETURNING global_id, at
	`, id, state.Version+1, r.namespaceArg(), ev.GetType(), r.workflowType, r.workflow.SchemaVersion(), bodyBytes, at, metaBytes).Scan(&gid, &storedAt); err != nil {
		return nil, &model.Rejection{Msg: err.Error()}
	}
	r.flushJetStreamAfterCommit(id, []jetStreamCommittedRow{{
		globalID: gid, eventNo: state.Version + 1, eventType: ev.GetType(), body: bodyBytes, meta: metaBytes, at: storedAt,
	}})

	newSS := &model.StoredState{ID: id, Version: state.Version + 1, State: state.State}
	_ = r.es.RemoveState(ctx, id)
	return newSS, nil
}

func (r *PGXRepo) GetCurrentState(ctx context.Context, id string) (*model.StoredState, error) {
	cached, err := r.es.GetState(ctx, id)
	if err == nil && cached != nil {
		var lastVersion int64
		err := r.pool.QueryRow(ctx, `
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

func (r *PGXRepo) loadStatePgxTx(ctx context.Context, tx pgx.Tx, id string, atVersion *int64) (*model.StoredState, error) {
	query := `
		SELECT body, workflow_version, schema_version, event_type FROM stored_events
		WHERE workflow_id = $1
	`
	args := []interface{}{id}
	if atVersion != nil {
		query += " AND workflow_version <= $2"
		args = append(args, *atVersion)
	}
	query += " ORDER BY workflow_version ASC"
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return r.replayPgxRows(rows, id)
}

func (r *PGXRepo) LoadState(ctx context.Context, id string, atVersion *int64) (*model.StoredState, error) {
	query := `
		SELECT body, workflow_version, schema_version, event_type FROM stored_events
		WHERE workflow_id = $1
	`
	args := []interface{}{id}
	if atVersion != nil {
		query += " AND workflow_version <= $2"
		args = append(args, *atVersion)
	}
	query += " ORDER BY workflow_version ASC"
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return r.replayPgxRows(rows, id)
}

func (r *PGXRepo) replayPgxRows(rows pgx.Rows, workflowID string) (*model.StoredState, error) {
	defer rows.Close()

	var events []model.Event
	lastVersion := int64(0)

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
		return nil, nil
	}

	var state model.State
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

func (r *PGXRepo) GetEvents(ctx context.Context, id string, fromVersion, limit int64) ([]*postgres.StoredEvent, error) {
	query := `
		SELECT global_id, workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed
		FROM stored_events
		WHERE workflow_id = $1 AND workflow_version >= $2
		ORDER BY workflow_version ASC
		LIMIT $3
	`
	rows, err := r.pool.Query(ctx, query, id, fromVersion, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*postgres.StoredEvent
	for rows.Next() {
		var e postgres.StoredEvent
		if err := rows.Scan(&e.GlobalID, &e.WorkflowID, &e.WorkflowVersion, &e.Namespace, &e.EventType, &e.WorkflowType, &e.SchemaVersion, &e.Body, &e.At, &e.Metadata, &e.Pushed); err != nil {
			continue
		}
		events = append(events, &e)
	}
	return events, nil
}

func (r *PGXRepo) GetWorkflowTags(ctx context.Context, workflowID string) ([]string, error) {
	var tags []string
	err := r.pool.QueryRow(ctx, `SELECT tags FROM workflow_metadata WHERE workflow_id = $1`, workflowID).Scan(&tags)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return tags, nil
}

func (r *PGXRepo) CreateDelay(ctx context.Context, workflowID, delayID string, delayUntil time.Time, eventVersion int64, nextCmd model.Command, cronExpr, tz string) error {
	nextCmdBytes, _ := json.Marshal(nextCmd)

	_, err := r.pool.Exec(ctx, `
		INSERT INTO delay_schedules (workflow_id, delay_id, workflow_type, delay_until, event_version, cron_expression, timezone, next_command, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (workflow_id, delay_id) DO UPDATE SET 
			delay_until = EXCLUDED.delay_until,
			next_command = EXCLUDED.next_command
	`, workflowID, delayID, r.workflowType, delayUntil, eventVersion, cronExpr, tz, nextCmdBytes, time.Now())
	return err
}

func (r *PGXRepo) RemoveDelay(ctx context.Context, workflowID, delayID string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM delay_schedules WHERE workflow_id = $1 AND delay_id = $2`, workflowID, delayID)
	return err
}

func (r *PGXRepo) GetPendingDelays(ctx context.Context, limit int) ([]*postgres.DelaySchedule, error) {
	query := `
		SELECT workflow_id, delay_id, workflow_type, delay_until, event_version, cron_expression, timezone, next_command, created_at
		FROM delay_schedules
		WHERE delay_until <= $1
		ORDER BY delay_until ASC
		LIMIT $2
	`
	rows, err := r.pool.Query(ctx, query, time.Now(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var delays []*postgres.DelaySchedule
	for rows.Next() {
		var d postgres.DelaySchedule
		if err := rows.Scan(&d.WorkflowID, &d.DelayID, &d.WorkflowType, &d.DelayUntil, &d.EventVersion, &d.CronExpression, &d.Timezone, &d.NextCommand, &d.CreatedAt); err != nil {
			continue
		}
		delays = append(delays, &d)
	}
	return delays, nil
}

func (r *PGXRepo) SaveSnapshot(ctx context.Context, workflowID string, version int64, state model.State) error {
	stateBytes, _ := json.Marshal(state)
	_, err := r.pool.Exec(ctx, `
		INSERT INTO snapshots (workflow_id, workflow_type, version, state, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (workflow_id) DO UPDATE SET version = EXCLUDED.version, state = EXCLUDED.state, created_at = EXCLUDED.created_at
	`, workflowID, r.workflowType, version, stateBytes, time.Now())
	return err
}

func (r *PGXRepo) GetSnapshot(ctx context.Context, workflowID string) (*postgres.Snapshot, error) {
	var s postgres.Snapshot
	err := r.pool.QueryRow(ctx, `
		SELECT workflow_id, workflow_type, version, state, created_at
		FROM snapshots WHERE workflow_id = $1
	`, workflowID).Scan(&s.WorkflowID, &s.WorkflowType, &s.Version, &s.State, &s.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func NewPGXPool(ctx context.Context, databaseURL string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}

	cfg.MaxConns = maxConns
	cfg.MinConns = 2
	cfg.HealthCheckPeriod = 1 * time.Minute
	cfg.MaxConnLifetime = 2 * time.Hour

	return pgxpool.NewWithConfig(ctx, cfg)
}

func (r *PGXRepo) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := r.pool.Exec(ctx, query, args...)
	return err
}

func (r *PGXRepo) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	return r.pool.QueryRow(ctx, query, args...)
}
