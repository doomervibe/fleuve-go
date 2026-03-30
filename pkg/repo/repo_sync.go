package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/fleuve/fleuve-go/pkg/delay"
	"github.com/fleuve/fleuve-go/pkg/model"
)

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "23505") ||
		(strings.Contains(s, "unique") && strings.Contains(s, "violat")) ||
		strings.Contains(s, "duplicate key")
}

func mergeWorkflowTagsIntoMetadata(e model.Event, tags []string) {
	if len(tags) == 0 {
		return
	}
	m := e.GetMetadata()
	if m == nil {
		m = map[string]any{}
	}
	m["workflow_tags"] = tags
	e.SetMetadata(m)
}

func (r *Repo) namespaceArg() any {
	if r.namespace == nil {
		return nil
	}
	return *r.namespace
}

func (r *Repo) handleSyncEventTx(ctx context.Context, tx *sql.Tx, workflowID string, eventVersion int64, e model.Event) error {
	ns := r.namespaceArg()
	switch ev := e.(type) {
	case *model.EvSubscriptionAdded:
		sub := ev.Sub
		_, err := tx.ExecContext(ctx, `
			INSERT INTO subscriptions (workflow_id, workflow_type, subscribed_to_workflow, subscribed_to_event_type, tags, tags_all, namespace)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (workflow_id, subscribed_to_workflow, subscribed_to_event_type) DO NOTHING
		`, workflowID, r.workflowType, sub.WorkflowID, sub.EventType, pq.Array(sub.Tags), pq.Array(sub.TagsAll), ns)
		return err
	case *model.EvSubscriptionRemoved:
		sub := ev.Sub
		_, err := tx.ExecContext(ctx, `
			DELETE FROM subscriptions WHERE workflow_id = $1 AND subscribed_to_workflow = $2 AND subscribed_to_event_type = $3
		`, workflowID, sub.WorkflowID, sub.EventType)
		return err
	case *model.EvExternalSubscriptionAdded:
		_, err := tx.ExecContext(ctx, `
			INSERT INTO external_subscriptions (workflow_id, workflow_type, topic)
			VALUES ($1, $2, $3)
			ON CONFLICT (workflow_id, topic) DO NOTHING
		`, workflowID, r.workflowType, ev.Sub.Topic)
		return err
	case *model.EvExternalSubscriptionRemoved:
		_, err := tx.ExecContext(ctx, `DELETE FROM external_subscriptions WHERE workflow_id = $1 AND topic = $2`, workflowID, ev.Topic)
		return err
	case *model.EvDelay:
		nextCmdBytes, _ := json.Marshal(ev.NextCmd)
		_, err := tx.ExecContext(ctx, `
			INSERT INTO delay_schedules (workflow_id, delay_id, workflow_type, delay_until, event_version, cron_expression, timezone, next_command, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (workflow_id, delay_id) DO UPDATE SET
				delay_until = EXCLUDED.delay_until,
				next_command = EXCLUDED.next_command
		`, workflowID, ev.ID, r.workflowType, ev.DelayUntil, eventVersion, ev.CronExpression, ev.Timezone, nextCmdBytes, time.Now())
		return err
	case *model.EvCancelSchedule:
		_, err := tx.ExecContext(ctx, `DELETE FROM delay_schedules WHERE workflow_id = $1 AND delay_id = $2`, workflowID, ev.DelayID)
		return err
	case *model.EvScheduleAdded:
		sch := ev.Schedule
		nextCmdBytes, _ := json.Marshal(sch.NextCmd)
		delayUntil := time.Now()
		if next := delay.NextCronFire(sch.CronExpression, sch.Timezone); next != nil {
			delayUntil = *next
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO delay_schedules (workflow_id, delay_id, workflow_type, delay_until, event_version, cron_expression, timezone, next_command, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (workflow_id, delay_id) DO UPDATE SET
				delay_until = EXCLUDED.delay_until,
				next_command = EXCLUDED.next_command
		`, workflowID, sch.ID, r.workflowType, delayUntil, eventVersion, sch.CronExpression, sch.Timezone, nextCmdBytes, time.Now())
		return err
	case *model.EvScheduleRemoved:
		_, err := tx.ExecContext(ctx, `DELETE FROM delay_schedules WHERE workflow_id = $1 AND delay_id = $2`, workflowID, ev.DelayID)
		return err
	default:
		return nil
	}
}

func (r *Repo) loadWorkflowTagsTx(ctx context.Context, tx *sql.Tx, workflowID string) []string {
	if r.dbWorkflowMetadata == "" {
		return nil
	}
	var tags pq.StringArray
	err := tx.QueryRowContext(ctx, `SELECT tags FROM workflow_metadata WHERE workflow_id = $1`, workflowID).Scan(&tags)
	if err != nil {
		return nil
	}
	return []string(tags)
}

func (r *Repo) maybeSaveSnapshot(ctx context.Context, workflowID string, version int64, state model.State) {
	if r.dbSnapshotModel == "" || r.snapshotInterval <= 0 || state == nil {
		return
	}
	if version%int64(r.snapshotInterval) != 0 {
		return
	}
	stateBytes, _ := json.Marshal(state)
	_, _ = r.db.ExecContext(ctx, `
		INSERT INTO snapshots (workflow_id, workflow_type, version, state, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (workflow_id) DO UPDATE SET version = EXCLUDED.version, state = EXCLUDED.state, created_at = EXCLUDED.created_at
	`, workflowID, r.workflowType, version, stateBytes, time.Now())
}
