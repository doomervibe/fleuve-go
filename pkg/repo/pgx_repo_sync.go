package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/doomervibe/fleuve-go/pkg/delay"
	"github.com/doomervibe/fleuve-go/pkg/model"
)

func isPGXUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return err != nil && errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (r *PGXRepo) namespaceArg() any {
	if r.namespace == nil {
		return nil
	}
	return *r.namespace
}

func (r *PGXRepo) handleSyncEventPgx(ctx context.Context, tx pgx.Tx, workflowID string, eventVersion int64, e model.Event) error {
	ns := r.namespaceArg()
	switch ev := e.(type) {
	case *model.EvSubscriptionAdded:
		sub := ev.Sub
		_, err := tx.Exec(ctx, `
			INSERT INTO subscriptions (workflow_id, workflow_type, subscribed_to_workflow, subscribed_to_event_type, tags, tags_all, namespace)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (workflow_id, subscribed_to_workflow, subscribed_to_event_type) DO NOTHING
		`, workflowID, r.workflowType, sub.WorkflowID, sub.EventType, sub.Tags, sub.TagsAll, ns)
		return err
	case *model.EvSubscriptionRemoved:
		sub := ev.Sub
		_, err := tx.Exec(ctx, `
			DELETE FROM subscriptions WHERE workflow_id = $1 AND subscribed_to_workflow = $2 AND subscribed_to_event_type = $3
		`, workflowID, sub.WorkflowID, sub.EventType)
		return err
	case *model.EvExternalSubscriptionAdded:
		_, err := tx.Exec(ctx, `
			INSERT INTO external_subscriptions (workflow_id, workflow_type, topic)
			VALUES ($1, $2, $3)
			ON CONFLICT (workflow_id, topic) DO NOTHING
		`, workflowID, r.workflowType, ev.Sub.Topic)
		return err
	case *model.EvExternalSubscriptionRemoved:
		_, err := tx.Exec(ctx, `DELETE FROM external_subscriptions WHERE workflow_id = $1 AND topic = $2`, workflowID, ev.Topic)
		return err
	case *model.EvDelay:
		nextCmdBytes, err := json.Marshal(ev.NextCmd)
		if err != nil {
			return fmt.Errorf("marshal delay command: %w", err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO delay_schedules (workflow_id, delay_id, workflow_type, delay_until, event_version, cron_expression, timezone, next_command, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (workflow_id, delay_id) DO UPDATE SET
				delay_until = EXCLUDED.delay_until,
				next_command = EXCLUDED.next_command
		`, workflowID, ev.ID, r.workflowType, ev.DelayUntil, eventVersion, ev.CronExpression, ev.Timezone, nextCmdBytes, time.Now())
		return err
	case *model.EvCancelSchedule:
		_, err := tx.Exec(ctx, `DELETE FROM delay_schedules WHERE workflow_id = $1 AND delay_id = $2`, workflowID, ev.DelayID)
		return err
	case *model.EvScheduleAdded:
		sch := ev.Schedule
		nextCmdBytes, err := json.Marshal(sch.NextCmd)
		if err != nil {
			return fmt.Errorf("marshal schedule command: %w", err)
		}
		delayUntil := time.Now()
		if next := delay.NextCronFire(sch.CronExpression, sch.Timezone); next != nil {
			delayUntil = *next
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO delay_schedules (workflow_id, delay_id, workflow_type, delay_until, event_version, cron_expression, timezone, next_command, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (workflow_id, delay_id) DO UPDATE SET
				delay_until = EXCLUDED.delay_until,
				next_command = EXCLUDED.next_command
		`, workflowID, sch.ID, r.workflowType, delayUntil, eventVersion, sch.CronExpression, sch.Timezone, nextCmdBytes, time.Now())
		return err
	case *model.EvScheduleRemoved:
		_, err := tx.Exec(ctx, `DELETE FROM delay_schedules WHERE workflow_id = $1 AND delay_id = $2`, workflowID, ev.DelayID)
		return err
	default:
		return nil
	}
}

func (r *PGXRepo) loadWorkflowTagsPgxTx(ctx context.Context, tx pgx.Tx, workflowID string) []string {
	var tags []string
	err := tx.QueryRow(ctx, `SELECT tags FROM workflow_metadata WHERE workflow_id = $1`, workflowID).Scan(&tags)
	if err != nil {
		return nil
	}
	return tags
}

func (r *PGXRepo) maybeSaveSnapshotPgx(ctx context.Context, workflowID string, version int64, state model.State) {
	if r.snapshotInterval <= 0 || state == nil {
		return
	}
	if version%int64(r.snapshotInterval) != 0 {
		return
	}
	_ = r.SaveSnapshot(ctx, workflowID, version, state)
}
