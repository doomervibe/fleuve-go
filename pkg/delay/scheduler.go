package delay

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

const (
	defaultCheckInterval = 1 * time.Second
	defaultSchemaVersion = 1
)

// delayScheduleRow represents a row from the delay_schedules table.
type delayScheduleRow struct {
	WorkflowID      string
	DelayID         string
	WorkflowType    string
	DelayUntil      time.Time
	CronExpression  string
	Timezone        string
	NextCommandJSON json.RawMessage
	CreatedAt       time.Time
}

// SchedulerOption is a functional option for DelayScheduler configuration.
type SchedulerOption func(*DelayScheduler)

// WithCheckInterval sets the interval between delay checks.
func WithCheckInterval(interval time.Duration) SchedulerOption {
	return func(s *DelayScheduler) { s.checkInterval = interval }
}

// WithDelayScheduleTable sets the delay_schedules table name.
func WithDelayScheduleTable(table string) SchedulerOption {
	return func(s *DelayScheduler) { s.delayScheduleTable = table }
}

// WithEventsTable sets the stored_events table name.
func WithEventsTable(table string) SchedulerOption {
	return func(s *DelayScheduler) { s.eventsTable = table }
}

// DelayScheduler handles one-shot and cron delay scheduling.
// It periodically checks for expired delays and resumes workflows
// by inserting EvDelayComplete events.
type DelayScheduler struct {
	pool               *pgxpool.Pool
	workflowType       string
	eventParser        model.EventParser
	checkInterval      time.Duration
	delayScheduleTable string
	eventsTable        string

	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// NewDelayScheduler creates a new DelayScheduler.
//
// Parameters:
//   - pool: PostgreSQL connection pool
//   - workflowType: the workflow type this scheduler handles
//   - eventParser: function to deserialize events from JSON
//   - opts: optional configuration
func NewDelayScheduler(
	pool *pgxpool.Pool,
	workflowType string,
	eventParser model.EventParser,
	opts ...SchedulerOption,
) *DelayScheduler {
	s := &DelayScheduler{
		pool:               pool,
		workflowType:       workflowType,
		eventParser:        eventParser,
		checkInterval:      defaultCheckInterval,
		delayScheduleTable: "delay_schedules",
		eventsTable:        "stored_events",
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// RegisterDelay registers a one-shot delay schedule.
// For ONE-SHOT delays only - cron delays are handled by sync events in the repo.
// Uses replace semantics: deletes existing schedule with same workflow_id/delay_id before inserting.
func (s *DelayScheduler) RegisterDelay(ctx context.Context, workflowID, delayID string, delayUntil time.Time, nextCmd model.Command, eventVersion int) error {
	nextCmdBytes, err := json.Marshal(nextCmd)
	if err != nil {
		return fmt.Errorf("failed to marshal next_command: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Delete existing schedule (replace semantics)
	_, err = tx.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE workflow_id = $1 AND delay_id = $2", s.delayScheduleTable),
		workflowID, delayID,
	)
	if err != nil {
		return fmt.Errorf("failed to delete existing schedule: %w", err)
	}

	// Insert new schedule
	_, err = tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (workflow_id, delay_id, workflow_type, delay_until, cron_expression, timezone, next_command, event_version, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			s.delayScheduleTable),
		workflowID, delayID, s.workflowType, delayUntil, "", "", nextCmdBytes, int64(eventVersion), time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert schedule: %w", err)
	}

	return tx.Commit(ctx)
}

// CancelDelay deletes a delay schedule for the given workflow and delay ID.
func (s *DelayScheduler) CancelDelay(ctx context.Context, workflowID, delayID string) error {
	_, err := s.pool.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE workflow_id = $1 AND delay_id = $2", s.delayScheduleTable),
		workflowID, delayID,
	)
	if err != nil {
		return fmt.Errorf("failed to cancel delay: %w", err)
	}
	return nil
}

// Start starts the delay check loop as a goroutine.
// The loop runs until Stop() is called or the context is cancelled.
func (s *DelayScheduler) Start(ctx context.Context) {
	ctx, s.cancelFunc = context.WithCancel(ctx)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.checkAndResume(ctx)
	}()
}

// Stop stops the scheduler and waits for the check loop to exit.
func (s *DelayScheduler) Stop() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	s.wg.Wait()
}

// checkAndResume is the main loop that checks for expired delays every checkInterval.
func (s *DelayScheduler) checkAndResume(ctx context.Context) {
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.processExpiredDelays(ctx)
		}
	}
}

// processExpiredDelays queries for expired delays and resumes each workflow.
func (s *DelayScheduler) processExpiredDelays(ctx context.Context) {
	now := time.Now().UTC()

	rows, err := s.pool.Query(ctx,
		fmt.Sprintf(`SELECT workflow_id, delay_id, workflow_type, delay_until, cron_expression, timezone, next_command, created_at
			FROM %s WHERE workflow_type = $1 AND delay_until <= $2`, s.delayScheduleTable),
		s.workflowType, now,
	)
	if err != nil {
		log.Printf("[delay-scheduler] failed to query expired delays: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var row delayScheduleRow
		if err := rows.Scan(
			&row.WorkflowID,
			&row.DelayID,
			&row.WorkflowType,
			&row.DelayUntil,
			&row.CronExpression,
			&row.Timezone,
			&row.NextCommandJSON,
			&row.CreatedAt,
		); err != nil {
			log.Printf("[delay-scheduler] failed to scan delay row: %v", err)
			continue
		}

		if err := s.resumeWorkflow(ctx, row); err != nil {
			log.Printf("[delay-scheduler] failed to resume workflow %s for delay %s: %v",
				row.WorkflowID, row.DelayID, err)
		}
	}

	if err := rows.Err(); err != nil {
		log.Printf("[delay-scheduler] error iterating delay rows: %v", err)
	}
}

// resumeWorkflow handles a single expired delay:
//  1. Gets current max version for workflow
//  2. Creates EvDelayComplete event and inserts it
//  3. For cron delays: computes next fire time and reschedules
//  4. For one-shot delays: deletes the schedule
func (s *DelayScheduler) resumeWorkflow(ctx context.Context, schedule delayScheduleRow) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get current max version for workflow (MAX is NULL when there are no matching rows).
	var maxVer sql.NullInt64
	err = tx.QueryRow(ctx,
		fmt.Sprintf("SELECT MAX(workflow_version) FROM %s WHERE workflow_id = $1", s.eventsTable),
		schedule.WorkflowID,
	).Scan(&maxVer)
	if err != nil {
		return fmt.Errorf("failed to get max version: %w", err)
	}

	// If none exists, workflow doesn't exist - delete schedule and return
	if !maxVer.Valid {
		_, err = tx.Exec(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE workflow_id = $1 AND delay_id = $2", s.delayScheduleTable),
			schedule.WorkflowID, schedule.DelayID,
		)
		if err != nil {
			return fmt.Errorf("failed to delete schedule for non-existent workflow: %w", err)
		}
		return tx.Commit(ctx)
	}

	currentMaxVersion := maxVer.Int64

	// Parse next_command
	var nextCmd model.Command
	if s.eventParser != nil && len(schedule.NextCommandJSON) > 0 {
		// Commands are serialized as events with a special type
		// We need to reconstruct the Command from the stored JSON
		var cmdData map[string]any
		if err := json.Unmarshal(schedule.NextCommandJSON, &cmdData); err != nil {
			return fmt.Errorf("failed to unmarshal next_command: %w", err)
		}
		// Store the raw command data to be included in EvDelayComplete
		nextCmd = &rawCommand{data: schedule.NextCommandJSON}
	}

	// Create EvDelayComplete event
	now := time.Now().UTC()
	event := &model.EvDelayComplete{
		DelayID: schedule.DelayID,
		At:      now,
		NextCmd: nextCmd,
	}

	// Insert event at version = current_max + 1
	newVersion := currentMaxVersion + 1
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	metadataBytes, _ := json.Marshal(event.GetMetadata())

	_, err = tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (workflow_id, workflow_version, namespace, event_type, workflow_type, schema_version, body, at, metadata, pushed)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)`, s.eventsTable),
		schedule.WorkflowID, newVersion, nil, event.Type(), s.workflowType, defaultSchemaVersion,
		body, now, metadataBytes,
	)
	if err != nil {
		return fmt.Errorf("failed to insert delay_complete event: %w", err)
	}

	// Handle cron vs one-shot
	if schedule.CronExpression != "" {
		// Compute next cron fire time
		nextFire, err := nextCronFire(schedule.CronExpression, schedule.Timezone)
		if err != nil {
			log.Printf("[delay-scheduler] invalid cron expression %q for workflow %s delay %s: %v",
				schedule.CronExpression, schedule.WorkflowID, schedule.DelayID, err)
			// Delete schedule on invalid cron
			_, err = tx.Exec(ctx,
				fmt.Sprintf("DELETE FROM %s WHERE workflow_id = $1 AND delay_id = $2", s.delayScheduleTable),
				schedule.WorkflowID, schedule.DelayID,
			)
			if err != nil {
				return fmt.Errorf("failed to delete schedule with invalid cron: %w", err)
			}
			return nil
		}

		// Delete old schedule and insert new with next fire time
		_, err = tx.Exec(ctx,
			fmt.Sprintf(`DELETE FROM %s WHERE workflow_id = $1 AND delay_id = $2`, s.delayScheduleTable),
			schedule.WorkflowID, schedule.DelayID,
		)
		if err != nil {
			return fmt.Errorf("failed to delete old cron schedule: %w", err)
		}

		_, err = tx.Exec(ctx,
			fmt.Sprintf(`INSERT INTO %s (workflow_id, delay_id, workflow_type, delay_until, cron_expression, timezone, next_command, event_version, created_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				s.delayScheduleTable),
			schedule.WorkflowID, schedule.DelayID, s.workflowType, nextFire,
			schedule.CronExpression, schedule.Timezone, schedule.NextCommandJSON, int64(0), time.Now().UTC(),
		)
		if err != nil {
			return fmt.Errorf("failed to insert new cron schedule: %w", err)
		}
	} else {
		// One-shot: delete schedule
		_, err = tx.Exec(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE workflow_id = $1 AND delay_id = $2", s.delayScheduleTable),
			schedule.WorkflowID, schedule.DelayID,
		)
		if err != nil {
			return fmt.Errorf("failed to delete one-shot schedule: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// nextCronFire computes the next fire time for a cron expression in the given timezone.
// Returns an error if the cron expression is invalid.
func nextCronFire(cronExpression, timezoneName string) (time.Time, error) {
	// Parse timezone, default to UTC, fallback to UTC on error
	var loc *time.Location
	if timezoneName != "" {
		var err error
		loc, err = time.LoadLocation(timezoneName)
		if err != nil {
			loc = time.UTC
		}
	} else {
		loc = time.UTC
	}

	// Parse cron expression using robfig/cron
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(cronExpression)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression: %w", err)
	}

	// Get next fire time in the specified timezone
	now := time.Now().In(loc)
	nextTime := schedule.Next(now)

	// Ensure timezone-aware (convert back to explicit timezone to preserve it)
	return nextTime.In(loc), nil
}

// rawCommand is a placeholder command type that holds raw JSON data.
// This is used when we don't have a full command parser but need to
// preserve the command data for the EvDelayComplete event.
type rawCommand struct {
	data json.RawMessage
}

func (r *rawCommand) CommandType() string {
	return "raw"
}
