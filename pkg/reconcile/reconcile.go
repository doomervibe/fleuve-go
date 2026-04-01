package reconcile

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultCheckInterval  = 5 * time.Minute
	defaultStuckThreshold = 30 * time.Minute
	defaultEventsTable    = "stored_events"
	defaultRecentWindow   = 10 * time.Second
)

// ReconcilerOption is a functional option for Reconciler configuration.
type ReconcilerOption func(*Reconciler)

// WithCheckInterval sets the interval between reconciliation cycles.
func WithCheckInterval(interval time.Duration) ReconcilerOption {
	return func(r *Reconciler) { r.checkInterval = interval }
}

// WithStuckThreshold sets the age threshold for considering an event stuck.
func WithStuckThreshold(threshold time.Duration) ReconcilerOption {
	return func(r *Reconciler) { r.stuckThreshold = threshold }
}

// WithEventsTable sets the stored_events table name.
func WithEventsTable(table string) ReconcilerOption {
	return func(r *Reconciler) { r.eventsTable = table }
}

// WithRecentWindow sets the window for excluding events from workflows with recent activity.
// Events from workflows that had activity within this window are not considered stuck,
// as they may be part of an active transaction or recent burst.
func WithRecentWindow(window time.Duration) ReconcilerOption {
	return func(r *Reconciler) { r.recentWindow = window }
}

// StuckEvent represents an event that has not been published within the expected timeframe.
type StuckEvent struct {
	GlobalID   int64
	WorkflowID string
	EventType  string
	At         time.Time
	Pushed     bool
	Age        time.Duration
}

// Reconciler monitors for stuck events and facilitates their republishing.
// Stuck events are events that were inserted but never published to the outbox
// (pushed = false) beyond the stuck threshold duration.
//
// The reconciler runs a periodic check and can trigger republishing of stuck events
// by resetting their pushed flag, which causes the OutboxPublisher to pick them up.
type Reconciler struct {
	pool           *pgxpool.Pool
	workflowType   string
	checkInterval  time.Duration
	stuckThreshold time.Duration
	eventsTable    string
	recentWindow   time.Duration

	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// NewReconciler creates a new Reconciler for stuck event monitoring.
//
// Parameters:
//   - pool: PostgreSQL connection pool
//   - workflowType: the workflow type this reconciler handles
//   - opts: optional configuration
func NewReconciler(
	pool *pgxpool.Pool,
	workflowType string,
	opts ...ReconcilerOption,
) *Reconciler {
	r := &Reconciler{
		pool:           pool,
		workflowType:   workflowType,
		checkInterval:  defaultCheckInterval,
		stuckThreshold: defaultStuckThreshold,
		eventsTable:    defaultEventsTable,
		recentWindow:   defaultRecentWindow,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Start starts the reconciliation loop as a goroutine.
// The loop runs until Stop() is called or the context is cancelled.
func (r *Reconciler) Start(ctx context.Context) {
	ctx, r.cancelFunc = context.WithCancel(ctx)

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.runLoop(ctx)
	}()
}

// Stop stops the reconciler and waits for the loop to exit.
func (r *Reconciler) Stop() {
	if r.cancelFunc != nil {
		r.cancelFunc()
	}
	r.wg.Wait()
}

// runLoop is the main loop that runs reconciliation cycles every checkInterval.
func (r *Reconciler) runLoop(ctx context.Context) {
	ticker := time.NewTicker(r.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runReconciliationCycle(ctx)
		}
	}
}

// runReconciliationCycle performs one reconciliation cycle:
//  1. Find stuck events
//  2. Republish them by resetting pushed flag
func (r *Reconciler) runReconciliationCycle(ctx context.Context) {
	events, err := r.FindStuckEvents(ctx)
	if err != nil {
		log.Printf("[reconcile] failed to find stuck events: %v", err)
		return
	}

	if len(events) == 0 {
		return
	}

	log.Printf("[reconcile] found %d stuck events for workflow type %s", len(events), r.workflowType)

	if err := r.RepublishStuckEvents(ctx, events); err != nil {
		log.Printf("[reconcile] failed to republish stuck events: %v", err)
		return
	}

	log.Printf("[reconcile] republished %d stuck events for workflow type %s", len(events), r.workflowType)
}

// FindStuckEvents finds events that are:
//   - Not pushed (pushed = false)
//   - Older than stuck threshold
//   - Not part of recent transactions (workflow has no activity within recent window)
//
// The recent window filter excludes events from workflows that have had very recent
// activity, as those events may be part of an active transaction burst that the
// OutboxPublisher hasn't caught up with yet.
func (r *Reconciler) FindStuckEvents(ctx context.Context) ([]StuckEvent, error) {
	cutoffTime := time.Now().UTC().Add(-r.stuckThreshold)
	recentCutoff := time.Now().UTC().Add(-r.recentWindow)

	// Query for stuck events, excluding those from workflows with recent activity.
	// This prevents false positives when a workflow has a burst of events that
	// the publisher is still processing.
	query := fmt.Sprintf(`
		SELECT e.global_id, e.workflow_id, e.event_type, e.at, e.pushed
		FROM %s e
		WHERE e.workflow_type = $1
		  AND e.pushed = false
		  AND e.at < $2
		  AND NOT EXISTS (
		      SELECT 1 FROM %s newer
		      WHERE newer.workflow_id = e.workflow_id
		        AND newer.at > $3
		  )
		ORDER BY e.global_id
	`, r.eventsTable, r.eventsTable)

	rows, err := r.pool.Query(ctx, query, r.workflowType, cutoffTime, recentCutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to query stuck events: %w", err)
	}
	defer rows.Close()

	now := time.Now().UTC()
	var events []StuckEvent
	for rows.Next() {
		var e StuckEvent
		if err := rows.Scan(&e.GlobalID, &e.WorkflowID, &e.EventType, &e.At, &e.Pushed); err != nil {
			return nil, fmt.Errorf("failed to scan stuck event row: %w", err)
		}
		e.Age = now.Sub(e.At)
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating stuck event rows: %w", err)
	}

	return events, nil
}

// RepublishStuckEvents marks events as pushed = false for republishing.
// This triggers the OutboxPublisher to pick them up on its next poll cycle.
//
// Note: Events found by FindStuckEvents are already pushed = false, so calling
// this function on those events is idempotent. This function is primarily useful
// for resetting events that were marked as pushed = true but need to be
// republished (e.g., due to downstream processing failures).
func (r *Reconciler) RepublishStuckEvents(ctx context.Context, events []StuckEvent) error {
	if len(events) == 0 {
		return nil
	}

	globalIDs := make([]int64, len(events))
	for i, e := range events {
		globalIDs[i] = e.GlobalID
	}

	query := fmt.Sprintf(
		`UPDATE %s SET pushed = false WHERE global_id = ANY($1)`,
		r.eventsTable,
	)

	result, err := r.pool.Exec(ctx, query, globalIDs)
	if err != nil {
		return fmt.Errorf("failed to mark events for republishing: %w", err)
	}

	if affected := result.RowsAffected(); affected > 0 {
		log.Printf("[reconcile] updated %d events for republishing", affected)
	}

	return nil
}
