package truncation

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultCheckInterval = 1 * time.Hour
	defaultMinRetention  = 7 * 24 * time.Hour
	defaultBatchSize     = 1000
)

// TruncationOption is a functional option for TruncationService configuration.
type TruncationOption func(*TruncationService)

// WithCheckInterval sets the interval between truncation cycles.
func WithCheckInterval(interval time.Duration) TruncationOption {
	return func(s *TruncationService) { s.checkInterval = interval }
}

// WithMinRetention sets the minimum retention period before events can be truncated.
func WithMinRetention(retention time.Duration) TruncationOption {
	return func(s *TruncationService) { s.minRetention = retention }
}

// WithBatchSize sets the maximum number of events to delete per workflow per cycle.
func WithBatchSize(size int) TruncationOption {
	return func(s *TruncationService) { s.batchSize = size }
}

// WithEventsTable sets the stored_events table name.
func WithEventsTable(table string) TruncationOption {
	return func(s *TruncationService) { s.eventsTable = table }
}

// WithSnapshotsTable sets the snapshots table name.
func WithSnapshotsTable(table string) TruncationOption {
	return func(s *TruncationService) { s.snapshotsTable = table }
}

// WithOffsetsTable sets the offsets table name.
func WithOffsetsTable(table string) TruncationOption {
	return func(s *TruncationService) { s.offsetsTable = table }
}

// snapshotRow represents a row from the snapshots table.
type snapshotRow struct {
	WorkflowID string
	Version    int64
}

// TruncationService safely deletes old events that are covered by snapshots.
// Events are only deleted when ALL of the following conditions are met:
//   - Event is before the snapshot version (snapshot covers it)
//   - Event's global_id is below minimum reader offset (all readers processed it)
//   - Event has been published to NATS (outbox complete, pushed = true)
//   - Event is older than minimum retention period
type TruncationService struct {
	pool           *pgxpool.Pool
	workflowType   string
	checkInterval  time.Duration
	minRetention   time.Duration
	batchSize      int
	eventsTable    string
	snapshotsTable string
	offsetsTable   string

	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// NewTruncationService creates a new TruncationService.
//
// Parameters:
//   - pool: PostgreSQL connection pool
//   - workflowType: the workflow type this truncator handles
//   - opts: optional configuration
func NewTruncationService(
	pool *pgxpool.Pool,
	workflowType string,
	opts ...TruncationOption,
) *TruncationService {
	s := &TruncationService{
		pool:           pool,
		workflowType:   workflowType,
		checkInterval:  defaultCheckInterval,
		minRetention:   defaultMinRetention,
		batchSize:      defaultBatchSize,
		eventsTable:    "stored_events",
		snapshotsTable: "snapshots",
		offsetsTable:   "offsets",
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Start starts the truncation loop as a goroutine.
// The loop runs until Stop() is called or the context is cancelled.
func (s *TruncationService) Start(ctx context.Context) {
	ctx, s.cancelFunc = context.WithCancel(ctx)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runLoop(ctx)
	}()
}

// Stop stops the truncation service and waits for the loop to exit.
func (s *TruncationService) Stop() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	s.wg.Wait()
}

// runLoop is the main loop that runs truncation cycles every checkInterval.
func (s *TruncationService) runLoop(ctx context.Context) {
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runTruncationCycle(ctx)
		}
	}
}

// runTruncationCycle performs one truncation cycle:
//  1. Get minimum reader offset from offsets table
//  2. Get all snapshots for this workflow type
//  3. For each snapshot, delete eligible events in batches
func (s *TruncationService) runTruncationCycle(ctx context.Context) {
	// Step 1: Get minimum reader offset
	minOffset, err := s.getMinReaderOffset(ctx)
	if err != nil {
		log.Printf("[truncation] failed to get min reader offset: %v", err)
		return
	}

	// If no readers have recorded offsets, we cannot safely truncate anything
	if minOffset == 0 {
		log.Printf("[truncation] no reader offsets recorded, skipping cycle")
		return
	}

	// Step 2: Get all snapshots for this workflow type
	snapshots, err := s.getSnapshots(ctx)
	if err != nil {
		log.Printf("[truncation] failed to get snapshots: %v", err)
		return
	}

	if len(snapshots) == 0 {
		log.Printf("[truncation] no snapshots found, skipping cycle")
		return
	}

	// Step 3: Calculate cutoff time
	cutoffTime := time.Now().UTC().Add(-s.minRetention)

	// Step 4: Delete eligible events for each snapshot
	var totalDeleted int64
	for _, snap := range snapshots {
		deleted, err := s.deleteEventsForSnapshot(ctx, snap, minOffset, cutoffTime)
		if err != nil {
			log.Printf("[truncation] failed to delete events for workflow %s: %v",
				snap.WorkflowID, err)
			continue
		}
		totalDeleted += deleted
	}

	if totalDeleted > 0 {
		log.Printf("[truncation] deleted %d events for workflow type %s",
			totalDeleted, s.workflowType)
	}
}

// getMinReaderOffset returns the minimum last_read_event_no from the offsets table.
// Returns 0 if no offsets exist.
func (s *TruncationService) getMinReaderOffset(ctx context.Context) (int64, error) {
	var minOffset *int64
	err := s.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT MIN(last_read_event_no) FROM %s", s.offsetsTable),
	).Scan(&minOffset)

	if err != nil {
		return 0, fmt.Errorf("failed to query min offset: %w", err)
	}

	if minOffset == nil {
		return 0, nil
	}

	return *minOffset, nil
}

// getSnapshots returns all (workflow_id, version) pairs from snapshots table
// for this workflow type.
func (s *TruncationService) getSnapshots(ctx context.Context) ([]snapshotRow, error) {
	rows, err := s.pool.Query(ctx,
		fmt.Sprintf("SELECT workflow_id, version FROM %s WHERE workflow_type = $1",
			s.snapshotsTable),
		s.workflowType,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %w", err)
	}
	defer rows.Close()

	var snapshots []snapshotRow
	for rows.Next() {
		var row snapshotRow
		if err := rows.Scan(&row.WorkflowID, &row.Version); err != nil {
			return nil, fmt.Errorf("failed to scan snapshot row: %w", err)
		}
		snapshots = append(snapshots, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating snapshot rows: %w", err)
	}

	return snapshots, nil
}

// deleteEventsForSnapshot deletes events that are:
//   - For the given workflow_id
//   - Before the snapshot version
//   - Have global_id below minOffset (all readers processed)
//   - Have been published to NATS (pushed = true)
//   - Older than cutoffTime
//
// Returns the number of deleted events.
func (s *TruncationService) deleteEventsForSnapshot(
	ctx context.Context,
	snap snapshotRow,
	minOffset int64,
	cutoffTime time.Time,
) (int64, error) {
	result, err := s.pool.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s
			WHERE workflow_id = $1
			AND workflow_version < $2
			AND global_id < $3
			AND pushed = true
			AND at < $4
			LIMIT %d`, s.eventsTable, s.batchSize),
		snap.WorkflowID,
		snap.Version,
		minOffset,
		cutoffTime,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to delete events: %w", err)
	}

	return result.RowsAffected(), nil
}
