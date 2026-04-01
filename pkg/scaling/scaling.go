package scaling

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/partitioning"
)

// Scaling operation statuses.
const (
	StatusPending       = "pending"
	StatusSynchronizing = "synchronizing"
	StatusCompleted     = "completed"
	StatusFailed        = "failed"
)

// Polling configuration for synchronization waiting.
const (
	pollInterval = 2 * time.Second
	pollTimeout  = 5 * time.Minute
)

// ScalingOperation represents a coordinated partition scaling operation.
// It tracks the target offset that all workers must reach before
// partition count changes can be safely applied.
type ScalingOperation struct {
	WorkflowType string
	TargetOffset int64
	Status       string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// scalingError wraps errors that occur during scaling operations.
type scalingError struct {
	op  string
	err error
}

func (e *scalingError) Error() string {
	return fmt.Sprintf("scaling %s: %v", e.op, e.err)
}

func (e *scalingError) Unwrap() error {
	return e.err
}

var (
	// ErrScalingInProgress is returned when a scaling operation is already active.
	ErrScalingInProgress = errors.New("scaling operation already in progress")
	// ErrTimeoutWaitingForSync is returned when workers fail to synchronize within the timeout.
	ErrTimeoutWaitingForSync = errors.New("timeout waiting for workers to synchronize")
)

// scale_up_partitions coordinates scaling up by adding new partitions.
// It ensures all existing workers have processed up to the target offset
// before initializing new partition readers at that offset.
//
// Steps:
//  1. Get max_offset from ALL existing partition readers
//  2. Create scaling_operation (status=pending)
//  3. Update status=synchronizing
//  4. Wait for ALL existing workers to reach target_offset (poll every 2s, timeout after 5 min)
//  5. Initialize new partition offsets to target_offset
//  6. Update status=completed
//  7. Delete scaling_operation
func ScaleUpPartitions(ctx context.Context, pool *pgxpool.Pool, workflowType string, newTotalPartitions int) error {
	// Get current max offset from all existing partition readers
	maxOffset, err := getMaxOffsetForWorkflow(ctx, pool, workflowType)
	if err != nil {
		return &scalingError{op: "get max offset", err: err}
	}

	// Get existing partition readers to determine the current partition count
	// for the sync wait. We must wait for THESE readers (with old naming), not
	// readers named with the new total partitions (which don't exist yet).
	existingReaders, err := getExistingPartitionReaders(ctx, pool, workflowType)
	if err != nil {
		return &scalingError{op: "get existing partition readers", err: err}
	}

	// Create scaling operation with pending status
	now := time.Now().UTC()
	_, err = pool.Exec(ctx,
		`INSERT INTO scaling_operations (workflow_type, target_offset, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (workflow_type) DO NOTHING`,
		workflowType, maxOffset, StatusPending, now, now,
	)
	if err != nil {
		return &scalingError{op: "create scaling operation", err: err}
	}

	// Check if we actually inserted (another process might have inserted first)
	var currentStatus string
	err = pool.QueryRow(ctx,
		`SELECT status FROM scaling_operations WHERE workflow_type = $1`,
		workflowType,
	).Scan(&currentStatus)
	if err != nil {
		return &scalingError{op: "check scaling operation", err: err}
	}
	if currentStatus != StatusPending {
		return ErrScalingInProgress
	}

	// Update status to synchronizing
	_, err = pool.Exec(ctx,
		`UPDATE scaling_operations SET status = $1, updated_at = $2 WHERE workflow_type = $3`,
		StatusSynchronizing, time.Now().UTC(), workflowType,
	)
	if err != nil {
		return &scalingError{op: "update status to synchronizing", err: err}
	}

	// Wait for all EXISTING workers to reach target offset.
	// Pass len(existingReaders) as the partition count so that reader names
	// generated internally (e.g., "wf_partition_0_of_2") match the currently
	// running workers. Passing newTotalPartitions would generate names with
	// the new total (e.g., "..._of_4") which don't exist yet, causing the wait
	// to time out or check for nonexistent readers.
	err = waitForWorkersToSync(ctx, pool, workflowType, len(existingReaders), maxOffset, false)
	if err != nil {
		_ = failScalingOperation(ctx, pool, workflowType)
		return err
	}

	// Generate ALL partition reader names with the NEW total.
	// initializePartitionOffsets uses UPDATE-only-if-below-target, so existing
	// readers are harmlessly no-opped. New readers get INSERTed at target_offset.
	allNewPartitions := make([]string, 0, newTotalPartitions)
	for i := 0; i < newTotalPartitions; i++ {
		readerName := partitioning.PartitionedReaderName(workflowType, i, newTotalPartitions)
		allNewPartitions = append(allNewPartitions, readerName)
	}

	// Initialize partition offsets to target_offset
	err = initializePartitionOffsets(ctx, pool, workflowType, allNewPartitions, maxOffset)
	if err != nil {
		_ = failScalingOperation(ctx, pool, workflowType)
		return &scalingError{op: "initialize partition offsets", err: err}
	}

	// Complete and clean up
	return completeScalingOperation(ctx, pool, workflowType)
}

// scale_down_partitions coordinates scaling down by removing partitions.
// It ensures ALL workers (including those being removed) have processed
// up to the target offset before reconfiguring remaining partitions.
//
// Steps:
//  1. Get max_offset from ALL partitions (including those being removed)
//  2. Create scaling_operation
//  3. Wait for ALL workers (including to-be-removed) to reach target_offset
//  4. Initialize remaining partition offsets to target_offset
//  5. Complete and clean up
func ScaleDownPartitions(ctx context.Context, pool *pgxpool.Pool, workflowType string, newTotalPartitions int) error {
	// We need to figure out the current partition count to get offsets from all partitions.
	// We determine this by looking at existing partition readers.
	currentPartitions, err := getExistingPartitionReaders(ctx, pool, workflowType)
	if err != nil {
		return &scalingError{op: "get existing partition readers", err: err}
	}

	if len(currentPartitions) == 0 {
		return &scalingError{op: "scale down", err: errors.New("no existing partition readers found")}
	}

	// Get max offset from ALL partitions (including those being removed)
	maxOffset, err := getMaxOffsetForWorkflow(ctx, pool, workflowType)
	if err != nil {
		return &scalingError{op: "get max offset", err: err}
	}

	// Create scaling operation with pending status
	now := time.Now().UTC()
	_, err = pool.Exec(ctx,
		`INSERT INTO scaling_operations (workflow_type, target_offset, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (workflow_type) DO NOTHING`,
		workflowType, maxOffset, StatusPending, now, now,
	)
	if err != nil {
		return &scalingError{op: "create scaling operation", err: err}
	}

	// Check if we actually inserted
	var currentStatus string
	err = pool.QueryRow(ctx,
		`SELECT status FROM scaling_operations WHERE workflow_type = $1`,
		workflowType,
	).Scan(&currentStatus)
	if err != nil {
		return &scalingError{op: "check scaling operation", err: err}
	}
	if currentStatus != StatusPending {
		return ErrScalingInProgress
	}

	// Update status to synchronizing
	_, err = pool.Exec(ctx,
		`UPDATE scaling_operations SET status = $1, updated_at = $2 WHERE workflow_type = $3`,
		StatusSynchronizing, time.Now().UTC(), workflowType,
	)
	if err != nil {
		return &scalingError{op: "update status to synchronizing", err: err}
	}

	// Wait for ALL workers (including to-be-removed) to reach target offset
	// We use the old partition count here to check all existing workers
	err = waitForWorkersToSync(ctx, pool, workflowType, len(currentPartitions), maxOffset, true)
	if err != nil {
		_ = failScalingOperation(ctx, pool, workflowType)
		return err
	}

	// Generate remaining partition reader names with new total
	remainingPartitions := make([]string, 0, newTotalPartitions)
	for i := 0; i < newTotalPartitions; i++ {
		readerName := partitioning.PartitionedReaderName(workflowType, i, newTotalPartitions)
		remainingPartitions = append(remainingPartitions, readerName)
	}

	// Initialize remaining partition offsets to target_offset
	err = initializePartitionOffsets(ctx, pool, workflowType, remainingPartitions, maxOffset)
	if err != nil {
		_ = failScalingOperation(ctx, pool, workflowType)
		return &scalingError{op: "initialize partition offsets", err: err}
	}

	// Complete and clean up
	return completeScalingOperation(ctx, pool, workflowType)
}

// check_scaling_operation checks if there is an active scaling operation for a workflow type.
// Returns the target offset and true if a pending or synchronizing operation exists.
func CheckScalingOperation(ctx context.Context, pool *pgxpool.Pool, workflowType string) (targetOffset int64, found bool, err error) {
	var status string
	err = pool.QueryRow(ctx,
		`SELECT target_offset, status FROM scaling_operations
		 WHERE workflow_type = $1 AND status IN ($2, $3)`,
		workflowType, StatusPending, StatusSynchronizing,
	).Scan(&targetOffset, &status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, &scalingError{op: "check scaling operation", err: err}
	}
	return targetOffset, true, nil
}

// initialize_partition_offsets sets partition reader offsets to the target offset.
// For existing readers: UPDATE only if current offset < target
// For new readers: INSERT with the target offset
func initializePartitionOffsets(ctx context.Context, pool *pgxpool.Pool, workflowType string, partitions []string, targetOffset int64) error {
	now := time.Now().UTC()

	for _, readerName := range partitions {
		// Try to update existing reader (only if current offset < target)
		result, err := pool.Exec(ctx,
			`UPDATE offsets SET last_read_event_no = $1, updated_at = $2
			 WHERE reader = $3 AND last_read_event_no < $1`,
			targetOffset, now, readerName,
		)
		if err != nil {
			return fmt.Errorf("update offset for %s: %w", readerName, err)
		}

		// If no rows were updated, the reader doesn't exist - insert it
		if result.RowsAffected() == 0 {
			_, err = pool.Exec(ctx,
				`INSERT INTO offsets (reader, last_read_event_no, updated_at)
				 VALUES ($1, $2, $3)
				 ON CONFLICT (reader) DO NOTHING`,
				readerName, targetOffset, now,
			)
			if err != nil {
				return fmt.Errorf("insert offset for %s: %w", readerName, err)
			}
		}
	}

	return nil
}

// getMaxOffsetForWorkflow retrieves the maximum offset from all existing partition readers
// for a given workflow type.
func getMaxOffsetForWorkflow(ctx context.Context, pool *pgxpool.Pool, workflowType string) (int64, error) {
	// Match readers that follow the partition naming convention
	// Pattern: {workflow_type}_runner_partition_{index}_of_{count}
	var maxOffset int64
	err := pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(last_read_event_no), 0)
		 FROM offsets
		 WHERE reader LIKE $1`,
		workflowType+"_runner_partition_%",
	).Scan(&maxOffset)
	if err != nil {
		return 0, fmt.Errorf("query max offset: %w", err)
	}
	return maxOffset, nil
}

// getExistingPartitionReaders returns all existing partition reader names for a workflow type.
func getExistingPartitionReaders(ctx context.Context, pool *pgxpool.Pool, workflowType string) ([]string, error) {
	rows, err := pool.Query(ctx,
		`SELECT reader FROM offsets WHERE reader LIKE $1 ORDER BY reader`,
		workflowType+"_runner_partition_%",
	)
	if err != nil {
		return nil, fmt.Errorf("query partition readers: %w", err)
	}
	defer rows.Close()

	var readers []string
	for rows.Next() {
		var reader string
		if err := rows.Scan(&reader); err != nil {
			return nil, fmt.Errorf("scan reader: %w", err)
		}
		readers = append(readers, reader)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate readers: %w", err)
	}

	return readers, nil
}

// waitForWorkersToSync polls until all partition workers have reached the target offset.
// If includeAllPartitions is true, it checks all partitions up to totalPartitions.
// If false, it only checks partitions 0 to totalPartitions-1 (for scale up).
func waitForWorkersToSync(ctx context.Context, pool *pgxpool.Pool, workflowType string, totalPartitions int, targetOffset int64, includeAllPartitions bool) error {
	ctx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ErrTimeoutWaitingForSync
			}
			return ctx.Err()
		case <-ticker.C:
			synced, err := checkAllWorkersSynced(ctx, pool, workflowType, totalPartitions, targetOffset, includeAllPartitions)
			if err != nil {
				return err
			}
			if synced {
				return nil
			}
		}
	}
}

// checkAllWorkersSynced verifies that all partition workers have reached or exceeded the target offset.
func checkAllWorkersSynced(ctx context.Context, pool *pgxpool.Pool, workflowType string, totalPartitions int, targetOffset int64, includeAllPartitions bool) (bool, error) {
	// Build reader names to check
	readers := make([]string, 0, totalPartitions)
	for i := 0; i < totalPartitions; i++ {
		readers = append(readers, partitioning.PartitionedReaderName(workflowType, i, totalPartitions))
	}

	if len(readers) == 0 {
		return true, nil
	}

	// Query for any reader that hasn't reached the target offset
	query := `SELECT COUNT(*) FROM offsets WHERE reader = ANY($1) AND last_read_event_no < $2`
	var laggingCount int
	err := pool.QueryRow(ctx, query, readers, targetOffset).Scan(&laggingCount)
	if err != nil {
		return false, fmt.Errorf("check workers synced: %w", err)
	}

	// If includeAllPartitions, also check for readers that don't exist yet
	// (they should have been initialized during scale operations)
	if includeAllPartitions {
		query = `SELECT COUNT(*) FROM unnest($1::varchar[]) AS reader WHERE NOT EXISTS (
			SELECT 1 FROM offsets WHERE offsets.reader = reader
		)`
		var missingCount int
		err := pool.QueryRow(ctx, query, readers).Scan(&missingCount)
		if err != nil {
			return false, fmt.Errorf("check missing readers: %w", err)
		}
		if missingCount > 0 {
			return false, nil
		}
	}

	return laggingCount == 0, nil
}

// failScalingOperation marks a scaling operation as failed.
func failScalingOperation(ctx context.Context, pool *pgxpool.Pool, workflowType string) error {
	_, err := pool.Exec(ctx,
		`UPDATE scaling_operations SET status = $1, updated_at = $2 WHERE workflow_type = $3`,
		StatusFailed, time.Now().UTC(), workflowType,
	)
	if err != nil {
		return fmt.Errorf("fail scaling operation: %w", err)
	}
	return nil
}

// completeScalingOperation marks a scaling operation as completed and deletes it.
func completeScalingOperation(ctx context.Context, pool *pgxpool.Pool, workflowType string) error {
	// Update status to completed
	_, err := pool.Exec(ctx,
		`UPDATE scaling_operations SET status = $1, updated_at = $2 WHERE workflow_type = $3`,
		StatusCompleted, time.Now().UTC(), workflowType,
	)
	if err != nil {
		return fmt.Errorf("complete scaling operation: %w", err)
	}

	// Delete the scaling operation
	_, err = pool.Exec(ctx,
		`DELETE FROM scaling_operations WHERE workflow_type = $1`,
		workflowType,
	)
	if err != nil {
		return fmt.Errorf("delete scaling operation: %w", err)
	}

	return nil
}
