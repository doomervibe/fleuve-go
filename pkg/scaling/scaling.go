package scaling

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type PartitionedRunnerConfig struct {
	WorkflowType string
	ReaderName   string
	WFIDRule     func(string) bool
	PartitionID  int
	TotalParts   int
}

type ScalingOperation struct {
	WorkflowType string    `db:"workflow_type"`
	TargetOffset int64     `db:"target_offset"`
	Status       string    `db:"status"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
}

func GetMaxOffset(ctx context.Context, db *sql.DB, readerPrefix string) (int64, error) {
	var offset int64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(last_read_event_no), 0) FROM offsets WHERE reader LIKE $1
	`, readerPrefix+"%").Scan(&offset)
	return offset, err
}

func GetMinOffset(ctx context.Context, db *sql.DB, readerPrefix string) (int64, error) {
	var offset int64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(MIN(last_read_event_no), 0) FROM offsets WHERE reader LIKE $1
	`, readerPrefix+"%").Scan(&offset)
	return offset, err
}

func MigrateOffsetsOnScaleUp(ctx context.Context, db *sql.DB, readerName string, initialOffset int64) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO offsets (reader, last_read_event_no) VALUES ($1, $2)
		ON CONFLICT (reader) DO UPDATE SET last_read_event_no = EXCLUDED.last_read_event_no
	`, readerName, initialOffset)
	return err
}

func MergeOffsetsOnScaleDown(ctx context.Context, db *sql.DB, readerName string) error {
	_, err := db.ExecContext(ctx, `
		DELETE FROM offsets WHERE reader = $1
	`, readerName)
	return err
}

func CreateScalingOperation(ctx context.Context, db *sql.DB, workflowType string, targetOffset int64) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO scaling_operations (workflow_type, target_offset, status, created_at, updated_at)
		VALUES ($1, $2, 'pending', NOW(), NOW())
		ON CONFLICT (workflow_type) DO UPDATE SET 
			target_offset = EXCLUDED.target_offset,
			status = EXCLUDED.status,
			updated_at = EXCLUDED.updated_at
	`, workflowType, targetOffset)
	return err
}

func WaitForWorkersToReachOffset(ctx context.Context, db *sql.DB, workflowType string, targetOffset int64, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var allReached bool
			err := db.QueryRowContext(ctx, `
				SELECT BOOL_AND(last_read_event_no >= $1) FROM offsets 
				WHERE reader LIKE $2 || '%'
			`, targetOffset, workflowType+"_runner").Scan(&allReached)
			if err != nil && err != sql.ErrNoRows {
				continue
			}
			if allReached {
				return nil
			}
		}
	}
}

func CompleteScalingOperation(ctx context.Context, db *sql.DB, workflowType string) error {
	_, err := db.ExecContext(ctx, `
		UPDATE scaling_operations SET status = 'completed', updated_at = NOW()
		WHERE workflow_type = $1
	`, workflowType)
	return err
}

func RebalancePartitions(ctx context.Context, db *sql.DB, workflowType string, numPartitions int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var maxOffset int64
	err = tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(last_read_event_no), 0) FROM offsets WHERE reader LIKE $1 || '%'
	`, workflowType).Scan(&maxOffset)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	_, err = tx.ExecContext(ctx, `DELETE FROM offsets WHERE reader LIKE $1 || '%'`, workflowType)
	if err != nil {
		return err
	}

	for i := 0; i < numPartitions; i++ {
		readerName := fmt.Sprintf("%s_runner_p%d", workflowType, i)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO offsets (reader, last_read_event_no) VALUES ($1, $2)
		`, readerName, 0)
		if err != nil {
			return err
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE scaling_operations SET status = 'completed', updated_at = NOW()
		WHERE workflow_type = $1
	`, workflowType)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func ScaleUpPartitions(ctx context.Context, db *sql.DB, workflowType string, newPartitionCount int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	maxOffset, err := GetMaxOffset(ctx, db, workflowType+"_runner")
	if err != nil {
		return err
	}

	if err := CreateScalingOperation(ctx, db, workflowType, maxOffset); err != nil {
		return err
	}

	for i := 0; i < newPartitionCount; i++ {
		readerName := fmt.Sprintf("%s_runner_p%d", workflowType, i)
		if err := MigrateOffsetsOnScaleUp(ctx, db, readerName, maxOffset); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func ScaleDownPartitions(ctx context.Context, db *sql.DB, workflowType string, newPartitionCount int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT reader FROM offsets WHERE reader LIKE $1 || '%' ORDER BY reader
	`, workflowType+"_runner")
	if err != nil {
		return err
	}
	defer rows.Close()

	var readers []string
	for rows.Next() {
		var reader string
		if err := rows.Scan(&reader); err != nil {
			continue
		}
		readers = append(readers, reader)
	}

	for i := newPartitionCount; i < len(readers); i++ {
		if err := MergeOffsetsOnScaleDown(ctx, db, readers[i]); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func GetActiveScalingOperation(ctx context.Context, db *sql.DB, workflowType string) (*ScalingOperation, error) {
	var op ScalingOperation
	err := db.QueryRowContext(ctx, `
		SELECT workflow_type, target_offset, status, created_at, updated_at
		FROM scaling_operations
		WHERE workflow_type = $1 AND status IN ('pending', 'synchronizing')
	`, workflowType).Scan(&op.WorkflowType, &op.TargetOffset, &op.Status, &op.CreatedAt, &op.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &op, nil
}
