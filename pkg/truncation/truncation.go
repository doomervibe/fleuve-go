package truncation

import (
	"context"
	"database/sql"
	"log"
	"time"
)

type TruncationService struct {
	db              *sql.DB
	workflowType    string
	minRetention    time.Duration
	batchSize       int
	checkInterval   time.Duration
	snapshotEnabled bool
	ctx             context.Context
	cancel          context.CancelFunc
	running         bool
}

type TruncationOption func(*TruncationService)

func WithMinRetention(d time.Duration) TruncationOption {
	return func(s *TruncationService) { s.minRetention = d }
}

func WithBatchSize(n int) TruncationOption {
	return func(s *TruncationService) { s.batchSize = n }
}

func WithCheckInterval(d time.Duration) TruncationOption {
	return func(s *TruncationService) { s.checkInterval = d }
}

func WithSnapshots(enabled bool) TruncationOption {
	return func(s *TruncationService) { s.snapshotEnabled = enabled }
}

func NewTruncationService(db *sql.DB, workflowType string, opts ...TruncationOption) *TruncationService {
	s := &TruncationService{
		db:              db,
		workflowType:    workflowType,
		minRetention:    7 * 24 * time.Hour,
		batchSize:       1000,
		checkInterval:   time.Hour,
		snapshotEnabled: true,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *TruncationService) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.running = true
	go s.runLoop()
	return nil
}

func (s *TruncationService) Stop() error {
	s.running = false
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

func (s *TruncationService) runLoop() {
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.truncate(); err != nil {
				log.Printf("Truncation error: %v", err)
			}
		}
	}
}

func (s *TruncationService) truncate() error {
	if !s.snapshotEnabled {
		log.Printf("Truncation skipped: snapshots not enabled")
		return nil
	}

	cutoff := time.Now().Add(-s.minRetention)

	rows, err := s.db.QueryContext(s.ctx, `
		SELECT DISTINCT workflow_id FROM stored_events 
		WHERE workflow_type = $1 AND at < $2
	`, s.workflowType, cutoff)
	if err != nil {
		return err
	}
	defer rows.Close()

	var workflowIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		workflowIDs = append(workflowIDs, id)
	}

	for _, wfID := range workflowIDs {
		if err := s.truncateWorkflow(wfID, cutoff); err != nil {
			log.Printf("Failed to truncate workflow %s: %v", wfID, err)
			continue
		}
	}

	return nil
}

func (s *TruncationService) truncateWorkflow(workflowID string, cutoff time.Time) error {
	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var snapshotVersion int64
	err = tx.QueryRowContext(s.ctx, `
		SELECT version FROM snapshots WHERE workflow_id = $1
	`, workflowID).Scan(&snapshotVersion)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	if snapshotVersion == 0 {
		return nil
	}

	result, err := tx.ExecContext(s.ctx, `
		DELETE FROM stored_events 
		WHERE workflow_id = $1 AND workflow_version < $2 AND at < $3
	`, workflowID, snapshotVersion, cutoff)
	if err != nil {
		return err
	}

	affected, _ := result.RowsAffected()
	if affected > 0 {
		log.Printf("Truncated %d events for workflow %s (snapshot version: %d)", affected, workflowID, snapshotVersion)
	}

	return tx.Commit()
}

func (s *TruncationService) TruncateNow(ctx context.Context) (int64, error) {
	cutoff := time.Now().Add(-s.minRetention)

	result, err := s.db.ExecContext(ctx, `
		DELETE FROM stored_events 
		WHERE workflow_type = $1 AND at < $2
		AND workflow_id IN (SELECT workflow_id FROM snapshots)
		AND workflow_version < (SELECT version FROM snapshots s WHERE s.workflow_id = stored_events.workflow_id)
	`, s.workflowType, cutoff)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

type ReconciliationService struct {
	db           *sql.DB
	workflowType string
	interval     time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewReconciliationService(db *sql.DB, workflowType string, interval time.Duration) *ReconciliationService {
	return &ReconciliationService{
		db:           db,
		workflowType: workflowType,
		interval:     interval,
	}
}

func (s *ReconciliationService) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	go s.runLoop()
	return nil
}

func (s *ReconciliationService) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

func (s *ReconciliationService) runLoop() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.reconcile(); err != nil {
				log.Printf("Reconciliation error: %v", err)
			}
		}
	}
}

func (s *ReconciliationService) reconcile() error {
	rows, err := s.db.QueryContext(s.ctx, `
		SELECT DISTINCT e.workflow_id, e.workflow_version, s.version
		FROM stored_events e
		LEFT JOIN snapshots s ON e.workflow_id = s.workflow_id
		WHERE e.workflow_type = $1
		AND (s.version IS NULL OR e.workflow_version > s.version)
		AND e.workflow_version > 0
		ORDER BY e.workflow_id
	`, s.workflowType)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var workflowID string
		var eventVersion, snapshotVersion int64
		if err := rows.Scan(&workflowID, &eventVersion, &snapshotVersion); err != nil {
			continue
		}

		if snapshotVersion == 0 {
			snapshotVersion = 0
		}

		log.Printf("Reconciliation: %s at v%d (snapshot: v%d)", workflowID, eventVersion, snapshotVersion)
	}

	return nil
}
