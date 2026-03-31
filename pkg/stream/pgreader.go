package stream

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"
)

type PGReader struct {
	db               *sql.DB
	eventModel       string
	offsetModel      string
	readerName       string
	batchSize        int
	stopAtOffset     *int64
	committedOffset  int64
	lastReadEventGID int64
	mu               sync.RWMutex
}

func NewPGReader(db *sql.DB, readerName string, batchSize int) *PGReader {
	return &PGReader{
		db:          db,
		readerName:  readerName,
		batchSize:   batchSize,
		eventModel:  "stored_events",
		offsetModel: "offsets",
	}
}

func (r *PGReader) Init(ctx context.Context) error {
	var offset int64
	err := r.db.QueryRowContext(ctx, `
		SELECT COALESCE(last_read_event_no, 0) FROM offsets WHERE reader = $1
	`, r.readerName).Scan(&offset)
	if err != nil && err != sql.ErrNoRows {
		offset = 0
	}
	r.committedOffset = offset
	return nil
}

func (r *PGReader) IterEvents(ctx context.Context) <-chan *ConsumedEvent {
	ch := make(chan *ConsumedEvent, 100)

	go func() {
		defer close(ch)
		sleeper := NewSleeper(100*time.Millisecond, 5*time.Second)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if r.stopAtOffset != nil && r.lastReadEventGID >= *r.stopAtOffset {
				return
			}

			events, err := r.readBatch(ctx)
			if err != nil {
				if serr := sleeper.Sleep(ctx); serr != nil && !errors.Is(serr, context.Canceled) {
					log.Printf("pgreader sleep: %v", serr)
				}
				continue
			}

			if len(events) == 0 {
				if serr := sleeper.Sleep(ctx); serr != nil && !errors.Is(serr, context.Canceled) {
					log.Printf("pgreader sleep: %v", serr)
				}
				continue
			}

			sleeper.Reset()
			for _, ev := range events {
				select {
				case <-ctx.Done():
					return
				case ch <- ev:
					r.mu.Lock()
					r.lastReadEventGID = ev.GlobalID
					r.mu.Unlock()
				}
			}
		}
	}()

	return ch
}

func (r *PGReader) readBatch(ctx context.Context) ([]*ConsumedEvent, error) {
	r.mu.RLock()
	offset := r.committedOffset
	r.mu.RUnlock()

	query := `
        SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata
        FROM stored_events
        WHERE global_id > $1
        ORDER BY global_id ASC
        LIMIT $2
    `

	var rows *sql.Rows
	var err error
	if r.stopAtOffset != nil {
		query = `
            SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata
            FROM stored_events
            WHERE global_id > $1 AND global_id <= $3
            ORDER BY global_id ASC
            LIMIT $2
        `
		rows, err = r.db.QueryContext(ctx, query, offset, r.batchSize, *r.stopAtOffset)
	} else {
		rows, err = r.db.QueryContext(ctx, query, offset, r.batchSize)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*ConsumedEvent
	for rows.Next() {
		var ev ConsumedEvent
		var bodyBytes, metaBytes []byte
		if err := rows.Scan(&ev.GlobalID, &ev.WorkflowID, &ev.WorkflowType, &ev.EventNo, &ev.EventType, &bodyBytes, &ev.At, &metaBytes); err != nil {
			continue
		}

		var raw map[string]interface{}
		if len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, &raw); err != nil {
				raw = nil
			}
		}
		ev.Event = &genericEvent{raw: raw}

		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &ev.Metadata); err != nil {
				ev.Metadata = nil
			}
		}
		if ev.Metadata == nil {
			ev.Metadata = make(map[string]any)
		}

		events = append(events, &ev)
	}

	return events, nil
}

type genericEvent struct {
	raw      map[string]interface{}
	metadata map[string]any
}

func (e *genericEvent) GetType() string {
	if t, ok := e.raw["type"].(string); ok {
		return t
	}
	return ""
}

func (e *genericEvent) GetMetadata() map[string]any {
	if e.metadata == nil {
		e.metadata = make(map[string]any)
	}
	return e.metadata
}

func (e *genericEvent) SetMetadata(m map[string]any) {
	e.metadata = m
}

func (r *PGReader) SetStopAtOffset(offset int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopAtOffset = &offset
}

func (r *PGReader) SetCommittedOffset(offset int64) {
	r.mu.Lock()
	r.committedOffset = offset
	r.mu.Unlock()

	if _, err := r.db.ExecContext(context.Background(), `
        INSERT INTO offsets (reader, last_read_event_no) VALUES ($1, $2)
        ON CONFLICT (reader) DO UPDATE SET last_read_event_no = EXCLUDED.last_read_event_no
    `, r.readerName, offset); err != nil {
		log.Printf("pgreader offset persist (%s): %v", r.readerName, err)
	}
}

func (r *PGReader) LastReadEventGID() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastReadEventGID
}

func (r *PGReader) Close() error {
	return nil
}
