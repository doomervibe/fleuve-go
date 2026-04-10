package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// =============================================================================
// Sleeper - Exponential Backoff
// =============================================================================

// Sleeper implements exponential backoff for event polling.
// Doubles sleep time on empty polls, resets to min on events found.
// Default: min=100ms, max=20s.
type Sleeper struct {
	minSleep  time.Duration
	maxSleep  time.Duration
	nextSleep time.Duration
	mu        sync.Mutex
}

// NewSleeper creates a new Sleeper with the given min and max durations.
func NewSleeper(minSleep, maxSleep time.Duration) *Sleeper {
	return &Sleeper{
		minSleep:  minSleep,
		maxSleep:  maxSleep,
		nextSleep: minSleep,
	}
}

// DefaultSleeper creates a Sleeper with default values (100ms min, 20s max).
func DefaultSleeper() *Sleeper {
	return NewSleeper(100*time.Millisecond, 20*time.Second)
}

// MarkGotEvents updates the internal sleep time based on whether events were found.
func (s *Sleeper) MarkGotEvents(gotEvents bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if gotEvents {
		s.nextSleep = s.minSleep
	} else {
		s.nextSleep = min(s.maxSleep, s.nextSleep*2)
	}
}

// Sleep waits for the appropriate duration based on whether events were found.
func (s *Sleeper) Sleep(ctx context.Context, gotEvents bool) error {
	s.MarkGotEvents(gotEvents)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.nextSleep):
		return nil
	}
}

// =============================================================================
// EventParser - Event Deserialization
// =============================================================================

// EventParser deserializes raw JSON into an Event. workflowType is the
// emitter's aggregate type from stored_events (e.g. "domain", "project");
// eventType is the per-event type string (e.g. "updated", "project_patch").
type EventParser func(workflowType string, eventType string, raw json.RawMessage) (Event, error)

// Event is the interface for events consumed from the stream.
type Event interface {
	Type() string
}

// =============================================================================
// ConsumedEvent - Lazy Validation
// =============================================================================

// ConsumedEvent represents an event consumed from the PostgreSQL stream.
// Key optimization: The event body is NOT deserialized until Event() is first accessed.
// The reader fetches raw JSON from PostgreSQL and stores it as rawBody.
// When Event() is accessed, the parser deserializes it using sync.Once for thread safety.
type ConsumedEvent struct {
	GlobalID     int64
	WorkflowID   string
	EventNo      int64 // workflow_version
	WorkflowType string
	EventType    string
	At           time.Time
	Metadata     map[string]any
	ReaderName   string

	rawBody     json.RawMessage
	parser      EventParser
	once        sync.Once
	parsedEvent Event
	parseErr    error
}

// NewConsumedEvent creates a new ConsumedEvent with lazy parsing.
func NewConsumedEvent(
	globalID int64,
	workflowID string,
	eventNo int64,
	workflowType string,
	eventType string,
	at time.Time,
	metadata map[string]any,
	readerName string,
	rawBody json.RawMessage,
	parser EventParser,
) *ConsumedEvent {
	return &ConsumedEvent{
		GlobalID:     globalID,
		WorkflowID:   workflowID,
		EventNo:      eventNo,
		WorkflowType: workflowType,
		EventType:    eventType,
		At:           at,
		Metadata:     metadata,
		ReaderName:   readerName,
		rawBody:      rawBody,
		parser:       parser,
	}
}

// Event returns the deserialized event, parsing from raw JSON on first access.
// Thread-safe via sync.Once.
func (e *ConsumedEvent) Event() (Event, error) {
	e.once.Do(func() {
		if e.parser != nil && len(e.rawBody) > 0 {
			e.parsedEvent, e.parseErr = e.parser(e.WorkflowType, e.EventType, e.rawBody)
		}
	})
	return e.parsedEvent, e.parseErr
}

// RawBody returns the raw JSON body without parsing.
func (e *ConsumedEvent) RawBody() json.RawMessage {
	return e.rawBody
}

// GetEventTags extracts tags from metadata.
func (e *ConsumedEvent) GetEventTags() []string {
	if e.Metadata == nil {
		return nil
	}
	tags, ok := e.Metadata["tags"]
	if !ok {
		return nil
	}
	return toStringSlice(tags)
}

// GetWorkflowTags extracts workflow_tags from metadata.
func (e *ConsumedEvent) GetWorkflowTags() []string {
	if e.Metadata == nil {
		return nil
	}
	tags, ok := e.Metadata["workflow_tags"]
	if !ok {
		return nil
	}
	return toStringSlice(tags)
}

// toStringSlice converts an any to a string slice.
func toStringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		result := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case nil:
		return nil
	default:
		return nil
	}
}

// =============================================================================
// Reader - PostgreSQL Polling
// =============================================================================

// ReaderConfig holds configuration for the Reader.
type ReaderConfig struct {
	ReaderName       string
	EventTypes       []string      // If empty, reads all event types
	FetchMetadata    bool          // Whether to SELECT metadata column
	BatchSize        int           // Default: 100
	MarkHorizonEvery time.Duration // Default: 10 seconds
	Sleeper          *Sleeper      // Default: DefaultSleeper()
	EventsTable      string        // Default: "stored_events"
	OffsetsTable     string        // Default: "offsets"
}

// ReaderOption is a functional option for Reader configuration.
type ReaderOption func(*ReaderConfig)

// WithEventTypes sets the event types to filter.
func WithEventTypes(types []string) ReaderOption {
	return func(c *ReaderConfig) { c.EventTypes = types }
}

// WithFetchMetadata enables/disables metadata fetching.
func WithFetchMetadata(fetch bool) ReaderOption {
	return func(c *ReaderConfig) { c.FetchMetadata = fetch }
}

// WithBatchSize sets the batch size for event fetching.
func WithBatchSize(size int) ReaderOption {
	return func(c *ReaderConfig) { c.BatchSize = size }
}

// WithMarkHorizonEvery sets the checkpoint interval.
func WithMarkHorizonEvery(d time.Duration) ReaderOption {
	return func(c *ReaderConfig) { c.MarkHorizonEvery = d }
}

// WithSleeper sets a custom sleeper.
func WithSleeper(s *Sleeper) ReaderOption {
	return func(c *ReaderConfig) { c.Sleeper = s }
}

// WithEventsTable sets the events table name.
func WithEventsTable(table string) ReaderOption {
	return func(c *ReaderConfig) { c.EventsTable = table }
}

// WithOffsetsTable sets the offsets table name.
func WithOffsetsTable(table string) ReaderOption {
	return func(c *ReaderConfig) { c.OffsetsTable = table }
}

// Reader implements PostgreSQL-based event stream polling.
// It polls for new events using global_id as the offset.
type Reader struct {
	pool   *pgxpool.Pool
	config ReaderConfig
	parser EventParser

	lastReadEventID int64
	markedInDB      int64
	committedOffset int64
	stopAtOffset    *int64

	mu         sync.RWMutex
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	markHorizonChan chan struct{}
}

// NewReader creates a new PostgreSQL-based event reader.
func NewReader(
	pool *pgxpool.Pool,
	readerName string,
	parser EventParser,
	opts ...ReaderOption,
) *Reader {
	config := ReaderConfig{
		ReaderName:       readerName,
		FetchMetadata:    true,
		BatchSize:        100,
		MarkHorizonEvery: 10 * time.Second,
		Sleeper:          DefaultSleeper(),
		EventsTable:      "stored_events",
		OffsetsTable:     "offsets",
	}
	for _, opt := range opts {
		opt(&config)
	}

	return &Reader{
		pool:            pool,
		config:          config,
		parser:          parser,
		markHorizonChan: make(chan struct{}, 1),
	}
}

// IterEvents returns a channel that yields events as they are read from the stream.
// The channel is closed when the context is cancelled or Stop is called.
func (r *Reader) IterEvents(ctx context.Context) <-chan *ConsumedEvent {
	eventCh := make(chan *ConsumedEvent, r.config.BatchSize)

	childCtx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.cancelFunc = cancel
	r.mu.Unlock()

	// Initialize offset from DB
	if err := r.initOffset(childCtx); err != nil {
		// Log error but continue - will start from 0
		_ = err
	}

	r.wg.Add(1)
	go r.pollLoop(childCtx, eventCh)

	return eventCh
}

// pollLoop is the main polling loop.
func (r *Reader) pollLoop(ctx context.Context, eventCh chan<- *ConsumedEvent) {
	defer r.wg.Done()
	defer close(eventCh)

	// Start background checkpoint task
	horizonCtx, horizonCancel := context.WithCancel(ctx)
	defer horizonCancel()
	r.wg.Add(1)
	go r.backgroundCheckpoint(horizonCtx)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		gotEvents := false
		events := r.fetchNewEvents(ctx)

		for _, event := range events {
			select {
			case <-ctx.Done():
				return
			case eventCh <- event:
				r.mu.Lock()
				r.lastReadEventID = event.GlobalID
				r.mu.Unlock()
				gotEvents = true

				// Check stop at offset
				if r.stopAtOffset != nil && event.GlobalID >= *r.stopAtOffset {
					return
				}
			}
		}

		if err := r.config.Sleeper.Sleep(ctx, gotEvents); err != nil {
			return
		}
	}
}

// fetchNewEvents fetches new events from PostgreSQL.
func (r *Reader) fetchNewEvents(ctx context.Context) []*ConsumedEvent {
	r.mu.RLock()
	lastID := r.lastReadEventID
	r.mu.RUnlock()

	// Lightweight existence check (avoids JSONB cast when idle)
	exists, err := r.checkEventsExist(ctx, lastID)
	if err != nil || !exists {
		return nil
	}

	// Full query with body
	return r.fetchEventsFull(ctx, lastID)
}

// checkEventsExist does a lightweight check to see if any events exist.
// Uses index-only scan on global_id to avoid JSONB cast overhead.
func (r *Reader) checkEventsExist(ctx context.Context, afterID int64) (bool, error) {
	query := r.buildExistenceQuery()
	args := []any{afterID}

	if len(r.config.EventTypes) > 0 {
		args = append(args, r.config.EventTypes)
	}

	var exists bool
	err := r.pool.QueryRow(ctx, query, args...).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// buildExistenceQuery builds the lightweight existence check query.
func (r *Reader) buildExistenceQuery() string {
	base := `SELECT EXISTS(SELECT 1 FROM ` + r.config.EventsTable + ` WHERE global_id > $1`
	if len(r.config.EventTypes) > 0 {
		base += ` AND event_type = ANY($2)`
	}
	base += ` LIMIT 1)`
	return base
}

// fetchEventsFull does the full query with event body.
func (r *Reader) fetchEventsFull(ctx context.Context, afterID int64) []*ConsumedEvent {
	query := r.buildFullQuery()
	args := []any{afterID}
	argIdx := 2

	if len(r.config.EventTypes) > 0 {
		query += ` AND event_type = ANY($` + itoa(argIdx) + `)`
		args = append(args, r.config.EventTypes)
		argIdx++
	}

	query += ` ORDER BY global_id LIMIT $` + itoa(argIdx)
	args = append(args, r.config.BatchSize)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var events []*ConsumedEvent
	for rows.Next() {
		event, err := r.scanEvent(rows)
		if err != nil {
			continue // Skip malformed events
		}
		events = append(events, event)
	}

	return events
}

// buildFullQuery builds the full event fetch query.
func (r *Reader) buildFullQuery() string {
	metadataCol := "metadata"
	if !r.config.FetchMetadata {
		metadataCol = "NULL::jsonb as metadata"
	}

	return `SELECT global_id, workflow_id, workflow_version, workflow_type, event_type,
	        body, at, ` + metadataCol + `
	        FROM ` + r.config.EventsTable + ` WHERE global_id > $1`
}

// scanEvent scans a single event from a row.
func (r *Reader) scanEvent(rows pgx.Rows) (*ConsumedEvent, error) {
	var globalID int64
	var workflowID, workflowType, eventType string
	var eventNo int64
	var body json.RawMessage
	var at time.Time
	var metadata json.RawMessage

	err := rows.Scan(&globalID, &workflowID, &eventNo, &workflowType, &eventType, &body, &at, &metadata)
	if err != nil {
		return nil, err
	}

	var metaMap map[string]any
	if len(metadata) > 0 {
		_ = json.Unmarshal(metadata, &metaMap)
	}
	if metaMap == nil {
		metaMap = make(map[string]any)
	}

	return NewConsumedEvent(
		globalID,
		workflowID,
		eventNo,
		workflowType,
		eventType,
		at,
		metaMap,
		r.config.ReaderName,
		body,
		r.parser,
	), nil
}

// initOffset initializes the reader offset from the database.
func (r *Reader) initOffset(ctx context.Context) error {
	var lastRead int64
	err := r.pool.QueryRow(ctx,
		`SELECT last_read_event_no FROM `+r.config.OffsetsTable+` WHERE reader = $1`,
		r.config.ReaderName,
	).Scan(&lastRead)

	if err == nil {
		r.mu.Lock()
		r.lastReadEventID = lastRead
		r.markedInDB = lastRead
		r.committedOffset = lastRead
		r.mu.Unlock()
	}
	return err
}

// SetCommittedOffset sets the committed offset (called by InflightTracker).
func (r *Reader) SetCommittedOffset(offset int64) {
	r.mu.Lock()
	r.committedOffset = offset
	r.mu.Unlock()
}

// SetStopAtOffset sets the offset at which the reader should stop.
func (r *Reader) SetStopAtOffset(offset int64) {
	r.mu.Lock()
	r.stopAtOffset = &offset
	r.mu.Unlock()
}

// CommittedOffset returns the current committed offset.
func (r *Reader) CommittedOffset() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.committedOffset
}

// Stop stops the reader and waits for all goroutines to finish.
func (r *Reader) Stop() {
	r.mu.Lock()
	if r.cancelFunc != nil {
		r.cancelFunc()
		r.cancelFunc = nil
	}
	r.mu.Unlock()
	r.wg.Wait()
}

// backgroundCheckpoint periodically checkpoints the offset to the database.
func (r *Reader) backgroundCheckpoint(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.config.MarkHorizonEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final checkpoint on shutdown
			r.markHorizon()
			return
		case <-ticker.C:
			r.markHorizon()
		case <-r.markHorizonChan:
			r.markHorizon()
		}
	}
}

// markHorizon checkpoints the current offset to the database.
func (r *Reader) markHorizon() {
	r.mu.Lock()
	lastNum := r.committedOffset
	if lastNum == 0 {
		lastNum = r.lastReadEventID
	}
	if lastNum == r.markedInDB {
		r.mu.Unlock()
		return // No change, skip write
	}
	r.mu.Unlock()

	_, err := r.pool.Exec(context.Background(),
		`INSERT INTO `+r.config.OffsetsTable+` (reader, last_read_event_no)
		 VALUES ($1, $2)
		 ON CONFLICT (reader) DO UPDATE SET last_read_event_no = $2`,
		r.config.ReaderName,
		lastNum,
	)
	if err == nil {
		r.mu.Lock()
		r.markedInDB = lastNum
		r.mu.Unlock()
	}
}

// itoa converts an int to a string (for building parameterized queries).
func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
