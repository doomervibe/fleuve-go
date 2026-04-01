package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"

	"github.com/doomervibe/fleuve-go/pkg/stream"
)

// TimeoutError is returned by JetStreamConsumer.Next when the fetch operation
// times out with no messages. This is a normal condition indicating the stream
// is idle, not a failure. Callers should treat this as a signal to retry.
var TimeoutError = fmt.Errorf("jetstream: fetch timed out, no messages available")

// =============================================================================
// Stream Configuration
// =============================================================================

// StreamName returns the JetStream stream name for a workflow type.
// Format: events_{workflow_type} (dots replaced with underscores).
func StreamName(workflowType string) string {
	return fmt.Sprintf("events_%s", strings.ReplaceAll(workflowType, ".", "_"))
}

// SubjectPrefix returns the NATS subject prefix for a workflow type.
// Format: events.{workflow_type}
func SubjectPrefix(workflowType string) string {
	return fmt.Sprintf("events.%s", workflowType)
}

// StreamConfig returns a NATS JetStream stream configuration for a workflow type.
//
// Properties:
//   - Stream name: events_{workflow_type}
//   - Subjects: events.{workflow_type}.*
//   - Retention: max_age=24h
//   - Storage: FILE
//   - Replicas: 1
//   - Duplicate window: 300s (for Nats-Msg-Id deduplication)
func StreamConfig(workflowType string) *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:       StreamName(workflowType),
		Subjects:   []string{SubjectPrefix(workflowType) + ".*"},
		MaxAge:     24 * time.Hour,
		Storage:    nats.FileStorage,
		Replicas:   1,
		Duplicates: 300 * time.Second,
	}
}

// EnsureStream creates the JetStream stream if it does not already exist.
// This is idempotent: returns nil if the stream already exists with any config.
func EnsureStream(js nats.JetStreamContext, workflowType string) error {
	cfg := StreamConfig(workflowType)

	// Check if stream already exists
	_, err := js.StreamInfo(cfg.Name)
	if err == nil {
		return nil
	}
	if err != nats.ErrStreamNotFound {
		return fmt.Errorf("jetstream: check stream %s: %w", cfg.Name, err)
	}

	// Create stream
	_, err = js.AddStream(cfg)
	if err != nil {
		return fmt.Errorf("jetstream: create stream %s: %w", cfg.Name, err)
	}
	return nil
}

// =============================================================================
// JetStreamPublisher — Outbox Pattern
// =============================================================================

// PublisherConfig holds configuration for JetStreamPublisher.
type PublisherConfig struct {
	BatchSize   int             // Max events per poll cycle. Default: 100
	EventsTable string          // PostgreSQL events table. Default: "stored_events"
	Sleeper     *stream.Sleeper // Exponential backoff sleeper. Default: stream.DefaultSleeper()
}

// PublisherOption is a functional option for JetStreamPublisher configuration.
type PublisherOption func(*PublisherConfig)

// WithEventsTable sets the PostgreSQL events table name.
func WithEventsTable(table string) PublisherOption {
	return func(c *PublisherConfig) { c.EventsTable = table }
}

// WithBatchSize sets the maximum number of events to publish per poll cycle.
func WithBatchSize(size int) PublisherOption {
	return func(c *PublisherConfig) { c.BatchSize = size }
}

// WithPublisherSleeper sets a custom exponential backoff sleeper.
func WithPublisherSleeper(s *stream.Sleeper) PublisherOption {
	return func(c *PublisherConfig) { c.Sleeper = s }
}

// JetStreamPublisher implements the Outbox pattern for reliable event delivery
// from PostgreSQL to NATS JetStream.
//
// Key design:
//   - Uses pg_try_advisory_lock to ensure only ONE publisher per workflow type
//   - Poll loop: SELECT events WHERE pushed=false, publish to NATS, mark pushed=true
//   - All publishes and updates happen in a single transaction
//   - NATS deduplication via Nats-Msg-Id header (workflow_id:workflow_version)
//   - Subject pattern: events.{workflow_type}.{event_type}
//   - Headers: Nats-Msg-Id, workflow_id, workflow_version, event_type, global_id, at, metadata
//   - If publish succeeds but commit fails, events will be re-published;
//     NATS duplicate window (300s) deduplicates within that window
//
// Usage:
//
//	publisher := jetstream.NewJetStreamPublisher(pool, js, "CounterWorkflow")
//	publisher.Start(ctx)
//	// ... run until shutdown ...
//	publisher.Stop()
type JetStreamPublisher struct {
	pool         *pgxpool.Pool
	js           nats.JetStreamContext
	workflowType string
	config       PublisherConfig
	wg           sync.WaitGroup
}

// NewJetStreamPublisher creates a new JetStreamPublisher for the given workflow type.
func NewJetStreamPublisher(
	pool *pgxpool.Pool,
	js nats.JetStreamContext,
	workflowType string,
	opts ...PublisherOption,
) *JetStreamPublisher {
	config := PublisherConfig{
		BatchSize:   100,
		EventsTable: "stored_events",
		Sleeper:     stream.DefaultSleeper(),
	}
	for _, opt := range opts {
		opt(&config)
	}

	return &JetStreamPublisher{
		pool:         pool,
		js:           js,
		workflowType: workflowType,
		config:       config,
	}
}

// Start begins the publisher poll loop in a background goroutine.
// The loop runs until ctx is cancelled or Stop is called.
// Ensures the JetStream stream exists before starting.
func (p *JetStreamPublisher) Start(ctx context.Context) {
	// Ensure stream exists — best-effort, don't block startup
	_ = EnsureStream(p.js, p.workflowType)

	p.wg.Add(1)
	go p.runLoop(ctx)
}

// Stop gracefully stops the publisher and waits for the poll loop to finish.
func (p *JetStreamPublisher) Stop() {
	p.wg.Wait()
}

// runLoop acquires a session-level advisory lock once per connection and keeps it while
// polling the outbox, matching Python's "hold lock for publisher lifetime" behavior
// and reducing lock thrashing under contention.
func (p *JetStreamPublisher) runLoop(ctx context.Context) {
	defer p.wg.Done()

	lockID := workflowTypeLockID(p.workflowType)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := p.pool.Acquire(ctx)
		if err != nil {
			if err := p.config.Sleeper.Sleep(ctx, false); err != nil {
				return
			}
			continue
		}

		var locked bool
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&locked); err != nil {
			conn.Release()
			if err := p.config.Sleeper.Sleep(ctx, false); err != nil {
				return
			}
			continue
		}
		if !locked {
			conn.Release()
			if err := p.config.Sleeper.Sleep(ctx, false); err != nil {
				return
			}
			continue
		}

		func() {
			defer func() {
				_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lockID)
				conn.Release()
			}()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				n, err := p.pollOnceWithLockedConn(ctx, conn)
				if err != nil {
					_ = err
				}
				if err := p.config.Sleeper.Sleep(ctx, n > 0); err != nil {
					return
				}
			}
		}()
	}
}

// pollOnceWithLockedConn runs one outbox poll cycle on a connection that already holds
// the workflow-type advisory lock (session-scoped).
func (p *JetStreamPublisher) pollOnceWithLockedConn(ctx context.Context, conn *pgxpool.Conn) (int, error) {
	// Begin transaction on the locked connection
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("jetstream: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// SELECT unpushed events for this workflow type, ordered by global_id
	selectQuery := fmt.Sprintf(
		`SELECT global_id, workflow_id, workflow_version, event_type, body, at, metadata
		 FROM %s WHERE pushed = false AND workflow_type = $1
		 ORDER BY global_id LIMIT $2`,
		p.config.EventsTable,
	)
	rows, err := tx.Query(ctx, selectQuery, p.workflowType, p.config.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("jetstream: select unpushed events: %w", err)
	}
	defer rows.Close()

	type eventRow struct {
		globalID    int64
		workflowID  string
		workflowVer int64
		eventType   string
		body        json.RawMessage
		at          time.Time
		metadata    json.RawMessage
	}

	var events []eventRow
	for rows.Next() {
		var e eventRow
		if err := rows.Scan(
			&e.globalID, &e.workflowID, &e.workflowVer,
			&e.eventType, &e.body, &e.at, &e.metadata,
		); err != nil {
			return 0, fmt.Errorf("jetstream: scan event row: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("jetstream: iterate event rows: %w", err)
	}

	if len(events) == 0 {
		return 0, nil // Nothing to publish
	}

	// Publish each event to NATS JetStream
	globalIDs := make([]int64, 0, len(events))
	for _, e := range events {
		subject := fmt.Sprintf("events.%s.%s", p.workflowType, e.eventType)
		msgID := fmt.Sprintf("%s:%d", e.workflowID, e.workflowVer)

		hdr := nats.Header{}
		hdr.Set("Nats-Msg-Id", msgID)
		hdr.Set("workflow_id", e.workflowID)
		hdr.Set("workflow_version", strconv.FormatInt(e.workflowVer, 10))
		hdr.Set("event_type", e.eventType)
		hdr.Set("global_id", strconv.FormatInt(e.globalID, 10))
		hdr.Set("at", e.at.UTC().Format(time.RFC3339Nano))
		if len(e.metadata) > 0 {
			hdr.Set("metadata", string(e.metadata))
		}

		if _, err := p.js.PublishMsg(&nats.Msg{
			Subject: subject,
			Data:    e.body,
			Header:  hdr,
		}); err != nil {
			return 0, fmt.Errorf("jetstream: publish %s/%s/v%d: %w",
				e.workflowID, e.eventType, e.workflowVer, err)
		}
		globalIDs = append(globalIDs, e.globalID)
	}

	// Mark all published events as pushed in the same transaction
	updateQuery := fmt.Sprintf(
		`UPDATE %s SET pushed = true WHERE global_id = ANY($1)`,
		p.config.EventsTable,
	)
	if _, err := tx.Exec(ctx, updateQuery, globalIDs); err != nil {
		return 0, fmt.Errorf("jetstream: mark pushed: %w", err)
	}

	// Commit — if this fails, events remain unpushed and will be retried.
	// NATS dedup (300s window) prevents duplicates on retry.
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("jetstream: commit: %w", err)
	}

	return len(events), nil
}

// =============================================================================
// JetStreamConsumer — Pull-Based Consumer
// =============================================================================

// ConsumerConfig holds configuration for JetStreamConsumer.
type ConsumerConfig struct {
	BatchSize    int           // Messages per fetch. Default: 10
	FetchTimeout time.Duration // Max wait per fetch. Default: 5s
}

// ConsumerOption is a functional option for JetStreamConsumer configuration.
type ConsumerOption func(*ConsumerConfig)

// WithConsumerBatchSize sets the number of messages to fetch per batch.
func WithConsumerBatchSize(size int) ConsumerOption {
	return func(c *ConsumerConfig) { c.BatchSize = size }
}

// WithFetchTimeout sets the maximum duration to wait for messages per fetch.
func WithFetchTimeout(d time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) { c.FetchTimeout = d }
}

// JetStreamConsumer is a pull-based NATS JetStream consumer that produces
// stream.ConsumedEvent values with lazy validation.
//
// Key design:
//   - Creates durable pull consumer with DeliverPolicy=ALL, AckPolicy=EXPLICIT, max_deliver=3
//   - Pulls messages in batches for efficiency, returns one at a time via Next()
//   - Parses NATS headers into ConsumedEvent fields
//   - Event body is NOT deserialized until Event() is first accessed (lazy validation)
//   - Returns (event, ack_callback) tuples — caller MUST call ack_callback
//   - TimeoutError is returned when no messages are available (normal condition)
//   - Close() naks any unprocessed buffered messages
//
// Not thread-safe: do not call Next() from multiple goroutines concurrently.
//
// Usage:
//
//	consumer, err := jetstream.NewJetStreamConsumer(js, "CounterWorkflow", "my-consumer", parser)
//	for {
//	    event, ack, err := consumer.Next(ctx)
//	    if errors.Is(err, jetstream.TimeoutError) {
//	        continue // normal idle, retry
//	    }
//	    if err != nil {
//	        break // real error
//	    }
//	    // process event (body parsed lazily on first event.Event() call)...
//	    if err := ack(); err != nil {
//	        break
//	    }
//	}
//	consumer.Close()
type JetStreamConsumer struct {
	js           nats.JetStreamContext
	workflowType string
	consumerName string
	parser       stream.EventParser
	config       ConsumerConfig

	sub        *nats.Subscription
	pending    []*nats.Msg
	pendingIdx int
}

// NewJetStreamConsumer creates a new pull-based JetStream consumer.
// It ensures the stream exists and creates (or binds to) a durable pull consumer.
func NewJetStreamConsumer(
	js nats.JetStreamContext,
	workflowType string,
	consumerName string,
	parser stream.EventParser,
	opts ...ConsumerOption,
) (*JetStreamConsumer, error) {
	config := ConsumerConfig{
		BatchSize:    10,
		FetchTimeout: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(&config)
	}

	c := &JetStreamConsumer{
		js:           js,
		workflowType: workflowType,
		consumerName: consumerName,
		parser:       parser,
		config:       config,
	}

	if err := c.init(); err != nil {
		return nil, err
	}

	return c, nil
}

// init sets up the JetStream stream and durable pull consumer.
func (c *JetStreamConsumer) init() error {
	// Ensure stream exists
	if err := EnsureStream(c.js, c.workflowType); err != nil {
		return err
	}

	streamName := StreamName(c.workflowType)

	// Create durable pull consumer (idempotent — ignore "already exists" errors)
	_, err := c.js.AddConsumer(streamName, &nats.ConsumerConfig{
		Name:          c.consumerName,
		Durable:       c.consumerName,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		MaxDeliver:    3,
		AckWait:       30 * time.Second,
	})
	if err != nil && !isConsumerExistsError(err) {
		return fmt.Errorf("jetstream: create consumer %s: %w", c.consumerName, err)
	}

	// Bind to the pull consumer (durable name is the second positional argument).
	sub, err := c.js.PullSubscribe(
		SubjectPrefix(c.workflowType)+".*",
		c.consumerName,
		nats.ManualAck(),
	)
	if err != nil {
		return fmt.Errorf("jetstream: pull subscribe: %w", err)
	}
	c.sub = sub

	return nil
}

// Next fetches the next event from the JetStream pull consumer.
//
// Returns:
//   - event: parsed ConsumedEvent with lazy validation (body deserialized on first Event() call)
//   - ack: callback to positively acknowledge the message — caller MUST call this after processing
//   - err: TimeoutError if no messages available (normal idle), or a real error
//
// The ack callback is idempotent — safe to call multiple times.
// If the caller does not call ack, the message will be redelivered after AckWait (30s).
func (c *JetStreamConsumer) Next(ctx context.Context) (*stream.ConsumedEvent, func() error, error) {
	// Return buffered message if available from previous fetch
	if c.pendingIdx < len(c.pending) {
		msg := c.pending[c.pendingIdx]
		c.pendingIdx++
		return c.parseMessage(msg)
	}

	// Fetch a new batch from NATS
	msgs, err := c.sub.Fetch(c.config.BatchSize, nats.MaxWait(c.config.FetchTimeout))
	if err != nil {
		if err == nats.ErrTimeout {
			return nil, nil, TimeoutError
		}
		return nil, nil, fmt.Errorf("jetstream: fetch: %w", err)
	}
	if len(msgs) == 0 {
		return nil, nil, TimeoutError
	}

	// Buffer all fetched messages for subsequent Next() calls
	c.pending = msgs
	c.pendingIdx = 0

	msg := c.pending[c.pendingIdx]
	c.pendingIdx++
	return c.parseMessage(msg)
}

// Close cleans up the consumer, negatively acknowledging any unprocessed
// buffered messages so they can be redelivered. Safe to call multiple times.
func (c *JetStreamConsumer) Close() {
	for i := c.pendingIdx; i < len(c.pending); i++ {
		_ = c.pending[i].Nak()
	}
	c.pending = nil
	c.pendingIdx = 0
}

// parseMessage converts a NATS message into a stream.ConsumedEvent with lazy validation.
// Extracts event metadata from NATS headers and passes raw body for deferred parsing.
func (c *JetStreamConsumer) parseMessage(msg *nats.Msg) (*stream.ConsumedEvent, func() error, error) {
	hdr := msg.Header

	workflowID := hdr.Get("workflow_id")
	if workflowID == "" {
		return nil, nil, fmt.Errorf("jetstream: missing workflow_id header")
	}

	workflowVerStr := hdr.Get("workflow_version")
	if workflowVerStr == "" {
		return nil, nil, fmt.Errorf("jetstream: missing workflow_version header")
	}
	workflowVer, err := strconv.ParseInt(workflowVerStr, 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("jetstream: parse workflow_version %q: %w", workflowVerStr, err)
	}

	eventType := hdr.Get("event_type")
	if eventType == "" {
		return nil, nil, fmt.Errorf("jetstream: missing event_type header")
	}

	globalIDStr := hdr.Get("global_id")
	if globalIDStr == "" {
		return nil, nil, fmt.Errorf("jetstream: missing global_id header")
	}
	globalID, err := strconv.ParseInt(globalIDStr, 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("jetstream: parse global_id %q: %w", globalIDStr, err)
	}

	atStr := hdr.Get("at")
	if atStr == "" {
		return nil, nil, fmt.Errorf("jetstream: missing at header")
	}
	at, err := time.Parse(time.RFC3339Nano, atStr)
	if err != nil {
		return nil, nil, fmt.Errorf("jetstream: parse at %q: %w", atStr, err)
	}

	// Parse metadata from header (optional — defaults to empty map)
	var metadata map[string]any
	if metadataStr := hdr.Get("metadata"); metadataStr != "" {
		if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
			return nil, nil, fmt.Errorf("jetstream: parse metadata: %w", err)
		}
	}
	if metadata == nil {
		metadata = make(map[string]any)
	}

	event := stream.NewConsumedEvent(
		globalID,
		workflowID,
		workflowVer,
		c.workflowType,
		eventType,
		at,
		metadata,
		c.consumerName,
		json.RawMessage(msg.Data),
		c.parser,
	)

	// Idempotent ack callback — safe to call multiple times
	var ackOnce sync.Once
	ack := func() error {
		var ackErr error
		ackOnce.Do(func() {
			ackErr = msg.Ack()
		})
		return ackErr
	}

	return event, ack, nil
}

// =============================================================================
// Internal Helpers
// =============================================================================

// workflowTypeLockID computes a stable int64 advisory lock ID from a workflow type string.
// Uses FNV-1a hash for deterministic, collision-resistant mapping.
func workflowTypeLockID(workflowType string) int64 {
	h := fnv.New64a()
	h.Write([]byte("fleuve_outbox_" + workflowType))
	return int64(h.Sum64() % (1 << 31))
}

// WorkflowTypeAdvisoryLockID returns the PostgreSQL advisory lock key used by JetStreamPublisher
// for the given workflow type. Exposed for integration tests.
func WorkflowTypeAdvisoryLockID(workflowType string) int64 {
	return workflowTypeLockID(workflowType)
}

// isConsumerExistsError checks if a NATS error indicates the consumer already exists.
// Handles variations in error messages across NATS server versions.
func isConsumerExistsError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "consumer already exists") ||
		strings.Contains(msg, "consumer name already in use")
}
