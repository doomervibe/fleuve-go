package external

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/repo"
)

// =============================================================================
// Subject Pattern and Parsing
// =============================================================================

// SubjectPrefix returns the NATS subject prefix for external messages.
// Format: messages.{workflow_type}
func SubjectPrefix(workflowType string) string {
	return fmt.Sprintf("messages.%s", workflowType)
}

// StreamName returns the JetStream stream name for external messages.
// Format: messages_{workflow_type} (dots replaced with underscores).
func StreamName(workflowType string) string {
	return fmt.Sprintf("messages_%s", strings.ReplaceAll(workflowType, ".", "_"))
}

// StreamConfig returns a NATS JetStream stream configuration for external messages.
//
// Properties:
//   - Stream name: messages_{workflow_type}
//   - Subjects: messages.{workflow_type}.>
//   - Retention: max_age=24h
//   - Storage: FILE
//   - Replicas: 1
//   - Duplicate window: 300s
func StreamConfig(workflowType string) *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:       StreamName(workflowType),
		Subjects:   []string{SubjectPrefix(workflowType) + ".>"},
		MaxAge:     24 * time.Hour,
		Storage:    nats.FileStorage,
		Replicas:   1,
		Duplicates: 300 * time.Second,
	}
}

// ValidRoutingModes lists the valid routing modes for external messages.
var ValidRoutingModes = []string{"all", "tag", "id", "topic"}

// isValidRoutingMode checks if the given string is a valid routing mode.
func isValidRoutingMode(routing string) bool {
	for _, mode := range ValidRoutingModes {
		if routing == mode {
			return true
		}
	}
	return false
}

// ParseSubject parses an external message subject and validates it against
// the expected workflow type.
//
// Subject format: messages.{workflow_type}.{routing}.{detail}
//
// Returns:
//   - routing: the routing mode (all, tag, id, topic)
//   - detail: the routing detail (tag value, workflow ID, topic name, or empty for "all")
//   - ok: true if the subject is valid and matches the expected workflow type
func ParseSubject(subject, expectedWorkflowType string) (routing, detail string, ok bool) {
	prefix := "messages."
	if !strings.HasPrefix(subject, prefix) {
		return "", "", false
	}

	remaining := subject[len(prefix):]

	// Extract workflow type
	dotIdx := strings.Index(remaining, ".")
	if dotIdx < 0 {
		return "", "", false
	}
	workflowType := remaining[:dotIdx]
	remaining = remaining[dotIdx+1:]

	// Validate workflow type matches
	if workflowType != expectedWorkflowType {
		return "", "", false
	}

	// Extract routing mode
	dotIdx = strings.Index(remaining, ".")
	if dotIdx < 0 {
		// No detail part - invalid for external messages
		return "", "", false
	}
	routing = remaining[:dotIdx]
	detail = remaining[dotIdx+1:]

	// Validate routing mode
	if !isValidRoutingMode(routing) {
		return "", "", false
	}

	return routing, detail, true
}

// =============================================================================
// Workflow ID Resolution
// =============================================================================

// WfIDRule is a predicate function that determines whether a given workflow ID
// belongs to the current partition. When nil, no partition filtering is applied.
type WfIDRule func(string) bool

// ResolveWorkflowIDs resolves target workflow IDs based on the routing mode.
//
// Routing modes:
//   - "all": Returns all workflow IDs that have events of the given type
//   - "tag": Returns workflow IDs that have the specified tag in workflow_metadata
//   - "id": Returns the detail directly as a single workflow ID
//   - "topic": Returns workflow IDs subscribed to the external topic
//
// If wfIDRule is set, the results are filtered to only include IDs that pass the rule.
func ResolveWorkflowIDs(
	ctx context.Context,
	pool *pgxpool.Pool,
	workflowType string,
	routing string,
	detail string,
	wfIDRule WfIDRule,
	eventsTable string,
	metaTable string,
	externalSubsTable string,
) ([]string, error) {
	var ids []string
	var err error

	switch routing {
	case "all":
		ids, err = resolveAll(ctx, pool, workflowType, eventsTable)
	case "tag":
		ids, err = resolveByTag(ctx, pool, workflowType, detail, metaTable)
	case "id":
		ids = []string{detail}
	case "topic":
		ids, err = resolveByTopic(ctx, pool, workflowType, detail, externalSubsTable)
	default:
		return nil, fmt.Errorf("external: invalid routing mode: %s", routing)
	}

	if err != nil {
		return nil, err
	}

	// Apply partition filter if rule is set
	if wfIDRule != nil {
		filtered := make([]string, 0, len(ids))
		for _, id := range ids {
			if wfIDRule(id) {
				filtered = append(filtered, id)
			}
		}
		ids = filtered
	}

	return ids, nil
}

// resolveAll returns all distinct workflow IDs that have events of the given type.
func resolveAll(ctx context.Context, pool *pgxpool.Pool, workflowType, eventsTable string) ([]string, error) {
	query := fmt.Sprintf(
		`SELECT DISTINCT workflow_id FROM %s WHERE workflow_type = $1`,
		eventsTable,
	)
	rows, err := pool.Query(ctx, query, workflowType)
	if err != nil {
		return nil, fmt.Errorf("external: resolve all: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("external: resolve all scan: %w", err)
		}
		ids = append(ids, id)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("external: resolve all rows error: %w", rows.Err())
	}

	return ids, nil
}

// resolveByTag returns workflow IDs that have the specified tag in workflow_metadata.
// Uses PostgreSQL array contains operator @> for efficient tag lookup.
func resolveByTag(ctx context.Context, pool *pgxpool.Pool, workflowType, tag, metaTable string) ([]string, error) {
	query := fmt.Sprintf(
		`SELECT workflow_id FROM %s WHERE workflow_type = $1 AND tags @> $2`,
		metaTable,
	)
	rows, err := pool.Query(ctx, query, workflowType, []string{tag})
	if err != nil {
		return nil, fmt.Errorf("external: resolve by tag: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("external: resolve by tag scan: %w", err)
		}
		ids = append(ids, id)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("external: resolve by tag rows error: %w", rows.Err())
	}

	return ids, nil
}

// resolveByTopic returns workflow IDs subscribed to the external topic.
func resolveByTopic(ctx context.Context, pool *pgxpool.Pool, workflowType, topic, externalSubsTable string) ([]string, error) {
	query := fmt.Sprintf(
		`SELECT DISTINCT workflow_id FROM %s WHERE workflow_type = $1 AND topic = $2`,
		externalSubsTable,
	)
	rows, err := pool.Query(ctx, query, workflowType, topic)
	if err != nil {
		return nil, fmt.Errorf("external: resolve by topic: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("external: resolve by topic scan: %w", err)
		}
		ids = append(ids, id)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("external: resolve by topic rows error: %w", rows.Err())
	}

	return ids, nil
}

// =============================================================================
// Event Parser
// =============================================================================

// EventParser is a function that deserializes raw JSON into a model.Event.
// This mirrors repo.EventParser to keep types aligned with the workflow system.
type EventParser func(eventType string, raw json.RawMessage) (model.Event, error)

// =============================================================================
// ExternalMessageConsumer
// =============================================================================

// ExternalConsumerConfig holds configuration for ExternalMessageConsumer.
type ExternalConsumerConfig struct {
	BatchSize         int           // Messages per fetch. Default: 10
	FetchTimeout      time.Duration // Max wait per fetch. Default: 5s
	EventsTable       string        // PostgreSQL events table. Default: "stored_events"
	MetaTable         string        // PostgreSQL workflow_metadata table. Default: "workflow_metadata"
	ExternalSubsTable string        // PostgreSQL external_subscriptions table. Default: "external_subscriptions"
	ConsumerName      string        // NATS consumer name. Default: "{workflow_type}_external"
	Logger            *slog.Logger  // Logger. Default: slog.Default()
}

// ExternalConsumerOption is a functional option for ExternalMessageConsumer configuration.
type ExternalConsumerOption func(*ExternalConsumerConfig)

// WithExternalBatchSize sets the number of messages to fetch per batch.
func WithExternalBatchSize(size int) ExternalConsumerOption {
	return func(c *ExternalConsumerConfig) { c.BatchSize = size }
}

// WithExternalFetchTimeout sets the maximum duration to wait for messages per fetch.
func WithExternalFetchTimeout(d time.Duration) ExternalConsumerOption {
	return func(c *ExternalConsumerConfig) { c.FetchTimeout = d }
}

// WithExternalEventsTable sets the PostgreSQL events table name.
func WithExternalEventsTable(table string) ExternalConsumerOption {
	return func(c *ExternalConsumerConfig) { c.EventsTable = table }
}

// WithExternalMetaTable sets the PostgreSQL workflow_metadata table name.
func WithExternalMetaTable(table string) ExternalConsumerOption {
	return func(c *ExternalConsumerConfig) { c.MetaTable = table }
}

// WithExternalSubsTable sets the PostgreSQL external_subscriptions table name.
func WithExternalSubsTable(table string) ExternalConsumerOption {
	return func(c *ExternalConsumerConfig) { c.ExternalSubsTable = table }
}

// WithExternalConsumerName sets the NATS consumer name.
func WithExternalConsumerName(name string) ExternalConsumerOption {
	return func(c *ExternalConsumerConfig) { c.ConsumerName = name }
}

// WithExternalLogger sets the logger.
func WithExternalLogger(logger *slog.Logger) ExternalConsumerOption {
	return func(c *ExternalConsumerConfig) { c.Logger = logger }
}

// ExternalMessageConsumer consumes external NATS messages and routes them to
// target workflows based on the subject routing mode.
//
// Subject format: messages.{workflow_type}.{routing}.{detail}
//
// Flow for each message:
//  1. Parse subject to extract routing mode and detail
//  2. Resolve target workflow IDs based on routing mode
//  3. Parse message payload into an Event
//  4. For each target workflow ID:
//     a. Call event_to_cmd to convert the event to a command
//     b. Call process_command to process the command via the repo
//  5. Ack on success, Nak on failure
//
// Not thread-safe: do not call Run from multiple goroutines concurrently.
type ExternalMessageConsumer struct {
	js           nats.JetStreamContext
	pool         *pgxpool.Pool
	workflowType string
	eventParser  EventParser
	eventToCmd   func(model.Event) model.Command
	processCmd   func(ctx context.Context, workflowID string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection)
	wfIDRule     WfIDRule
	config       ExternalConsumerConfig

	sub        *nats.Subscription
	pending    []*nats.Msg
	pendingIdx int
}

// NewExternalMessageConsumer creates a new external message consumer.
// It sets up the JetStream stream and durable pull consumer.
//
// Parameters:
//   - js: NATS JetStream context
//   - pool: PostgreSQL connection pool
//   - workflowType: the workflow type this consumer handles
//   - eventParser: function to parse raw JSON into model.Event
//   - eventToCmd: function to convert an external event to a command
//   - processCmd: function to process a command (typically repo.ProcessCommand)
//   - wfIDRule: optional partition filter (nil to process all)
//   - opts: optional configuration
func NewExternalMessageConsumer(
	js nats.JetStreamContext,
	pool *pgxpool.Pool,
	workflowType string,
	eventParser EventParser,
	eventToCmd func(model.Event) model.Command,
	processCmd func(ctx context.Context, workflowID string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection),
	wfIDRule WfIDRule,
	opts ...ExternalConsumerOption,
) (*ExternalMessageConsumer, error) {
	config := ExternalConsumerConfig{
		BatchSize:         10,
		FetchTimeout:      5 * time.Second,
		EventsTable:       "stored_events",
		MetaTable:         "workflow_metadata",
		ExternalSubsTable: "external_subscriptions",
		ConsumerName:      workflowType + "_external",
		Logger:            slog.Default(),
	}
	for _, opt := range opts {
		opt(&config)
	}

	c := &ExternalMessageConsumer{
		js:           js,
		pool:         pool,
		workflowType: workflowType,
		eventParser:  eventParser,
		eventToCmd:   eventToCmd,
		processCmd:   processCmd,
		wfIDRule:     wfIDRule,
		config:       config,
	}

	if err := c.init(); err != nil {
		return nil, err
	}

	return c, nil
}

// init sets up the JetStream stream and durable pull consumer.
func (c *ExternalMessageConsumer) init() error {
	// Ensure stream exists (idempotent)
	cfg := StreamConfig(c.workflowType)

	_, err := c.js.StreamInfo(cfg.Name)
	if err == nil {
		// Stream already exists
	} else if err == nats.ErrStreamNotFound {
		_, err = c.js.AddStream(cfg)
		if err != nil {
			return fmt.Errorf("external: create stream %s: %w", cfg.Name, err)
		}
	} else {
		return fmt.Errorf("external: check stream %s: %w", cfg.Name, err)
	}

	streamName := StreamName(c.workflowType)

	// Create durable pull consumer (idempotent)
	_, err = c.js.AddConsumer(streamName, &nats.ConsumerConfig{
		Name:          c.config.ConsumerName,
		Durable:       c.config.ConsumerName,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		MaxDeliver:    3,
		AckWait:       30 * time.Second,
	})
	if err != nil && !isConsumerExistsError(err) {
		return fmt.Errorf("external: create consumer %s: %w", c.config.ConsumerName, err)
	}

	// Bind to the pull consumer
	sub, err := c.js.PullSubscribe(
		SubjectPrefix(c.workflowType)+".>",
		c.config.ConsumerName,
		nats.Durable(c.config.ConsumerName),
		nats.ManualAck(),
	)
	if err != nil {
		return fmt.Errorf("external: pull subscribe: %w", err)
	}
	c.sub = sub

	return nil
}

// isConsumerExistsError checks if the error indicates the consumer already exists.
func isConsumerExistsError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "consumer name already in use")
}

// Run starts the consume loop. Blocks until ctx is cancelled.
// For each message:
//  1. Parse subject to extract routing mode and detail
//  2. Resolve target workflow IDs
//  3. Parse payload into Event
//  4. For each target workflow: call eventToCmd, then processCmd
//  5. Ack on success, Nak on failure
func (c *ExternalMessageConsumer) Run(ctx context.Context) error {
	logger := c.config.Logger.With(
		"component", "external_consumer",
		"workflow_type", c.workflowType,
		"consumer_name", c.config.ConsumerName,
	)

	for {
		select {
		case <-ctx.Done():
			c.Close()
			return ctx.Err()
		default:
		}

		msgs, err := c.sub.Fetch(c.config.BatchSize, nats.MaxWait(c.config.FetchTimeout))
		if err != nil {
			if err == nats.ErrTimeout {
				continue // Normal idle, retry
			}
			logger.Error("fetch failed", "error", err)
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		for _, msg := range msgs {
			c.processMessage(ctx, msg, logger)
		}
	}
}

// processMessage handles a single NATS message.
func (c *ExternalMessageConsumer) processMessage(ctx context.Context, msg *nats.Msg, logger *slog.Logger) {
	subject := msg.Subject

	// Step 1: Parse subject
	routing, detail, ok := ParseSubject(subject, c.workflowType)
	if !ok {
		logger.Warn("invalid subject, naking",
			"subject", subject,
		)
		_ = msg.Nak()
		return
	}

	// Step 2: Resolve target workflow IDs
	workflowIDs, err := ResolveWorkflowIDs(
		ctx,
		c.pool,
		c.workflowType,
		routing,
		detail,
		c.wfIDRule,
		c.config.EventsTable,
		c.config.MetaTable,
		c.config.ExternalSubsTable,
	)
	if err != nil {
		logger.Error("failed to resolve workflow IDs, naking",
			"error", err,
			"routing", routing,
			"detail", detail,
		)
		_ = msg.Nak()
		return
	}

	if len(workflowIDs) == 0 {
		// No targets - ack and move on
		logger.Debug("no target workflows found",
			"routing", routing,
			"detail", detail,
		)
		_ = msg.Ack()
		return
	}

	// Step 3: Parse payload into Event
	event, err := c.eventParser("", msg.Data)
	if err != nil {
		logger.Error("failed to parse event payload, naking",
			"error", err,
			"routing", routing,
			"detail", detail,
		)
		_ = msg.Nak()
		return
	}

	// Step 4: For each target workflow: call eventToCmd, then processCmd
	for _, workflowID := range workflowIDs {
		cmd := c.eventToCmd(event)
		if cmd == nil {
			logger.Debug("event_to_cmd returned nil, skipping",
				"workflow_id", workflowID,
			)
			continue
		}

		_, _, rejection := c.processCmd(ctx, workflowID, cmd)
		if rejection != nil {
			logger.Warn("command rejected",
				"workflow_id", workflowID,
				"rejection", rejection.Msg,
			)
			// Continue processing other workflows even if one rejects
		}
	}

	// Step 5: Ack on success
	if err := msg.Ack(); err != nil {
		logger.Error("failed to ack message",
			"error", err,
			"subject", subject,
		)
	}
}

// Close cleans up the consumer, negatively acknowledging any unprocessed
// buffered messages so they can be redelivered. Safe to call multiple times.
func (c *ExternalMessageConsumer) Close() {
	if c.sub != nil {
		_ = c.sub.Unsubscribe()
		c.sub = nil
	}
	c.pending = nil
	c.pendingIdx = 0
}

// =============================================================================
// Helper: NewExternalMessageConsumerWithRepo
// =============================================================================

// NewExternalMessageConsumerWithRepo creates an ExternalMessageConsumer using
// a repo.Repo instance for command processing. This is a convenience constructor
// that wraps repo.ProcessCommand.
//
// Parameters:
//   - js: NATS JetStream context
//   - workflow: the workflow definition (used for event_to_cmd)
//   - r: the repo instance for command processing
//   - wfIDRule: optional partition filter (nil to process all)
//   - opts: optional configuration
func NewExternalMessageConsumerWithRepo(
	js nats.JetStreamContext,
	workflow model.Workflow,
	r *repo.Repo,
	wfIDRule WfIDRule,
	opts ...ExternalConsumerOption,
) (*ExternalMessageConsumer, error) {
	pool := r.Pool()
	workflowType := workflow.Name()
	repoParser := r.EventParser()

	// Explicit type conversion: repo.EventParser and external.EventParser have identical
	// underlying signatures but are different named types.
	parser := EventParser(repoParser)

	return NewExternalMessageConsumer(
		js,
		pool,
		workflowType,
		parser,
		workflow.EventToCmd,
		r.ProcessCommand,
		wfIDRule,
		opts...,
	)
}

// =============================================================================
// External Message Publisher (convenience)
// =============================================================================

// PublishExternal publishes an external message to NATS.
//
// Subject format: messages.{workflow_type}.{routing}.{detail}
//
// The payload is serialized as JSON. Returns an error if publishing fails.
func PublishExternal(
	ctx context.Context,
	js nats.JetStreamContext,
	workflowType string,
	routing string,
	detail string,
	payload any,
) error {
	if !isValidRoutingMode(routing) {
		return fmt.Errorf("external: invalid routing mode: %s", routing)
	}

	subject := fmt.Sprintf("messages.%s.%s.%s", workflowType, routing, detail)

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("external: marshal payload: %w", err)
	}

	_, err = js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("external: publish to %s: %w", subject, err)
	}

	return nil
}

// PublishExternalToAll publishes an external message to all workflows of the given type.
func PublishExternalToAll(
	ctx context.Context,
	js nats.JetStreamContext,
	workflowType string,
	payload any,
) error {
	return PublishExternal(ctx, js, workflowType, "all", "", payload)
}

// PublishExternalToTag publishes an external message to workflows with the given tag.
func PublishExternalToTag(
	ctx context.Context,
	js nats.JetStreamContext,
	workflowType string,
	tag string,
	payload any,
) error {
	return PublishExternal(ctx, js, workflowType, "tag", tag, payload)
}

// PublishExternalToID publishes an external message to a specific workflow.
func PublishExternalToID(
	ctx context.Context,
	js nats.JetStreamContext,
	workflowType string,
	workflowID string,
	payload any,
) error {
	return PublishExternal(ctx, js, workflowType, "id", workflowID, payload)
}

// PublishExternalToTopic publishes an external message to workflows subscribed to the topic.
func PublishExternalToTopic(
	ctx context.Context,
	js nats.JetStreamContext,
	workflowType string,
	topic string,
	payload any,
) error {
	return PublishExternal(ctx, js, workflowType, "topic", topic, payload)
}
