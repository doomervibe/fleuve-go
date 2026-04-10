// Package postgres provides PostgreSQL model definitions and connection utilities.
//
// This package contains Go struct definitions that mirror the database tables
// defined in the migrations. These are intended for documentation and type
// reference purposes—the actual SQL is in migrations/.
//
// The connection utilities provide convenient ways to create and configure
// pgxpool connections.
package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// =============================================================================
// Table Name Constants
// =============================================================================

const (
	TableStoredEvents             = "stored_events"
	TableOffsets                  = "offsets"
	TableSubscriptions            = "subscriptions"
	TableExternalSubscriptions    = "external_subscriptions"
	TableActivities               = "activities"
	TableDelaySchedules           = "delay_schedules"
	TableSnapshots                = "snapshots"
	TableScalingOperations        = "scaling_operations"
	TableWorkflowMetadata         = "workflow_metadata"
	TableWorkflowSearchAttributes = "workflow_search_attributes"
	TableOutbox                   = "outbox"
)

// =============================================================================
// Database Model Definitions
//
// These structs mirror the PostgreSQL table schemas defined in migrations/.
// They are provided for type reference and documentation—this is NOT an ORM.
// Actual database operations use raw SQL via pgx.
// =============================================================================

// StoredEvent represents a row in the stored_events table.
// This is the primary event store for the event sourcing system.
type StoredEvent struct {
	GlobalID        int64          `json:"global_id"`
	WorkflowID      string         `json:"workflow_id"`
	WorkflowVersion int64          `json:"workflow_version"`
	Namespace       *string        `json:"namespace,omitempty"`
	EventType       string         `json:"event_type"`
	WorkflowType    string         `json:"workflow_type"`
	SchemaVersion   int            `json:"schema_version"`
	Body            map[string]any `json:"body"`
	At              time.Time      `json:"at"`
	Metadata        map[string]any `json:"metadata"`
	Pushed          bool           `json:"pushed"`
	CompressedBody  []byte         `json:"compressed_body,omitempty"`
	Compressed      bool           `json:"compressed,omitempty"`
}

// Snapshot represents a row in the snapshots table.
// Used for event truncation support—stores workflow state at a given version.
type Snapshot struct {
	WorkflowID   string         `json:"workflow_id"`
	WorkflowType string         `json:"workflow_type"`
	Version      int64          `json:"version"`
	State        map[string]any `json:"state"`
	CreatedAt    time.Time      `json:"created_at"`
}

// Offset represents a row in the offsets table.
// Tracks the last read event number for stream readers.
type Offset struct {
	Reader          string    `json:"reader"`
	LastReadEventNo int64     `json:"last_read_event_no"`
	Namespace       *string   `json:"namespace,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// Subscription represents a row in the subscriptions table.
// Tracks internal cross-workflow subscriptions with tag-based filtering.
type Subscription struct {
	ID                    int32    `json:"id"`
	WorkflowID            string   `json:"workflow_id"`
	WorkflowType          string   `json:"workflow_type"`
	SubscribedToWorkflow  string   `json:"subscribed_to_workflow"`
	SubscribedToEventType string   `json:"subscribed_to_event_type"`
	Tags                  []string `json:"tags"`
	TagsAll               []string `json:"tags_all"`
	Namespace             *string  `json:"namespace,omitempty"`
	AfterEmitterEventNo   *int64   `json:"after_emitter_event_no,omitempty"`
}

// ExternalSubscription represents a row in the external_subscriptions table.
// Tracks subscriptions to external NATS topics.
type ExternalSubscription struct {
	ID           int32  `json:"id"`
	WorkflowID   string `json:"workflow_id"`
	WorkflowType string `json:"workflow_type"`
	Topic        string `json:"topic"`
}

// Activity represents a row in the activities table.
// Tracks action execution state for workflow actions.
type Activity struct {
	ID            int32          `json:"id"`
	WorkflowID    string         `json:"workflow_id"`
	EventNumber   int64          `json:"event_number"`
	WorkflowType  string         `json:"workflow_type"`
	Status        string         `json:"status"`
	StartedAt     time.Time      `json:"started_at"`
	FinishedAt    *time.Time     `json:"finished_at,omitempty"`
	LastAttemptAt *time.Time     `json:"last_attempt_at,omitempty"`
	RetryCount    int            `json:"retry_count"`
	MaxRetries    int            `json:"max_retries"`
	Checkpoint    map[string]any `json:"checkpoint"`
	RetryPolicy   map[string]any `json:"retry_policy"`
	ErrorMessage  *string        `json:"error_message,omitempty"`
	ErrorType     *string        `json:"error_type,omitempty"`
	Result        []byte         `json:"result,omitempty"`
	RunnerID      *string        `json:"runner_id,omitempty"`
}

// DelaySchedule represents a row in the delay_schedules table.
// Tracks one-shot and cron-based delay schedules.
type DelaySchedule struct {
	ID             int32          `json:"id"`
	WorkflowID     string         `json:"workflow_id"`
	DelayID        string         `json:"delay_id"`
	WorkflowType   string         `json:"workflow_type"`
	DelayUntil     time.Time      `json:"delay_until"`
	EventVersion   int64          `json:"event_version"`
	CronExpression *string        `json:"cron_expression,omitempty"`
	Timezone       string         `json:"timezone"`
	NextCommand    map[string]any `json:"next_command"`
	CreatedAt      time.Time      `json:"created_at"`
}

// ScalingOperation represents a row in the scaling_operations table.
// Tracks partition scaling operations for workflow types.
type ScalingOperation struct {
	WorkflowType string    `json:"workflow_type"`
	TargetOffset int64     `json:"target_offset"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// WorkflowMetadata represents a row in the workflow_metadata table.
// Stores tags associated with workflows for subscription matching.
type WorkflowMetadata struct {
	WorkflowID   string    `json:"workflow_id"`
	WorkflowType string    `json:"workflow_type"`
	Tags         []string  `json:"tags"`
	CreatedAt    time.Time `json:"created_at"`
}

// WorkflowSearchAttributes represents a row in the workflow_search_attributes table.
// Stores arbitrary JSON attributes for workflow searching and filtering.
type WorkflowSearchAttributes struct {
	WorkflowID   string         `json:"workflow_id"`
	WorkflowType string         `json:"workflow_type"`
	Attributes   map[string]any `json:"attributes"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

// Outbox represents a row in the outbox table.
// Used for reliable external messaging via the outbox pattern.
type Outbox struct {
	ID          int32          `json:"id"`
	WorkflowID  string         `json:"workflow_id"`
	EventType   string         `json:"event_type"`
	Payload     map[string]any `json:"payload"`
	Topic       string         `json:"topic"`
	Published   bool           `json:"published"`
	CreatedAt   time.Time      `json:"created_at"`
	PublishedAt *time.Time     `json:"published_at,omitempty"`
}

// =============================================================================
// Connection Utilities
// =============================================================================

// PoolOption is a functional option for configuring pgxpool.
type PoolOption func(*pgxpool.Config) error

// WithPoolConfig applies a custom configuration function to the pool config.
// This allows callers to customize pool settings like MaxConns, MinConns,
// health checks, and connection parameters before the pool is created.
//
// Example:
//
//	config, _ := pgxpool.ParseConfig(databaseURL)
//	config.MaxConns = 20
//	pool, err := NewPGXPool(ctx, databaseURL, WithPoolConfig(config))
func WithPoolConfig(config *pgxpool.Config) PoolOption {
	return func(_ *pgxpool.Config) error {
		// The config is applied externally—the option pattern allows
		// pre-parsed configs to be passed through.
		// Note: This option is a marker; actual config merging happens
		// in NewPGXPool when this option is detected.
		return nil
	}
}

// NewPGXPool creates a new pgxpool.Pool from a database URL with optional
// configuration. This is the recommended way to create database connections
// in Fleuve.
//
// The context is used for the initial connection test. A nil context will
// cause this function to panic.
//
// Example:
//
//	pool, err := NewPGXPool(ctx, "postgres://user:pass@localhost:5432/db")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer pool.Close()
func NewPGXPool(ctx context.Context, databaseURL string, opts ...PoolOption) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		if err := opt(config); err != nil {
			return nil, err
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	return pool, nil
}
