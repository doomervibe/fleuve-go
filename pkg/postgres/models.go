package postgres

import (
	"time"

	"github.com/fleuve/fleuve-go/pkg/model"
)

type StoredEvent struct {
	GlobalID        int64     `db:"global_id" json:"global_id"`
	WorkflowID      string    `db:"workflow_id" json:"workflow_id"`
	WorkflowVersion int64     `db:"workflow_version" json:"workflow_version"`
	Namespace       *string   `db:"namespace" json:"namespace,omitempty"`
	EventType       string    `db:"event_type" json:"event_type"`
	WorkflowType    string    `db:"workflow_type" json:"workflow_type"`
	SchemaVersion   int       `db:"schema_version" json:"schema_version"`
	Body            []byte    `db:"body" json:"body"`
	At              time.Time `db:"at" json:"at"`
	Metadata        []byte    `db:"metadata" json:"metadata"`
	Pushed          bool      `db:"pushed" json:"pushed"`
}

type Offset struct {
	Reader          string  `db:"reader" json:"reader"`
	LastReadEventNo int64   `db:"last_read_event_no" json:"last_read_event_no"`
	Namespace       *string `db:"namespace" json:"namespace,omitempty"`
}

type Subscription struct {
	WorkflowID            string   `db:"workflow_id" json:"workflow_id"`
	WorkflowType          string   `db:"workflow_type" json:"workflow_type"`
	SubscribedToWorkflow  string   `db:"subscribed_to_workflow" json:"subscribed_to_workflow"`
	SubscribedToEventType string   `db:"subscribed_to_event_type" json:"subscribed_to_event_type"`
	Tags                  []string `db:"tags" json:"tags"`
	TagsAll               []string `db:"tags_all" json:"tags_all"`
	Namespace             *string  `db:"namespace" json:"namespace,omitempty"`
}

type ExternalSubscription struct {
	WorkflowID   string `db:"workflow_id" json:"workflow_id"`
	WorkflowType string `db:"workflow_type" json:"workflow_type"`
	Topic        string `db:"topic" json:"topic"`
}

type Activity struct {
	WorkflowID    string     `db:"workflow_id" json:"workflow_id"`
	EventNumber   int64      `db:"event_number" json:"event_number"`
	Status        string     `db:"status" json:"status"`
	StartedAt     time.Time  `db:"started_at" json:"started_at"`
	FinishedAt    *time.Time `db:"finished_at" json:"finished_at,omitempty"`
	LastAttemptAt *time.Time `db:"last_attempt_at" json:"last_attempt_at,omitempty"`
	RetryCount    int        `db:"retry_count" json:"retry_count"`
	MaxRetries    int        `db:"max_retries" json:"max_retries"`
	Checkpoint    []byte     `db:"checkpoint" json:"checkpoint"`
	RetryPolicy   []byte     `db:"retry_policy" json:"retry_policy"`
	ErrorMessage  *string    `db:"error_message" json:"error_message,omitempty"`
	ErrorType     *string    `db:"error_type" json:"error_type,omitempty"`
	Result        []byte     `db:"result" json:"result,omitempty"`
	RunnerID      *string    `db:"runner_id" json:"runner_id,omitempty"`
}

type DelaySchedule struct {
	WorkflowID     string    `db:"workflow_id" json:"workflow_id"`
	DelayID        string    `db:"delay_id" json:"delay_id"`
	WorkflowType   string    `db:"workflow_type" json:"workflow_type"`
	DelayUntil     time.Time `db:"delay_until" json:"delay_until"`
	EventVersion   int64     `db:"event_version" json:"event_version"`
	CronExpression *string   `db:"cron_expression" json:"cron_expression,omitempty"`
	Timezone       *string   `db:"timezone" json:"timezone,omitempty"`
	NextCommand    []byte    `db:"next_command" json:"next_command"`
	CreatedAt      time.Time `db:"created_at" json:"created_at"`
}

type Snapshot struct {
	WorkflowID   string    `db:"workflow_id" json:"workflow_id"`
	WorkflowType string    `db:"workflow_type" json:"workflow_type"`
	Version      int64     `db:"version" json:"version"`
	State        []byte    `db:"state" json:"state"`
	CreatedAt    time.Time `db:"created_at" json:"created_at"`
}

type ScalingOperation struct {
	WorkflowType string    `db:"workflow_type" json:"workflow_type"`
	TargetOffset int64     `db:"target_offset" json:"target_offset"`
	Status       string    `db:"status" json:"status"`
	CreatedAt    time.Time `db:"created_at" json:"created_at"`
	UpdatedAt    time.Time `db:"updated_at" json:"updated_at"`
}

type WorkflowMetadata struct {
	WorkflowID   string    `db:"workflow_id" json:"workflow_id"`
	WorkflowType string    `db:"workflow_type" json:"workflow_type"`
	Tags         []string  `db:"tags" json:"tags"`
	CreatedAt    time.Time `db:"created_at" json:"created_at"`
}

type WorkflowSearchAttributes struct {
	WorkflowID   string    `db:"workflow_id" json:"workflow_id"`
	WorkflowType string    `db:"workflow_type" json:"workflow_type"`
	Attributes   []byte    `db:"attributes" json:"attributes"`
	UpdatedAt    time.Time `db:"updated_at" json:"updated_at"`
}

const (
	ActionStatusPending   = "pending"
	ActionStatusRunning   = "running"
	ActionStatusCompleted = "completed"
	ActionStatusFailed    = "failed"
	ActionStatusRetrying  = "retrying"
	ActionStatusCancelled = "cancelled"
)

func RetryPolicyToJSON(p model.RetryPolicy) ([]byte, error) {
	return jsonMarshal(p)
}

func JSONToRetryPolicy(b []byte) (model.RetryPolicy, error) {
	var p model.RetryPolicy
	err := jsonUnmarshal(b, &p)
	return p, err
}

func jsonMarshal(v interface{}) ([]byte, error) {
	return []byte{}, nil
}

func jsonUnmarshal(data []byte, v interface{}) error {
	return nil
}
