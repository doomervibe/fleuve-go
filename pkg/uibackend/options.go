package uibackend

import (
	"context"
	"errors"
	"fmt"
	"unicode"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

// ErrStateUnresolved is returned by [StateResolver] to skip custom resolution and
// use [Options.Replay] or the latest stored event body (Python-style degraded mode).
var ErrStateUnresolved = errors.New("uibackend: defer workflow state to replay or latest event")

// StateResolver supplies workflow list/detail state and version. Return
// ErrStateUnresolved to fall through to [Options.Replay] or JSON from the latest event.
// On success, err must be nil; version <= 0 means use the latest event version.
type StateResolver func(ctx context.Context, workflowID, workflowType string) (state map[string]any, version int64, err error)

// WorkflowReplay configures typed state for a workflow_type using event replay.
type WorkflowReplay struct {
	Workflow model.Workflow
	Parser   model.EventParser
}

// Options configures the read-only UI API handler. Pool is required.
// Table names default to Fleuve migration names; overrides must be safe
// identifiers (letters, digits, underscore) or NewHandler returns an error.
type Options struct {
	Pool *pgxpool.Pool

	EventsTable        string
	SubscriptionsTable string
	ActivitiesTable    string
	DelaysTable        string

	// StateResolver optional custom state (e.g. snapshot service). See [StateResolver].
	StateResolver StateResolver
	// Replay keyed by workflow_type: when set and StateResolver does not handle the
	// workflow (or returns [ErrStateUnresolved]), replay all events for UI state.
	Replay map[string]WorkflowReplay
}

const (
	defaultEventsTable        = "stored_events"
	defaultSubscriptionsTable = "subscriptions"
	defaultActivitiesTable    = "workflow_activities"
	defaultDelaysTable        = "delay_schedules"
)

func (o Options) quotedEvents() (string, error) { return quoteIdent(o.EventsTable, defaultEventsTable) }
func (o Options) quotedSubscriptions() (string, error) {
	return quoteIdent(o.SubscriptionsTable, defaultSubscriptionsTable)
}
func (o Options) quotedActivities() (string, error) {
	return quoteIdent(o.ActivitiesTable, defaultActivitiesTable)
}
func (o Options) quotedDelays() (string, error) { return quoteIdent(o.DelaysTable, defaultDelaysTable) }

func quoteIdent(name, def string) (string, error) {
	if name == "" {
		name = def
	}
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			continue
		}
		return "", fmt.Errorf("uibackend: invalid SQL identifier %q", name)
	}
	return `"` + name + `"`, nil
}
