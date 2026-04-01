package model

import (
	"encoding/json"
	"fmt"
)

// EventWithVersion pairs an event with its schema version from storage.
// Used during replay to support schema upcasting.
type EventWithVersion struct {
	Event         Event
	SchemaVersion int
	EventType     string
}

// RawStoredEvent represents an event as stored in the database,
// before deserialization. Used for the upcast path.
type RawStoredEvent struct {
	GlobalID        int64
	WorkflowID      string
	WorkflowVersion int64
	EventType       string
	WorkflowType    string
	SchemaVersion   int
	BodyRaw         json.RawMessage // Raw JSON from body_raw column
	At              string
	Metadata        json.RawMessage
}

// EventParser is a function that deserializes raw JSON into an Event.
// Different event types may have different parsers.
type EventParser func(eventType string, raw json.RawMessage) (Event, error)

// ReplayConfig holds configuration for event replay.
type ReplayConfig struct {
	// Workflow is the workflow instance for upcast and evolve.
	Workflow Workflow
	// Parser deserializes raw JSON into events.
	Parser EventParser
	// CurrentSchemaVersion is the current schema version of the workflow.
	CurrentSchemaVersion int
}

// ReplayEvents replays a slice of events through evolve.
// This is the simple path without upcasting.
func ReplayEvents(wf Workflow, state State, events []Event) State {
	return EvolveAll(wf, state, events)
}

// ReplayRawEvents replays raw stored events through upcast and evolve.
// This is the upcast path used when body_raw column exists.
//
// For each event:
//  1. Extract raw JSON from BodyRaw
//  2. Call Workflow.Upcast(eventType, schemaVersion, rawDict) if schemaVersion < current
//  3. Parse upcasted JSON through EventParser
//  4. Evolve state with the parsed event
func ReplayRawEvents(config ReplayConfig, state State, rawEvents []RawStoredEvent) (State, error) {
	for _, raw := range rawEvents {
		event, err := ParseAndUpcastEvent(config, raw)
		if err != nil {
			return nil, fmt.Errorf("failed to replay event %d: %w", raw.GlobalID, err)
		}
		state = Evolve(config.Workflow, state, event)
	}
	return state, nil
}

// ParseAndUpcastEvent parses a raw stored event, applying upcasting if needed.
func ParseAndUpcastEvent(config ReplayConfig, raw RawStoredEvent) (Event, error) {
	var rawData map[string]any
	if err := json.Unmarshal(raw.BodyRaw, &rawData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal raw event data: %w", err)
	}

	// Apply upcasting if the stored schema version is older than current
	if raw.SchemaVersion < config.CurrentSchemaVersion {
		rawData = config.Workflow.Upcast(raw.EventType, raw.SchemaVersion, rawData)
		if rawData == nil {
			return nil, fmt.Errorf("upcast returned nil for event type %s version %d",
				raw.EventType, raw.SchemaVersion)
		}
	}

	// Marshal the (possibly upcasted) data back to JSON for parsing
	upcastedJSON, err := json.Marshal(rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal upcasted data: %w", err)
	}

	// Parse through the event parser
	event, err := config.Parser(raw.EventType, upcastedJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to parse event type %s: %w", raw.EventType, err)
	}

	return event, nil
}

// ShouldUseUpcastPath determines whether to use the upcast path based on
// whether any events have schema versions older than current.
// Returns true if upcasting is needed.
func ShouldUseUpcastPath(events []RawStoredEvent, currentVersion int) bool {
	for _, e := range events {
		if e.SchemaVersion < currentVersion {
			return true
		}
	}
	return false
}

// IsTerminalState checks if a workflow should be considered terminated
// based on its last event. Returns true if the workflow is complete
// and should not accept new commands.
//
// CRITICAL: EvSystemCancel is NOT treated as final - the state is kept
// for lifecycle checks. This function handles that exception.
func IsTerminalState(wf Workflow, lastEvent Event) bool {
	if lastEvent == nil {
		return false
	}

	// Cancel is NOT terminal - state is kept for lifecycle checks
	if _, ok := lastEvent.(*EvSystemCancel); ok {
		return false
	}

	return wf.IsFinalEvent(lastEvent)
}

// EventsToSlice is a helper to convert variadic events to a slice.
// Useful for Decide implementations that return a known set of events.
func EventsToSlice(events ...Event) []Event {
	return events
}

// EventsFromPointer converts a pointer to an event to a slice.
// Returns nil if the event is nil.
func EventsFromPointer(event Event) []Event {
	if event == nil {
		return nil
	}
	return []Event{event}
}

// MergeMetadata merges source metadata into target metadata.
// Values in source override values in target.
// If target is nil, a new map is created.
func MergeMetadata(target, source map[string]any) map[string]any {
	if target == nil {
		target = make(map[string]any)
	}
	for k, v := range source {
		target[k] = v
	}
	return target
}

// SetWorkflowTagsInMetadata injects workflow tags into event metadata.
// This is called by the repo before inserting events.
func SetWorkflowTagsInMetadata(event Event, tags []string) {
	if len(tags) == 0 {
		return
	}

	// Events that embed EventBase should implement a GetMetadata method
	// Use interface assertion to access metadata
	if getter, ok := event.(interface{ GetMetadata() map[string]any }); ok {
		metadata := getter.GetMetadata()
		if metadata != nil {
			metadata["workflow_tags"] = tags
		}
	}
}

// ParseMetadata parses raw JSON metadata into a map.
func ParseMetadata(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return make(map[string]any)
	}
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return make(map[string]any)
	}
	return metadata
}

// ExtractTagsFromMetadata extracts the "tags" field from metadata as a string slice.
func ExtractTagsFromMetadata(metadata map[string]any) []string {
	if metadata == nil {
		return nil
	}
	tags, ok := metadata["tags"]
	if !ok {
		return nil
	}
	return toStringSlice(tags)
}

// ExtractWorkflowTagsFromMetadata extracts the "workflow_tags" field from metadata.
func ExtractWorkflowTagsFromMetadata(metadata map[string]any) []string {
	if metadata == nil {
		return nil
	}
	tags, ok := metadata["workflow_tags"]
	if !ok {
		return nil
	}
	return toStringSlice(tags)
}

// toStringSlice converts an any to a string slice.
// Handles []string, []any, and nil cases.
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
