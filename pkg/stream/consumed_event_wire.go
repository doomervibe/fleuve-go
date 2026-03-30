package stream

import (
	"encoding/json"
	"time"
)

// MarshalConsumedEventWire builds JSON for JetStream (same shape PGReader uses logically).
func MarshalConsumedEventWire(globalID int64, workflowID, workflowType string, eventNo int64, eventType string, bodyJSON, metadataJSON []byte, at time.Time) ([]byte, error) {
	if len(metadataJSON) == 0 {
		metadataJSON = []byte("{}")
	}
	if len(bodyJSON) == 0 {
		bodyJSON = []byte("{}")
	}
	w := struct {
		GlobalID     int64           `json:"global_id"`
		WorkflowID   string          `json:"workflow_id"`
		WorkflowType string          `json:"workflow_type"`
		EventNo      int64           `json:"event_no"`
		EventType    string          `json:"event_type"`
		Event        json.RawMessage `json:"event"`
		At           time.Time       `json:"at"`
		Metadata     json.RawMessage `json:"metadata"`
	}{
		GlobalID:     globalID,
		WorkflowID:   workflowID,
		WorkflowType: workflowType,
		EventNo:      eventNo,
		EventType:    eventType,
		Event:        bodyJSON,
		At:           at,
		Metadata:     metadataJSON,
	}
	return json.Marshal(w)
}

// UnmarshalConsumedEventMessage parses NATS / JetStream JSON into a ConsumedEvent with a generic JSON event body (like PGReader).
func UnmarshalConsumedEventMessage(data []byte) (*ConsumedEvent, error) {
	var aux struct {
		GlobalID     int64           `json:"global_id"`
		WorkflowID   string          `json:"workflow_id"`
		WorkflowType string          `json:"workflow_type"`
		EventNo      int64           `json:"event_no"`
		EventType    string          `json:"event_type"`
		Event        json.RawMessage `json:"event"`
		At           time.Time       `json:"at"`
		Metadata     json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return nil, err
	}
	raw := make(map[string]any)
	if len(aux.Event) > 0 {
		if err := json.Unmarshal(aux.Event, &raw); err != nil {
			return nil, err
		}
	}
	meta := make(map[string]any)
	if len(aux.Metadata) > 0 {
		_ = json.Unmarshal(aux.Metadata, &meta)
	}
	return &ConsumedEvent{
		GlobalID:     aux.GlobalID,
		WorkflowID:   aux.WorkflowID,
		WorkflowType: aux.WorkflowType,
		EventNo:      aux.EventNo,
		EventType:    aux.EventType,
		Event:        &genericEvent{raw: raw},
		At:           aux.At,
		Metadata:     meta,
	}, nil
}
