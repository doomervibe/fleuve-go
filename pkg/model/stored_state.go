package model

import (
	"encoding/json"
)

type StoredState struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
	State   State  `json:"state"`
}

func (s *StoredState) MarshalJSON() ([]byte, error) {
	type Alias struct {
		ID      string          `json:"id"`
		Version int64           `json:"version"`
		State   json.RawMessage `json:"state"`
	}
	stateBytes, err := json.Marshal(s.State)
	if err != nil {
		return nil, err
	}
	return json.Marshal(&Alias{
		ID:      s.ID,
		Version: s.Version,
		State:   stateBytes,
	})
}

type WorkflowNotFound struct {
	ID           string
	WorkflowType string
}

func (e *WorkflowNotFound) Error() string {
	return "workflow " + e.ID + " of type " + e.WorkflowType + " not found"
}
