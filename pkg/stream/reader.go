package stream

import (
	"context"
	"time"

	"github.com/fleuve/fleuve-go/pkg/model"
)

type ConsumedEvent struct {
	GlobalID     int64          `json:"global_id"`
	WorkflowID   string         `json:"workflow_id"`
	WorkflowType string         `json:"workflow_type"`
	EventNo      int64          `json:"event_no"`
	EventType    string         `json:"event_type"`
	Event        model.Event    `json:"event"`
	At           time.Time      `json:"at"`
	Metadata     map[string]any `json:"metadata"`
}

type Reader interface {
	IterEvents(ctx context.Context) <-chan *ConsumedEvent
	SetStopAtOffset(offset int64)
	SetCommittedOffset(offset int64)
	LastReadEventGID() int64
	Close() error
}

type Sleeper struct {
	baseDelay time.Duration
	maxDelay  time.Duration
	attempts  int
}

func NewSleeper(baseDelay, maxDelay time.Duration) *Sleeper {
	return &Sleeper{baseDelay: baseDelay, maxDelay: maxDelay, attempts: 0}
}

func (s *Sleeper) Sleep(ctx context.Context) error {
	s.attempts++
	delay := s.baseDelay * time.Duration(1<<(s.attempts-1))
	if delay > s.maxDelay {
		delay = s.maxDelay
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}

func (s *Sleeper) Reset() {
	s.attempts = 0
}
