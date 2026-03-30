package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// JetStreamPublisher publishes committed workflow events to the Fleuve JetStream stream
// (same stream name and subject layout as NATSReader).
type JetStreamPublisher struct {
	js           jetstream.JetStream
	streamName   string
	workflowType string
}

// NewJetStreamPublisher builds a publisher for workflowType (stream FLEUVE_{workflowType}).
func NewJetStreamPublisher(js jetstream.JetStream, workflowType string) *JetStreamPublisher {
	return &JetStreamPublisher{
		js:           js,
		streamName:   "FLEUVE_" + workflowType,
		workflowType: workflowType,
	}
}

// EnsureStream creates the JetStream stream if missing (idempotent).
func (p *JetStreamPublisher) EnsureStream(ctx context.Context) error {
	if p == nil || p.js == nil {
		return fmt.Errorf("nil jetstream")
	}
	_, err := p.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      p.streamName,
		Subjects:  []string{p.streamName + ".>"},
		Replicas:  1,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    7 * 24 * time.Hour,
	})
	if err != nil {
		if err == jetstream.ErrStreamNameAlreadyInUse {
			return nil
		}
		return err
	}
	return nil
}

// PublishWorkflowCommittedEvent publishes one stored_events row equivalent for runners.
// global_id is the PostgreSQL global_id (the NATS consumer overwrites ConsumedEvent.GlobalID with stream sequence).
func (p *JetStreamPublisher) PublishWorkflowCommittedEvent(ctx context.Context, globalID int64, workflowID, workflowType string, eventNo int64, eventType string, body, metadata []byte, at time.Time) error {
	if p == nil || p.js == nil {
		return nil
	}
	payload, err := MarshalConsumedEventWire(globalID, workflowID, workflowType, eventNo, eventType, body, metadata, at)
	if err != nil {
		return err
	}
	_, err = p.js.Publish(ctx, p.streamName+".evt", payload)
	return err
}
