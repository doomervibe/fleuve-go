package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/stream"
)

// backfillSubscription delivers commands for emitter events already in stored_events above
// the subscription horizon. Called when EvSubscriptionAdded is processed so subscribers
// do not miss events that committed before the subscription row.
//
// Only rows with global_id strictly less than this EvSubscriptionAdded event's global_id are
// replayed, so emitter events appended after the subscription (later global_id) are delivered
// exactly once via the normal stream path.
func (r *Runner) backfillSubscription(ctx context.Context, consumed *stream.ConsumedEvent, sub *model.Sub) error {
	if sub.AfterEmitterEventNo == nil {
		return nil
	}
	if sub.WorkflowID == "*" {
		log.Printf("[runner:%s] skip subscription backfill: wildcard subscribed_to_workflow", r.cfg.ReaderName)
		return nil
	}
	emitterID := sub.WorkflowID
	subscriberID := consumed.WorkflowID
	horizon := *sub.AfterEmitterEventNo

	const q = `
		SELECT workflow_version, workflow_type, event_type, body, metadata
		FROM stored_events
		WHERE workflow_id = $1 AND workflow_version > $2 AND global_id < $3
		ORDER BY workflow_version ASC`

	rows, err := r.cfg.Pool.Query(ctx, q, emitterID, horizon, consumed.GlobalID)
	if err != nil {
		return fmt.Errorf("backfill subscription query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var eventNo int64
		var wfType, eventType string
		var body []byte
		var metaRaw []byte
		if err := rows.Scan(&eventNo, &wfType, &eventType, &body, &metaRaw); err != nil {
			return fmt.Errorf("backfill subscription scan: %w", err)
		}
		if !subscriptionEmittedEventMatches(sub, emitterID, eventType, tagsFromMetadata(metaRaw, "tags"), tagsFromMetadata(metaRaw, "workflow_tags")) {
			continue
		}
		parsed, err := r.streamDeserializeFn(wfType, eventType, body)
		if err != nil {
			log.Printf("[runner:%s] backfill skip parse emitter %s v%d: %v", r.cfg.ReaderName, emitterID, eventNo, err)
			continue
		}
		injectEmitterMetaForBackfill(parsed, emitterID, wfType, eventNo)
		cmd := r.cfg.Workflow.EventToCmd(parsed)
		if cmd == nil {
			continue
		}
		_, _, rejection := r.cfg.Repo.ProcessCommand(ctx, subscriberID, cmd)
		if rejection != nil {
			log.Printf("[runner:%s] backfill command rejected for subscriber %s: %s",
				r.cfg.ReaderName, subscriberID, rejection.Msg)
		}
	}
	return rows.Err()
}

func injectEmitterMetaForBackfill(ev model.Event, emitterID, emitterType string, eventNo int64) {
	type metaTarget interface {
		GetMetadata() map[string]any
	}
	t, ok := ev.(metaTarget)
	if !ok {
		return
	}
	m := t.GetMetadata()
	m[model.MetaEmitterWorkflowVersion] = eventNo
	m[model.MetaEmitterWorkflowID] = emitterID
	m[model.MetaEmitterWorkflowType] = emitterType
}

func subscriptionEmittedEventMatches(sub *model.Sub, emitterWorkflowID, eventType string, eventTags, workflowTags []string) bool {
	if !sub.MatchesWorkflowAndEvent(emitterWorkflowID, eventType) {
		return false
	}
	return sub.MatchesTags(eventTags, workflowTags)
}

func tagsFromMetadata(metaJSON []byte, key string) []string {
	if len(metaJSON) == 0 {
		return nil
	}
	var meta map[string]any
	if err := json.Unmarshal(metaJSON, &meta); err != nil || meta == nil {
		return nil
	}
	v, ok := meta[key]
	if !ok || v == nil {
		return nil
	}
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}
