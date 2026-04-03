package runner

import (
	"context"
	"fmt"
	"sort"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/stream"
)

// workflowsToNotify returns the sorted list of workflow IDs that should receive
// the command derived from this event.
//
// Mirrors Python WorkflowsRunner.workflows_to_notify:
//   - Same-type EvDelayComplete → add the emitter's own workflow ID (routes back to itself).
//   - Same-type EvDirectMessage → add the target workflow ID.
//   - All events → query subscriptions table for matching subscribers.
//
// The IsMine partition filter is applied before returning.
func (r *Runner) workflowsToNotify(ctx context.Context, consumed *stream.ConsumedEvent, parsedEvent model.Event) ([]string, error) {
	out := make(map[string]struct{})

	if consumed.WorkflowType == r.cfg.WorkflowType {
		switch e := parsedEvent.(type) {
		case *model.EvDelayComplete:
			// delay_complete routes back to the workflow that set the delay.
			out[consumed.WorkflowID] = struct{}{}
		case *model.EvDirectMessage:
			out[e.TargetWorkflowID] = struct{}{}
		}
	}

	subscribed, err := r.findSubscriptions(ctx, consumed)
	if err != nil {
		return nil, err
	}
	for _, wfID := range subscribed {
		out[wfID] = struct{}{}
	}

	result := make([]string, 0, len(out))
	for wfID := range out {
		if r.cfg.IsMine(wfID) {
			result = append(result, wfID)
		}
	}
	sort.Strings(result)
	return result, nil
}

// findSubscriptions queries the subscriptions table for workflows of this runner's
// workflow type that are subscribed to the given event.
//
// Mirrors Python WorkflowsRunner._find_subscriptions_from_db with this SQL:
//
//	SELECT DISTINCT workflow_id
//	FROM subscriptions
//	WHERE workflow_type = $1            -- this runner's workflow type (subscriber side)
//	  AND (
//	        (subscribed_to_event_type = ANY('*', $event_type)  AND subscribed_to_workflow = $emitter_id)
//	     OR (subscribed_to_event_type = $event_type            AND subscribed_to_workflow = ANY('*', $emitter_id))
//	  )
//
// TODO: tag matching (tags / tags_all columns) is not yet implemented.
func (r *Runner) findSubscriptions(ctx context.Context, consumed *stream.ConsumedEvent) ([]string, error) {
	if consumed.EventType == "" {
		return nil, nil
	}

	const query = `
		SELECT DISTINCT workflow_id
		FROM subscriptions
		WHERE workflow_type = $1
		  AND (
		        (subscribed_to_event_type = ANY($2) AND subscribed_to_workflow = $3)
		     OR (subscribed_to_event_type = $4      AND subscribed_to_workflow = ANY($5))
		  )`

	rows, err := r.cfg.Pool.Query(ctx, query,
		r.cfg.WorkflowType,                 // $1 — subscriber's workflow type
		[]string{"*", consumed.EventType},  // $2 — subscribed_to_event_type IN ('*', event_type)
		consumed.WorkflowID,                // $3
		consumed.EventType,                 // $4
		[]string{"*", consumed.WorkflowID}, // $5 — subscribed_to_workflow IN ('*', emitter_id)
	)
	if err != nil {
		return nil, fmt.Errorf("findSubscriptions: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var wfID string
		if err := rows.Scan(&wfID); err != nil {
			return nil, fmt.Errorf("findSubscriptions scan: %w", err)
		}
		out = append(out, wfID)
	}
	return out, rows.Err()
}
