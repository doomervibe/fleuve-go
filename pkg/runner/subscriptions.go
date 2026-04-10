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
// workflow type that are subscribed to the given event, then applies model.Sub
// tag matching (tags OR / tags_all AND) against event and workflow tags from
// consumed metadata — same rules as subscriptionEmittedEventMatches in backfill.
func (r *Runner) findSubscriptions(ctx context.Context, consumed *stream.ConsumedEvent) ([]string, error) {
	if consumed.EventType == "" {
		return nil, nil
	}

	eventTags := consumed.GetEventTags()
	workflowTags := consumed.GetWorkflowTags()

	const query = `
		SELECT workflow_id, subscribed_to_workflow, subscribed_to_event_type, tags, tags_all, after_emitter_event_no
		FROM subscriptions
		WHERE workflow_type = $1
		  AND (
		        (subscribed_to_event_type = ANY($2) AND subscribed_to_workflow = $3)
		     OR (subscribed_to_event_type = $4      AND subscribed_to_workflow = ANY($5))
		  )
		  AND (after_emitter_event_no IS NULL OR $6 > after_emitter_event_no)`

	rows, err := r.cfg.Pool.Query(ctx, query,
		r.cfg.WorkflowType,
		[]string{"*", consumed.EventType},
		consumed.WorkflowID,
		consumed.EventType,
		[]string{"*", consumed.WorkflowID},
		consumed.EventNo,
	)
	if err != nil {
		return nil, fmt.Errorf("findSubscriptions: %w", err)
	}
	defer rows.Close()

	seen := make(map[string]struct{})
	for rows.Next() {
		var wfID, subWf, subEv string
		var tags, tagsAll []string
		var horizon *int64
		if err := rows.Scan(&wfID, &subWf, &subEv, &tags, &tagsAll, &horizon); err != nil {
			return nil, fmt.Errorf("findSubscriptions scan: %w", err)
		}
		sub := model.Sub{
			WorkflowID:          subWf,
			EventType:           subEv,
			Tags:                tags,
			TagsAll:             tagsAll,
			AfterEmitterEventNo: horizon,
		}
		if !sub.MatchesWorkflowAndEvent(consumed.WorkflowID, consumed.EventType) {
			continue
		}
		if !sub.MatchesTags(eventTags, workflowTags) {
			continue
		}
		seen[wfID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(seen))
	for wfID := range seen {
		out = append(out, wfID)
	}
	sort.Strings(out)
	return out, nil
}
