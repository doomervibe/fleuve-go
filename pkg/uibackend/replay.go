package uibackend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

func sanitizeLogMessage(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == '\n' || r == '\r' || r < 0x20 {
			b.WriteByte(' ')
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func stateToMap(s model.State) map[string]any {
	if s == nil {
		return map[string]any{}
	}
	b, err := json.Marshal(s)
	if err != nil {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{}
	}
	return m
}

// compositeParser wraps a workflow-specific parser with a fallback to
// model.ParseFrameworkEvent. This ensures framework events (subscription_added,
// delay, delay_complete, etc.) never cause replay to fail just because the
// workflow's own ParseEvent doesn't list them.
func compositeParser(workflowParser model.EventParser) model.EventParser {
	return func(eventType string, raw json.RawMessage) (model.Event, error) {
		ev, err := workflowParser(eventType, raw)
		if err == nil {
			return ev, nil
		}
		fwEv, fwErr := model.ParseFrameworkEvent(eventType, raw)
		if fwErr == nil {
			return fwEv, nil
		}
		return nil, err // return the original workflow parser error
	}
}

// replayWorkflowState loads all events for workflowID and folds them through
// model.ReplayRawEvents. wfType must match the row's workflow_type.
func (h *handler) replayWorkflowState(ctx context.Context, workflowID, wfType string, wr WorkflowReplay) (map[string]any, int64, error) {
	return h.replayWorkflowStateUpTo(ctx, workflowID, wfType, 0, wr)
}

// replayWorkflowStateUpTo is like replayWorkflowState but only replays events
// up to and including maxVersion. Pass 0 to replay all events.
func (h *handler) replayWorkflowStateUpTo(ctx context.Context, workflowID, wfType string, maxVersion int64, wr WorkflowReplay) (map[string]any, int64, error) {
	cap := int64(math.MaxInt64)
	if maxVersion > 0 {
		cap = maxVersion
	}
	q := fmt.Sprintf(`
		SELECT workflow_version, event_type, COALESCE(schema_version, 1), body
		FROM %s WHERE workflow_id = $1 AND workflow_type = $2 AND workflow_version <= $3
		ORDER BY workflow_version ASC`, h.ev)
	rows, err := h.pool.Query(ctx, q, workflowID, wfType, cap)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var raws []model.RawStoredEvent
	var lastVer int64
	for rows.Next() {
		var ver int64
		var et string
		var sv int32
		var body []byte
		if err := rows.Scan(&ver, &et, &sv, &body); err != nil {
			return nil, 0, err
		}
		raws = append(raws, model.RawStoredEvent{
			WorkflowVersion: ver,
			EventType:       et,
			SchemaVersion:   int(sv),
			BodyRaw:         body,
		})
		lastVer = ver
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if len(raws) == 0 {
		return map[string]any{}, 0, nil
	}

	cfg := model.ReplayConfig{
		Workflow:             wr.Workflow,
		Parser:               compositeParser(wr.Parser),
		CurrentSchemaVersion: wr.Workflow.SchemaVersion(),
	}
	state, err := model.ReplayRawEvents(cfg, nil, raws)
	if err != nil {
		return nil, 0, err
	}
	return stateToMap(state), lastVer, nil
}

func (h *handler) resolveWorkflowState(ctx context.Context, workflowID, wfType string, latestVer int64, latestBody []byte) (map[string]any, int64) {
	if h.stateResolver != nil {
		st, ver, err := h.stateResolver(ctx, workflowID, wfType)
		if err == nil {
			if st == nil {
				st = map[string]any{}
			}
			if ver <= 0 {
				ver = latestVer
			}
			return st, ver
		}
		if !errors.Is(err, ErrStateUnresolved) {
			log.Printf("uibackend: StateResolver(%q,%q): %s", sanitizeLogMessage(workflowID), sanitizeLogMessage(wfType), sanitizeLogMessage(err.Error())) // #nosec G706 -- fields stripped of control chars
		}
	}
	if wr, ok := h.replayByType[wfType]; ok {
		st, ver, err := h.replayWorkflowState(ctx, workflowID, wfType, wr)
		if err == nil {
			return st, ver
		}
		log.Printf("uibackend: replay state %q: %s", sanitizeLogMessage(workflowID), sanitizeLogMessage(err.Error())) // #nosec G706 -- fields stripped of control chars
	}
	return jsonObject(latestBody), latestVer
}
