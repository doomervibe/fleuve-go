package uibackend

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"

	"github.com/doomervibe/fleuve-go/pkg/uiembed"
)

// NewHandler returns an [http.Handler] for GET /health and /api/* (same JSON
// contract as Python FleuveUIBackend). Paths outside those return 404.
func NewHandler(opts Options) (http.Handler, error) {
	if opts.Pool == nil {
		return nil, fmt.Errorf("uibackend: Pool is required")
	}
	ev, err := opts.quotedEvents()
	if err != nil {
		return nil, err
	}
	sub, err := opts.quotedSubscriptions()
	if err != nil {
		return nil, err
	}
	act, err := opts.quotedActivities()
	if err != nil {
		return nil, err
	}
	del, err := opts.quotedDelays()
	if err != nil {
		return nil, err
	}
	replayCopy := make(map[string]WorkflowReplay)
	for k, v := range opts.Replay {
		replayCopy[k] = v
	}
	h := &handler{
		pool:          opts.Pool,
		ev:            ev,
		sub:           sub,
		act:           act,
		del:           del,
		stateResolver: opts.StateResolver,
		replayByType:  replayCopy,
	}
	return h, nil
}

// NewCombinedHandler serves /health and /api/* via the UI API and all other GET
// paths via the embedded React app (pkg/uiembed).
func NewCombinedHandler(uiTitle string, opts Options) (http.Handler, error) {
	api, err := NewHandler(opts)
	if err != nil {
		return nil, err
	}
	ui := uiembed.NewHandler(uiTitle)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/api" {
			api.ServeHTTP(w, r)
			return
		}
		ui.ServeHTTP(w, r)
	}), nil
}

type handler struct {
	pool          *pgxpool.Pool
	ev            string
	sub           string
	act           string
	del           string
	stateResolver StateResolver
	replayByType  map[string]WorkflowReplay
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/api" {
		h.serveAPI(w, r)
		return
	}
	http.NotFound(w, r)
}

func (h *handler) serveAPI(w http.ResponseWriter, r *http.Request) {
	apiPath := strings.TrimPrefix(r.URL.Path, "/api")
	if apiPath == "" {
		apiPath = "/"
	}
	switch {
	case r.Method == http.MethodGet && apiPath == "/workflow-types":
		h.getWorkflowTypes(w, r)
	case r.Method == http.MethodGet && apiPath == "/workflows":
		h.listWorkflows(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(apiPath, "/workflows/"):
		h.routeWorkflows(w, r, strings.TrimPrefix(apiPath, "/workflows/"))
	case r.Method == http.MethodGet && apiPath == "/events":
		h.listEvents(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(apiPath, "/events/"):
		h.getEvent(w, r, strings.TrimPrefix(apiPath, "/events/"))
	case r.Method == http.MethodGet && apiPath == "/activities":
		h.listActivities(w, r, activityFilterFromQuery(r))
	case r.Method == http.MethodGet && apiPath == "/delays":
		h.listDelays(w, r, "", "", 0, 0)
	case r.Method == http.MethodGet && apiPath == "/stats":
		h.getStats(w, r)
	case r.Method == http.MethodPost && apiPath == "/workflows/batch/cancel":
		h.batchCancel(w, r)
	case r.Method == http.MethodPost && apiPath == "/workflows/batch/replay":
		h.batchReplay(w, r)
	default:
		writeError(w, http.StatusNotFound, "Not found")
	}
}

func (h *handler) routeWorkflows(w http.ResponseWriter, r *http.Request, rest string) {
	rest = strings.TrimPrefix(rest, "/")
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "Not found")
		return
	}
	id := parts[0]
	switch {
	case len(parts) == 1:
		if r.Method == http.MethodGet {
			h.getWorkflow(w, r, id)
			return
		}
	case len(parts) == 2 && parts[1] == "events" && r.Method == http.MethodGet:
		h.getWorkflowEvents(w, r, id)
		return
	case len(parts) == 3 && parts[1] == "state" && r.Method == http.MethodGet:
		v, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid version")
			return
		}
		h.getWorkflowStateAtVersion(w, r, id, v)
		return
	case len(parts) == 4 && parts[1] == "state-diff" && r.Method == http.MethodGet:
		v1, e1 := strconv.ParseInt(parts[2], 10, 64)
		v2, e2 := strconv.ParseInt(parts[3], 10, 64)
		if e1 != nil || e2 != nil {
			writeError(w, http.StatusBadRequest, "invalid version")
			return
		}
		h.getWorkflowStateDiff(w, r, id, v1, v2)
		return
	case len(parts) == 2 && parts[1] == "activities" && r.Method == http.MethodGet:
		f := activityFilterFromQuery(r)
		f.workflowID = id
		f.limit = 1000
		f.offset = 0
		h.listActivities(w, r, f)
		return
	case len(parts) == 2 && parts[1] == "delays" && r.Method == http.MethodGet:
		h.listDelays(w, r, "", id, 1000, 0)
		return
	}
	writeError(w, http.StatusNotFound, "Not found")
}

type activityFilter struct {
	workflowID   string
	workflowType string
	status       string
	limit        int
	offset       int
}

func activityFilterFromQuery(r *http.Request) activityFilter {
	q := r.URL.Query()
	return activityFilter{
		workflowID:   q.Get("workflow_id"),
		workflowType: q.Get("workflow_type"),
		status:       q.Get("status"),
		limit:        clampLimit(q.Get("limit"), 100, 1000),
		offset:       clampOffset(q.Get("offset")),
	}
}

func clampLimit(s string, def, max int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

func clampOffset(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"detail": msg})
}

func parseQueryTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	s = strings.ReplaceAll(s, " ", "T")
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
	}
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func jsonObject(b []byte) map[string]any {
	if len(b) == 0 {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{}
	}
	return m
}

func (h *handler) getWorkflowTypes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := fmt.Sprintf(`SELECT DISTINCT workflow_type FROM %s WHERE workflow_type <> '' ORDER BY workflow_type`, h.ev)
	rows, err := h.pool.Query(ctx, q)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	var out []workflowTypeInfo
	for rows.Next() {
		var wt string
		if err := rows.Scan(&wt); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		st, err := h.workflowTypeStats(ctx, wt)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

type workflowTypeInfo struct {
	WorkflowType  string     `json:"workflow_type"`
	WorkflowCount int64      `json:"workflow_count"`
	EventCount    int64      `json:"event_count"`
	LastEventAt   *time.Time `json:"last_event_at,omitempty"`
}

func (h *handler) workflowTypeStats(ctx context.Context, workflowType string) (workflowTypeInfo, error) {
	var wCount, eCount int64
	var lastAt *time.Time
	q1 := fmt.Sprintf(`SELECT COUNT(DISTINCT workflow_id) FROM %s WHERE workflow_type = $1`, h.ev)
	if err := h.pool.QueryRow(ctx, q1, workflowType).Scan(&wCount); err != nil {
		return workflowTypeInfo{}, err
	}
	q2 := fmt.Sprintf(`SELECT COUNT(global_id) FROM %s WHERE workflow_type = $1`, h.ev)
	if err := h.pool.QueryRow(ctx, q2, workflowType).Scan(&eCount); err != nil {
		return workflowTypeInfo{}, err
	}
	q3 := fmt.Sprintf(`SELECT MAX(at) FROM %s WHERE workflow_type = $1`, h.ev)
	var maxT pgtype.Timestamptz
	_ = h.pool.QueryRow(ctx, q3, workflowType).Scan(&maxT)
	if maxT.Valid {
		t := maxT.Time
		lastAt = &t
	}
	return workflowTypeInfo{
		WorkflowType:  workflowType,
		WorkflowCount: wCount,
		EventCount:    eCount,
		LastEventAt:   lastAt,
	}, nil
}

func (h *handler) listWorkflows(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	wfType := q.Get("workflow_type")
	search := q.Get("search")
	limit := clampLimit(q.Get("limit"), 100, 1000)
	offset := clampOffset(q.Get("offset"))
	after, hasAfter := parseQueryTime(q.Get("created_after"))
	before, hasBefore := parseQueryTime(q.Get("created_before"))

	var sb strings.Builder
	args := []any{}
	sb.WriteString(fmt.Sprintf(`SELECT DISTINCT e.workflow_id FROM %s e WHERE 1=1`, h.ev))
	if wfType != "" {
		args = append(args, wfType)
		sb.WriteString(fmt.Sprintf(` AND e.workflow_type = $%d`, len(args)))
	}
	if search != "" {
		args = append(args, "%"+search+"%")
		sb.WriteString(fmt.Sprintf(` AND e.workflow_id LIKE $%d`, len(args)))
	}
	if hasAfter || hasBefore {
		inner := fmt.Sprintf(`SELECT workflow_id, MIN(at) AS first_at FROM %s GROUP BY workflow_id`, h.ev)
		sub := `SELECT workflow_id FROM (` + inner + `) first_events WHERE 1=1`
		if hasAfter {
			args = append(args, after)
			sub += fmt.Sprintf(` AND first_at >= $%d`, len(args))
		}
		if hasBefore {
			args = append(args, before)
			sub += fmt.Sprintf(` AND first_at <= $%d`, len(args))
		}
		sb.WriteString(` AND e.workflow_id IN (` + sub + `)`)
	}
	args = append(args, limit, offset)
	sb.WriteString(fmt.Sprintf(` LIMIT $%d OFFSET $%d`, len(args)-1, len(args)))

	rows, err := h.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var workflows []workflowSummary
	for _, workflowID := range ids {
		sum, err := h.workflowSummary(ctx, workflowID)
		if err != nil {
			log.Printf("uibackend: workflow %q: %v", workflowID, err)
			continue
		}
		workflows = append(workflows, sum)
	}
	writeJSON(w, http.StatusOK, workflows)
}

type workflowSummary struct {
	WorkflowID   string         `json:"workflow_id"`
	WorkflowType string         `json:"workflow_type"`
	Version      int64          `json:"version"`
	State        map[string]any `json:"state"`
	CreatedAt    *time.Time     `json:"created_at,omitempty"`
	UpdatedAt    *time.Time     `json:"updated_at,omitempty"`
	IsCompleted  bool           `json:"is_completed"`
}

func (h *handler) workflowSummary(ctx context.Context, workflowID string) (workflowSummary, error) {
	qLast := fmt.Sprintf(`
		SELECT workflow_type, workflow_version, body, at FROM %s
		WHERE workflow_id = $1 ORDER BY workflow_version DESC LIMIT 1`, h.ev)
	var wt string
	var ver int64
	var body []byte
	var atLast time.Time
	err := h.pool.QueryRow(ctx, qLast, workflowID).Scan(&wt, &ver, &body, &atLast)
	if err != nil {
		return workflowSummary{}, err
	}
	qFirst := fmt.Sprintf(`
		SELECT at FROM %s WHERE workflow_id = $1 ORDER BY workflow_version ASC LIMIT 1`, h.ev)
	var atFirst time.Time
	_ = h.pool.QueryRow(ctx, qFirst, workflowID).Scan(&atFirst)
	st, vOut := h.resolveWorkflowState(ctx, workflowID, wt, ver, body)
	return workflowSummary{
		WorkflowID:   workflowID,
		WorkflowType: wt,
		Version:      vOut,
		State:        st,
		CreatedAt:    &atFirst,
		UpdatedAt:    &atLast,
		IsCompleted:  false,
	}, nil
}

type workflowDetail struct {
	WorkflowID    string              `json:"workflow_id"`
	WorkflowType  string              `json:"workflow_type"`
	Version       int64               `json:"version"`
	State         map[string]any      `json:"state"`
	CreatedAt     *time.Time          `json:"created_at,omitempty"`
	UpdatedAt     *time.Time          `json:"updated_at,omitempty"`
	IsCompleted   bool                `json:"is_completed"`
	Subscriptions []map[string]string `json:"subscriptions"`
}

func (h *handler) getWorkflow(w http.ResponseWriter, r *http.Request, workflowID string) {
	ctx := r.Context()
	qLast := fmt.Sprintf(`
		SELECT workflow_type, workflow_version, body, at FROM %s
		WHERE workflow_id = $1 ORDER BY workflow_version DESC LIMIT 1`, h.ev)
	var wt string
	var ver int64
	var body []byte
	var atLast time.Time
	err := h.pool.QueryRow(ctx, qLast, workflowID).Scan(&wt, &ver, &body, &atLast)
	if err != nil {
		writeError(w, http.StatusNotFound, "Workflow not found")
		return
	}
	qFirst := fmt.Sprintf(`
		SELECT at FROM %s WHERE workflow_id = $1 ORDER BY workflow_version ASC LIMIT 1`, h.ev)
	var atFirst time.Time
	_ = h.pool.QueryRow(ctx, qFirst, workflowID).Scan(&atFirst)

	qSub := fmt.Sprintf(`
		SELECT subscribed_to_workflow, subscribed_to_event_type, after_emitter_event_no FROM %s WHERE workflow_id = $1`, h.sub)
	rows, err := h.pool.Query(ctx, qSub, workflowID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	var subs []map[string]string
	for rows.Next() {
		var swf, evT string
		var h *int64
		if err := rows.Scan(&swf, &evT, &h); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		m := map[string]string{
			"workflow_id": swf,
			"event_type":  evT,
		}
		if h != nil {
			m["after_emitter_event_no"] = fmt.Sprintf("%d", *h)
		}
		subs = append(subs, m)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	st, vOut := h.resolveWorkflowState(ctx, workflowID, wt, ver, body)
	writeJSON(w, http.StatusOK, workflowDetail{
		WorkflowID:    workflowID,
		WorkflowType:  wt,
		Version:       vOut,
		State:         st,
		CreatedAt:     &atFirst,
		UpdatedAt:     &atLast,
		IsCompleted:   false,
		Subscriptions: subs,
	})
}

type eventResponse struct {
	GlobalID        int64          `json:"global_id"`
	WorkflowID      string         `json:"workflow_id"`
	WorkflowType    string         `json:"workflow_type"`
	WorkflowVersion int64          `json:"workflow_version"`
	EventType       string         `json:"event_type"`
	Body            map[string]any `json:"body"`
	At              time.Time      `json:"at"`
	Metadata        map[string]any `json:"metadata"`
}

func (h *handler) getWorkflowEvents(w http.ResponseWriter, r *http.Request, workflowID string) {
	ctx := r.Context()
	q := fmt.Sprintf(`
		SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata
		FROM %s WHERE workflow_id = $1 ORDER BY workflow_version ASC`, h.ev)
	rows, err := h.pool.Query(ctx, q, workflowID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	var events []eventResponse
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, events)
}

type eventScanner interface {
	Scan(dest ...any) error
}

func scanEventRow(row eventScanner) (eventResponse, error) {
	var ev eventResponse
	var body, meta []byte
	if err := row.Scan(&ev.GlobalID, &ev.WorkflowID, &ev.WorkflowType, &ev.WorkflowVersion, &ev.EventType, &body, &ev.At, &meta); err != nil {
		return eventResponse{}, err
	}
	ev.Body = jsonObject(body)
	ev.Metadata = jsonObject(meta)
	return ev, nil
}

func (h *handler) getWorkflowStateAtVersion(w http.ResponseWriter, r *http.Request, workflowID string, version int64) {
	ctx := r.Context()
	q := fmt.Sprintf(`
		SELECT workflow_version, event_type, body, at FROM %s
		WHERE workflow_id = $1 AND workflow_version <= $2
		ORDER BY workflow_version ASC`, h.ev)
	rows, err := h.pool.Query(ctx, q, workflowID, version)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	type evLite struct {
		Version int64          `json:"version"`
		Type    string         `json:"type"`
		Body    map[string]any `json:"body"`
		At      string         `json:"at"`
	}
	var list []evLite
	for rows.Next() {
		var ver int64
		var et string
		var body []byte
		var at time.Time
		if err := rows.Scan(&ver, &et, &body, &at); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		list = append(list, evLite{Version: ver, Type: et, Body: jsonObject(body), At: at.Format(time.RFC3339Nano)})
	}
	if len(list) == 0 {
		writeError(w, http.StatusNotFound, "No events found for this version")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"workflow_id": workflowID,
		"version":     version,
		"events":      list,
		"note":        "State reconstruction requires workflow-specific code. Showing events instead.",
	})
}

func (h *handler) getWorkflowStateDiff(w http.ResponseWriter, r *http.Request, workflowID string, v1, v2 int64) {
	ctx := r.Context()
	load := func(maxVer int64) ([]map[string]any, error) {
		q := fmt.Sprintf(`
			SELECT workflow_version, event_type, body, at FROM %s
			WHERE workflow_id = $1 AND workflow_version <= $2
			ORDER BY workflow_version ASC`, h.ev)
		rows, err := h.pool.Query(ctx, q, workflowID, maxVer)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []map[string]any
		for rows.Next() {
			var ver int64
			var et string
			var body []byte
			var at time.Time
			if err := rows.Scan(&ver, &et, &body, &at); err != nil {
				return nil, err
			}
			out = append(out, map[string]any{
				"version": ver,
				"type":    et,
				"body":    jsonObject(body),
				"at":      at.Format(time.RFC3339Nano),
			})
		}
		return out, rows.Err()
	}
	events1, err := load(v1)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	events2, err := load(v2)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(events1) == 0 {
		writeError(w, http.StatusNotFound, fmt.Sprintf("No events for version %d", v1))
		return
	}
	if len(events2) == 0 {
		writeError(w, http.StatusNotFound, fmt.Sprintf("No events for version %d", v2))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"workflow_id": workflowID,
		"version1":    v1,
		"version2":    v2,
		"state_v1":    map[string]any{"version": v1, "events": events1},
		"state_v2":    map[string]any{"version": v2, "events": events2},
	})
}

func (h *handler) listEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	wfType := q.Get("workflow_type")
	wfID := q.Get("workflow_id")
	evType := q.Get("event_type")
	limit := clampLimit(q.Get("limit"), 100, 1000)
	offset := clampOffset(q.Get("offset"))
	after, hasAfter := parseQueryTime(q.Get("created_after"))
	before, hasBefore := parseQueryTime(q.Get("created_before"))

	var sb strings.Builder
	args := []any{}
	sb.WriteString(fmt.Sprintf(`SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata FROM %s WHERE 1=1`, h.ev))
	if wfType != "" {
		args = append(args, wfType)
		sb.WriteString(fmt.Sprintf(` AND workflow_type = $%d`, len(args)))
	}
	if wfID != "" {
		args = append(args, wfID)
		sb.WriteString(fmt.Sprintf(` AND workflow_id = $%d`, len(args)))
	}
	if evType != "" {
		args = append(args, evType)
		sb.WriteString(fmt.Sprintf(` AND event_type = $%d`, len(args)))
	}
	if hasAfter {
		args = append(args, after)
		sb.WriteString(fmt.Sprintf(` AND at >= $%d`, len(args)))
	}
	if hasBefore {
		args = append(args, before)
		sb.WriteString(fmt.Sprintf(` AND at <= $%d`, len(args)))
	}
	args = append(args, limit, offset)
	sb.WriteString(fmt.Sprintf(` ORDER BY global_id DESC LIMIT $%d OFFSET $%d`, len(args)-1, len(args)))

	rows, err := h.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	var events []eventResponse
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (h *handler) getEvent(w http.ResponseWriter, r *http.Request, idStr string) {
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid event id")
		return
	}
	ctx := r.Context()
	q := fmt.Sprintf(`
		SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata
		FROM %s WHERE global_id = $1`, h.ev)
	row := h.pool.QueryRow(ctx, q, id)
	ev, err := scanEventRow(row)
	if err != nil {
		if err == pgx.ErrNoRows {
			writeError(w, http.StatusNotFound, "Event not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

func deriveActionType(checkpoint map[string]any, eventType string, eventBody map[string]any) string {
	candidates := []string{
		stringField(checkpoint, "action_type"),
		stringField(checkpoint, "action"),
		stringField(checkpoint, "step"),
		stringField(checkpoint, "operation"),
		stringField(eventBody, "action_type"),
		stringField(eventBody, "type"),
		eventType,
	}
	for _, c := range candidates {
		if strings.TrimSpace(c) != "" {
			return strings.TrimSpace(c)
		}
	}
	return "unknown"
}

func stringField(m map[string]any, k string) string {
	v, ok := m[k]
	if !ok || v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

type activityResponse struct {
	WorkflowID    string         `json:"workflow_id"`
	WorkflowType  string         `json:"workflow_type"`
	EventNumber   int64          `json:"event_number"`
	Status        string         `json:"status"`
	StartedAt     time.Time      `json:"started_at"`
	FinishedAt    *time.Time     `json:"finished_at,omitempty"`
	LastAttemptAt *time.Time     `json:"last_attempt_at,omitempty"`
	RetryCount    int            `json:"retry_count"`
	MaxRetries    int            `json:"max_retries"`
	ErrorMessage  *string        `json:"error_message,omitempty"`
	ErrorType     *string        `json:"error_type,omitempty"`
	Checkpoint    map[string]any `json:"checkpoint"`
	ActionType    string         `json:"action_type"`
	RunnerID      *string        `json:"runner_id,omitempty"`
}

func (h *handler) listActivities(w http.ResponseWriter, r *http.Request, f activityFilter) {
	ctx := r.Context()
	var sb strings.Builder
	args := []any{}
	sb.WriteString(fmt.Sprintf(`SELECT workflow_id, workflow_type, event_number, status, started_at, finished_at, last_attempt_at,
		retry_count, max_retries, error_message, error_type, checkpoint, runner_id FROM %s WHERE 1=1`, h.act))
	if f.workflowID != "" {
		args = append(args, f.workflowID)
		sb.WriteString(fmt.Sprintf(` AND workflow_id = $%d`, len(args)))
	}
	if f.workflowType != "" {
		args = append(args, f.workflowType)
		sb.WriteString(fmt.Sprintf(` AND workflow_type = $%d`, len(args)))
	}
	if f.status != "" {
		args = append(args, f.status)
		sb.WriteString(fmt.Sprintf(` AND status = $%d`, len(args)))
	}
	args = append(args, f.limit, f.offset)
	sb.WriteString(fmt.Sprintf(` ORDER BY started_at DESC LIMIT $%d OFFSET $%d`, len(args)-1, len(args)))

	rows, err := h.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	type rowKey struct {
		wid string
		ver int64
	}
	type actRow struct {
		wid, wtype, status string
		eventNumber        int64
		startedAt          time.Time
		finishedAt         pgtype.Timestamptz
		lastAttempt        pgtype.Timestamptz
		retryCount         int
		maxRetries         int
		errMsg             pgtype.Text
		errType            pgtype.Text
		checkpoint         []byte
		runnerID           pgtype.Text
	}
	var activities []actRow
	for rows.Next() {
		var a actRow
		if err := rows.Scan(&a.wid, &a.wtype, &a.eventNumber, &a.status, &a.startedAt, &a.finishedAt, &a.lastAttempt,
			&a.retryCount, &a.maxRetries, &a.errMsg, &a.errType, &a.checkpoint, &a.runnerID); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		activities = append(activities, a)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	eventMap := make(map[rowKey]struct {
		evType string
		body   map[string]any
	})
	if len(activities) > 0 {
		wids := make(map[string]struct{})
		vers := make(map[int64]struct{})
		for _, a := range activities {
			wids[a.wid] = struct{}{}
			vers[a.eventNumber] = struct{}{}
		}
		if len(wids) > 0 && len(vers) > 0 {
			widList := make([]string, 0, len(wids))
			for w := range wids {
				widList = append(widList, w)
			}
			verList := make([]int64, 0, len(vers))
			for v := range vers {
				verList = append(verList, v)
			}
			q := fmt.Sprintf(`
				SELECT workflow_id, workflow_version, event_type, body FROM %s
				WHERE workflow_id = ANY($1::text[]) AND workflow_version = ANY($2::bigint[])`, h.ev)
			erows, err := h.pool.Query(ctx, q, widList, verList)
			if err == nil {
				for erows.Next() {
					var wid string
					var ver int64
					var et string
					var body []byte
					if err := erows.Scan(&wid, &ver, &et, &body); err == nil {
						eventMap[rowKey{wid, ver}] = struct {
							evType string
							body   map[string]any
						}{et, jsonObject(body)}
					}
				}
				erows.Close()
			}
		}
	}

	var out []activityResponse
	for _, a := range activities {
		cp := jsonObject(a.checkpoint)
		em := eventMap[rowKey{a.wid, a.eventNumber}]
		at := deriveActionType(cp, em.evType, em.body)
		var fin, last *time.Time
		if a.finishedAt.Valid {
			t := a.finishedAt.Time
			fin = &t
		}
		if a.lastAttempt.Valid {
			t := a.lastAttempt.Time
			last = &t
		}
		var errMsg, errType, run *string
		if a.errMsg.Valid {
			s := a.errMsg.String
			errMsg = &s
		}
		if a.errType.Valid {
			s := a.errType.String
			errType = &s
		}
		if a.runnerID.Valid {
			s := a.runnerID.String
			run = &s
		}
		out = append(out, activityResponse{
			WorkflowID:    a.wid,
			WorkflowType:  a.wtype,
			EventNumber:   a.eventNumber,
			Status:        a.status,
			StartedAt:     a.startedAt,
			FinishedAt:    fin,
			LastAttemptAt: last,
			RetryCount:    a.retryCount,
			MaxRetries:    a.maxRetries,
			ErrorMessage:  errMsg,
			ErrorType:     errType,
			Checkpoint:    cp,
			ActionType:    at,
			RunnerID:      run,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

type delayResponse struct {
	WorkflowID      string         `json:"workflow_id"`
	WorkflowType    string         `json:"workflow_type"`
	DelayID         string         `json:"delay_id"`
	DelayUntil      time.Time      `json:"delay_until"`
	EventVersion    int64          `json:"event_version"`
	CreatedAt       time.Time      `json:"created_at"`
	NextCommand     map[string]any `json:"next_command"`
	DelayType       string         `json:"delay_type"`
	NextCommandType string         `json:"next_command_type"`
	CronExpression  *string        `json:"cron_expression,omitempty"`
	CronTimezone    *string        `json:"cron_timezone,omitempty"`
	NextFireTimes   []time.Time    `json:"next_fire_times,omitempty"`
}

func (h *handler) listDelays(w http.ResponseWriter, r *http.Request, workflowType, workflowID string, limit, offset int) {
	ctx := r.Context()
	qs := r.URL.Query()
	if workflowType == "" {
		workflowType = qs.Get("workflow_type")
	}
	if workflowID == "" {
		workflowID = qs.Get("workflow_id")
	}
	if limit == 0 {
		limit = clampLimit(qs.Get("limit"), 100, 1000)
		offset = clampOffset(qs.Get("offset"))
	}

	var sb strings.Builder
	args := []any{}
	sb.WriteString(fmt.Sprintf(`
		SELECT workflow_id, workflow_type, delay_id, delay_until, event_version, created_at, next_command,
			cron_expression, timezone
		FROM %s WHERE 1=1`, h.del))
	if workflowType != "" {
		args = append(args, workflowType)
		sb.WriteString(fmt.Sprintf(` AND workflow_type = $%d`, len(args)))
	}
	if workflowID != "" {
		args = append(args, workflowID)
		sb.WriteString(fmt.Sprintf(` AND workflow_id = $%d`, len(args)))
	}
	args = append(args, limit, offset)
	sb.WriteString(fmt.Sprintf(` ORDER BY delay_until ASC LIMIT $%d OFFSET $%d`, len(args)-1, len(args)))

	rows, err := h.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var out []delayResponse
	for rows.Next() {
		var wid, wtype, did string
		var until, created time.Time
		var evVer int64
		var nextCmd []byte
		var cronExpr, tz pgtype.Text
		if err := rows.Scan(&wid, &wtype, &did, &until, &evVer, &created, &nextCmd, &cronExpr, &tz); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		nc := jsonObject(nextCmd)
		nextType := strings.TrimSpace(stringField(nc, "type"))
		var cronPtr, tzPtr *string
		var delayType string
		cronStr := ""
		if cronExpr.Valid {
			cronStr = cronExpr.String
			cronPtr = &cronStr
		}
		tzName := "UTC"
		if tz.Valid && strings.TrimSpace(tz.String) != "" {
			tzName = tz.String
			tzCopy := tzName
			tzPtr = &tzCopy
		}
		if nextType != "" {
			delayType = nextType
		} else if cronStr != "" {
			delayType = "cron"
		} else {
			delayType = "delay"
		}
		didOut := did
		if strings.TrimSpace(didOut) == "" {
			didOut = fmt.Sprintf("%s:%d:%d", wid, evVer, until.Unix())
		}
		var fireTimes []time.Time
		if cronStr != "" {
			fireTimes = nextCronFireTimes(cronStr, tzName)
		}
		out = append(out, delayResponse{
			WorkflowID:      wid,
			WorkflowType:    wtype,
			DelayID:         didOut,
			DelayUntil:      until,
			EventVersion:    evVer,
			CreatedAt:       created,
			NextCommand:     nc,
			DelayType:       delayType,
			NextCommandType: nextType,
			CronExpression:  cronPtr,
			CronTimezone:    tzPtr,
			NextFireTimes:   fireTimes,
		})
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func nextCronFireTimes(expr, tzName string) []time.Time {
	loc, err := time.LoadLocation(tzName)
	if err != nil {
		loc = time.UTC
	}
	var sched cron.Schedule
	parsed := false
	parsers := []cron.Parser{
		cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
	}
	for _, p := range parsers {
		s, err := p.Parse(expr)
		if err == nil {
			sched = s
			parsed = true
			break
		}
	}
	if !parsed {
		return nil
	}
	now := time.Now().In(loc)
	t := now
	var out []time.Time
	for i := 0; i < 5; i++ {
		t = sched.Next(t)
		out = append(out, t.In(loc))
	}
	return out
}

type statsResponse struct {
	TotalWorkflows     int64            `json:"total_workflows"`
	WorkflowsByType    map[string]int64 `json:"workflows_by_type"`
	WorkflowsByState   map[string]int64 `json:"workflows_by_state"`
	TotalEvents        int64            `json:"total_events"`
	EventsByType       map[string]int64 `json:"events_by_type"`
	TotalActivities    int64            `json:"total_activities"`
	ActivitiesByStatus map[string]int64 `json:"activities_by_status"`
	PendingActivities  int64            `json:"pending_activities"`
	FailedActivities   int64            `json:"failed_activities"`
	TotalDelays        int64            `json:"total_delays"`
	ActiveDelays       int64            `json:"active_delays"`
}

func (h *handler) getStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var out statsResponse
	out.WorkflowsByType = make(map[string]int64)
	out.WorkflowsByState = make(map[string]int64)
	out.EventsByType = make(map[string]int64)
	out.ActivitiesByStatus = make(map[string]int64)

	q := fmt.Sprintf(`SELECT COUNT(DISTINCT workflow_id) FROM %s`, h.ev)
	_ = h.pool.QueryRow(ctx, q).Scan(&out.TotalWorkflows)

	rows, err := h.pool.Query(ctx, fmt.Sprintf(
		`SELECT workflow_type, COUNT(DISTINCT workflow_id) FROM %s GROUP BY workflow_type`, h.ev))
	if err == nil {
		for rows.Next() {
			var wt string
			var c int64
			if rows.Scan(&wt, &c) == nil {
				out.WorkflowsByType[wt] = c
			}
		}
		rows.Close()
	}

	_ = h.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(global_id) FROM %s`, h.ev)).Scan(&out.TotalEvents)

	rows2, err := h.pool.Query(ctx, fmt.Sprintf(
		`SELECT event_type, COUNT(global_id) FROM %s GROUP BY event_type`, h.ev))
	if err == nil {
		for rows2.Next() {
			var et string
			var c int64
			if rows2.Scan(&et, &c) == nil {
				out.EventsByType[et] = c
			}
		}
		rows2.Close()
	}

	_ = h.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, h.act)).Scan(&out.TotalActivities)

	rows3, err := h.pool.Query(ctx, fmt.Sprintf(
		`SELECT status, COUNT(*) FROM %s GROUP BY status`, h.act))
	if err == nil {
		for rows3.Next() {
			var st string
			var c int64
			if rows3.Scan(&st, &c) == nil {
				out.ActivitiesByStatus[st] = c
			}
		}
		rows3.Close()
	}

	_ = h.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE status = 'pending'`, h.act)).Scan(&out.PendingActivities)
	_ = h.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE status = 'failed'`, h.act)).Scan(&out.FailedActivities)

	_ = h.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, h.del)).Scan(&out.TotalDelays)
	_ = h.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE delay_until > NOW()`, h.del)).Scan(&out.ActiveDelays)

	writeJSON(w, http.StatusOK, out)
}

func (h *handler) batchCancel(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if !nonEmptyWorkflowIDs(body["workflow_ids"]) {
		writeError(w, http.StatusBadRequest, "workflow_ids required")
		return
	}
	writeError(w, http.StatusNotImplemented,
		"Batch cancel requires Fleuve gateway. Use POST /commands/{workflow_type}/{workflow_id}/cancel per workflow.")
}

func (h *handler) batchReplay(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if !nonEmptyWorkflowIDs(body["workflow_ids"]) {
		writeError(w, http.StatusBadRequest, "workflow_ids required")
		return
	}
	writeError(w, http.StatusNotImplemented, "Batch replay requires Fleuve repo integration. Not yet implemented.")
}

func nonEmptyWorkflowIDs(v any) bool {
	switch x := v.(type) {
	case []any:
		return len(x) > 0
	case []string:
		return len(x) > 0
	default:
		return false
	}
}
