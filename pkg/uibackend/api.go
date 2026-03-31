package uibackend

import (
	"context"
	"encoding/json"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/delay"
	"github.com/doomervibe/fleuve-go/pkg/uiembed"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type SessionMaker interface {
	NewSession(ctx context.Context) (Session, error)
}

type Session interface {
	Query(ctx context.Context, query string, args ...interface{}) ([]Row, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) (Row, error)
	Exec(ctx context.Context, query string, args ...interface{}) error
	Close() error
}

type Row interface {
	Scan(dest ...interface{}) error
}

type FleuveUIBackend struct {
	sessionMaker     SessionMaker
	frontendDistPath string
	frontendFS       fs.FS
	disableBundledUI bool
	uiTitle          string
}

func NewFleuveUIBackend(
	sessionMaker SessionMaker,
	frontendDistPath string,
	opts ...Option,
) *FleuveUIBackend {
	uiTitle := os.Getenv("FLEUVE_UI_TITLE")
	if uiTitle == "" {
		cwd, _ := os.Getwd()
		uiTitle = cases.Title(language.English).String(strings.ReplaceAll(filepath.Base(cwd), "_", " "))
	}

	b := &FleuveUIBackend{
		sessionMaker:     sessionMaker,
		frontendDistPath: frontendDistPath,
		uiTitle:          uiTitle,
	}
	for _, o := range opts {
		o(b)
	}
	if frontendDistPath == "" && !b.disableBundledUI {
		b.frontendFS = uiembed.Dist
	}
	return b
}

// ServesStaticUI is true when index.html is served from disk or the embedded bundle.
func (b *FleuveUIBackend) ServesStaticUI() bool {
	return b.frontendDistPath != "" || b.frontendFS != nil
}

func (b *FleuveUIBackend) normalizeFrontendRel(rel string) string {
	rel = path.Clean("/" + rel)
	return strings.TrimPrefix(rel, "/")
}

// resolveDiskFrontendPath returns an absolute path under frontendDistPath, or fs.ErrNotExist if rel escapes the root.
func (b *FleuveUIBackend) resolveDiskFrontendPath(rel string) (string, error) {
	root, err := filepath.Abs(b.frontendDistPath)
	if err != nil {
		return "", err
	}
	full := filepath.Join(root, rel)
	absJoined, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	sep := string(os.PathSeparator)
	if absJoined != root && !strings.HasPrefix(absJoined, root+sep) {
		return "", fs.ErrNotExist
	}
	return absJoined, nil
}

func (b *FleuveUIBackend) readFrontend(rel string) ([]byte, error) {
	rel = b.normalizeFrontendRel(rel)
	if b.frontendDistPath != "" {
		p, err := b.resolveDiskFrontendPath(rel)
		if err != nil {
			return nil, err
		}
		return os.ReadFile(p) // #nosec G304 -- path confined by resolveDiskFrontendPath
	}
	if b.frontendFS != nil {
		return fs.ReadFile(b.frontendFS, rel)
	}
	return nil, os.ErrNotExist
}

func (b *FleuveUIBackend) frontendFileExists(rel string) bool {
	rel = b.normalizeFrontendRel(rel)
	if b.frontendDistPath != "" {
		p, err := b.resolveDiskFrontendPath(rel)
		if err != nil {
			return false
		}
		_, err = os.Stat(p) // #nosec G304 -- path confined by resolveDiskFrontendPath
		return err == nil
	}
	if b.frontendFS != nil {
		_, err := fs.Stat(b.frontendFS, rel)
		return err == nil
	}
	return false
}

func (b *FleuveUIBackend) writeFrontendResponse(w http.ResponseWriter, r *http.Request, rel string) {
	data, err := b.readFrontend(rel)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	ext := filepath.Ext(rel)
	ct := mime.TypeByExtension(ext)
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data) // #nosec G705 -- trusted static UI assets from dist or embed
}

func (b *FleuveUIBackend) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", b.health)
	// /{$} is exact root only; avoids conflicting with GET /{full_path...} (Go 1.22+ ServeMux).
	mux.HandleFunc("GET /{$}", b.root)
	mux.HandleFunc("GET /api/workflow-types", b.getWorkflowTypes)
	mux.HandleFunc("GET /api/workflows", b.listWorkflows)
	mux.HandleFunc("GET /api/workflows/{workflow_id}", b.getWorkflow)
	mux.HandleFunc("GET /api/workflows/{workflow_id}/events", b.getWorkflowEvents)
	mux.HandleFunc("GET /api/workflows/{workflow_id}/state/{version}", b.getWorkflowStateAtVersion)
	mux.HandleFunc("GET /api/workflows/{workflow_id}/state-diff/{v1}/{v2}", b.getWorkflowStateDiff)
	mux.HandleFunc("GET /api/workflows/{workflow_id}/activities", b.getWorkflowActivities)
	mux.HandleFunc("GET /api/workflows/{workflow_id}/delays", b.getWorkflowDelays)
	mux.HandleFunc("GET /api/events", b.listEvents)
	mux.HandleFunc("GET /api/events/{event_id}", b.getEvent)
	mux.HandleFunc("GET /api/activities", b.listActivities)
	mux.HandleFunc("GET /api/delays", b.listDelays)
	mux.HandleFunc("GET /api/stats", b.getStats)
	mux.HandleFunc("POST /api/workflows/batch/cancel", b.batchCancel)
	mux.HandleFunc("POST /api/workflows/batch/replay", b.batchReplay)
	mux.HandleFunc("GET /assets/", b.serveAssets)
	mux.HandleFunc("GET /{full_path...}", b.serveReactApp)
}

func (b *FleuveUIBackend) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (b *FleuveUIBackend) root(w http.ResponseWriter, r *http.Request) {
	if b.ServesStaticUI() && b.frontendFileExists("index.html") {
		b.serveIndexHTML(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "ok",
		"message": "Fleuve Framework UI API",
		"web_app": "not_built",
	})
}

func (b *FleuveUIBackend) serveIndexHTML(w http.ResponseWriter, r *http.Request) {
	content, err := b.readFrontend("index.html")
	if err != nil {
		http.Error(w, "index.html not found", http.StatusNotFound)
		return
	}
	resolvedTitle := b.uiTitle
	if resolvedTitle == "" {
		resolvedTitle = "Fleuve"
	}
	html := strings.ReplaceAll(string(content), "{{project_title}}", resolvedTitle)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(html)) // #nosec G705 -- trusted index.html from dist or embed
}

func (b *FleuveUIBackend) serveAssets(w http.ResponseWriter, r *http.Request) {
	if !b.ServesStaticUI() {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	rel := strings.TrimPrefix(r.URL.Path, "/")
	if !b.frontendFileExists(rel) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	b.writeFrontendResponse(w, r, rel)
}

func (b *FleuveUIBackend) serveReactApp(w http.ResponseWriter, r *http.Request) {
	if !b.ServesStaticUI() {
		http.Error(w, "web app not built", http.StatusNotFound)
		return
	}

	fullPath := r.PathValue("full_path")
	if strings.HasPrefix(fullPath, "api/") || fullPath == "health" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	rel := filepath.ToSlash(strings.TrimPrefix(fullPath, "/"))
	if rel != "" && b.frontendFileExists(rel) {
		b.writeFrontendResponse(w, r, rel)
		return
	}

	b.serveIndexHTML(w, r)
}

type WorkflowTypeInfo struct {
	WorkflowType  string     `json:"workflow_type"`
	WorkflowCount int        `json:"workflow_count"`
	EventCount    int        `json:"event_count"`
	LastEventAt   *time.Time `json:"last_event_at,omitempty"`
}

func (b *FleuveUIBackend) getWorkflowTypes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	rows, err := sess.Query(ctx, `
		SELECT workflow_type, COUNT(DISTINCT workflow_id) as workflow_count, 
		       COUNT(*) as event_count, MAX(at) as last_event_at
		FROM stored_events
		GROUP BY workflow_type
	`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	types := make([]WorkflowTypeInfo, 0)
	for _, row := range rows {
		var ti WorkflowTypeInfo
		var lastEventAt *time.Time
		if err := row.Scan(&ti.WorkflowType, &ti.WorkflowCount, &ti.EventCount, &lastEventAt); err != nil {
			continue
		}
		ti.LastEventAt = lastEventAt
		types = append(types, ti)
	}

	writeJSON(w, http.StatusOK, types)
}

type WorkflowSummary struct {
	WorkflowID   string                 `json:"workflow_id"`
	WorkflowType string                 `json:"workflow_type"`
	Version      int64                  `json:"version"`
	State        map[string]interface{} `json:"state"`
	CreatedAt    *time.Time             `json:"created_at,omitempty"`
	UpdatedAt    *time.Time             `json:"updated_at,omitempty"`
	IsCompleted  bool                   `json:"is_completed"`
}

func (b *FleuveUIBackend) listWorkflows(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	workflowType := r.URL.Query().Get("workflow_type")
	search := r.URL.Query().Get("search")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}

	// DISTINCT ON avoids DISTINCT + window functions (fragile) and matches “one row per workflow”.
	query := `
		SELECT sub.workflow_id, sub.workflow_type, sub.version, sub.updated_at, sub.created_at, sub.state
		FROM (
			SELECT DISTINCT ON (se.workflow_id)
				se.workflow_id,
				se.workflow_type,
				se.workflow_version AS version,
				se.at AS updated_at,
				(SELECT MIN(e.at) FROM stored_events e WHERE e.workflow_id = se.workflow_id) AS created_at,
				se.body AS state
			FROM stored_events se
			WHERE 1=1
	`
	args := []interface{}{}
	argIdx := 1

	if workflowType != "" {
		query += " AND se.workflow_type = $" + strconv.Itoa(argIdx)
		args = append(args, workflowType)
		argIdx++
	}
	if search != "" {
		query += " AND se.workflow_id LIKE $" + strconv.Itoa(argIdx)
		args = append(args, "%"+search+"%")
		argIdx++
	}

	query += `
			ORDER BY se.workflow_id, se.workflow_version DESC, se.at DESC
		) sub
		ORDER BY sub.updated_at DESC NULLS LAST, sub.workflow_id ASC
		LIMIT $` + strconv.Itoa(argIdx) + ` OFFSET $` + strconv.Itoa(argIdx+1)
	args = append(args, limit, offset)

	rows, err := sess.Query(ctx, query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	workflows := make([]WorkflowSummary, 0)
	for _, row := range rows {
		var ws WorkflowSummary
		var stateBytes []byte
		if err := row.Scan(&ws.WorkflowID, &ws.WorkflowType, &ws.Version, &ws.UpdatedAt, &ws.CreatedAt, &stateBytes); err != nil {
			continue
		}
		if len(stateBytes) > 0 {
			if err := json.Unmarshal(stateBytes, &ws.State); err != nil {
				ws.State = nil
			}
		}
		if ws.State == nil {
			ws.State = map[string]interface{}{}
		}
		workflows = append(workflows, ws)
	}

	writeJSON(w, http.StatusOK, workflows)
}

type WorkflowDetail struct {
	WorkflowID    string                 `json:"workflow_id"`
	WorkflowType  string                 `json:"workflow_type"`
	Version       int64                  `json:"version"`
	State         map[string]interface{} `json:"state"`
	CreatedAt     *time.Time             `json:"created_at,omitempty"`
	UpdatedAt     *time.Time             `json:"updated_at,omitempty"`
	IsCompleted   bool                   `json:"is_completed"`
	Subscriptions []map[string]string    `json:"subscriptions"`
}

func (b *FleuveUIBackend) getWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflow_id")
	ctx := r.Context()

	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	row, err := sess.QueryRow(ctx, `
		SELECT workflow_id, workflow_type, workflow_version, at, body
		FROM stored_events
		WHERE workflow_id = $1
		ORDER BY workflow_version DESC
		LIMIT 1
	`, workflowID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var wd WorkflowDetail
	var stateBytes []byte
	var updatedAt time.Time
	if err := row.Scan(&wd.WorkflowID, &wd.WorkflowType, &wd.Version, &updatedAt, &stateBytes); err != nil {
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	wd.UpdatedAt = &updatedAt
	if len(stateBytes) > 0 {
		if err := json.Unmarshal(stateBytes, &wd.State); err != nil {
			wd.State = nil
		}
	}
	if wd.State == nil {
		wd.State = map[string]interface{}{}
	}
	wd.Subscriptions = []map[string]string{}

	writeJSON(w, http.StatusOK, wd)
}

type EventResponse struct {
	GlobalID        int64                  `json:"global_id"`
	WorkflowID      string                 `json:"workflow_id"`
	WorkflowType    string                 `json:"workflow_type"`
	WorkflowVersion int64                  `json:"workflow_version"`
	EventType       string                 `json:"event_type"`
	Body            map[string]interface{} `json:"body"`
	At              time.Time              `json:"at"`
	Metadata        map[string]interface{} `json:"metadata"`
}

func (b *FleuveUIBackend) getWorkflowEvents(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflow_id")
	ctx := r.Context()

	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	rows, err := sess.Query(ctx, `
		SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata
		FROM stored_events
		WHERE workflow_id = $1
		ORDER BY workflow_version ASC
	`, workflowID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	events := make([]EventResponse, 0)
	for _, row := range rows {
		var er EventResponse
		var bodyBytes, metaBytes []byte
		if err := row.Scan(&er.GlobalID, &er.WorkflowID, &er.WorkflowType, &er.WorkflowVersion, &er.EventType, &bodyBytes, &er.At, &metaBytes); err != nil {
			continue
		}
		if len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, &er.Body); err != nil {
				er.Body = nil
			}
		}
		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &er.Metadata); err != nil {
				er.Metadata = nil
			}
		}
		if er.Body == nil {
			er.Body = map[string]interface{}{}
		}
		if er.Metadata == nil {
			er.Metadata = map[string]interface{}{}
		}
		events = append(events, er)
	}

	writeJSON(w, http.StatusOK, events)
}

func (b *FleuveUIBackend) getWorkflowStateAtVersion(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflow_id")
	versionStr := r.PathValue("version")
	version, _ := strconv.ParseInt(versionStr, 10, 64)
	ctx := r.Context()

	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	rows, err := sess.Query(ctx, `
		SELECT workflow_version, event_type, body, at
		FROM stored_events
		WHERE workflow_id = $1 AND workflow_version <= $2
		ORDER BY workflow_version ASC
	`, workflowID, version)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	type eventInfo struct {
		Version int64                  `json:"version"`
		Type    string                 `json:"type"`
		Body    map[string]interface{} `json:"body"`
		At      string                 `json:"at"`
	}

	events := make([]eventInfo, 0)
	for _, row := range rows {
		var ei eventInfo
		var bodyBytes []byte
		var at time.Time
		if err := row.Scan(&ei.Version, &ei.Type, &bodyBytes, &at); err != nil {
			continue
		}
		if len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, &ei.Body); err != nil {
				ei.Body = nil
			}
		}
		if ei.Body == nil {
			ei.Body = map[string]interface{}{}
		}
		ei.At = at.Format(time.RFC3339)
		events = append(events, ei)
	}

	if len(events) == 0 {
		writeError(w, http.StatusNotFound, "no events found for this version")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"workflow_id": workflowID,
		"version":     version,
		"events":      events,
		"note":        "State reconstruction requires workflow-specific code. Showing events instead.",
	})
}

func (b *FleuveUIBackend) getWorkflowStateDiff(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflow_id")
	v1Str := r.PathValue("v1")
	v2Str := r.PathValue("v2")
	v1, _ := strconv.ParseInt(v1Str, 10, 64)
	v2, _ := strconv.ParseInt(v2Str, 10, 64)
	ctx := r.Context()

	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	query := `
		SELECT workflow_version, event_type, body, at
		FROM stored_events
		WHERE workflow_id = $1 AND workflow_version <= $2
		ORDER BY workflow_version ASC
	`

	rows1, err := sess.Query(ctx, query, workflowID, v1)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	rows2, err := sess.Query(ctx, query, workflowID, v2)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	type eventInfo struct {
		Version int64                  `json:"version"`
		Type    string                 `json:"type"`
		Body    map[string]interface{} `json:"body"`
		At      string                 `json:"at"`
	}

	toEvents := func(rows []Row) []eventInfo {
		events := make([]eventInfo, 0)
		for _, row := range rows {
			var ei eventInfo
			var bodyBytes []byte
			var at time.Time
			if err := row.Scan(&ei.Version, &ei.Type, &bodyBytes, &at); err != nil {
				continue
			}
			if len(bodyBytes) > 0 {
				if err := json.Unmarshal(bodyBytes, &ei.Body); err != nil {
					ei.Body = nil
				}
			}
			if ei.Body == nil {
				ei.Body = map[string]interface{}{}
			}
			ei.At = at.Format(time.RFC3339)
			events = append(events, ei)
		}
		return events
	}

	events1 := toEvents(rows1)
	events2 := toEvents(rows2)

	if len(events1) == 0 || len(events2) == 0 {
		writeError(w, http.StatusNotFound, "no events found for one or both versions")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"workflow_id": workflowID,
		"version1":    v1,
		"version2":    v2,
		"state_v1": map[string]interface{}{
			"version": v1,
			"events":  events1,
		},
		"state_v2": map[string]interface{}{
			"version": v2,
			"events":  events2,
		},
	})
}

func (b *FleuveUIBackend) getWorkflowActivities(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflow_id")
	b.listActivitiesWithFilter(w, r, workflowID, "", "")
}

func (b *FleuveUIBackend) getWorkflowDelays(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflow_id")
	b.listDelaysWithFilter(w, r, "", workflowID)
}

func (b *FleuveUIBackend) listEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	workflowType := r.URL.Query().Get("workflow_type")
	workflowID := r.URL.Query().Get("workflow_id")
	eventType := r.URL.Query().Get("event_type")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata
		FROM stored_events
		WHERE 1=1
	`
	args := []interface{}{}
	argIdx := 1

	if workflowType != "" {
		query += " AND workflow_type = $" + strconv.Itoa(argIdx)
		args = append(args, workflowType)
		argIdx++
	}
	if workflowID != "" {
		query += " AND workflow_id = $" + strconv.Itoa(argIdx)
		args = append(args, workflowID)
		argIdx++
	}
	if eventType != "" {
		query += " AND event_type = $" + strconv.Itoa(argIdx)
		args = append(args, eventType)
		argIdx++
	}

	query += " ORDER BY global_id DESC LIMIT $" + strconv.Itoa(argIdx) + " OFFSET $" + strconv.Itoa(argIdx+1)
	args = append(args, limit, offset)

	rows, err := sess.Query(ctx, query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	events := make([]EventResponse, 0)
	for _, row := range rows {
		var er EventResponse
		var bodyBytes, metaBytes []byte
		if err := row.Scan(&er.GlobalID, &er.WorkflowID, &er.WorkflowType, &er.WorkflowVersion, &er.EventType, &bodyBytes, &er.At, &metaBytes); err != nil {
			continue
		}
		if len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, &er.Body); err != nil {
				er.Body = nil
			}
		}
		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &er.Metadata); err != nil {
				er.Metadata = nil
			}
		}
		if er.Body == nil {
			er.Body = map[string]interface{}{}
		}
		if er.Metadata == nil {
			er.Metadata = map[string]interface{}{}
		}
		events = append(events, er)
	}

	writeJSON(w, http.StatusOK, events)
}

func (b *FleuveUIBackend) getEvent(w http.ResponseWriter, r *http.Request) {
	eventIDStr := r.PathValue("event_id")
	eventID, _ := strconv.ParseInt(eventIDStr, 10, 64)
	ctx := r.Context()

	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	row, err := sess.QueryRow(ctx, `
		SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata
		FROM stored_events
		WHERE global_id = $1
	`, eventID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var er EventResponse
	var bodyBytes, metaBytes []byte
	if err := row.Scan(&er.GlobalID, &er.WorkflowID, &er.WorkflowType, &er.WorkflowVersion, &er.EventType, &bodyBytes, &er.At, &metaBytes); err != nil {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}
	if len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, &er.Body); err != nil {
			er.Body = nil
		}
	}
	if len(metaBytes) > 0 {
		if err := json.Unmarshal(metaBytes, &er.Metadata); err != nil {
			er.Metadata = nil
		}
	}
	if er.Body == nil {
		er.Body = map[string]interface{}{}
	}
	if er.Metadata == nil {
		er.Metadata = map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, er)
}

type ActivityResponse struct {
	WorkflowID    string                 `json:"workflow_id"`
	WorkflowType  string                 `json:"workflow_type"`
	EventNumber   int64                  `json:"event_number"`
	Status        string                 `json:"status"`
	StartedAt     time.Time              `json:"started_at"`
	FinishedAt    *time.Time             `json:"finished_at,omitempty"`
	LastAttemptAt *time.Time             `json:"last_attempt_at,omitempty"`
	RetryCount    int                    `json:"retry_count"`
	MaxRetries    int                    `json:"max_retries"`
	ErrorMessage  *string                `json:"error_message,omitempty"`
	ErrorType     *string                `json:"error_type,omitempty"`
	Checkpoint    map[string]interface{} `json:"checkpoint"`
	ActionType    string                 `json:"action_type"`
	RunnerID      *string                `json:"runner_id,omitempty"`
}

func (b *FleuveUIBackend) listActivities(w http.ResponseWriter, r *http.Request) {
	b.listActivitiesWithFilter(w, r, "", "", "")
}

func (b *FleuveUIBackend) listActivitiesWithFilter(w http.ResponseWriter, r *http.Request, workflowID, workflowType, status string) {
	ctx := r.Context()
	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	if workflowID == "" {
		workflowID = r.URL.Query().Get("workflow_id")
	}
	if workflowType == "" {
		workflowType = r.URL.Query().Get("workflow_type")
	}
	if status == "" {
		status = r.URL.Query().Get("status")
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT workflow_id, workflow_type, event_number, status, started_at, finished_at,
		       last_attempt_at, retry_count, max_retries, error_message, error_type, checkpoint, runner_id
		FROM activities
		WHERE 1=1
	`
	args := []interface{}{}
	argIdx := 1

	if workflowID != "" {
		query += " AND workflow_id = $" + strconv.Itoa(argIdx)
		args = append(args, workflowID)
		argIdx++
	}
	if workflowType != "" {
		query += " AND workflow_type = $" + strconv.Itoa(argIdx)
		args = append(args, workflowType)
		argIdx++
	}
	if status != "" {
		query += " AND status = $" + strconv.Itoa(argIdx)
		args = append(args, status)
		argIdx++
	}

	query += " ORDER BY started_at DESC LIMIT $" + strconv.Itoa(argIdx) + " OFFSET $" + strconv.Itoa(argIdx+1)
	args = append(args, limit, offset)

	rows, err := sess.Query(ctx, query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	activities := make([]ActivityResponse, 0)
	for _, row := range rows {
		var ar ActivityResponse
		var checkpointBytes []byte
		if err := row.Scan(&ar.WorkflowID, &ar.WorkflowType, &ar.EventNumber, &ar.Status, &ar.StartedAt, &ar.FinishedAt,
			&ar.LastAttemptAt, &ar.RetryCount, &ar.MaxRetries, &ar.ErrorMessage, &ar.ErrorType, &checkpointBytes, &ar.RunnerID); err != nil {
			continue
		}
		if len(checkpointBytes) > 0 {
			if err := json.Unmarshal(checkpointBytes, &ar.Checkpoint); err != nil {
				ar.Checkpoint = nil
			}
		}
		if ar.Checkpoint == nil {
			ar.Checkpoint = map[string]interface{}{}
		}
		activities = append(activities, ar)
	}

	writeJSON(w, http.StatusOK, activities)
}

type DelayResponse struct {
	WorkflowID      string                 `json:"workflow_id"`
	WorkflowType    string                 `json:"workflow_type"`
	DelayID         string                 `json:"delay_id"`
	DelayUntil      time.Time              `json:"delay_until"`
	EventVersion    int64                  `json:"event_version"`
	CreatedAt       time.Time              `json:"created_at"`
	NextCommand     map[string]interface{} `json:"next_command"`
	DelayType       string                 `json:"delay_type"`
	NextCommandType string                 `json:"next_command_type"`
	CronExpression  *string                `json:"cron_expression,omitempty"`
	CronTimezone    *string                `json:"cron_timezone,omitempty"`
	NextFireTimes   []time.Time            `json:"next_fire_times,omitempty"`
}

func (b *FleuveUIBackend) listDelays(w http.ResponseWriter, r *http.Request) {
	b.listDelaysWithFilter(w, r, "", "")
}

func (b *FleuveUIBackend) listDelaysWithFilter(w http.ResponseWriter, r *http.Request, workflowType, workflowID string) {
	ctx := r.Context()
	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	if workflowType == "" {
		workflowType = r.URL.Query().Get("workflow_type")
	}
	if workflowID == "" {
		workflowID = r.URL.Query().Get("workflow_id")
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT workflow_id, workflow_type, delay_id, delay_until, event_version, created_at,
		       next_command, cron_expression, timezone
		FROM delay_schedules
		WHERE 1=1
	`
	args := []interface{}{}
	argIdx := 1

	if workflowType != "" {
		query += " AND workflow_type = $" + strconv.Itoa(argIdx)
		args = append(args, workflowType)
		argIdx++
	}
	if workflowID != "" {
		query += " AND workflow_id = $" + strconv.Itoa(argIdx)
		args = append(args, workflowID)
		argIdx++
	}

	query += " ORDER BY delay_until ASC LIMIT $" + strconv.Itoa(argIdx) + " OFFSET $" + strconv.Itoa(argIdx+1)
	args = append(args, limit, offset)

	rows, err := sess.Query(ctx, query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	delays := make([]DelayResponse, 0)
	for _, row := range rows {
		var dr DelayResponse
		var nextCmdBytes []byte
		var cronExpr, tz *string
		if err := row.Scan(&dr.WorkflowID, &dr.WorkflowType, &dr.DelayID, &dr.DelayUntil, &dr.EventVersion,
			&dr.CreatedAt, &nextCmdBytes, &cronExpr, &tz); err != nil {
			continue
		}
		if len(nextCmdBytes) > 0 {
			if err := json.Unmarshal(nextCmdBytes, &dr.NextCommand); err != nil {
				dr.NextCommand = nil
			}
		}
		if dr.NextCommand == nil {
			dr.NextCommand = map[string]interface{}{}
		}
		dr.CronExpression = cronExpr
		dr.CronTimezone = tz

		if cronExpr != nil && *cronExpr != "" {
			dr.DelayType = "cron"
			dr.NextFireTimes = delay.NextCronFires(*cronExpr, "", 5)
		} else {
			dr.DelayType = "delay"
		}

		delays = append(delays, dr)
	}

	writeJSON(w, http.StatusOK, delays)
}

type StatsResponse struct {
	TotalWorkflows     int            `json:"total_workflows"`
	WorkflowsByType    map[string]int `json:"workflows_by_type"`
	WorkflowsByState   map[string]int `json:"workflows_by_state"`
	TotalEvents        int            `json:"total_events"`
	EventsByType       map[string]int `json:"events_by_type"`
	TotalActivities    int            `json:"total_activities"`
	ActivitiesByStatus map[string]int `json:"activities_by_status"`
	PendingActivities  int            `json:"pending_activities"`
	FailedActivities   int            `json:"failed_activities"`
	TotalDelays        int            `json:"total_delays"`
	ActiveDelays       int            `json:"active_delays"`
}

func (b *FleuveUIBackend) getStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sess, err := b.sessionMaker.NewSession(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer sess.Close()

	stats := StatsResponse{
		WorkflowsByType:    make(map[string]int),
		WorkflowsByState:   make(map[string]int),
		EventsByType:       make(map[string]int),
		ActivitiesByStatus: make(map[string]int),
	}

	if row, qerr := sess.QueryRow(ctx, `SELECT COUNT(DISTINCT workflow_id) FROM stored_events`); qerr == nil && row != nil {
		if err := row.Scan(&stats.TotalWorkflows); err != nil {
			stats.TotalWorkflows = 0
		}
	}
	if row, qerr := sess.QueryRow(ctx, `SELECT COUNT(*) FROM stored_events`); qerr == nil && row != nil {
		if err := row.Scan(&stats.TotalEvents); err != nil {
			stats.TotalEvents = 0
		}
	}
	if row, qerr := sess.QueryRow(ctx, `SELECT COUNT(*) FROM activities`); qerr == nil && row != nil {
		if err := row.Scan(&stats.TotalActivities); err != nil {
			stats.TotalActivities = 0
		}
	}
	if row, qerr := sess.QueryRow(ctx, `SELECT COUNT(*) FROM activities WHERE status = 'pending'`); qerr == nil && row != nil {
		if err := row.Scan(&stats.PendingActivities); err != nil {
			stats.PendingActivities = 0
		}
	}
	if row, qerr := sess.QueryRow(ctx, `SELECT COUNT(*) FROM activities WHERE status = 'failed'`); qerr == nil && row != nil {
		if err := row.Scan(&stats.FailedActivities); err != nil {
			stats.FailedActivities = 0
		}
	}
	if row, qerr := sess.QueryRow(ctx, `SELECT COUNT(*) FROM delay_schedules`); qerr == nil && row != nil {
		if err := row.Scan(&stats.TotalDelays); err != nil {
			stats.TotalDelays = 0
		}
	}
	if row, qerr := sess.QueryRow(ctx, `SELECT COUNT(*) FROM delay_schedules WHERE delay_until > NOW()`); qerr == nil && row != nil {
		if err := row.Scan(&stats.ActiveDelays); err != nil {
			stats.ActiveDelays = 0
		}
	}

	if rows, qerr := sess.Query(ctx, `SELECT workflow_type, COUNT(DISTINCT workflow_id) FROM stored_events GROUP BY workflow_type`); qerr == nil {
		for _, row := range rows {
			var wt string
			var count int
			if err := row.Scan(&wt, &count); err == nil {
				stats.WorkflowsByType[wt] = count
			}
		}
	}
	if rows, qerr := sess.Query(ctx, `SELECT event_type, COUNT(*) FROM stored_events GROUP BY event_type`); qerr == nil {
		for _, row := range rows {
			var et string
			var count int
			if err := row.Scan(&et, &count); err == nil {
				stats.EventsByType[et] = count
			}
		}
	}
	if rows, qerr := sess.Query(ctx, `SELECT status, COUNT(*) FROM activities GROUP BY status`); qerr == nil {
		for _, row := range rows {
			var st string
			var count int
			if err := row.Scan(&st, &count); err == nil {
				stats.ActivitiesByStatus[st] = count
			}
		}
	}

	writeJSON(w, http.StatusOK, stats)
}

func (b *FleuveUIBackend) batchCancel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowIDs []string `json:"workflow_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(body.WorkflowIDs) == 0 {
		writeError(w, http.StatusBadRequest, "workflow_ids required")
		return
	}
	writeError(w, http.StatusNotImplemented, "batch cancel requires Fleuve gateway. Use POST /commands/{workflow_type}/{workflow_id}/cancel per workflow.")
}

func (b *FleuveUIBackend) batchReplay(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowIDs []string `json:"workflow_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(body.WorkflowIDs) == 0 {
		writeError(w, http.StatusBadRequest, "workflow_ids required")
		return
	}
	writeError(w, http.StatusNotImplemented, "batch replay requires Fleuve repo integration. Not yet implemented.")
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}
