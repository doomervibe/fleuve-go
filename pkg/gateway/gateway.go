package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/doomervibe/fleuve-go/pkg/actions"
	"github.com/doomervibe/fleuve-go/pkg/model"
)

type CommandParser func(cmdType string, payload map[string]any) (model.Command, error)

type Repository interface {
	CreateNew(ctx context.Context, cmd model.Command, id string, tags []string) (*model.StoredState, error)
	ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection)
	PauseWorkflow(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection)
	ResumeWorkflow(ctx context.Context, id string) (*model.StoredState, *model.Rejection)
	CancelWorkflow(ctx context.Context, id string, reason string, actionExecutor *actions.ActionExecutor) (*model.StoredState, *model.Rejection)
}

type FleuveCommandGateway struct {
	repos          map[string]Repository
	parsers        map[string]CommandParser
	workflows      map[string]model.Workflow
	actionExecutor *actions.ActionExecutor
}

func NewFleuveCommandGateway() *FleuveCommandGateway {
	return &FleuveCommandGateway{
		repos:     make(map[string]Repository),
		parsers:   make(map[string]CommandParser),
		workflows: make(map[string]model.Workflow),
	}
}

func (g *FleuveCommandGateway) RegisterWorkflowType(workflowType string, repo Repository, parser CommandParser) {
	g.repos[workflowType] = repo
	g.parsers[workflowType] = parser
}

// RegisterWorkflowModel registers the workflow implementation for a type (required for failed-action retry).
func (g *FleuveCommandGateway) RegisterWorkflowModel(workflowType string, wf model.Workflow) {
	g.workflows[workflowType] = wf
}

func (g *FleuveCommandGateway) SetActionExecutor(executor *actions.ActionExecutor) {
	g.actionExecutor = executor
}

func (g *FleuveCommandGateway) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /commands/{workflow_type}", g.createWorkflow)
	mux.HandleFunc("POST /commands/{workflow_type}/{workflow_id}", g.processCommand)
	mux.HandleFunc("POST /commands/{workflow_type}/{workflow_id}/pause", g.pauseWorkflow)
	mux.HandleFunc("POST /commands/{workflow_type}/{workflow_id}/resume", g.resumeWorkflow)
	mux.HandleFunc("POST /commands/{workflow_type}/{workflow_id}/cancel", g.cancelWorkflow)
	mux.HandleFunc("POST /commands/{workflow_type}/{workflow_id}/retry/{event_number}", g.retryFailedAction)
}

type CreateWorkflowRequest struct {
	WorkflowID  string         `json:"workflow_id"`
	CommandType string         `json:"command_type"`
	Payload     map[string]any `json:"payload"`
}

type ProcessCommandRequest struct {
	CommandType string         `json:"command_type"`
	Payload     map[string]any `json:"payload"`
}

type CommandResponse struct {
	Status      string `json:"status"`
	WorkflowID  string `json:"workflow_id,omitempty"`
	Version     int64  `json:"version,omitempty"`
	EventsCount int    `json:"events_count,omitempty"`
	Message     string `json:"message,omitempty"`
}

func (g *FleuveCommandGateway) createWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflow_type")
	repo, ok := g.repos[workflowType]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown workflow type: "+workflowType)
		return
	}

	parser, ok := g.parsers[workflowType]
	if !ok {
		writeError(w, http.StatusNotImplemented, "no command parser for workflow type: "+workflowType)
		return
	}

	var req CreateWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.WorkflowID == "" {
		writeError(w, http.StatusBadRequest, "workflow_id is required")
		return
	}

	cmd, err := parser(req.CommandType, req.Payload)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	state, err := repo.CreateNew(r.Context(), cmd, req.WorkflowID, nil)
	if err != nil {
		var exists *model.AlreadyExists
		if errors.As(err, &exists) {
			writeError(w, http.StatusConflict, exists.Msg)
			return
		}
		var rej *model.Rejection
		if errors.As(err, &rej) {
			writeError(w, http.StatusBadRequest, rej.Msg)
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, CommandResponse{
		Status:     "ok",
		WorkflowID: state.ID,
		Version:    state.Version,
	})
}

func (g *FleuveCommandGateway) processCommand(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflow_type")
	workflowID := r.PathValue("workflow_id")

	repo, ok := g.repos[workflowType]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown workflow type: "+workflowType)
		return
	}

	parser, ok := g.parsers[workflowType]
	if !ok {
		writeError(w, http.StatusNotImplemented, "no command parser for workflow type: "+workflowType)
		return
	}

	var req ProcessCommandRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	cmd, err := parser(req.CommandType, req.Payload)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	state, events, rejection := repo.ProcessCommand(r.Context(), workflowID, cmd)
	if rejection != nil {
		writeError(w, http.StatusBadRequest, rejection.Msg)
		return
	}

	writeJSON(w, http.StatusOK, CommandResponse{
		Status:      "ok",
		WorkflowID:  state.ID,
		Version:     state.Version,
		EventsCount: len(events),
	})
}

func (g *FleuveCommandGateway) pauseWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflow_type")
	workflowID := r.PathValue("workflow_id")
	reason := r.URL.Query().Get("reason")

	repo, ok := g.repos[workflowType]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown workflow type: "+workflowType)
		return
	}

	_, rejection := repo.PauseWorkflow(r.Context(), workflowID, reason)
	if rejection != nil {
		writeError(w, http.StatusBadRequest, rejection.Msg)
		return
	}

	writeJSON(w, http.StatusOK, CommandResponse{
		Status:  "ok",
		Message: "Workflow paused",
	})
}

func (g *FleuveCommandGateway) resumeWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflow_type")
	workflowID := r.PathValue("workflow_id")

	repo, ok := g.repos[workflowType]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown workflow type: "+workflowType)
		return
	}

	_, rejection := repo.ResumeWorkflow(r.Context(), workflowID)
	if rejection != nil {
		writeError(w, http.StatusBadRequest, rejection.Msg)
		return
	}

	writeJSON(w, http.StatusOK, CommandResponse{
		Status:  "ok",
		Message: "Workflow resumed",
	})
}

func (g *FleuveCommandGateway) cancelWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflow_type")
	workflowID := r.PathValue("workflow_id")
	reason := r.URL.Query().Get("reason")

	repo, ok := g.repos[workflowType]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown workflow type: "+workflowType)
		return
	}

	_, rejection := repo.CancelWorkflow(r.Context(), workflowID, reason, g.actionExecutor)
	if rejection != nil {
		writeError(w, http.StatusBadRequest, rejection.Msg)
		return
	}

	writeJSON(w, http.StatusOK, CommandResponse{
		Status:  "ok",
		Message: "Workflow cancelled",
	})
}

func (g *FleuveCommandGateway) retryFailedAction(w http.ResponseWriter, r *http.Request) {
	if g.actionExecutor == nil {
		writeError(w, http.StatusNotImplemented, "retry not available: action_executor not configured")
		return
	}

	workflowType := r.PathValue("workflow_type")
	wf, ok := g.workflows[workflowType]
	if !ok || wf == nil {
		writeError(w, http.StatusBadRequest, "unknown workflow type for retry: "+workflowType)
		return
	}

	workflowID := r.PathValue("workflow_id")
	eventNumberStr := r.PathValue("event_number")
	eventNumber, err := strconv.ParseInt(eventNumberStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid event_number")
		return
	}

	if err := g.actionExecutor.RequeueFailedAction(r.Context(), workflowType, workflowID, eventNumber, wf); err != nil {
		switch {
		case errors.Is(err, actions.ErrActivityNotFound):
			writeError(w, http.StatusNotFound, err.Error())
		case errors.Is(err, actions.ErrActivityNotFailed):
			writeError(w, http.StatusConflict, err.Error())
		default:
			writeError(w, http.StatusBadRequest, fmt.Sprintf("retry failed: %v", err))
		}
		return
	}

	writeJSON(w, http.StatusOK, CommandResponse{
		Status:  "ok",
		Message: "Action re-queued for retry",
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}
