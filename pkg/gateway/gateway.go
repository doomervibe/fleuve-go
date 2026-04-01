package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/doomervibe/fleuve-go/pkg/actions"
	"github.com/doomervibe/fleuve-go/pkg/model"
)

// CommandParser is a function that parses a command from a JSON payload.
type CommandParser func(cmdType string, payload map[string]any) (model.Command, error)

// Repository defines the workflow repository operations needed by the gateway.
type Repository interface {
	ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection)
	CreateNew(ctx context.Context, cmd model.Command, id string, tags []string) (*model.StoredState, error)
	PauseWorkflow(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection)
	ResumeWorkflow(ctx context.Context, id string) (*model.StoredState, *model.Rejection)
	CancelWorkflow(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection)
}

// Gateway provides an HTTP API for workflow commands.
type Gateway struct {
	repos           map[string]Repository
	parsers         map[string]CommandParser
	workflows       map[string]model.Workflow
	actionExecutors map[string]*actions.ActionExecutor
}

// NewGateway creates a new Gateway instance.
func NewGateway() *Gateway {
	return &Gateway{
		repos:           make(map[string]Repository),
		parsers:         make(map[string]CommandParser),
		workflows:       make(map[string]model.Workflow),
		actionExecutors: make(map[string]*actions.ActionExecutor),
	}
}

// RegisterWorkflowType registers a workflow type with its repository, command parser,
// workflow definition, and optional action executor.
func (g *Gateway) RegisterWorkflowType(
	name string,
	repo Repository,
	parser CommandParser,
	workflow model.Workflow,
	actionExecutor *actions.ActionExecutor,
) {
	g.repos[name] = repo
	g.parsers[name] = parser
	g.workflows[name] = workflow
	if actionExecutor != nil {
		g.actionExecutors[name] = actionExecutor
	}
}

// commandRequest represents the JSON request body for command endpoints.
type commandRequest struct {
	CommandType string         `json:"command_type"`
	Payload     map[string]any `json:"payload"`
	WorkflowID  string         `json:"workflow_id,omitempty"`
}

// lifecycleRequest represents the JSON request body for lifecycle endpoints.
type lifecycleRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ServeHTTP implements http.Handler for the Gateway.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/")
	path = strings.TrimSuffix(path, "/")
	parts := strings.Split(path, "/")

	// All routes start with "commands"
	if len(parts) < 2 || parts[0] != "commands" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	workflowType := parts[1]

	// Check if workflow type is registered
	if _, ok := g.repos[workflowType]; !ok {
		http.Error(w, "unknown workflow type", http.StatusNotFound)
		return
	}

	switch {
	case len(parts) == 2:
		// POST /commands/{workflow_type} - Create new workflow
		g.handleCreate(w, r, workflowType)

	case len(parts) == 3:
		// POST /commands/{workflow_type}/{workflow_id} - Process command
		g.handleCommand(w, r, workflowType, parts[2])

	case len(parts) == 4:
		switch parts[3] {
		case "pause":
			// POST /commands/{workflow_type}/{workflow_id}/pause
			g.handlePause(w, r, workflowType, parts[2])
		case "resume":
			// POST /commands/{workflow_type}/{workflow_id}/resume
			g.handleResume(w, r, workflowType, parts[2])
		case "cancel":
			// POST /commands/{workflow_type}/{workflow_id}/cancel
			g.handleCancel(w, r, workflowType, parts[2])
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}

	case len(parts) == 5 && parts[3] == "retry":
		// POST /commands/{workflow_type}/{workflow_id}/retry/{event_number}
		g.handleRetry(w, r, workflowType, parts[2], parts[4])

	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// handleCreate handles POST /commands/{workflow_type}
func (g *Gateway) handleCreate(w http.ResponseWriter, r *http.Request, workflowType string) {
	parser, ok := g.parsers[workflowType]
	if !ok {
		http.Error(w, "no command parser registered", http.StatusNotImplemented)
		return
	}

	var req commandRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.CommandType == "" {
		http.Error(w, "command_type is required", http.StatusBadRequest)
		return
	}

	cmd, err := parser(req.CommandType, req.Payload)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse command: %v", err), http.StatusBadRequest)
		return
	}

	repo := g.repos[workflowType]
	state, err := repo.CreateNew(r.Context(), cmd, req.WorkflowID, nil)
	if err != nil {
		g.writeError(w, err)
		return
	}

	g.writeSuccess(w, state)
}

// handleCommand handles POST /commands/{workflow_type}/{workflow_id}
func (g *Gateway) handleCommand(w http.ResponseWriter, r *http.Request, workflowType, workflowID string) {
	parser, ok := g.parsers[workflowType]
	if !ok {
		http.Error(w, "no command parser registered", http.StatusNotImplemented)
		return
	}

	var req commandRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.CommandType == "" {
		http.Error(w, "command_type is required", http.StatusBadRequest)
		return
	}

	cmd, err := parser(req.CommandType, req.Payload)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse command: %v", err), http.StatusBadRequest)
		return
	}

	repo := g.repos[workflowType]
	state, events, rejection := repo.ProcessCommand(r.Context(), workflowID, cmd)
	if rejection != nil {
		http.Error(w, rejection.Msg, http.StatusBadRequest)
		return
	}

	g.writeSuccessWithEvents(w, state, events)
}

// handlePause handles POST /commands/{workflow_type}/{workflow_id}/pause
func (g *Gateway) handlePause(w http.ResponseWriter, r *http.Request, workflowType, workflowID string) {
	var req lifecycleRequest
	// Body is optional for pause
	_ = json.NewDecoder(r.Body).Decode(&req)

	repo := g.repos[workflowType]
	state, rejection := repo.PauseWorkflow(r.Context(), workflowID, req.Reason)
	if rejection != nil {
		http.Error(w, rejection.Msg, http.StatusBadRequest)
		return
	}

	g.writeSuccess(w, state)
}

// handleResume handles POST /commands/{workflow_type}/{workflow_id}/resume
func (g *Gateway) handleResume(w http.ResponseWriter, r *http.Request, workflowType, workflowID string) {
	repo := g.repos[workflowType]
	state, rejection := repo.ResumeWorkflow(r.Context(), workflowID)
	if rejection != nil {
		http.Error(w, rejection.Msg, http.StatusBadRequest)
		return
	}

	g.writeSuccess(w, state)
}

// handleCancel handles POST /commands/{workflow_type}/{workflow_id}/cancel
func (g *Gateway) handleCancel(w http.ResponseWriter, r *http.Request, workflowType, workflowID string) {
	var req lifecycleRequest
	// Body is optional for cancel
	_ = json.NewDecoder(r.Body).Decode(&req)

	repo := g.repos[workflowType]
	state, rejection := repo.CancelWorkflow(r.Context(), workflowID, req.Reason)
	if rejection != nil {
		http.Error(w, rejection.Msg, http.StatusBadRequest)
		return
	}

	g.writeSuccess(w, state)
}

// handleRetry handles POST /commands/{workflow_type}/{workflow_id}/retry/{event_number}
func (g *Gateway) handleRetry(w http.ResponseWriter, r *http.Request, workflowType, workflowID, eventNumberStr string) {
	executor, ok := g.actionExecutors[workflowType]
	if !ok {
		http.Error(w, "no action executor registered", http.StatusNotImplemented)
		return
	}

	eventNumber, err := strconv.Atoi(eventNumberStr)
	if err != nil {
		http.Error(w, "invalid event number", http.StatusBadRequest)
		return
	}

	// Create a ConsumedEvent for the retry
	// The actual event data will be loaded by the executor from the activity record
	consumedEvent := &model.ConsumedEvent{
		WorkflowID:   workflowID,
		WorkflowType: workflowType,
		EventNo:      int64(eventNumber),
	}

	if err := executor.ExecuteAction(consumedEvent); err != nil {
		if errors.Is(err, actions.ErrAlreadyRunning) {
			http.Error(w, "action is already running", http.StatusConflict)
			return
		}
		if errors.Is(err, actions.ErrActionCompleted) {
			http.Error(w, "action already completed", http.StatusBadRequest)
			return
		}
		http.Error(w, fmt.Sprintf("failed to retry action: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"status":       "retry_initiated",
		"workflow_id":  workflowID,
		"event_number": eventNumber,
	})
}

// writeError writes an appropriate error response based on the error type.
func (g *Gateway) writeError(w http.ResponseWriter, err error) {
	var alreadyExists *model.AlreadyExists
	if errors.As(err, &alreadyExists) {
		http.Error(w, alreadyExists.Error(), http.StatusBadRequest)
		return
	}

	var rejection *model.Rejection
	if errors.As(err, &rejection) {
		http.Error(w, rejection.Msg, http.StatusBadRequest)
		return
	}

	http.Error(w, fmt.Sprintf("internal error: %v", err), http.StatusInternalServerError)
}

// writeSuccess writes a success response with the stored state.
func (g *Gateway) writeSuccess(w http.ResponseWriter, state *model.StoredState) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"workflow_id": state.ID,
		"version":     state.Version,
	})
}

// writeSuccessWithEvents writes a success response with the stored state and events.
func (g *Gateway) writeSuccessWithEvents(w http.ResponseWriter, state *model.StoredState, events []model.Event) {
	eventTypes := make([]string, len(events))
	for i, e := range events {
		eventTypes[i] = e.Type()
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"workflow_id": state.ID,
		"version":     state.Version,
		"events":      eventTypes,
	})
}
