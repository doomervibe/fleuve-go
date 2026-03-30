package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fleuve/fleuve-go/pkg/actions"
	"github.com/fleuve/fleuve-go/pkg/model"
)

type stubRepo struct {
	onCreate  func(ctx context.Context, cmd model.Command, id string, tags []string) (*model.StoredState, error)
	onProcess func(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection)
	onPause   func(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection)
	onResume  func(ctx context.Context, id string) (*model.StoredState, *model.Rejection)
	onCancel  func(ctx context.Context, id string, reason string, ae *actions.ActionExecutor) (*model.StoredState, *model.Rejection)
}

func (s *stubRepo) CreateNew(ctx context.Context, cmd model.Command, id string, tags []string) (*model.StoredState, error) {
	if s.onCreate != nil {
		return s.onCreate(ctx, cmd, id, tags)
	}
	return nil, &model.Rejection{Msg: "not implemented"}
}

func (s *stubRepo) ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection) {
	if s.onProcess != nil {
		return s.onProcess(ctx, id, cmd)
	}
	return nil, nil, &model.Rejection{Msg: "not implemented"}
}

func (s *stubRepo) PauseWorkflow(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection) {
	if s.onPause != nil {
		return s.onPause(ctx, id, reason)
	}
	return nil, &model.Rejection{Msg: "not implemented"}
}

func (s *stubRepo) ResumeWorkflow(ctx context.Context, id string) (*model.StoredState, *model.Rejection) {
	if s.onResume != nil {
		return s.onResume(ctx, id)
	}
	return nil, &model.Rejection{Msg: "not implemented"}
}

func (s *stubRepo) CancelWorkflow(ctx context.Context, id string, reason string, ae *actions.ActionExecutor) (*model.StoredState, *model.Rejection) {
	if s.onCancel != nil {
		return s.onCancel(ctx, id, reason, ae)
	}
	return nil, &model.Rejection{Msg: "not implemented"}
}

type addCommand struct {
	N int `json:"n"`
}

func parseAdd(cmdType string, payload map[string]any) (model.Command, error) {
	if cmdType != "add" {
		return nil, fmt.Errorf("unknown command_type")
	}
	n, _ := payload["n"].(float64)
	return &addCommand{N: int(n)}, nil
}

func TestGatewayCreateWorkflow(t *testing.T) {
	repo := &stubRepo{
		onCreate: func(ctx context.Context, cmd model.Command, id string, tags []string) (*model.StoredState, error) {
			if id != "wf-1" {
				t.Errorf("id %q", id)
			}
			if c, ok := cmd.(*addCommand); !ok || c.N != 3 {
				t.Errorf("cmd %#v", cmd)
			}
			return &model.StoredState{ID: id, Version: 1}, nil
		},
	}
	g := NewFleuveCommandGateway()
	g.RegisterWorkflowType("math", repo, parseAdd)
	mux := http.NewServeMux()
	g.RegisterRoutes(mux)

	body := map[string]any{
		"workflow_id":  "wf-1",
		"command_type": "add",
		"payload":      map[string]any{"n": 3.0},
	}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/commands/math", bytes.NewReader(b))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var resp CommandResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "ok" || resp.WorkflowID != "wf-1" || resp.Version != 1 {
		t.Fatalf("%+v", resp)
	}
}

func TestGatewayUnknownWorkflowType(t *testing.T) {
	g := NewFleuveCommandGateway()
	mux := http.NewServeMux()
	g.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/commands/missing", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestGatewayProcessCommand(t *testing.T) {
	repo := &stubRepo{
		onProcess: func(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection) {
			return &model.StoredState{ID: id, Version: 2}, []model.Event{&model.EvSystemResume{}}, nil
		},
	}
	g := NewFleuveCommandGateway()
	g.RegisterWorkflowType("math", repo, parseAdd)
	mux := http.NewServeMux()
	g.RegisterRoutes(mux)

	body := map[string]any{"command_type": "add", "payload": map[string]any{"n": 1.0}}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/commands/math/wf-9", bytes.NewReader(b))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var resp CommandResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.EventsCount != 1 || resp.Version != 2 {
		t.Fatalf("%+v", resp)
	}
}

func TestGatewayRetryWithoutExecutor(t *testing.T) {
	g := NewFleuveCommandGateway()
	g.RegisterWorkflowType("math", &stubRepo{}, parseAdd)
	mux := http.NewServeMux()
	g.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/commands/math/wf-1/retry/5", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestGatewayCreateWorkflowMissingID(t *testing.T) {
	g := NewFleuveCommandGateway()
	g.RegisterWorkflowType("math", &stubRepo{}, parseAdd)
	mux := http.NewServeMux()
	g.RegisterRoutes(mux)
	body := map[string]any{"command_type": "add", "payload": map[string]any{"n": 1.0}}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/commands/math", bytes.NewReader(b))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestGatewayPauseResumeCancel(t *testing.T) {
	var sawPauseReason, sawCancelReason string
	repo := &stubRepo{
		onPause: func(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection) {
			sawPauseReason = reason
			if id != "wf-p" {
				t.Errorf("pause id %q", id)
			}
			return &model.StoredState{ID: id}, nil
		},
		onResume: func(ctx context.Context, id string) (*model.StoredState, *model.Rejection) {
			if id != "wf-p" {
				t.Errorf("resume id %q", id)
			}
			return &model.StoredState{ID: id}, nil
		},
		onCancel: func(ctx context.Context, id string, reason string, ae *actions.ActionExecutor) (*model.StoredState, *model.Rejection) {
			sawCancelReason = reason
			if id != "wf-p" {
				t.Errorf("cancel id %q", id)
			}
			return &model.StoredState{ID: id}, nil
		},
	}
	g := NewFleuveCommandGateway()
	g.RegisterWorkflowType("math", repo, parseAdd)
	mux := http.NewServeMux()
	g.RegisterRoutes(mux)

	pauseReq := httptest.NewRequest(http.MethodPost, "/commands/math/wf-p/pause?reason=hold", nil)
	recP := httptest.NewRecorder()
	mux.ServeHTTP(recP, pauseReq)
	if recP.Code != http.StatusOK {
		t.Fatalf("pause status %d: %s", recP.Code, recP.Body.String())
	}
	if sawPauseReason != "hold" {
		t.Fatalf("pause reason %q", sawPauseReason)
	}

	recR := httptest.NewRecorder()
	mux.ServeHTTP(recR, httptest.NewRequest(http.MethodPost, "/commands/math/wf-p/resume", nil))
	if recR.Code != http.StatusOK {
		t.Fatalf("resume status %d", recR.Code)
	}

	recC := httptest.NewRecorder()
	mux.ServeHTTP(recC, httptest.NewRequest(http.MethodPost, "/commands/math/wf-p/cancel?reason=stop", nil))
	if recC.Code != http.StatusOK {
		t.Fatalf("cancel status %d", recC.Code)
	}
	if sawCancelReason != "stop" {
		t.Fatalf("cancel reason %q", sawCancelReason)
	}
}

func TestGatewayCreateWorkflowInvalidJSON(t *testing.T) {
	g := NewFleuveCommandGateway()
	g.RegisterWorkflowType("math", &stubRepo{}, parseAdd)
	mux := http.NewServeMux()
	g.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/commands/math", bytes.NewReader([]byte(`not json`)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayCreateWorkflowUnknownCommandType(t *testing.T) {
	g := NewFleuveCommandGateway()
	g.RegisterWorkflowType("math", &stubRepo{}, parseAdd)
	mux := http.NewServeMux()
	g.RegisterRoutes(mux)
	body := map[string]any{
		"workflow_id":  "wf-1",
		"command_type": "unknown",
		"payload":      map[string]any{},
	}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/commands/math", bytes.NewReader(b))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayCreateWorkflowConflict(t *testing.T) {
	repo := &stubRepo{
		onCreate: func(ctx context.Context, cmd model.Command, id string, tags []string) (*model.StoredState, error) {
			return nil, &model.AlreadyExists{Rejection: model.Rejection{Msg: "exists"}}
		},
	}
	g := NewFleuveCommandGateway()
	g.RegisterWorkflowType("math", repo, parseAdd)
	mux := http.NewServeMux()
	g.RegisterRoutes(mux)
	body := map[string]any{"workflow_id": "wf-1", "command_type": "add", "payload": map[string]any{"n": 1.0}}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/commands/math", bytes.NewReader(b))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayPauseRejection(t *testing.T) {
	repo := &stubRepo{
		onPause: func(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection) {
			return nil, &model.Rejection{Msg: "already paused"}
		},
	}
	g := NewFleuveCommandGateway()
	g.RegisterWorkflowType("math", repo, parseAdd)
	mux := http.NewServeMux()
	g.RegisterRoutes(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/commands/math/wf-x/pause", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d", rec.Code)
	}
}
