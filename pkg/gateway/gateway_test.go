package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

type testCmd struct{}

func (testCmd) CommandType() string { return "noop" }

type stubRepo struct {
	createRet *model.StoredState
	createErr error
}

func (s *stubRepo) CreateNew(ctx context.Context, cmd model.Command, id string, tags []string) (*model.StoredState, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}
	if s.createRet != nil {
		return s.createRet, nil
	}
	return &model.StoredState{ID: id, Version: 1}, nil
}

func (s *stubRepo) ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection) {
	return nil, nil, &model.Rejection{Msg: "not used in test"}
}

func (s *stubRepo) PauseWorkflow(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection) {
	return nil, &model.Rejection{Msg: "not used"}
}

func (s *stubRepo) ResumeWorkflow(ctx context.Context, id string) (*model.StoredState, *model.Rejection) {
	return nil, &model.Rejection{Msg: "not used"}
}

func (s *stubRepo) CancelWorkflow(ctx context.Context, id string, reason string) (*model.StoredState, *model.Rejection) {
	return nil, &model.Rejection{Msg: "not used"}
}

type stubWorkflow struct{}

func (stubWorkflow) Name() string { return "stub" }
func (stubWorkflow) SchemaVersion() int {
	return 1
}
func (stubWorkflow) Upcast(string, int, map[string]any) map[string]any { return nil }
func (stubWorkflow) Decide(model.State, model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}
func (stubWorkflow) Evolve(model.State, model.Event) model.State { return nil }
func (stubWorkflow) EventToCmd(model.Event) model.Command        { return nil }
func (stubWorkflow) IsFinalEvent(model.Event) bool               { return false }

func newTestGateway() *Gateway {
	g := NewGateway()
	repo := &stubRepo{}
	parser := func(cmdType string, payload map[string]any) (model.Command, error) {
		if cmdType != "noop" {
			return nil, io.EOF
		}
		return testCmd{}, nil
	}
	g.RegisterWorkflowType("counter", repo, parser, stubWorkflow{}, nil)
	return g
}

func TestGateway_methodNotAllowed(t *testing.T) {
	g := newTestGateway()
	req := httptest.NewRequest(http.MethodGet, "/commands/counter", nil)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestGateway_unknownWorkflowType(t *testing.T) {
	g := newTestGateway()
	body := bytes.NewBufferString(`{"command_type":"noop","payload":{}}`)
	req := httptest.NewRequest(http.MethodPost, "/commands/unknown", body)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestGateway_create_invalidJSON(t *testing.T) {
	g := newTestGateway()
	req := httptest.NewRequest(http.MethodPost, "/commands/counter", bytes.NewBufferString(`{`))
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestGateway_create_missingCommandType(t *testing.T) {
	g := newTestGateway()
	body := bytes.NewBufferString(`{"payload":{}}`)
	req := httptest.NewRequest(http.MethodPost, "/commands/counter", body)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestGateway_create_success(t *testing.T) {
	g := newTestGateway()
	body := bytes.NewBufferString(`{"command_type":"noop","payload":{},"workflow_id":"wf-1"}`)
	req := httptest.NewRequest(http.MethodPost, "/commands/counter", body)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d body=%s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["workflow_id"] != "wf-1" {
		t.Fatalf("workflow_id = %v", out["workflow_id"])
	}
	if out["version"] != float64(1) {
		t.Fatalf("version = %v", out["version"])
	}
}

func TestGateway_notFoundPath(t *testing.T) {
	g := newTestGateway()
	req := httptest.NewRequest(http.MethodPost, "/api/foo", nil)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d", rec.Code)
	}
}
