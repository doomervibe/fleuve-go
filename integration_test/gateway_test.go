//go:build realdeps

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/doomervibe/fleuve-go/pkg/gateway"
	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/repo"
)

func gatewayCommandParser(cmdType string, payload map[string]any) (model.Command, error) {
	switch cmdType {
	case "increment":
		amt, ok := payload["amount"].(float64)
		if !ok {
			return nil, fmt.Errorf("amount required")
		}
		return &testIncrementCmd{Amount: int(amt)}, nil
	case "noop":
		return &testNoOpCmd{}, nil
	case "finalize":
		return &testFinalCmd{}, nil
	default:
		return nil, fmt.Errorf("unknown command type: %s", cmdType)
	}
}

// newTestGateway registers a unique workflow type and serves it on httptest.Server.
func newTestGateway(t *testing.T) (gw *gateway.Gateway, r *repo.Repo, srv *httptest.Server, wfType string) {
	t.Helper()
	t.Cleanup(func() { CleanTables(t) })

	wfType = "gwtest_" + strings.ReplaceAll(UniqueID(t, "gw"), "-", "_")
	wf := &testWorkflow{name: wfType}
	r = NewTestRepo(t, wf)
	gw = gateway.NewGateway()
	gw.RegisterWorkflowType(wfType, r, gatewayCommandParser, wf, nil)
	srv = httptest.NewServer(gw)
	t.Cleanup(srv.Close)
	return gw, r, srv, wfType
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

func TestGateway_CreateWorkflow(t *testing.T) {
	_, _, srv, wfType := newTestGateway(t)
	wid := UniqueID(t, "gwcreate")

	resp := postJSON(t, srv.URL+"/commands/"+wfType, map[string]any{
		"command_type": "increment",
		"payload":      map[string]any{"amount": 2.0},
		"workflow_id":  wid,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, string(b))
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["workflow_id"] != wid {
		t.Fatalf("expected workflow_id %q, got %v", wid, out["workflow_id"])
	}
	if v, ok := out["version"].(float64); !ok || int(v) != 1 {
		t.Fatalf("expected version 1, got %v", out["version"])
	}
}

func TestGateway_CreateWorkflow_AlreadyExists(t *testing.T) {
	_, _, srv, wfType := newTestGateway(t)
	id := UniqueID(t, "gwdup")

	resp := postJSON(t, srv.URL+"/commands/"+wfType, map[string]any{
		"command_type": "increment",
		"payload":      map[string]any{"amount": 1.0},
		"workflow_id":  id,
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first create: %d", resp.StatusCode)
	}

	resp2 := postJSON(t, srv.URL+"/commands/"+wfType, map[string]any{
		"command_type": "increment",
		"payload":      map[string]any{"amount": 1.0},
		"workflow_id":  id,
	})
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("expected 400, got %d: %s", resp2.StatusCode, string(b))
	}
}

func TestGateway_CreateWorkflow_WithID(t *testing.T) {
	_, _, srv, wfType := newTestGateway(t)
	id := UniqueID(t, "gwid")

	resp := postJSON(t, srv.URL+"/commands/"+wfType, map[string]any{
		"command_type": "increment",
		"payload":      map[string]any{"amount": 5.0},
		"workflow_id":  id,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, string(b))
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out["workflow_id"] != id {
		t.Errorf("expected workflow_id %q, got %v", id, out["workflow_id"])
	}
}

func TestGateway_ProcessCommand(t *testing.T) {
	_, _, srv, wfType := newTestGateway(t)
	id := UniqueID(t, "gwpc")

	resp := postJSON(t, srv.URL+"/commands/"+wfType, map[string]any{
		"command_type": "increment",
		"payload":      map[string]any{"amount": 2.0},
		"workflow_id":  id,
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create: %d", resp.StatusCode)
	}

	resp2 := postJSON(t, srv.URL+"/commands/"+wfType+"/"+id, map[string]any{
		"command_type": "increment",
		"payload":      map[string]any{"amount": 3.0},
	})
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("process: %d %s", resp2.StatusCode, string(b))
	}
	var out map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if int(out["version"].(float64)) != 2 {
		t.Errorf("expected version 2, got %v", out["version"])
	}
	evs, ok := out["events"].([]any)
	if !ok || len(evs) == 0 {
		t.Errorf("expected events in response, got %v", out["events"])
	}
}

func TestGateway_ProcessCommand_NotFound(t *testing.T) {
	_, _, srv, wfType := newTestGateway(t)

	resp := postJSON(t, srv.URL+"/commands/"+wfType+"/"+UniqueID(t, "missing"), map[string]any{
		"command_type": "increment",
		"payload":      map[string]any{"amount": 1.0},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, string(b))
	}
}

func TestGateway_PauseResume(t *testing.T) {
	_, _, srv, wfType := newTestGateway(t)
	id := UniqueID(t, "gwpr")

	postJSON(t, srv.URL+"/commands/"+wfType, map[string]any{
		"command_type": "increment",
		"payload":      map[string]any{"amount": 1.0},
		"workflow_id":  id,
	}).Body.Close()

	resp := postJSON(t, srv.URL+"/commands/"+wfType+"/"+id+"/pause", map[string]any{"reason": "r1"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("pause: %d %s", resp.StatusCode, string(b))
	}

	resp2 := postJSON(t, srv.URL+"/commands/"+wfType+"/"+id+"/resume", nil)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("resume: %d %s", resp2.StatusCode, string(b))
	}
}

func TestGateway_Cancel(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfType := "gwtest_" + strings.ReplaceAll(UniqueID(t, "gwc"), "-", "_")
	wf := &testWorkflow{name: wfType}
	r := NewTestRepo(t, wf)
	gw := gateway.NewGateway()
	gw.RegisterWorkflowType(wfType, r, gatewayCommandParser, wf, nil)
	srv := httptest.NewServer(gw)
	defer srv.Close()

	id := UniqueID(t, "gwcx")
	postJSON(t, srv.URL+"/commands/"+wfType, map[string]any{
		"command_type": "increment",
		"payload":      map[string]any{"amount": 1.0},
		"workflow_id":  id,
	}).Body.Close()

	resp := postJSON(t, srv.URL+"/commands/"+wfType+"/"+id+"/cancel", map[string]any{"reason": "done"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel: %d", resp.StatusCode)
	}

	ctx := context.Background()
	_, _, rej := r.ProcessCommand(ctx, id, &testIncrementCmd{Amount: 1})
	if rej == nil {
		t.Fatal("expected command rejected after cancel")
	}
}

func TestGateway_UnknownWorkflowType(t *testing.T) {
	_, _, srv, _ := newTestGateway(t)

	resp := postJSON(t, srv.URL+"/commands/no_such_type_ever", map[string]any{
		"command_type": "increment",
		"payload":      map[string]any{"amount": 1.0},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestGateway_MethodNotAllowed(t *testing.T) {
	_, _, srv, wfType := newTestGateway(t)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/commands/"+wfType, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestGateway_InvalidJSON(t *testing.T) {
	_, _, srv, wfType := newTestGateway(t)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/commands/"+wfType, strings.NewReader("{not json"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}
