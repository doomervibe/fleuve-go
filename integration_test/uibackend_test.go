//go:build realdeps

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/uibackend"
)

func TestUIBackend_Health(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	h, err := uibackend.NewHandler(uibackend.Options{Pool: GetTestPool(t)})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("body %#v", body)
	}
}

func TestUIBackend_WorkflowDetail_ReplayState(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "ui_" + UniqueID(t, "wf")
	wf := &testWorkflow{name: wfName}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	wid := UniqueID(t, "uiw")
	if _, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 5}, wid, nil); err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	h, err := uibackend.NewHandler(uibackend.Options{
		Pool: GetTestPool(t),
		Replay: map[string]uibackend.WorkflowReplay{
			wfName: {Workflow: wf, Parser: model.EventParser(testEventParser)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/api/workflows/" + wid)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status %d: %s", res.StatusCode, b)
	}
	var detail struct {
		WorkflowID string         `json:"workflow_id"`
		State      map[string]any `json:"state"`
		Version    int64          `json:"version"`
	}
	if err := json.NewDecoder(res.Body).Decode(&detail); err != nil {
		t.Fatal(err)
	}
	if detail.WorkflowID != wid {
		t.Fatalf("workflow_id %q", detail.WorkflowID)
	}
	counter, ok := detail.State["counter"].(float64)
	if !ok {
		t.Fatalf("expected numeric counter in state, got %#v", detail.State["counter"])
	}
	if int(counter) != 5 {
		t.Fatalf("counter want 5 got %v", counter)
	}
}

func TestUIBackend_StateResolver(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "ui_" + UniqueID(t, "sr")
	wf := &testWorkflow{name: wfName}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	wid := UniqueID(t, "uis")
	if _, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil); err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	h, err := uibackend.NewHandler(uibackend.Options{
		Pool: GetTestPool(t),
		StateResolver: func(ctx context.Context, workflowID, workflowType string) (map[string]any, int64, error) {
			if workflowID == wid {
				return map[string]any{"from_resolver": true}, 99, nil
			}
			return nil, 0, uibackend.ErrStateUnresolved
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/api/workflows/" + wid)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var detail struct {
		State   map[string]any `json:"state"`
		Version int64          `json:"version"`
	}
	if err := json.NewDecoder(res.Body).Decode(&detail); err != nil {
		t.Fatal(err)
	}
	if detail.Version != 99 {
		t.Fatalf("version want 99 got %d", detail.Version)
	}
	v, ok := detail.State["from_resolver"].(bool)
	if !ok || !v {
		t.Fatalf("state %#v", detail.State)
	}
}
