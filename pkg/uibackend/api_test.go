package uibackend

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type okSession struct{}

func (o *okSession) Query(ctx context.Context, query string, args ...interface{}) ([]Row, error) {
	return nil, nil
}

func (o *okSession) QueryRow(ctx context.Context, query string, args ...interface{}) (Row, error) {
	return nil, fmt.Errorf("no row")
}

func (o *okSession) Exec(ctx context.Context, query string, args ...interface{}) error {
	return nil
}

func (o *okSession) Close() error {
	return nil
}

type stubMaker struct {
	err  error
	sess Session
}

func (m *stubMaker) NewSession(ctx context.Context) (Session, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.sess != nil {
		return m.sess, nil
	}
	return &okSession{}, nil
}

func TestUIBackendHealth(t *testing.T) {
	b := NewFleuveUIBackend(&stubMaker{}, "", WithoutBundledUI())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", b.health)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUIBackendWorkflowTypesSessionError(t *testing.T) {
	b := NewFleuveUIBackend(&stubMaker{err: fmt.Errorf("db unavailable")}, "", WithoutBundledUI())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/workflow-types", b.getWorkflowTypes)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/workflow-types", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUIBackendBatchReplayNotImplemented(t *testing.T) {
	b := NewFleuveUIBackend(&stubMaker{}, "", WithoutBundledUI())
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/workflows/batch/replay", b.batchReplay)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/workflows/batch/replay", strings.NewReader(`{"workflow_ids":["wf-1"]}`))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUIBackendRootJSONWhenNoDist(t *testing.T) {
	b := NewFleuveUIBackend(&stubMaker{}, "", WithoutBundledUI())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", b.root)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Fleuve") {
		t.Fatalf("body %q", rec.Body.String())
	}
}

func TestUIBackendRootServesEmbeddedIndex(t *testing.T) {
	b := NewFleuveUIBackend(&stubMaker{}, "")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", b.root)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<!DOCTYPE html>") && !strings.Contains(body, "<html") {
		prefixLen := 120
		if len(body) < prefixLen {
			prefixLen = len(body)
		}
		t.Fatalf("expected HTML, got %q", body[:prefixLen])
	}
	if strings.Contains(body, "{{project_title}}") {
		t.Fatalf("title placeholder should be replaced")
	}
	if !strings.Contains(body, `id="root"`) {
		t.Fatalf("expected React root mount in vendored index")
	}
}
