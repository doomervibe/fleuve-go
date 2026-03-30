//go:build realdeps

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fleuve/fleuve-go/pkg/model"
	"github.com/fleuve/fleuve-go/pkg/repo"
)

type e2eStartCmd struct{}

type e2eStartedEvt struct {
	model.EventBase
	Type string `json:"type"`
}

func (e *e2eStartedEvt) GetType() string {
	if e.Type != "" {
		return e.Type
	}
	return "e2e_started"
}

type e2eWorkflow struct{}

func (w *e2eWorkflow) Name() string                            { return "E2EWorkflow" }
func (w *e2eWorkflow) SchemaVersion() int                    { return 1 }
func (w *e2eWorkflow) Upcast(t string, sv int, raw map[string]any) map[string]any {
	return raw
}

func (w *e2eWorkflow) DecodeEvent(eventType string, schemaVersion int, raw map[string]any) (model.Event, error) {
	if eventType == "e2e_started" {
		b, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		var e e2eStartedEvt
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		return &e, nil
	}
	return model.DecodeBuiltinReplayEvent(eventType, raw)
}

func (w *e2eWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	if _, ok := cmd.(*e2eStartCmd); ok {
		return []model.Event{&e2eStartedEvt{Type: "e2e_started"}}, nil
	}
	return nil, &model.Rejection{Msg: "unknown cmd"}
}

type e2eState struct {
	model.StateBase
	Done bool `json:"done"`
}

func (s *e2eState) GetSubscriptions() []model.Sub                 { return nil }
func (s *e2eState) GetExternalSubscriptions() []model.ExternalSub { return nil }
func (s *e2eState) GetLifecycle() model.LifecycleState            { return model.LifecycleActive }
func (s *e2eState) GetSchedules() []model.Schedule                { return nil }
func (s *e2eState) Copy() model.State {
	return &e2eState{StateBase: *s.StateBase.Copy(), Done: s.Done}
}

func (w *e2eWorkflow) Evolve(state model.State, event model.Event) model.State {
	return &e2eState{StateBase: *model.NewStateBase(), Done: true}
}

func (w *e2eWorkflow) EventToCmd(e model.Event) model.Command { return nil }

func (w *e2eWorkflow) IsFinalEvent(e model.Event) bool { return false }

func TestRealdepsCreateNewInsertsStoredEvent(t *testing.T) {
	dbURL := os.Getenv("FLEUVE_DATABASE_URL")
	if dbURL == "" {
		t.Skip("set FLEUVE_DATABASE_URL for realdeps E2E repo test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := repo.NewPGXPool(ctx, dbURL, 4)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	storage := repo.NewInProcessEphemeralStorage(100)
	r := repo.NewPGXRepo(pool, "E2EWorkflow", &e2eWorkflow{}, storage)

	id := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	_, err = r.CreateNew(ctx, &e2eStartCmd{}, id, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	var cnt int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM stored_events WHERE workflow_id = $1`, id).Scan(&cnt)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if cnt < 1 {
		t.Fatalf("expected stored_events row for %s, got count=%d", id, cnt)
	}
}
