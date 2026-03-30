package repo

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doomervibe/fleuve-go/pkg/model"
)

type arsState struct {
	model.StateBase
	Counter int `json:"counter"`
}

func (s *arsState) GetSubscriptions() []model.Sub                 { return s.Subscriptions }
func (s *arsState) GetExternalSubscriptions() []model.ExternalSub { return s.ExternalSubscriptions }
func (s *arsState) GetLifecycle() model.LifecycleState            { return s.Lifecycle }
func (s *arsState) GetSchedules() []model.Schedule                { return s.Schedules }
func (s *arsState) Copy() model.State {
	c := *s
	c.StateBase = *s.StateBase.Copy()
	return &c
}

type arsIncCmd struct {
	N int `json:"n"`
}

type arsIncEvt struct {
	model.EventBase
	Type   string `json:"type"`
	Amount int    `json:"amount"`
}

func (e *arsIncEvt) GetType() string { return e.Type }

type arsWorkflow struct{}

func (w *arsWorkflow) Name() string { return "ars_wf" }

func (w *arsWorkflow) SchemaVersion() int { return 1 }

func (w *arsWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	return rawData
}

func (w *arsWorkflow) DecodeEvent(eventType string, schemaVersion int, raw map[string]any) (model.Event, error) {
	if eventType == "inc" {
		b, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		var e arsIncEvt
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		return &e, nil
	}
	return model.DecodeBuiltinReplayEvent(eventType, raw)
}

type noopCmd struct{}

func (w *arsWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	if _, ok := cmd.(*noopCmd); ok {
		return nil, nil
	}
	c, ok := cmd.(*arsIncCmd)
	if !ok {
		return nil, &model.Rejection{Msg: "bad cmd"}
	}
	if c.N < 0 {
		return nil, &model.Rejection{Msg: "negative"}
	}
	if c.N == 0 {
		return nil, &model.Rejection{Msg: "no events"}
	}
	return []model.Event{&arsIncEvt{Type: "inc", Amount: c.N}}, nil
}

func (w *arsWorkflow) Evolve(state model.State, event model.Event) model.State {
	var s *arsState
	if state != nil {
		s = state.(*arsState).Copy().(*arsState)
	} else {
		s = &arsState{StateBase: *model.NewStateBase()}
	}
	if e, ok := event.(*arsIncEvt); ok {
		s.Counter += e.Amount
	}
	return s
}

func (w *arsWorkflow) EventToCmd(e model.Event) model.Command { return nil }

func (w *arsWorkflow) IsFinalEvent(e model.Event) bool { return false }

type spyEphemeral struct {
	puts    []*model.StoredState
	removes []string
}

func (s *spyEphemeral) PutState(ctx context.Context, st *model.StoredState) error {
	s.puts = append(s.puts, st)
	return nil
}

func (s *spyEphemeral) GetState(ctx context.Context, workflowID string) (*model.StoredState, error) {
	return nil, nil
}

func (s *spyEphemeral) RemoveState(ctx context.Context, workflowID string) error {
	s.removes = append(s.removes, workflowID)
	return nil
}

type trustSpy struct {
	cur  *model.StoredState
	puts []*model.StoredState
}

func (t *trustSpy) PutState(ctx context.Context, s *model.StoredState) error {
	t.puts = append(t.puts, s)
	return nil
}

func (t *trustSpy) GetState(ctx context.Context, id string) (*model.StoredState, error) {
	if t.cur != nil && t.cur.ID == id {
		return t.cur, nil
	}
	return nil, nil
}

func (t *trustSpy) RemoveState(ctx context.Context, id string) error { return nil }

func TestRepoCreateNewCommitsAndCaches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spy := &spyEphemeral{}
	r := NewRepo(db, "ars_wf", &arsWorkflow{}, spy)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO stored_events`).
		WithArgs("wf1", int64(1), sqlmock.AnyArg(), "inc", "ars_wf", 1, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	ss, err := r.CreateNew(context.Background(), &arsIncCmd{N: 2}, "wf1", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if ss == nil || ss.Version != 1 {
		t.Fatalf("stored state: %#v", ss)
	}
	if ss.State.(*arsState).Counter != 2 {
		t.Fatalf("counter: %d", ss.State.(*arsState).Counter)
	}
	if len(spy.puts) != 1 || spy.puts[0].ID != "wf1" {
		t.Fatalf("ephemeral puts: %#v", spy.puts)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRepoCreateNewRejectionNoDB(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spy := &spyEphemeral{}
	r := NewRepo(db, "ars_wf", &arsWorkflow{}, spy)

	_, cerr := r.CreateNew(context.Background(), &arsIncCmd{N: -1}, "wf1", nil)
	if cerr == nil {
		t.Fatal("expected error")
	}
	if rj, ok := cerr.(*model.Rejection); !ok || rj.Msg != "negative" {
		t.Fatalf("got %#v", cerr)
	}
	if len(spy.puts) != 0 {
		t.Fatal("unexpected cache write")
	}
}

func TestRepoCreateNewRejectionZeroEvents(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	r := NewRepo(db, "ars_wf", &arsWorkflow{}, &spyEphemeral{})
	_, cerr := r.CreateNew(context.Background(), &arsIncCmd{N: 0}, "wf1", nil)
	if cerr == nil {
		t.Fatal("expected error")
	}
	if rj, ok := cerr.(*model.Rejection); !ok || rj.Msg != "no events" {
		t.Fatalf("got %#v", cerr)
	}
}

func TestRepoProcessCommandAppendsEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spy := &trustSpy{
		cur: &model.StoredState{
			ID:      "wf1",
			Version: 1,
			State:   &arsState{StateBase: *model.NewStateBase(), Counter: 10},
		},
	}
	r := NewRepo(db, "ars_wf", &arsWorkflow{}, spy)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT global_id FROM stored_events WHERE workflow_id = \$1 AND workflow_version = 1 FOR UPDATE`).
		WithArgs("wf1").
		WillReturnRows(sqlmock.NewRows([]string{"global_id"}).AddRow(int64(42)))

	bodyJSON := `{"type":"inc","amount":10}`
	loadRows := sqlmock.NewRows([]string{"body", "workflow_version", "schema_version", "event_type"}).
		AddRow([]byte(bodyJSON), int64(1), 1, "inc")
	mock.ExpectQuery(`SELECT body, workflow_version, schema_version, event_type FROM stored_events`).
		WithArgs("wf1", int64(0)).
		WillReturnRows(loadRows)

	mock.ExpectExec(`INSERT INTO stored_events`).
		WithArgs("wf1", int64(2), sqlmock.AnyArg(), "inc", "ars_wf", 1, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	ss, events, rej := r.ProcessCommand(context.Background(), "wf1", &arsIncCmd{N: 3})
	if rej != nil {
		t.Fatalf("rejection: %v", rej)
	}
	if len(events) != 1 || ss == nil || ss.Version != 2 {
		t.Fatalf("ss=%#v events=%d", ss, len(events))
	}
	if ss.State.(*arsState).Counter != 13 {
		t.Fatalf("counter %d", ss.State.(*arsState).Counter)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if len(spy.puts) != 1 || spy.puts[0].Version != 2 {
		t.Fatalf("puts %#v", spy.puts)
	}
}

func TestRepoProcessCommandNoEventsNoDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spy := &trustSpy{
		cur: &model.StoredState{
			ID:      "wf1",
			Version: 4,
			State:   &arsState{StateBase: *model.NewStateBase(), Counter: 1},
		},
	}
	r := NewRepo(db, "ars_wf", &arsWorkflow{}, spy)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT global_id FROM stored_events WHERE workflow_id = \$1 AND workflow_version = 1 FOR UPDATE`).
		WithArgs("wf1").
		WillReturnRows(sqlmock.NewRows([]string{"global_id"}).AddRow(int64(1)))

	bodyJSON := `{"type":"inc","amount":1}`
	loadRows := sqlmock.NewRows([]string{"body", "workflow_version", "schema_version", "event_type"}).
		AddRow([]byte(bodyJSON), int64(4), 1, "inc")
	mock.ExpectQuery(`SELECT body, workflow_version, schema_version, event_type FROM stored_events`).
		WithArgs("wf1", int64(0)).
		WillReturnRows(loadRows)

	mock.ExpectRollback()

	ss, events, rej := r.ProcessCommand(context.Background(), "wf1", &noopCmd{})
	if rej != nil || len(events) != 0 {
		t.Fatalf("rej=%v len=%d", rej, len(events))
	}
	if ss == nil || ss.Version != 4 {
		t.Fatalf("ss %#v", ss)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRepoGetCurrentStateVerifiesCache(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	spy := &trustSpy{
		cur: &model.StoredState{
			ID:      "wf1",
			Version: 1,
			State:   &arsState{StateBase: *model.NewStateBase(), Counter: 5},
		},
	}
	r := NewRepo(db, "ars_wf", &arsWorkflow{}, spy)

	mock.ExpectQuery(`SELECT workflow_version FROM stored_events WHERE workflow_id = \$1 ORDER BY workflow_version DESC LIMIT 1`).
		WithArgs("wf1").
		WillReturnRows(sqlmock.NewRows([]string{"workflow_version"}).AddRow(int64(1)))

	ss, err := r.GetCurrentState(context.Background(), "wf1")
	if err != nil {
		t.Fatal(err)
	}
	if ss.Version != 1 || ss.State.(*arsState).Counter != 5 {
		t.Fatalf("state %#v", ss)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRepoLoadStateReplayConcreteEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	r := NewRepo(db, "ars_wf", &arsWorkflow{}, &spyEphemeral{})

	bodyJSON := `{"type":"inc","amount":7}`
	loadRows := sqlmock.NewRows([]string{"body", "workflow_version", "schema_version", "event_type"}).
		AddRow([]byte(bodyJSON), int64(1), 1, "inc")
	mock.ExpectQuery(`SELECT body, workflow_version, schema_version, event_type FROM stored_events`).
		WithArgs("wf1", int64(0)).
		WillReturnRows(loadRows)

	ss, err := r.LoadState(context.Background(), "wf1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ss == nil || ss.Version != 1 {
		t.Fatalf("ss %#v", ss)
	}
	if ss.State.(*arsState).Counter != 7 {
		t.Fatalf("counter %d", ss.State.(*arsState).Counter)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
