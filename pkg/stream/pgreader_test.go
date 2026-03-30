package stream

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGReaderReadBatchMapsRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	r := NewPGReader(db, "test_reader", 10)
	r.committedOffset = 0

	at := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"global_id", "workflow_id", "workflow_type", "workflow_version", "event_type", "body", "at", "metadata",
	}).AddRow(int64(42), "wf-1", "counter", int64(3), "inc", []byte(`{"type":"inc","amount":1}`), at, []byte(`{"tags":["a"]}`))

	mock.ExpectQuery(`SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata`).
		WithArgs(int64(0), 10).
		WillReturnRows(rows)

	got, err := r.readBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	ev := got[0]
	if ev.GlobalID != 42 || ev.WorkflowID != "wf-1" || ev.WorkflowType != "counter" || ev.EventNo != 3 || ev.EventType != "inc" {
		t.Fatalf("%+v", ev)
	}
	if ev.Event == nil || ev.Event.GetType() != "inc" {
		t.Fatalf("event type %#v", ev.Event)
	}
	tags, _ := ev.Metadata["tags"].([]interface{})
	if len(tags) != 1 || tags[0] != "a" {
		t.Fatalf("metadata %#v", ev.Metadata)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPGReaderInitUsesOffsetRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	r := NewPGReader(db, "my_reader", 5)
	rows := sqlmock.NewRows([]string{"coalesce"}).AddRow(int64(99))
	mock.ExpectQuery(`SELECT COALESCE\(last_read_event_no, 0\) FROM offsets WHERE reader = \$1`).
		WithArgs("my_reader").
		WillReturnRows(rows)

	if err := r.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.committedOffset != 99 {
		t.Fatalf("offset=%d", r.committedOffset)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPGReaderSetCommittedOffsetExec(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	r := NewPGReader(db, "off_reader", 10)
	mock.ExpectExec(`INSERT INTO offsets`).
		WithArgs("off_reader", int64(100)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	r.SetCommittedOffset(100)
	if r.committedOffset != 100 {
		t.Fatalf("offset=%d", r.committedOffset)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPGReaderIterEventsYieldsRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	r := NewPGReader(db, "iter_reader", 10)
	r.committedOffset = 0
	r.SetStopAtOffset(99)

	at := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	col := []string{
		"global_id", "workflow_id", "workflow_type", "workflow_version", "event_type", "body", "at", "metadata",
	}
	rows := sqlmock.NewRows(col).
		AddRow(int64(1), "wf-1", "counter", int64(1), "inc", []byte(`{"type":"inc"}`), at, []byte(`{}`))
	empty := sqlmock.NewRows(col)

	mock.ExpectQuery(`SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata`).
		WithArgs(int64(0), 10, int64(99)).
		WillReturnRows(rows)
	mock.ExpectQuery(`SELECT global_id, workflow_id, workflow_type, workflow_version, event_type, body, at, metadata`).
		WithArgs(int64(0), 10, int64(99)).
		WillReturnRows(empty)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := r.IterEvents(ctx)
	ev := <-ch
	if ev == nil || ev.GlobalID != 1 {
		t.Fatalf("event %#v", ev)
	}
	cancel()
	for range ch {
	}
}
