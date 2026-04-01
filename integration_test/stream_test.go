//go:build realdeps

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/stream"
)

// integrationStreamParser bridges test model events to stream.Event.
func integrationStreamParser(eventType string, raw json.RawMessage) (stream.Event, error) {
	ev, err := testEventParser(eventType, raw)
	if err != nil {
		return nil, err
	}
	return modelAsStreamEvent{inner: ev}, nil
}

type modelAsStreamEvent struct {
	inner model.Event
}

func (m modelAsStreamEvent) Type() string { return m.inner.Type() }

func getOffsetRow(t *testing.T, readerName string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var off int64
	err := GetTestPool(t).QueryRow(ctx,
		`SELECT last_read_event_no FROM offsets WHERE reader = $1`, readerName,
	).Scan(&off)
	if err != nil {
		t.Fatalf("offsets query: %v", err)
	}
	return off
}

func TestStreamReader_BasicPoll(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "sr_" + UniqueID(t, "wf")
	wf := &testWorkflow{name: wfName}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	wid := UniqueID(t, "srb")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 2}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}
	_, _, rej := r.ProcessCommand(ctx, wid, &testIncrementCmd{Amount: 3})
	if rej != nil {
		t.Fatalf("ProcessCommand: %v", rej.Msg)
	}

	readerName := "rdr_" + UniqueID(t, "basic")
	rd := stream.NewReader(GetTestPool(t), readerName, integrationStreamParser,
		stream.WithSleeper(stream.NewSleeper(10*time.Millisecond, time.Second)),
		stream.WithMarkHorizonEvery(50*time.Millisecond),
		stream.WithBatchSize(50),
	)

	rctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := rd.IterEvents(rctx)
	defer rd.Stop()

	var got int
	deadline := time.Now().Add(5 * time.Second)
	for got < 2 && time.Now().Before(deadline) {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("channel closed early")
			}
			if ev == nil {
				continue
			}
			if ev.WorkflowID == wid {
				got++
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	if got < 2 {
		t.Fatalf("expected at least 2 events for workflow, got %d", got)
	}
}

func TestStreamReader_OffsetCheckpointing(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "sr_" + UniqueID(t, "off")
	wf := &testWorkflow{name: wfName}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	wid := UniqueID(t, "sro")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	readerName := "rdr_" + UniqueID(t, "off")
	rd := stream.NewReader(GetTestPool(t), readerName, integrationStreamParser,
		stream.WithSleeper(stream.NewSleeper(10*time.Millisecond, time.Second)),
		stream.WithMarkHorizonEvery(30*time.Millisecond),
		stream.WithBatchSize(50),
	)
	rctx, cancel := context.WithCancel(context.Background())
	ch := rd.IterEvents(rctx)

	// Read one matching event
	deadline := time.Now().Add(5 * time.Second)
	var lastGID int64
	for time.Now().Before(deadline) {
		select {
		case ev, ok := <-ch:
			if !ok {
				goto doneRead
			}
			if ev != nil && ev.WorkflowID == wid {
				lastGID = ev.GlobalID
				rd.SetCommittedOffset(ev.GlobalID)
				goto doneRead
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
doneRead:
	cancel()
	rd.Stop()

	if lastGID == 0 {
		t.Fatal("did not observe event")
	}
	time.Sleep(150 * time.Millisecond)
	if off := getOffsetRow(t, readerName); off < lastGID {
		t.Errorf("offsets last_read_event_no want >= %d, got %d", lastGID, off)
	}
}

func TestStreamReader_StopAtOffset(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "sr_" + UniqueID(t, "sa")
	wf := &testWorkflow{name: wfName}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	wid := UniqueID(t, "srs")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}
	_, _, rej := r.ProcessCommand(ctx, wid, &testIncrementCmd{Amount: 1})
	if rej != nil {
		t.Fatalf("ProcessCommand: %v", rej.Msg)
	}

	var g2 int64
	qctx, qcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer qcancel()
	err = GetTestPool(t).QueryRow(qctx,
		`SELECT global_id FROM stored_events WHERE workflow_id = $1 AND workflow_version = 2`,
		wid,
	).Scan(&g2)
	if err != nil {
		t.Fatalf("lookup global_id v2: %v", err)
	}

	readerName := "rdr_" + UniqueID(t, "stop")
	rd := stream.NewReader(GetTestPool(t), readerName, integrationStreamParser,
		stream.WithSleeper(stream.NewSleeper(5*time.Millisecond, 200*time.Millisecond)),
		stream.WithBatchSize(20),
	)
	rd.SetStopAtOffset(g2)

	rctx, cancel := context.WithCancel(context.Background())
	ch := rd.IterEvents(rctx)
	count := 0
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev, ok := <-ch:
			if !ok {
				goto stopped
			}
			if ev != nil {
				count++
			}
		case <-time.After(30 * time.Millisecond):
		}
	}
stopped:
	cancel()
	rd.Stop()
	if count < 1 {
		t.Fatalf("expected at least one event before stop, got %d", count)
	}
}

func TestStreamReader_ResumeFromOffset(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "sr_" + UniqueID(t, "rs")
	wf := &testWorkflow{name: wfName}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	wid := UniqueID(t, "srr")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	readerName := "rdr_" + UniqueID(t, "resume")
	sleeper := stream.NewSleeper(10*time.Millisecond, time.Second)

	runOnce := func() int {
		rd := stream.NewReader(GetTestPool(t), readerName, integrationStreamParser,
			stream.WithSleeper(sleeper),
			stream.WithMarkHorizonEvery(40*time.Millisecond),
			stream.WithBatchSize(20),
		)
		rctx, cancel := context.WithCancel(context.Background())
		ch := rd.IterEvents(rctx)
		n := 0
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case ev, ok := <-ch:
				if !ok {
					cancel()
					rd.Stop()
					return n
				}
				if ev != nil && ev.WorkflowID == wid {
					n++
					rd.SetCommittedOffset(ev.GlobalID)
					cancel()
					rd.Stop()
					return n
				}
			case <-time.After(40 * time.Millisecond):
			}
		}
		cancel()
		rd.Stop()
		return n
	}

	if runOnce() != 1 {
		t.Fatal("first reader pass expected 1 event")
	}
	time.Sleep(200 * time.Millisecond)
	if runOnce() != 0 {
		t.Fatal("second reader with same name should resume past checkpoint and not re-read same event")
	}
}
