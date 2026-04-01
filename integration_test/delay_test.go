//go:build realdeps

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/delay"
	"github.com/doomervibe/fleuve-go/pkg/model"
)

func countEventType(t *testing.T, workflowID, eventType string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	err := GetTestPool(t).QueryRow(ctx,
		`SELECT COUNT(*) FROM stored_events WHERE workflow_id = $1 AND event_type = $2`,
		workflowID, eventType,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count event type: %v", err)
	}
	return n
}

func TestDelayScheduler_OneShotFire(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "dl_" + UniqueID(t, "wf")
	wf := &testWorkflow{name: wfName}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	wid := UniqueID(t, "d1")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	s := delay.NewDelayScheduler(GetTestPool(t), wfName, testEventParser,
		delay.WithCheckInterval(40*time.Millisecond),
	)
	if err := s.RegisterDelay(ctx, wid, "delay-a", time.Now().UTC().Add(-time.Minute), &testIncrementCmd{Amount: 2}, 0); err != nil {
		t.Fatalf("RegisterDelay: %v", err)
	}

	sctx, cancel := context.WithCancel(context.Background())
	s.Start(sctx)
	defer func() {
		cancel()
		s.Stop()
	}()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if countEventType(t, wid, (&model.EvDelayComplete{}).Type()) >= 1 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if countEventType(t, wid, "delay_complete") < 1 {
		t.Fatal("expected delay_complete event")
	}
	if CountDelaySchedules(t, wid) != 0 {
		t.Fatalf("expected schedule removed, got %d rows", CountDelaySchedules(t, wid))
	}
}

func TestDelayScheduler_NotYetDue(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "dl_" + UniqueID(t, "nd")
	wf := &testWorkflow{name: wfName}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	wid := UniqueID(t, "d2")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	s := delay.NewDelayScheduler(GetTestPool(t), wfName, testEventParser,
		delay.WithCheckInterval(40*time.Millisecond),
	)
	future := time.Now().UTC().Add(24 * time.Hour)
	if err := s.RegisterDelay(ctx, wid, "delay-future", future, &testIncrementCmd{Amount: 1}, 0); err != nil {
		t.Fatalf("RegisterDelay: %v", err)
	}

	sctx, cancel := context.WithCancel(context.Background())
	s.Start(sctx)
	defer func() {
		cancel()
		s.Stop()
	}()

	time.Sleep(300 * time.Millisecond)
	if CountDelaySchedules(t, wid) != 1 {
		t.Fatalf("expected schedule to remain, got count %d", CountDelaySchedules(t, wid))
	}
	if countEventType(t, wid, "delay_complete") != 0 {
		t.Fatal("did not expect delay_complete for future delay")
	}
}

func TestDelayScheduler_CancelDelay(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "dl_" + UniqueID(t, "cx")
	wf := &testWorkflow{name: wfName}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	wid := UniqueID(t, "d3")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	s := delay.NewDelayScheduler(GetTestPool(t), wfName, testEventParser)
	if err := s.RegisterDelay(ctx, wid, "delay-x", time.Now().UTC().Add(time.Hour), &testNoOpCmd{}, 0); err != nil {
		t.Fatalf("RegisterDelay: %v", err)
	}
	if err := s.CancelDelay(ctx, wid, "delay-x"); err != nil {
		t.Fatalf("CancelDelay: %v", err)
	}
	if CountDelaySchedules(t, wid) != 0 {
		t.Fatalf("expected schedule deleted, got %d", CountDelaySchedules(t, wid))
	}
}

func TestDelayScheduler_WorkflowNotFound(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfName := "dl_" + UniqueID(t, "nf")
	_ = NewTestRepo(t, &testWorkflow{name: wfName}) // same pool / migrations
	ctx := context.Background()
	missingWid := UniqueID(t, "missing")

	s := delay.NewDelayScheduler(GetTestPool(t), wfName, testEventParser,
		delay.WithCheckInterval(40*time.Millisecond),
	)
	if err := s.RegisterDelay(ctx, missingWid, "orphan", time.Now().UTC().Add(-time.Minute), &testIncrementCmd{Amount: 1}, 0); err != nil {
		t.Fatalf("RegisterDelay: %v", err)
	}

	sctx, cancel := context.WithCancel(context.Background())
	s.Start(sctx)
	defer func() {
		cancel()
		s.Stop()
	}()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if CountDelaySchedules(t, missingWid) == 0 {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatal("expected orphaned schedule removed when workflow has no events")
}
