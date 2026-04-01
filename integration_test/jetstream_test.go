//go:build realdeps

package integration

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/doomervibe/fleuve-go/pkg/jetstream"
	"github.com/doomervibe/fleuve-go/pkg/stream"
)

func natsURL() string {
	u := os.Getenv("FLEUVE_NATS_URL")
	if u == "" {
		return "nats://localhost:4223"
	}
	return u
}

func newTestNATSConnection(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(natsURL())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(func() { _ = nc.Drain() })
	return nc
}

// jetstreamTestStreamParser returns a minimal stream.Event (lazy body not needed for these tests).
func jetstreamTestStreamParser(eventType string, raw json.RawMessage) (stream.Event, error) {
	return streamEventWrap{typ: eventType}, nil
}

type streamEventWrap struct {
	typ string
}

func (w streamEventWrap) Type() string { return w.typ }

func TestJetStream_PublishConsume(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfType := "js_" + strings.ReplaceAll(UniqueID(t, "pc"), "-", "_")
	wf := &testWorkflow{name: wfType}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	wid := UniqueID(t, "jspc")
	_, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 7}, wid, nil)
	if err != nil {
		t.Fatalf("CreateNew: %v", err)
	}

	nc := newTestNATSConnection(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	pub := jetstream.NewJetStreamPublisher(GetTestPool(t), js, wfType,
		jetstream.WithPublisherSleeper(stream.NewSleeper(5*time.Millisecond, 200*time.Millisecond)),
		jetstream.WithBatchSize(50),
	)
	pctx, cancel := context.WithCancel(context.Background())
	pub.Start(pctx)
	defer func() {
		cancel()
		pub.Stop()
	}()

	consName := "c_" + strings.ReplaceAll(UniqueID(t, "cons"), "-", "_")
	consumer, err := jetstream.NewJetStreamConsumer(js, wfType, consName, jetstreamTestStreamParser,
		jetstream.WithFetchTimeout(2*time.Second),
		jetstream.WithConsumerBatchSize(32),
	)
	if err != nil {
		t.Fatalf("NewJetStreamConsumer: %v", err)
	}
	defer consumer.Close()

	var got *stream.ConsumedEvent
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ev, ack, err := consumer.Next(ctx)
		if err == jetstream.TimeoutError {
			continue
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = ev
		if err := ack(); err != nil {
			t.Fatalf("ack: %v", err)
		}
		break
	}
	if got == nil {
		t.Fatal("timed out waiting for JetStream message")
	}
	if got.WorkflowID != wid {
		t.Errorf("workflow_id want %q got %q", wid, got.WorkflowID)
	}
	if got.EventType != "test_incremented" {
		t.Errorf("event_type want test_incremented got %q", got.EventType)
	}
	if got.WorkflowType != wfType {
		t.Errorf("workflow_type want %q got %q", wfType, got.WorkflowType)
	}
}

func TestJetStream_AdvisoryLock(t *testing.T) {
	ctx := context.Background()
	pool := GetTestPool(t)
	lockID := jetstream.WorkflowTypeAdvisoryLockID("advisory_lock_wf_a")

	c1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire c1: %v", err)
	}
	defer c1.Release()

	var locked bool
	if err := c1.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&locked); err != nil {
		t.Fatalf("try lock c1: %v", err)
	}
	if !locked {
		t.Fatal("expected first connection to acquire advisory lock")
	}

	c2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire c2: %v", err)
	}
	defer c2.Release()

	var locked2 bool
	if err := c2.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&locked2); err != nil {
		t.Fatalf("try lock c2: %v", err)
	}
	if locked2 {
		t.Fatal("expected second connection to fail advisory lock while first holds it")
	}

	var unlocked bool
	if err := c1.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&unlocked); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if !unlocked {
		t.Fatal("expected advisory_unlock to return true")
	}

	if err := c2.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&locked2); err != nil {
		t.Fatalf("try lock c2 after unlock: %v", err)
	}
	if !locked2 {
		t.Fatal("expected second connection to acquire lock after unlock")
	}
	_, _ = c2.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockID)
}

func TestJetStream_AdvisoryLock_DifferentTypes(t *testing.T) {
	ctx := context.Background()
	pool := GetTestPool(t)
	idA := jetstream.WorkflowTypeAdvisoryLockID("type_alpha")
	idB := jetstream.WorkflowTypeAdvisoryLockID("type_beta")

	c1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Release()
	c2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Release()

	var okA, okB bool
	if err := c1.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", idA).Scan(&okA); err != nil {
		t.Fatal(err)
	}
	if err := c2.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", idB).Scan(&okB); err != nil {
		t.Fatal(err)
	}
	if !okA || !okB {
		t.Fatalf("expected both locks independent: okA=%v okB=%v", okA, okB)
	}
	_, _ = c1.Exec(ctx, "SELECT pg_advisory_unlock($1)", idA)
	_, _ = c2.Exec(ctx, "SELECT pg_advisory_unlock($1)", idB)
}

// TestJetStream_Dedup verifies JetStream suppresses a second publish with the same Nats-Msg-Id within the duplicate window.
func TestJetStream_Dedup(t *testing.T) {
	wfType := "js_" + strings.ReplaceAll(UniqueID(t, "dd"), "-", "_")
	nc := newTestNATSConnection(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := jetstream.EnsureStream(js, wfType); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	subject := "events." + wfType + ".test_incremented"
	body := []byte(`{"amount":1}`)
	msgID := "dedup-" + UniqueID(t, "mid")

	publish := func() {
		hdr := nats.Header{}
		hdr.Set("Nats-Msg-Id", msgID)
		hdr.Set("workflow_id", "wf-dedup")
		hdr.Set("workflow_version", "1")
		hdr.Set("event_type", "test_incremented")
		hdr.Set("global_id", "42")
		hdr.Set("at", time.Now().UTC().Format(time.RFC3339Nano))
		_, err := js.PublishMsg(&nats.Msg{Subject: subject, Data: body, Header: hdr})
		if err != nil {
			t.Fatalf("PublishMsg: %v", err)
		}
	}
	publish()
	publish()

	consName := "dedup_" + strings.ReplaceAll(UniqueID(t, "dc"), "-", "_")
	consumer, err := jetstream.NewJetStreamConsumer(js, wfType, consName, jetstreamTestStreamParser,
		jetstream.WithFetchTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	ctx := context.Background()
	received := 0
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		ev, ack, err := consumer.Next(ctx)
		if err == jetstream.TimeoutError {
			if received >= 1 {
				break
			}
			continue
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		received++
		_ = ev
		_ = ack()
		if received > 1 {
			t.Fatalf("expected at most one message after duplicate Msg-Id publish, got extra delivery")
		}
	}
	if received != 1 {
		t.Errorf("deduplicated publish: expected exactly 1 consumer delivery, got %d", received)
	}
}

func TestJetStream_ConcurrentPublish(t *testing.T) {
	t.Cleanup(func() { CleanTables(t) })
	wfType := "js_" + strings.ReplaceAll(UniqueID(t, "cp"), "-", "_")
	wf := &testWorkflow{name: wfType}
	r := NewTestRepo(t, wf)
	ctx := context.Background()
	const n = 6
	for i := 0; i < n; i++ {
		id := UniqueID(t, "jsg")
		if _, err := r.CreateNew(ctx, &testIncrementCmd{Amount: 1}, id, nil); err != nil {
			t.Fatalf("CreateNew %d: %v", i, err)
		}
	}

	nc := newTestNATSConnection(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	pub := jetstream.NewJetStreamPublisher(GetTestPool(t), js, wfType,
		jetstream.WithPublisherSleeper(stream.NewSleeper(5*time.Millisecond, 200*time.Millisecond)),
		jetstream.WithBatchSize(100),
	)
	pctx, cancel := context.WithCancel(context.Background())
	pub.Start(pctx)
	defer func() {
		cancel()
		pub.Stop()
	}()

	consName := "jc_" + strings.ReplaceAll(UniqueID(t, "jc"), "-", "_")
	consumer, err := jetstream.NewJetStreamConsumer(js, wfType, consName, jetstreamTestStreamParser,
		jetstream.WithFetchTimeout(2*time.Second),
		jetstream.WithConsumerBatchSize(64),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	seen := make(map[string]bool)
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < n && time.Now().Before(deadline) {
		ev, ack, err := consumer.Next(ctx)
		if err == jetstream.TimeoutError {
			continue
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		seen[ev.WorkflowID] = true
		_ = ack()
	}
	if len(seen) != n {
		t.Errorf("expected %d distinct workflow messages, got %d", n, len(seen))
	}
}
