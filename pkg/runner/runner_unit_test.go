package runner

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/stream"
)

func TestInflightTrackerSequentialCommit(t *testing.T) {
	tr := NewInflightTracker()
	tr.Register(10)
	tr.Register(20)
	tr.MarkDone(10)
	if tr.CommittableOffset() != 10 {
		t.Fatalf("after 10: %d", tr.CommittableOffset())
	}
	tr.MarkDone(20)
	if tr.CommittableOffset() != 20 {
		t.Fatalf("after 20: %d", tr.CommittableOffset())
	}
}

func TestInflightTrackerOutOfOrderWaitsForLowest(t *testing.T) {
	tr := NewInflightTracker()
	tr.Register(5)
	tr.Register(10)
	tr.MarkDone(10)
	if tr.CommittableOffset() != 0 {
		t.Fatalf("should not skip 5: %d", tr.CommittableOffset())
	}
	tr.MarkDone(5)
	if tr.CommittableOffset() != 10 {
		t.Fatalf("got %d", tr.CommittableOffset())
	}
}

func TestTokenBucketAcquireImmediate(t *testing.T) {
	tb := NewTokenBucket(1e9)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tb.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
}

type otherEvt struct{ model.EventBase }

func (o *otherEvt) GetType() string { return "other" }

func TestProcessEventNilEventToCmdDoesNotCallRepo(t *testing.T) {
	repo := &countingRepo{}
	r := NewWorkflowsRunner(&relayWorkflow{}, repo, &sliceStreamReader{}, noopSideEffects{})
	r.subscriptionCache = map[string][]*CachedSubscription{
		"L1": {{
			SubscribedToWorkflow:  "*",
			SubscribedToEventType: "other",
		}},
	}
	ev := &stream.ConsumedEvent{
		WorkflowID:   "src",
		WorkflowType: "foreign",
		EventType:    "other",
		Event:        &otherEvt{},
	}
	r.processEvent(context.Background(), ev)
	if repo.processCount() != 0 {
		t.Fatalf("unexpected ProcessCommand: %d", repo.processCount())
	}
}

func TestCachedSubscriptionMatchesEvent(t *testing.T) {
	sub := &CachedSubscription{
		SubscribedToWorkflow:  "*",
		SubscribedToEventType: "order_created",
		Tags:                  []string{"vip"},
	}
	evTags := map[string]struct{}{"vip": {}}
	if !sub.MatchesEvent("wf-1", "order_created", evTags, nil) {
		t.Fatal("expected match")
	}
	if sub.MatchesEvent("wf-1", "other", evTags, nil) {
		t.Fatal("event type should not match")
	}
	sub2 := &CachedSubscription{
		SubscribedToWorkflow:  "wf-9",
		SubscribedToEventType: "*",
	}
	if sub2.MatchesEvent("wf-1", "x", nil, nil) {
		t.Fatal("workflow filter")
	}
}

type sliceStreamReader struct {
	events []*stream.ConsumedEvent
	off    int64
}

func (s *sliceStreamReader) IterEvents(ctx context.Context) <-chan *stream.ConsumedEvent {
	ch := make(chan *stream.ConsumedEvent, 1)
	go func() {
		for _, e := range s.events {
			select {
			case <-ctx.Done():
				return
			case ch <- e:
			}
		}
		<-ctx.Done()
	}()
	return ch
}

func (s *sliceStreamReader) SetStopAtOffset(offset int64) {}

func (s *sliceStreamReader) SetCommittedOffset(offset int64) { atomic.StoreInt64(&s.off, offset) }

func (s *sliceStreamReader) LastReadEventGID() int64 { return atomic.LoadInt64(&s.off) }

func (s *sliceStreamReader) Close() error { return nil }

type pingEvent struct {
	model.EventBase
}

func (p *pingEvent) GetType() string { return "ping" }

type relayCmd struct {
	Delta int `json:"delta"`
}

type relayWorkflow struct{}

func (w *relayWorkflow) Name() string { return "relay" }

func (w *relayWorkflow) SchemaVersion() int { return 1 }

func (w *relayWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	return rawData
}

func (w *relayWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	return nil, nil
}

func (w *relayWorkflow) Evolve(state model.State, event model.Event) model.State { return state }

func (w *relayWorkflow) EventToCmd(e model.Event) model.Command {
	if e.GetType() == "ping" {
		return &relayCmd{Delta: 1}
	}
	return nil
}

func (w *relayWorkflow) IsFinalEvent(e model.Event) bool { return false }

type countingRepo struct {
	mu       sync.Mutex
	nProcess int
}

func (r *countingRepo) ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection) {
	r.mu.Lock()
	r.nProcess++
	r.mu.Unlock()
	return &model.StoredState{ID: id, Version: 1}, nil, nil
}

func (r *countingRepo) GetWorkflowTags(ctx context.Context, workflowID string) ([]string, error) {
	return nil, nil
}

func (r *countingRepo) processCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nProcess
}

type noopSideEffects struct{}

func (noopSideEffects) MaybeActOn(ctx context.Context, event *stream.ConsumedEvent) error { return nil }

func TestWorkflowsToNotifyDelayCompleteSelf(t *testing.T) {
	wf := &relayWorkflow{}
	r := NewWorkflowsRunner(wf, &countingRepo{}, &sliceStreamReader{}, nil)
	ev := &stream.ConsumedEvent{
		WorkflowType: "relay",
		WorkflowID:   "wf-self",
		EventType:    "delay_complete",
	}
	ids := r.workflowsToNotify(ev)
	if len(ids) != 1 || ids[0] != "wf-self" {
		t.Fatalf("got %#v", ids)
	}
}

func TestWorkflowsToNotifySubscriptionMatch(t *testing.T) {
	wf := &relayWorkflow{}
	r := NewWorkflowsRunner(wf, &countingRepo{}, &sliceStreamReader{}, nil)
	r.subscriptionCache = map[string][]*CachedSubscription{
		"listener-1": {{
			SubscribedToWorkflow:  "*",
			SubscribedToEventType: "order_created",
			Tags:                  []string{"vip"},
		}},
	}
	ev := &stream.ConsumedEvent{
		WorkflowID:   "order-src",
		WorkflowType: "orders",
		EventType:    "order_created",
		Metadata: map[string]any{
			"tags": []string{"vip"},
		},
	}
	ids := r.workflowsToNotify(ev)
	if len(ids) != 1 || ids[0] != "listener-1" {
		t.Fatalf("got %#v", ids)
	}
}

func TestWorkflowsToNotifyWFIDRuleFilters(t *testing.T) {
	wf := &relayWorkflow{}
	r := NewWorkflowsRunner(wf, &countingRepo{}, &sliceStreamReader{}, nil, WithWFIDRule(func(id string) bool {
		return id == "allowed"
	}))
	r.subscriptionCache = map[string][]*CachedSubscription{
		"blocked-wf": {{
			SubscribedToWorkflow:  "*",
			SubscribedToEventType: "e",
		}},
	}
	ev := &stream.ConsumedEvent{
		WorkflowID:   "src",
		WorkflowType: "other",
		EventType:    "e",
	}
	ids := r.workflowsToNotify(ev)
	if len(ids) != 0 {
		t.Fatalf("expected filter out, got %#v", ids)
	}
}

// delay_complete on the runner's workflow type maps the event's workflow_id into
// workflowsToNotify (see runner.go); subscription-based routing is covered separately.
func TestWorkflowsRunnerProcessEventDispatchesCommand(t *testing.T) {
	repo := &countingRepo{}
	reader := &sliceStreamReader{
		events: []*stream.ConsumedEvent{
			{
				GlobalID:     1,
				WorkflowID:   "wf-a",
				WorkflowType: "relay",
				EventNo:      1,
				EventType:    "delay_complete",
				Event:        &pingEvent{},
				At:           time.Now(),
			},
		},
	}
	wf := &relayWorkflow{}
	r := NewWorkflowsRunner(wf, repo, reader, noopSideEffects{}, WithMaxInflight(10))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(2 * time.Second)
	for repo.processCount() < 1 {
		select {
		case <-deadline:
			t.Fatalf("ProcessCommand not called, count=%d", repo.processCount())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := r.Stop(); err != nil {
		t.Fatal(err)
	}
}
