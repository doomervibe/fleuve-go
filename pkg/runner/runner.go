package runner

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/stream"
)

type TokenBucket struct {
	rate      float64
	tokens    float64
	lastCheck time.Time
	mu        sync.Mutex
}

func NewTokenBucket(rate float64) *TokenBucket {
	return &TokenBucket{
		rate:      rate,
		tokens:    rate,
		lastCheck: time.Now(),
	}
}

func (tb *TokenBucket) Acquire(ctx context.Context) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	for {
		now := time.Now()
		elapsed := now.Sub(tb.lastCheck).Seconds()
		tb.tokens = min(tb.rate, tb.tokens+elapsed*tb.rate)
		tb.lastCheck = now

		if tb.tokens >= 1.0 {
			tb.tokens -= 1.0
			return nil
		}

		sleepTime := (1.0 - tb.tokens) / tb.rate
		tb.mu.Unlock()
		select {
		case <-ctx.Done():
			tb.mu.Lock()
			return ctx.Err()
		case <-time.After(time.Duration(sleepTime * float64(time.Second))):
		}
		tb.mu.Lock()
	}
}

type InflightTracker struct {
	mu        sync.Mutex
	pending   map[int64]bool
	committed int64
}

func NewInflightTracker() *InflightTracker {
	return &InflightTracker{
		pending: make(map[int64]bool),
	}
}

func (t *InflightTracker) Register(globalID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending[globalID] = false
}

func (t *InflightTracker) MarkDone(globalID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending[globalID] = true
	t.advance()
}

func (t *InflightTracker) advance() {
	for {
		// Find the smallest uncommitted global_id
		minID := int64(-1)
		for gid := range t.pending {
			if minID == -1 || gid < minID {
				minID = gid
			}
		}
		if minID == -1 {
			return
		}
		if t.pending[minID] {
			t.committed = minID
			delete(t.pending, minID)
		} else {
			break
		}
	}
}

func (t *InflightTracker) CommittableOffset() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.committed
}

func (t *InflightTracker) Size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

type CachedSubscription struct {
	WorkflowID            string
	SubscribedToWorkflow  string
	SubscribedToEventType string
	Tags                  []string
	TagsAll               []string
}

func (s *CachedSubscription) MatchesEvent(eventWorkflowID, eventType string, eventTags, workflowTags map[string]struct{}) bool {
	if s.SubscribedToWorkflow != "*" && s.SubscribedToWorkflow != eventWorkflowID {
		return false
	}
	if s.SubscribedToEventType != "*" && s.SubscribedToEventType != eventType {
		return false
	}

	allTags := make(map[string]struct{})
	for k := range eventTags {
		allTags[k] = struct{}{}
	}
	for k := range workflowTags {
		allTags[k] = struct{}{}
	}

	if len(s.Tags) > 0 {
		found := false
		for _, tag := range s.Tags {
			if _, ok := allTags[tag]; ok {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(s.TagsAll) > 0 {
		for _, tag := range s.TagsAll {
			if _, ok := allTags[tag]; !ok {
				return false
			}
		}
	}

	return true
}

type Repository interface {
	ProcessCommand(ctx context.Context, id string, cmd model.Command) (*model.StoredState, []model.Event, *model.Rejection)
	GetWorkflowTags(ctx context.Context, workflowID string) ([]string, error)
}

type SideEffects interface {
	MaybeActOn(ctx context.Context, event *stream.ConsumedEvent) error
}

type WorkflowsRunner struct {
	name                    string
	workflowType            model.Workflow
	repo                    Repository
	stream                  stream.Reader
	sideEffects             SideEffects
	wfIDRule                func(string) bool
	dbScalingOperationModel string
	scalingCheckInterval    int
	eventsProcessed         int64
	targetOffsetForScaling  *int64
	externalMessageConsumer interface{}
	maxInflight             int
	tokenBucket             *TokenBucket
	subscriptionCache       map[string][]*CachedSubscription
	cacheInitialized        bool
	hasTagSubscriptions     bool
	subscriptionsPool       *pgxpool.Pool
	ctx                     context.Context
	cancel                  context.CancelFunc
	wg                      sync.WaitGroup
}

type RunnerOption func(*WorkflowsRunner)

func WithName(name string) RunnerOption {
	return func(r *WorkflowsRunner) { r.name = name }
}

func WithMaxInflight(n int) RunnerOption {
	return func(r *WorkflowsRunner) { r.maxInflight = n }
}

func WithMaxEventsPerSecond(rate float64) RunnerOption {
	return func(r *WorkflowsRunner) { r.tokenBucket = NewTokenBucket(rate) }
}

func WithWFIDRule(rule func(string) bool) RunnerOption {
	return func(r *WorkflowsRunner) { r.wfIDRule = rule }
}

// WithSubscriptionDB loads cross-workflow subscriptions from PostgreSQL for this runner's workflow type.
func WithSubscriptionDB(pool *pgxpool.Pool) RunnerOption {
	return func(r *WorkflowsRunner) { r.subscriptionsPool = pool }
}

func NewWorkflowsRunner(
	workflowType model.Workflow,
	repo Repository,
	reader stream.Reader,
	se SideEffects,
	opts ...RunnerOption,
) *WorkflowsRunner {
	r := &WorkflowsRunner{
		name:                 workflowType.Name() + "_runner",
		workflowType:         workflowType,
		repo:                 repo,
		stream:               reader,
		sideEffects:          se,
		maxInflight:          1,
		scalingCheckInterval: 50,
		subscriptionCache:    make(map[string][]*CachedSubscription),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *WorkflowsRunner) Start(ctx context.Context) error {
	r.ctx, r.cancel = context.WithCancel(ctx)

	if err := r.loadSubscriptionCache(ctx); err != nil {
		slog.Error("subscription_cache_load", "err", err)
	}

	r.wg.Add(1)
	go r.run()

	return nil
}

func (r *WorkflowsRunner) Stop() error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	return nil
}

func (r *WorkflowsRunner) loadSubscriptionCache(ctx context.Context) error {
	r.subscriptionCache = make(map[string][]*CachedSubscription)
	r.cacheInitialized = true
	r.hasTagSubscriptions = false

	if r.subscriptionsPool == nil {
		return nil
	}

	rows, err := r.subscriptionsPool.Query(ctx, `
		SELECT workflow_id, subscribed_to_workflow, subscribed_to_event_type, COALESCE(tags, '{}'), COALESCE(tags_all, '{}')
		FROM subscriptions
		WHERE workflow_type = $1
	`, r.workflowType.Name())
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var wfID, subWF, subEv string
		var tags, tagsAll []string
		if err := rows.Scan(&wfID, &subWF, &subEv, &tags, &tagsAll); err != nil {
			return err
		}
		sub := &CachedSubscription{
			WorkflowID:            wfID,
			SubscribedToWorkflow:  subWF,
			SubscribedToEventType: subEv,
			Tags:                  tags,
			TagsAll:               tagsAll,
		}
		r.subscriptionCache[wfID] = append(r.subscriptionCache[wfID], sub)
		if len(tags) > 0 || len(tagsAll) > 0 {
			r.hasTagSubscriptions = true
		}
	}
	return rows.Err()
}

func (r *WorkflowsRunner) run() {
	defer r.wg.Done()

	inflight := NewInflightTracker()
	pending := make(map[int64]context.CancelFunc)
	var pendingMu sync.Mutex

	events := r.stream.IterEvents(r.ctx)

	for {
		select {
		case <-r.ctx.Done():
			r.drainPending(pending, &pendingMu)
			return
		case event, ok := <-events:
			if !ok {
				r.drainPending(pending, &pendingMu)
				return
			}

			atomic.AddInt64(&r.eventsProcessed, 1)

			if r.tokenBucket != nil {
				if err := r.tokenBucket.Acquire(r.ctx); err != nil {
					return
				}
			}

			for len(pending) >= r.maxInflight {
				time.Sleep(10 * time.Millisecond)
			}

			inflight.Register(event.GlobalID)

			taskCtx, cancel := context.WithCancel(r.ctx)
			pendingMu.Lock()
			pending[event.GlobalID] = cancel
			pendingMu.Unlock()

			r.wg.Add(1)
			go func(ev *stream.ConsumedEvent) {
				defer r.wg.Done()
				defer func() {
					pendingMu.Lock()
					delete(pending, ev.GlobalID)
					pendingMu.Unlock()
					inflight.MarkDone(ev.GlobalID)
					r.stream.SetCommittedOffset(inflight.CommittableOffset())
				}()

				r.processEvent(taskCtx, ev)
			}(event)
		}
	}
}

func (r *WorkflowsRunner) drainPending(pending map[int64]context.CancelFunc, mu *sync.Mutex) {
	mu.Lock()
	for _, cancel := range pending {
		cancel()
	}
	mu.Unlock()
}

func (r *WorkflowsRunner) processEvent(ctx context.Context, event *stream.ConsumedEvent) {
	if r.toBeActOn(event) {
		if err := r.sideEffects.MaybeActOn(ctx, event); err != nil {
			slog.Error("side_effect_failed", "workflow_id", event.WorkflowID, "event_no", event.EventNo, "err", err)
		}
	}

	cmd := r.workflowType.EventToCmd(event.Event)
	if cmd == nil {
		return
	}

	workflowIDs := r.workflowsToNotify(event)
	for _, wfID := range workflowIDs {
		_, _, rej := r.repo.ProcessCommand(ctx, wfID, cmd)
		if rej != nil {
			slog.Error("process_command_rejected", "target_workflow_id", wfID, "source_workflow_id", event.WorkflowID, "msg", rej.Msg)
		}
	}
}

func (r *WorkflowsRunner) toBeActOn(event *stream.ConsumedEvent) bool {
	if event.WorkflowType != r.workflowType.Name() {
		return false
	}
	if r.wfIDRule == nil {
		return true
	}
	return r.wfIDRule(event.WorkflowID)
}

func (r *WorkflowsRunner) workflowsToNotify(event *stream.ConsumedEvent) []string {
	result := make(map[string]struct{})

	eventType := event.EventType
	if eventType == "" {
		if event.Event != nil {
			eventType = event.Event.GetType()
		}
	}
	if eventType == "" {
		return nil
	}

	if event.WorkflowType == r.workflowType.Name() {
		if eventType == "delay_complete" {
			result[event.WorkflowID] = struct{}{}
		}
	}

	for wfID := range r.subscriptionCache {
		for _, sub := range r.subscriptionCache[wfID] {
			eventTags := make(map[string]struct{})
			workflowTags := make(map[string]struct{})
			if event.Metadata != nil {
				if tags, ok := event.Metadata["tags"].([]string); ok {
					for _, t := range tags {
						eventTags[t] = struct{}{}
					}
				}
				if tags, ok := event.Metadata["workflow_tags"].([]string); ok {
					for _, t := range tags {
						workflowTags[t] = struct{}{}
					}
				}
			}
			if sub.MatchesEvent(event.WorkflowID, eventType, eventTags, workflowTags) {
				result[wfID] = struct{}{}
				break
			}
		}
	}

	out := make([]string, 0, len(result))
	for wfID := range result {
		if r.wfIDRule == nil || r.wfIDRule(wfID) {
			out = append(out, wfID)
		}
	}
	return out
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
