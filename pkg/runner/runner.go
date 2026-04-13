// Package runner provides an embeddable workflow event runner.
//
// The Runner polls for events from PostgreSQL, applies adapter side effects
// (maybe_act_on), and routes commands to subscriber workflows — matching the
// behaviour of the Python WorkflowsRunner class.
//
// Usage:
//
//	r := runner.New(runner.Config{
//	    Pool:         pool,
//	    WorkflowType: wf.Name(),
//	    Workflow:     wf,
//	    Repo:         myRepo,
//	    EventParser:  myParser,
//	    ReaderName:   "my_runner",
//	})
//	if err := r.Start(ctx); err != nil {
//	    log.Fatal(err)
//	}
package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/actions"
	"github.com/doomervibe/fleuve-go/pkg/delay"
	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/reconcile"
	"github.com/doomervibe/fleuve-go/pkg/repo"
	"github.com/doomervibe/fleuve-go/pkg/stream"
	"github.com/doomervibe/fleuve-go/pkg/truncation"
)

// Config holds all configuration for a Runner instance.
// Pool, WorkflowType, Workflow, Repo, and EventParser are required.
type Config struct {
	// Pool is the PostgreSQL connection pool (required).
	Pool *pgxpool.Pool

	// WorkflowType is the workflow type string, e.g. "CounterWorkflow" (required).
	WorkflowType string

	// Workflow is the workflow implementation (required).
	Workflow model.Workflow

	// Adapter provides side-effect callbacks. nil = no adapter side effects.
	Adapter model.Adapter

	// Repo is the pre-built workflow repository (required).
	Repo *repo.Repo

	// EventParser deserializes raw JSON into model.Event (required).
	// Used for repo replay and as a fallback when StreamDeserialize is nil.
	EventParser repo.EventParser

	// StreamDeserialize parses stored_events rows for this runner's stream reader.
	// The emitter's workflow_type and event_type are both provided so ambiguous
	// short names like "updated" deserialize correctly. When nil, EventParser
	// is called with (eventType, raw) only (legacy).
	StreamDeserialize func(workflowType string, eventType string, raw json.RawMessage) (model.Event, error)

	// ReaderName is the unique consumer name for offset tracking.
	// Defaults to "<WorkflowType>_runner".
	ReaderName string

	// MaxConcurrentActions limits total concurrent adapter actions.
	// 0 = unlimited.
	MaxConcurrentActions int

	// DelayCheckInterval is how often to scan for expired delays.
	// 0 = default 1s.
	DelayCheckInterval time.Duration

	// IsMine filters events by partition ownership.
	// nil = this runner owns all workflows.
	IsMine func(workflowID string) bool

	// SubscriptionsTable is the subscriptions table name.
	// Defaults to "subscriptions". Must match the table name used by repo.Repo
	// (see repo.WithSubscriptionsTable).
	SubscriptionsTable string

	// Namespace restricts subscription lookups to a specific namespace.
	// nil = no namespace filtering (matches all rows). Must match the namespace
	// configured on repo.Repo (see repo.WithNamespace).
	Namespace *string

	// EnableTruncation starts the truncation service for this workflow type.
	EnableTruncation bool
}

// Runner is an embeddable workflow event runner.
// Create one with New, then call Start to begin processing.
type Runner struct {
	cfg                 Config
	reader              *stream.Reader
	actionExecutor      *actions.ActionExecutor
	delayScheduler      *delay.DelayScheduler
	reconciler          *reconcile.Reconciler
	truncationSvc       *truncation.TruncationService
	streamDeserializeFn func(workflowType string, eventType string, raw json.RawMessage) (model.Event, error)
	stopFn              context.CancelFunc
}

// New creates a Runner from cfg, wiring up the stream reader, action executor,
// delay scheduler, and reconciler.  Start must be called to begin processing.
func New(cfg Config) *Runner {
	if cfg.ReaderName == "" {
		cfg.ReaderName = cfg.WorkflowType + "_runner"
	}
	if cfg.IsMine == nil {
		cfg.IsMine = func(string) bool { return true }
	}
	if cfg.SubscriptionsTable == "" {
		cfg.SubscriptionsTable = "subscriptions"
	}
	checkInterval := cfg.DelayCheckInterval
	if checkInterval == 0 {
		checkInterval = time.Second
	}

	streamDeserialize := cfg.StreamDeserialize
	if streamDeserialize == nil {
		streamDeserialize = func(_ string, eventType string, raw json.RawMessage) (model.Event, error) {
			return cfg.EventParser(eventType, raw)
		}
	}

	// Stream reader: typed deserialization by emitter workflow_type + event_type.
	streamParser := func(workflowType string, eventType string, raw json.RawMessage) (stream.Event, error) {
		return streamDeserialize(workflowType, eventType, raw)
	}
	reader := stream.NewReader(cfg.Pool, cfg.ReaderName, streamParser,
		stream.WithFetchMetadata(true),
		stream.WithBatchSize(100),
	)

	// Action executor: only created when an Adapter is present.
	var actionExecutor *actions.ActionExecutor
	if cfg.Adapter != nil {
		executorOpts := actions.ExecutorOptions{
			RunnerID: cfg.ReaderName,
		}
		if cfg.MaxConcurrentActions > 0 {
			executorOpts.MaxConcurrentActions = cfg.MaxConcurrentActions
		}
		actionExecutor = actions.NewActionExecutor(cfg.Pool, cfg.Adapter, cfg.Repo, executorOpts)
	}

	delayScheduler := delay.NewDelayScheduler(
		cfg.Pool,
		cfg.WorkflowType,
		model.EventParser(cfg.EventParser),
		delay.WithCheckInterval(checkInterval),
	)

	r := &Runner{
		cfg:                 cfg,
		reader:              reader,
		actionExecutor:      actionExecutor,
		delayScheduler:      delayScheduler,
		reconciler:          reconcile.NewReconciler(cfg.Pool, cfg.WorkflowType),
		streamDeserializeFn: streamDeserialize,
	}
	if cfg.EnableTruncation {
		r.truncationSvc = truncation.NewTruncationService(cfg.Pool, cfg.WorkflowType)
	}
	return r
}

// Start begins all background services and blocks until ctx is cancelled.
// Returns nil after a clean shutdown.
func (r *Runner) Start(ctx context.Context) error {
	ctx, r.stopFn = context.WithCancel(ctx)
	defer r.stopFn()

	if r.actionExecutor != nil {
		r.actionExecutor.Start()
		log.Printf("[runner:%s] started action executor", r.cfg.ReaderName)
	}
	r.delayScheduler.Start(ctx)
	log.Printf("[runner:%s] started delay scheduler", r.cfg.ReaderName)

	r.reconciler.Start(ctx)
	log.Printf("[runner:%s] started reconciler", r.cfg.ReaderName)

	if r.truncationSvc != nil {
		r.truncationSvc.Start(ctx)
		log.Printf("[runner:%s] started truncation service", r.cfg.ReaderName)
	}

	eventCh := r.reader.IterEvents(ctx)
	log.Printf("[runner:%s] started event loop", r.cfg.ReaderName)

	inflight := NewInflightTracker()
	for event := range eventCh {
		inflight.Register(event.GlobalID)

		parsedEvent, err := event.Event()
		if err != nil {
			log.Printf("[runner:%s] failed to parse event %d: %v", r.cfg.ReaderName, event.GlobalID, err)
			inflight.MarkDone(event.GlobalID)
			r.reader.SetCommittedOffset(inflight.CommittableOffset())
			continue
		}

		if !r.cfg.IsMine(event.WorkflowID) {
			inflight.MarkDone(event.GlobalID)
			r.reader.SetCommittedOffset(inflight.CommittableOffset())
			continue
		}

		if err := r.processEvent(ctx, event, parsedEvent); err != nil {
			log.Printf("[runner:%s] error processing event %d (workflow %s): %v",
				r.cfg.ReaderName, event.GlobalID, event.WorkflowID, err)
		}
		inflight.MarkDone(event.GlobalID)
		r.reader.SetCommittedOffset(inflight.CommittableOffset())
	}

	log.Printf("[runner:%s] shutting down", r.cfg.ReaderName)
	r.shutdown()
	log.Printf("[runner:%s] shutdown complete", r.cfg.ReaderName)
	return nil
}

// Stop signals the runner to stop. Safe to call from any goroutine.
func (r *Runner) Stop() {
	if r.stopFn != nil {
		r.stopFn()
	}
}

// shutdown stops all background services in reverse start order.
func (r *Runner) shutdown() {
	if r.truncationSvc != nil {
		r.truncationSvc.Stop()
	}
	r.reconciler.Stop()
	r.delayScheduler.Stop()
	if r.actionExecutor != nil {
		r.actionExecutor.Stop()
	}
	r.reader.Stop()
}

// processEvent implements the Python WorkflowsRunner._process_event + SideEffects.maybe_act_on
// event routing contract:
//
//  1. For same-type events: run side effects first (cancel, one-shot delay, adapter ActOn).
//     EvActionCancel returns early — mirrors Python.
//  2. Call workflow.EventToCmd.  nil → return (no command to route).
//  3. Find subscribers via workflowsToNotify and call repo.ProcessCommand for each.
func (r *Runner) processEvent(ctx context.Context, consumed *stream.ConsumedEvent, parsedEvent model.Event) error {
	// Step 1 — side effects for same-type events (Python SideEffects.maybe_act_on).
	if consumed.WorkflowType == r.cfg.WorkflowType {
		switch e := parsedEvent.(type) {
		case *model.EvActionCancel:
			// Cancel in-flight actions and return early — Python does the same.
			if r.actionExecutor != nil {
				r.actionExecutor.CancelWorkflowActions(consumed.WorkflowID, e.EventNumbers)
			}
			return nil

		case *model.EvDelay:
			// One-shot delays are registered here; cron delays are already
			// persisted by the repo in the same transaction as the EvDelay event.
			if !e.IsCron() {
				if err := r.delayScheduler.RegisterDelay(
					ctx,
					consumed.WorkflowID,
					e.ID,
					e.DelayUntil,
					e.NextCmd,
					int(consumed.EventNo),
				); err != nil {
					log.Printf("[runner:%s] failed to register one-shot delay %s/%s: %v",
						r.cfg.ReaderName, consumed.WorkflowID, e.ID, err)
				}
			}
		}

		// Adapter side effects — run for ALL same-type events where ToBeActOn is true.
		if r.cfg.Adapter != nil && r.actionExecutor != nil {
			me := streamToModelEvent(consumed, parsedEvent)
			if r.cfg.Adapter.ToBeActOn(me) {
				if err := r.actionExecutor.ExecuteAction(me); err != nil {
					log.Printf("[runner:%s] action execution failed for event %d: %v",
						r.cfg.ReaderName, consumed.GlobalID, err)
				}
			}
		}

		// Subscription horizon backfill: deliver missed emitter events from stored_events.
		if e, ok := parsedEvent.(*model.EvSubscriptionAdded); ok && e.Sub.AfterEmitterEventNo != nil {
			if err := r.backfillSubscription(ctx, consumed, &e.Sub); err != nil {
				log.Printf("[runner:%s] subscription backfill failed: %v", r.cfg.ReaderName, err)
			}
		}
	}

	injectEmitterMetaForSubscribers(parsedEvent, consumed)

	// Step 2 — map event to a command; nil means this event is not routable.
	cmd := r.cfg.Workflow.EventToCmd(parsedEvent)
	if cmd == nil {
		return nil
	}

	// Step 3 — find subscriber workflows and deliver the command.
	workflowIDs, err := r.workflowsToNotify(ctx, consumed, parsedEvent)
	if err != nil {
		return fmt.Errorf("workflowsToNotify: %w", err)
	}

	for _, wfID := range workflowIDs {
		// When the adapter handles this event type, route through adapter
		// so it can enrich the command (e.g. load full project context for
		// cross-workflow subscription events like project_patch → domain).
		if r.cfg.Adapter != nil {
			me := streamToModelEvent(consumed, parsedEvent)
			me.WorkflowID = wfID // target the subscriber workflow
			if r.cfg.Adapter.ToBeActOn(me) {
				ch, aErr := r.cfg.Adapter.ActOn(ctx, me, nil)
				if aErr != nil {
					log.Printf("[runner:%s] subscription adapter ActOn failed for %s: %v",
						r.cfg.ReaderName, wfID, aErr)
				} else {
					for yield := range ch {
						if cy, ok := yield.(model.CommandYield); ok {
							_, _, rej := r.cfg.Repo.ProcessCommand(ctx, wfID, cy.Cmd)
							if rej != nil {
								log.Printf("[runner:%s] subscription adapter command rejected for %s: %s",
									r.cfg.ReaderName, wfID, rej.Msg)
							}
						}
					}
				}
				continue
			}
		}
		_, _, rejection := r.cfg.Repo.ProcessCommand(ctx, wfID, cmd)
		if rejection != nil {
			log.Printf("[runner:%s] command rejected for workflow %s: %s",
				r.cfg.ReaderName, wfID, rejection.Msg)
		}
	}

	return nil
}

// streamToModelEvent converts a stream.ConsumedEvent to model.ConsumedEvent for adapter use.
func streamToModelEvent(consumed *stream.ConsumedEvent, parsedEvent model.Event) *model.ConsumedEvent {
	return &model.ConsumedEvent{
		GlobalID:     consumed.GlobalID,
		WorkflowID:   consumed.WorkflowID,
		WorkflowType: consumed.WorkflowType,
		EventNo:      consumed.EventNo,
		EventType:    consumed.EventType,
		Event:        parsedEvent,
		At:           consumed.At.Format(time.RFC3339),
		Metadata:     consumed.Metadata,
	}
}

// injectEmitterMetaForSubscribers records emitter identity and version on the
// event before Workflow.EventToCmd so subscribers can route cross-domain
// commands (e.g. recipe planner, recipe aggregate).
func injectEmitterMetaForSubscribers(ev model.Event, consumed *stream.ConsumedEvent) {
	type metaTarget interface {
		GetMetadata() map[string]any
	}
	t, ok := ev.(metaTarget)
	if !ok {
		return
	}
	m := t.GetMetadata()
	m[model.MetaEmitterWorkflowVersion] = consumed.EventNo
	m[model.MetaEmitterWorkflowID] = consumed.WorkflowID
	m[model.MetaEmitterWorkflowType] = consumed.WorkflowType
}
