// Package main implements the fleuve workflow runner process.
//
// The runner polls for events from PostgreSQL (or JetStream) and processes them:
//   - External events matching subscriptions are converted to commands via EventToCmd
//   - Internal events from other workflows are routed based on subscriptions
//   - Actions are executed via the configured Adapter
//   - Delay scheduling handles time-based workflow resumption
//
// Usage:
//
//	go run cmd/runner/main.go -type <workflow_type> [flags]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/actions"
	"github.com/doomervibe/fleuve-go/pkg/config"
	"github.com/doomervibe/fleuve-go/pkg/delay"
	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/partitioning"
	"github.com/doomervibe/fleuve-go/pkg/reconcile"
	"github.com/doomervibe/fleuve-go/pkg/repo"
	"github.com/doomervibe/fleuve-go/pkg/stream"
	"github.com/doomervibe/fleuve-go/pkg/truncation"
)

func main() {
	// Parse flags
	fs := flag.NewFlagSet("runner", flag.ExitOnError)
	workflowType := fs.String("type", "", "Workflow type to run (required)")
	partitionIndex := fs.Int("partition-index", -1, "Partition index (0-based, -1 for unpartitioned)")
	totalPartitions := fs.Int("total-partitions", 1, "Total number of partitions")
	configPath := fs.String("config", "", "Path to fleuve.toml config file")
	runnerID := fs.String("runner-id", "", "Unique runner ID (default: auto-generated)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Fleuve workflow runner process

Usage:
  runner -type <workflow_type> [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	fs.Parse(os.Args[1:])

	if *workflowType == "" {
		fmt.Fprintln(os.Stderr, "error: -type is required")
		fs.Usage()
		os.Exit(1)
	}

	if *partitionIndex >= 0 && *totalPartitions <= 0 {
		fmt.Fprintln(os.Stderr, "error: -total-partitions must be positive when -partition-index is set")
		os.Exit(1)
	}

	if *partitionIndex >= *totalPartitions {
		fmt.Fprintf(os.Stderr, "error: -partition-index (%d) must be less than -total-partitions (%d)\n",
			*partitionIndex, *totalPartitions)
		os.Exit(1)
	}

	// Generate runner ID if not provided
	if *runnerID == "" {
		if *partitionIndex >= 0 {
			*runnerID = partitioning.PartitionedReaderName(*workflowType, *partitionIndex, *totalPartitions)
		} else {
			*runnerID = fmt.Sprintf("%s_runner_%d", *workflowType, time.Now().UnixNano())
		}
	}

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Validate required config
	if cfg.DatabaseURL == "" {
		log.Fatal("database_url is required (set in fleuve.toml or DATABASE_URL env var)")
	}

	// Set up context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Create database pool
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to parse database URL: %v", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Verify database connection
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}
	log.Printf("[runner] connected to database")

	// Load workflow implementation
	// NOTE: In a real deployment, workflows are registered via imports or plugins.
	// This placeholder demonstrates the expected interface.
	workflow := getWorkflow(*workflowType)
	if workflow == nil {
		log.Fatalf("workflow type %q not registered", *workflowType)
	}
	log.Printf("[runner] loaded workflow: %s (schema version %d)", workflow.Name(), workflow.SchemaVersion())

	// Create event parser
	// NOTE: The actual parser should be provided by the workflow implementation.
	// This is a placeholder that would be replaced with workflow-specific parsing.
	eventParser := getEventParser(*workflowType)

	// Create ephemeral storage (cache)
	var es repo.EphemeralStorage
	cacheSize := cfg.MaxCacheSize
	if cacheSize <= 0 {
		cacheSize = 10000 // Default cache size
	}
	es = repo.NewInProcessEuphemeralStorage(cacheSize)
	log.Printf("[runner] created in-memory cache (max_size=%d)", cacheSize)

	// Create repository with all options
	repoOpts := []repo.RepoOption{
		repo.WithEventParser(eventParser),
	}
	if cfg.Namespace != "" {
		repoOpts = append(repoOpts, repo.WithNamespace(cfg.Namespace))
	}
	if cfg.SnapshotInterval > 0 {
		repoOpts = append(repoOpts, repo.WithSnapshotInterval(cfg.SnapshotInterval))
	}
	if *partitionIndex >= 0 {
		// Trust cache when partitioned - this runner is the sole writer for its partition
		repoOpts = append(repoOpts, repo.WithTrustCache(true))
	}

	r := repo.NewRepo(pool, *workflowType, workflow, es, repoOpts...)
	log.Printf("[runner] created repository")

	// Create stream reader
	readerName := *runnerID
	readerOpts := []stream.ReaderOption{
		stream.WithFetchMetadata(true),
		stream.WithBatchSize(100),
	}
	if cfg.Namespace != "" {
		// Filter by namespace in events table
		readerOpts = append(readerOpts, stream.WithEventsTable("stored_events"))
	}

	// Wrap repo.EventParser to stream.EventParser (model.Event satisfies stream.Event)
	streamParser := func(eventType string, raw json.RawMessage) (stream.Event, error) {
		return eventParser(eventType, raw)
	}
	reader := stream.NewReader(pool, readerName, streamParser, readerOpts...)
	log.Printf("[runner] created stream reader: %s", readerName)

	// Create action executor (if adapter is configured)
	var actionExecutor *actions.ActionExecutor
	adapter := getAdapter(*workflowType)
	if adapter != nil {
		executorOpts := actions.ExecutorOptions{
			RunnerID: *runnerID,
		}
		if cfg.MaxInflight > 0 {
			executorOpts.MaxConcurrentActions = cfg.MaxInflight
		}
		actionExecutor = actions.NewActionExecutor(pool, adapter, r, executorOpts)
		actionExecutor.Start()
		log.Printf("[runner] started action executor")
	}

	// Create delay scheduler
	delayScheduler := delay.NewDelayScheduler(
		pool,
		*workflowType,
		// Convert repo.EventParser to model.EventParser (same signature, different named types)
		model.EventParser(eventParser),
		delay.WithCheckInterval(1*time.Second),
	)
	delayScheduler.Start(ctx)
	log.Printf("[runner] started delay scheduler")

	// Start truncation service (if enabled)
	var truncationSvc *truncation.TruncationService
	if cfg.EnableTruncation {
		truncationSvc = truncation.NewTruncationService(pool, *workflowType)
		truncationSvc.Start(ctx)
		log.Printf("[runner] started truncation service")
	}

	// Start reconciler for stuck event recovery
	reconciler := reconcile.NewReconciler(pool, *workflowType)
	reconciler.Start(ctx)
	log.Printf("[runner] started reconciler")

	// Create partition filter (if partitioned)
	var isMine func(string) bool
	if *partitionIndex >= 0 {
		isMine = func(workflowID string) bool {
			return partitioning.IsMine(workflowID, *partitionIndex, *totalPartitions)
		}
		log.Printf("[runner] partition mode: index=%d, total=%d", *partitionIndex, *totalPartitions)
	} else {
		isMine = func(string) bool { return true }
	}

	// Start the event processing loop
	eventCh := reader.IterEvents(ctx)
	log.Printf("[runner] started event processing loop")

	// Main processing loop
	running := true
	for running {
		select {
		case <-sigCh:
			log.Printf("[runner] received shutdown signal")
			running = false

		case event, ok := <-eventCh:
			if !ok {
				log.Printf("[runner] event channel closed")
				running = false
				continue
			}

			// Parse the event (lazy parsing in ConsumedEvent)
			parsedEvent, err := event.Event()
			if err != nil {
				log.Printf("[runner] failed to parse event %d: %v", event.GlobalID, err)
				continue
			}

			// Check partition ownership
			if !isMine(event.WorkflowID) {
				continue
			}

			// Process the event
			if err := processEvent(ctx, r, workflow, adapter, actionExecutor, event, parsedEvent); err != nil {
				log.Printf("[runner] failed to process event %d for workflow %s: %v",
					event.GlobalID, event.WorkflowID, err)
			}
		}
	}

	// Graceful shutdown
	log.Printf("[runner] shutting down...")

	// Stop components in reverse order
	if truncationSvc != nil {
		truncationSvc.Stop()
		log.Printf("[runner] stopped truncation service")
	}

	reconciler.Stop()
	log.Printf("[runner] stopped reconciler")

	delayScheduler.Stop()
	log.Printf("[runner] stopped delay scheduler")

	if actionExecutor != nil {
		actionExecutor.Stop()
		log.Printf("[runner] stopped action executor")
	}

	reader.Stop()
	log.Printf("[runner] stopped reader")

	log.Printf("[runner] shutdown complete")
}

// processEvent handles a single consumed event.
func processEvent(
	ctx context.Context,
	r *repo.Repo,
	workflow model.Workflow,
	adapter model.Adapter,
	actionExecutor *actions.ActionExecutor,
	consumed *stream.ConsumedEvent,
	parsedEvent model.Event,
) error {
	// Handle system events - these are already processed by the repo
	// when pause/resume/cancel are called, or by the delay scheduler
	switch parsedEvent.(type) {
	case *model.EvDelayComplete, *model.EvSystemPause, *model.EvSystemResume, *model.EvSystemCancel:
		// System events are handled by their respective components
		// The runner may need to trigger actions for delay_complete
		if actionExecutor != nil && adapter != nil {
			// Convert stream.ConsumedEvent to model.ConsumedEvent for adapter
			modelEvent := &model.ConsumedEvent{
				GlobalID:     consumed.GlobalID,
				WorkflowID:   consumed.WorkflowID,
				WorkflowType: consumed.WorkflowType,
				EventNo:      consumed.EventNo,
				EventType:    consumed.EventType,
				Event:        parsedEvent,
				At:           consumed.At.Format(time.RFC3339),
				Metadata:     consumed.Metadata,
			}
			if adapter.ToBeActOn(modelEvent) {
				if err := actionExecutor.ExecuteAction(modelEvent); err != nil {
					return fmt.Errorf("action execution failed: %w", err)
				}
			}
		}
		return nil
	}

	// Check if this event should trigger a command via EventToCmd
	cmd := workflow.EventToCmd(parsedEvent)
	if cmd == nil {
		// Event not relevant to this workflow
		return nil
	}

	// Process the command through the repo
	state, events, rejection := r.ProcessCommand(ctx, consumed.WorkflowID, cmd)
	if rejection != nil {
		// Rejection is not necessarily an error - it's a valid business response
		log.Printf("[runner] command rejected for workflow %s: %s",
			consumed.WorkflowID, rejection.Msg)
		return nil
	}

	if state == nil {
		return fmt.Errorf("nil state returned from ProcessCommand")
	}

	// Check if any events should trigger actions
	if actionExecutor != nil && adapter != nil && len(events) > 0 {
		for _, evt := range events {
			modelEvent := &model.ConsumedEvent{
				WorkflowID:   consumed.WorkflowID,
				WorkflowType: consumed.WorkflowType,
				EventNo:      state.Version,
				EventType:    evt.Type(),
				Event:        evt,
				At:           time.Now().UTC().Format(time.RFC3339),
			}
			if adapter.ToBeActOn(modelEvent) {
				if err := actionExecutor.ExecuteAction(modelEvent); err != nil {
					log.Printf("[runner] action execution failed for event %s: %v", evt.Type(), err)
					// Don't return error - action failure doesn't affect event processing
				}
			}
		}
	}

	return nil
}

// =============================================================================
// Workflow Registration
// =============================================================================

// workflowRegistry holds registered workflow implementations.
// In a real deployment, workflows are registered via init() functions
// or a plugin system.
var workflowRegistry = make(map[string]model.Workflow)

// adapterRegistry holds registered adapters for workflows.
var adapterRegistry = make(map[string]model.Adapter)

// parserRegistry holds registered event parsers for workflows.
var parserRegistry = make(map[string]repo.EventParser)

// RegisterWorkflow registers a workflow implementation.
// Call this from init() functions in workflow packages.
func RegisterWorkflow(workflow model.Workflow) {
	workflowRegistry[workflow.Name()] = workflow
}

// RegisterAdapter registers an adapter for a workflow type.
func RegisterAdapter(workflowType string, adapter model.Adapter) {
	adapterRegistry[workflowType] = adapter
}

// RegisterEventParser registers an event parser for a workflow type.
func RegisterEventParser(workflowType string, parser repo.EventParser) {
	parserRegistry[workflowType] = parser
}

// getWorkflow returns the registered workflow for the given type.
func getWorkflow(workflowType string) model.Workflow {
	return workflowRegistry[workflowType]
}

// getAdapter returns the registered adapter for the given workflow type.
func getAdapter(workflowType string) model.Adapter {
	return adapterRegistry[workflowType]
}

// getEventParser returns the registered event parser for the given workflow type.
func getEventParser(workflowType string) repo.EventParser {
	if parser, ok := parserRegistry[workflowType]; ok {
		return parser
	}
	// Return a no-op parser if none registered
	return func(eventType string, raw json.RawMessage) (model.Event, error) {
		return nil, fmt.Errorf("no event parser registered for workflow type %s", workflowType)
	}
}
