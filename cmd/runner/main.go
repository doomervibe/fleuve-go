// Package main is the fleuve workflow runner process.
//
// It parses flags, builds a runner.Config from the loaded config file,
// and delegates all event-processing logic to pkg/runner.
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

	"github.com/doomervibe/fleuve-go/pkg/config"
	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/partitioning"
	"github.com/doomervibe/fleuve-go/pkg/repo"
	"github.com/doomervibe/fleuve-go/pkg/runner"
)

func main() {
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

	_ = fs.Parse(os.Args[1:])

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

	if *runnerID == "" {
		if *partitionIndex >= 0 {
			*runnerID = partitioning.PartitionedReaderName(*workflowType, *partitionIndex, *totalPartitions)
		} else {
			*runnerID = fmt.Sprintf("%s_runner_%d", *workflowType, time.Now().UnixNano())
		}
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	if cfg.DatabaseURL == "" {
		log.Fatal("database_url is required (set in fleuve.toml or FLEUVE_DATABASE_URL env var)")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("[runner] received shutdown signal")
		cancel()
	}()

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to parse database URL: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}
	log.Printf("[runner] connected to database")

	workflow := getWorkflow(*workflowType)
	if workflow == nil {
		log.Fatalf("workflow type %q not registered", *workflowType)
	}
	log.Printf("[runner] loaded workflow: %s (schema version %d)", workflow.Name(), workflow.SchemaVersion())

	eventParser := getEventParser(*workflowType)

	// Build repository
	cacheSize := cfg.MaxCacheSize
	if cacheSize <= 0 {
		cacheSize = 10000
	}
	es := repo.NewInProcessEuphemeralStorage(cacheSize)

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
		repoOpts = append(repoOpts, repo.WithTrustCache(true))
	}
	r := repo.NewRepo(pool, *workflowType, workflow, es, repoOpts...)

	// Partition filter
	var isMine func(string) bool
	if *partitionIndex >= 0 {
		isMine = func(workflowID string) bool {
			return partitioning.IsMine(workflowID, *partitionIndex, *totalPartitions)
		}
		log.Printf("[runner] partition mode: index=%d, total=%d", *partitionIndex, *totalPartitions)
	}

	runnerCfg := runner.Config{
		Pool:             pool,
		WorkflowType:     *workflowType,
		Workflow:         workflow,
		Adapter:          getAdapter(*workflowType),
		Repo:             r,
		EventParser:      eventParser,
		ReaderName:       *runnerID,
		EnableTruncation: cfg.EnableTruncation,
		IsMine:           isMine,
	}
	if cfg.MaxInflight > 0 {
		runnerCfg.MaxConcurrentActions = cfg.MaxInflight
	}

	if err := runner.New(runnerCfg).Start(ctx); err != nil {
		log.Fatalf("runner exited with error: %v", err)
	}
}

// =============================================================================
// Workflow Registration
// =============================================================================

var workflowRegistry = make(map[string]model.Workflow)
var adapterRegistry = make(map[string]model.Adapter)
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

func getWorkflow(workflowType string) model.Workflow {
	return workflowRegistry[workflowType]
}

func getAdapter(workflowType string) model.Adapter {
	return adapterRegistry[workflowType]
}

func getEventParser(workflowType string) repo.EventParser {
	if parser, ok := parserRegistry[workflowType]; ok {
		return parser
	}
	return func(eventType string, _ json.RawMessage) (model.Event, error) {
		return nil, fmt.Errorf("no event parser registered for workflow type %s", workflowType)
	}
}
