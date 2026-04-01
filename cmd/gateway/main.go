// Package main implements the fleuve gateway HTTP server.
//
// The gateway provides an HTTP API for workflow commands:
//   - POST /commands/{workflow_type} - Create new workflow
//   - POST /commands/{workflow_type}/{workflow_id} - Process command
//   - POST /commands/{workflow_type}/{workflow_id}/pause - Pause workflow
//   - POST /commands/{workflow_type}/{workflow_id}/resume - Resume workflow
//   - POST /commands/{workflow_type}/{workflow_id}/cancel - Cancel workflow
//   - POST /commands/{workflow_type}/{workflow_id}/retry/{event_number} - Retry action
//
// Usage:
//
//	go run cmd/gateway/main.go -addr :8080 [flags]
//
// Optional: -with-ui serves the embedded admin UI and GET /api/* on the same address.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/actions"
	"github.com/doomervibe/fleuve-go/pkg/config"
	"github.com/doomervibe/fleuve-go/pkg/gateway"
	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/repo"
	"github.com/doomervibe/fleuve-go/pkg/uiembed"
	"github.com/doomervibe/fleuve-go/pkg/uibackend"
)

func main() {
	// Parse flags
	fs := flag.NewFlagSet("gateway", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "HTTP server listen address")
	configPath := fs.String("config", "", "Path to fleuve.toml config file")
	readTimeout := fs.Duration("read-timeout", 30*time.Second, "HTTP read timeout")
	writeTimeout := fs.Duration("write-timeout", 30*time.Second, "HTTP write timeout")
	idleTimeout := fs.Duration("idle-timeout", 120*time.Second, "HTTP idle timeout")
	withUI := fs.Bool("with-ui", false, "Also serve embedded admin UI: GET /, /health, /api/* (same database pool)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Fleuve gateway HTTP server

Usage:
  gateway [flags]

Flags:
`)
		fs.PrintDefaults()
	}

	fs.Parse(os.Args[1:])

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
	log.Printf("[gateway] connected to database")

	// Create gateway
	gw := gateway.NewGateway()

	// Register all workflow types
	registeredCount := 0
	for workflowType, workflow := range workflowRegistry {
		// Create ephemeral storage for each workflow type
		cacheSize := cfg.MaxCacheSize
		if cacheSize <= 0 {
			cacheSize = 10000
		}
		es := repo.NewInProcessEuphemeralStorage(cacheSize)

		// Create event parser for this workflow type
		eventParser := getEventParser(workflowType)

		// Create repository with options
		repoOpts := []repo.RepoOption{
			repo.WithEventParser(eventParser),
		}
		if cfg.Namespace != "" {
			repoOpts = append(repoOpts, repo.WithNamespace(cfg.Namespace))
		}
		if cfg.SnapshotInterval > 0 {
			repoOpts = append(repoOpts, repo.WithSnapshotInterval(cfg.SnapshotInterval))
		}

		r := repo.NewRepo(pool, workflowType, workflow, es, repoOpts...)

		// Create command parser for this workflow type
		commandParser := getCommandParser(workflowType)

		// Get adapter for this workflow type (may be nil)
		var actionExecutor *actions.ActionExecutor
		adapter := getAdapter(workflowType)
		if adapter != nil {
			executorOpts := actions.ExecutorOptions{}
			if cfg.MaxInflight > 0 {
				executorOpts.MaxConcurrentActions = cfg.MaxInflight
			}
			actionExecutor = actions.NewActionExecutor(pool, adapter, r, executorOpts)
			actionExecutor.Start()
			log.Printf("[gateway] started action executor for %s", workflowType)
		}

		// Register with gateway
		gw.RegisterWorkflowType(workflowType, r, commandParser, workflow, actionExecutor)
		registeredCount++
	}

	if registeredCount == 0 {
		log.Print("[gateway] warning: no workflow types registered")
	} else {
		log.Printf("[gateway] registered %d workflow type(s)", registeredCount)
	}

	rootHandler := http.Handler(gw)
	if *withUI {
		replay := make(map[string]uibackend.WorkflowReplay)
		for wt, wf := range workflowRegistry {
			p := getEventParser(wt)
			replay[wt] = uibackend.WorkflowReplay{
				Workflow: wf,
				Parser: func(et string, raw json.RawMessage) (model.Event, error) {
					return p(et, raw)
				},
			}
		}
		title := uiembed.ResolveUITitle()
		combined, err := uibackend.NewCombinedHandler(title, uibackend.Options{
			Pool:   pool,
			Replay: replay,
		})
		if err != nil {
			log.Fatalf("uibackend: %v", err)
		}
		rootHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/commands") {
				gw.ServeHTTP(w, r)
				return
			}
			combined.ServeHTTP(w, r)
		})
		log.Printf("[gateway] -with-ui: admin UI + /api on same address (title %q)", title)
	}

	// Create HTTP server
	srv := &http.Server{
		Addr:         *addr,
		Handler:      rootHandler,
		ReadTimeout:  *readTimeout,
		WriteTimeout: *writeTimeout,
		IdleTimeout:  *idleTimeout,
	}

	// Start server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		log.Printf("[gateway] starting HTTP server on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
		close(serverErr)
	}()

	// Wait for shutdown signal or server error
	select {
	case err := <-serverErr:
		if err != nil {
			log.Fatalf("[gateway] server error: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("[gateway] received signal: %v", sig)
	}

	// Graceful shutdown
	log.Printf("[gateway] shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[gateway] server shutdown error: %v", err)
	}

	log.Printf("[gateway] shutdown complete")
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

// commandParserRegistry holds registered command parsers for workflows.
var commandParserRegistry = make(map[string]gateway.CommandParser)

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

// RegisterCommandParser registers a command parser for a workflow type.
// The commandParser function should parse a command type and payload into a model.Command.
func RegisterCommandParser(workflowType string, parser gateway.CommandParser) {
	commandParserRegistry[workflowType] = parser
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

// getCommandParser returns the registered command parser for the given workflow type.
func getCommandParser(workflowType string) gateway.CommandParser {
	if parser, ok := commandParserRegistry[workflowType]; ok {
		return parser
	}
	// Return a default parser that returns an error if none registered
	return func(cmdType string, payload map[string]any) (model.Command, error) {
		return nil, fmt.Errorf("no command parser registered for workflow type %s", workflowType)
	}
}
