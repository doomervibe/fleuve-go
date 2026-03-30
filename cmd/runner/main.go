package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/fleuve/fleuve-go/pkg/actions"
	"github.com/fleuve/fleuve-go/pkg/config"
	"github.com/fleuve/fleuve-go/pkg/fleuvecmd"
	"github.com/fleuve/fleuve-go/pkg/repo"
	"github.com/fleuve/fleuve-go/pkg/runner"
	"github.com/fleuve/fleuve-go/pkg/stream"
	"github.com/fleuve/fleuve-go/pkg/tracing"
)

func main() {
	configPath := flag.String("config", "", "Path to fleuve.toml")
	workflowType := flag.String("type", "CounterWorkflow", "Workflow type to run (built-in: CounterWorkflow)")
	flag.Parse()

	cfg, err := config.LoadFleuveToml(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if cfg.DatabaseURL == "" {
		log.Fatal("Database URL is required (set FLEUVE_DATABASE_URL or database_url in config)")
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	otelCfg := tracing.OTelConfigFromFleuve(cfg)
	tr, err := tracing.InitializeGlobalTracer(otelCfg)
	if err != nil {
		slog.Warn("otel_init", "err", err)
	}
	if tr != nil {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tr.Shutdown(ctx)
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := repo.NewPGXPool(ctx, cfg.DatabaseURL, 16)
	if err != nil {
		log.Fatalf("database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("database ping: %v", err)
	}

	wf, r := fleuvecmd.NewCounterRepo(pool, cfg, nil)
	if *workflowType != wf.Name() {
		log.Fatalf("unsupported -type %q (supported: %s)", *workflowType, wf.Name())
	}

	ae := actions.NewActionExecutor(actions.NoopModelAdapter(), r,
		actions.WithRunnerName(fleuvecmd.RunnerName()),
		actions.WithActivityPersistence(pool, wf),
	)
	if err := ae.Start(ctx); err != nil {
		log.Fatalf("action executor: %v", err)
	}
	defer func() { _ = ae.Stop() }()

	var streamReader stream.Reader
	var cleanup func()

	if cfg.EnableJetstream && cfg.NatsURL != "" {
		nr, err := stream.NewNATSReader(cfg.NatsURL, wf.Name(),
			stream.WithNATSPersistOffset(pool, wf.Name()+"_nats"),
		)
		if err != nil {
			log.Fatalf("nats reader: %v", err)
		}
		if err := nr.Init(ctx); err != nil {
			nr.Close()
			log.Fatalf("nats init: %v", err)
		}
		streamReader = nr
		cleanup = func() { _ = nr.Close() }
		log.Printf("Using NATS JetStream at %s", cfg.NatsURL)
	} else {
		db, err := sql.Open("pgx", cfg.DatabaseURL)
		if err != nil {
			log.Fatalf("sql open: %v", err)
		}
		pr := stream.NewPGReader(db, wf.Name()+"_pg", 50)
		if err := pr.Init(ctx); err != nil {
			_ = db.Close()
			log.Fatalf("pg reader init: %v", err)
		}
		streamReader = pr
		cleanup = func() { _ = db.Close() }
		log.Printf("Using PostgreSQL outbox/poll reader (enable_jetstream=false)")
	}
	defer cleanup()

	se := &runner.ActionExecutorSideEffects{Exec: ae}

	runOpts := []runner.RunnerOption{runner.WithSubscriptionDB(pool)}
	if cfg.MaxInflight > 0 {
		runOpts = append(runOpts, runner.WithMaxInflight(cfg.MaxInflight))
	}
	if cfg.MaxEventsPerSecond > 0 {
		runOpts = append(runOpts, runner.WithMaxEventsPerSecond(cfg.MaxEventsPerSecond))
	}

	wr := runner.NewWorkflowsRunner(wf, r, streamReader, se, runOpts...)
	if err := wr.Start(ctx); err != nil {
		log.Fatalf("runner: %v", err)
	}
	defer func() { _ = wr.Stop() }()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Fleuve runner started workflow_type=%s", wf.Name())
	select {
	case <-sigChan:
		log.Println("shutdown signal")
	case <-ctx.Done():
	}
	cancel()
	log.Println("Runner stopped")
}
