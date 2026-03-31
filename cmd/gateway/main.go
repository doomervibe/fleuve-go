package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/doomervibe/fleuve-go/pkg/config"
	"github.com/doomervibe/fleuve-go/pkg/counterworkflow"
	"github.com/doomervibe/fleuve-go/pkg/fleuvecmd"
	"github.com/doomervibe/fleuve-go/pkg/gateway"
	"github.com/doomervibe/fleuve-go/pkg/repo"
	"github.com/doomervibe/fleuve-go/pkg/stream"
	"github.com/doomervibe/fleuve-go/pkg/tracing"
)

func main() {
	configPath := flag.String("config", "", "Path to fleuve.toml")
	addr := flag.String("addr", ":8080", "Address to listen on")
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

	ctx := context.Background()
	pool, err := repo.NewPGXPool(ctx, cfg.DatabaseURL, 16)
	if err != nil {
		log.Fatalf("database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("database ping: %v", err)
	}

	gw := gateway.NewFleuveCommandGateway()

	var natsConn *nats.Conn
	defer func() {
		if natsConn != nil {
			natsConn.Close()
		}
	}()
	var jetPub repo.JetStreamCommittedPublisher
	if cfg.EnableJetstream && cfg.NatsURL != "" {
		nc, err := nats.Connect(cfg.NatsURL,
			nats.ReconnectWait(2*time.Second),
			nats.MaxReconnects(-1),
		)
		if err != nil {
			slog.Warn("nats_connect", "err", err)
		} else {
			natsConn = nc
			js, err := jetstream.New(nc)
			if err != nil {
				slog.Warn("jetstream_new", "err", err)
			} else {
				pub := stream.NewJetStreamPublisher(js, counterworkflow.New().Name())
				if err := pub.EnsureStream(ctx); err != nil {
					slog.Warn("jetstream_ensure_stream", "err", err)
				} else {
					jetPub = pub
				}
			}
		}
	}

	ae := fleuvecmd.WireCounterGateway(gw, pool, cfg, jetPub)
	if err := ae.Start(ctx); err != nil {
		log.Fatalf("action executor: %v", err)
	}
	defer func() { _ = ae.Stop() }()

	mux := http.NewServeMux()
	gw.RegisterRoutes(mux)

	server := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
	}

	go func() {
		log.Printf("Starting Fleuve gateway on %s", *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down gateway...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Gateway shutdown: %v", err)
	}
}
