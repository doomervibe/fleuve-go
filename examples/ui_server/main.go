// ui_server is a non-production reference that wires fleuve config, a Postgres
// pool, and the vendored UI + read API (pkg/uibackend + pkg/uiembed).
//
// Run from repo root:
//
//	go run ./examples/ui_server -addr :3000
//
// Requires database_url in fleuve.toml or FLEUVE_DATABASE_URL.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doomervibe/fleuve-go/pkg/config"
	"github.com/doomervibe/fleuve-go/pkg/uiembed"
	"github.com/doomervibe/fleuve-go/pkg/uibackend"
)

func main() {
	addr := flag.String("addr", ":3000", "HTTP listen address")
	configPath := flag.String("config", "", "Path to fleuve.toml (optional)")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if cfg.DatabaseURL == "" {
		log.Fatal("database_url is required (fleuve.toml [fleuve] or FLEUVE_DATABASE_URL)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping: %v", err)
	}

	title := uiembed.ResolveUITitle()
	handler, err := uibackend.NewCombinedHandler(title, uibackend.Options{Pool: pool})
	if err != nil {
		log.Fatalf("uibackend: %v", err)
	}

	srv := &http.Server{
		Addr:         *addr,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("fleuve ui_server listening on %s (title %q)", *addr, title)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
