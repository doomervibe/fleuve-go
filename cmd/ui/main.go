package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/config"
	"github.com/doomervibe/fleuve-go/pkg/repo"
	"github.com/doomervibe/fleuve-go/pkg/uibackend"
)

func main() {
	configPath := flag.String("config", "", "Path to fleuve.toml")
	addr := flag.String("addr", ":3000", "Address to listen on")
	frontendDist := flag.String("frontend", "", "Path to frontend dist directory (overrides embedded UI)")
	apiOnly := flag.Bool("api-only", false, "Serve JSON API only at / (no embedded UI)")
	flag.Parse()

	cfg, err := config.LoadFleuveToml(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if cfg.DatabaseURL == "" {
		log.Fatal("Database URL is required (set FLEUVE_DATABASE_URL or database_url in config)")
	}

	if cfg.UITitle != "" {
		_ = os.Setenv("FLEUVE_UI_TITLE", cfg.UITitle)
	}

	frontendPath := *frontendDist
	if frontendPath == "" {
		frontendPath = os.Getenv("FLEUVE_FRONTEND_DIST")
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

	sessionMaker := uibackend.NewPGXPoolSessionMaker(pool)
	var uiOpts []uibackend.Option
	if *apiOnly {
		uiOpts = append(uiOpts, uibackend.WithoutBundledUI())
	}
	backend := uibackend.NewFleuveUIBackend(sessionMaker, frontendPath, uiOpts...)

	mux := http.NewServeMux()
	backend.RegisterRoutes(mux)

	handler := http.Handler(mux)
	if !backend.ServesStaticUI() {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			mux.ServeHTTP(w, r)
		})
	}

	server := &http.Server{
		Addr:    *addr,
		Handler: handler,
	}

	go func() {
		log.Printf("Starting Fleuve UI server on %s (PostgreSQL admin API)", *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down UI server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("UI shutdown: %v", err)
	}
}
