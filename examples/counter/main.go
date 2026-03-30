// Package main demonstrates a simple counter workflow
// Run: go run main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/config"
	"github.com/doomervibe/fleuve-go/pkg/counterworkflow"
	"github.com/doomervibe/fleuve-go/pkg/model"
	"github.com/doomervibe/fleuve-go/pkg/repo"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.LoadFleuveToml("")
	if err != nil {
		log.Printf("Warning: Could not load config file: %v", err)
	}

	dbURL := cfg.DatabaseURL
	if dbURL == "" {
		dbURL = os.Getenv("FLEUVE_DATABASE_URL")
	}
	if dbURL == "" {
		dbURL = "postgresql://postgres:postgres@localhost:5432/fleuve?sslmode=disable"
	}

	log.Printf("Connecting to database...")

	pool, err := repo.NewPGXPool(ctx, dbURL, 10)
	if err != nil {
		log.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Println("Connected to database successfully!")

	storage := repo.NewInProcessEphemeralStorage(1000)
	workflow := counterworkflow.New()
	repository := repo.NewPGXRepo(
		pool,
		workflow.Name(),
		workflow,
		storage,
	)

	log.Println("\n=== Creating new counter workflow ===")
	state, err := repository.CreateNew(ctx, &counterworkflow.IncrementCmd{Amount: 5}, "counter-001", nil)
	if err != nil {
		log.Fatalf("Failed to create workflow: %v", err)
	}
	log.Printf("Created workflow: ID=%s, Version=%d", state.ID, state.Version)

	printState(state)

	log.Println("\n=== Incrementing counter by 10 ===")
	state, events, rejection := repository.ProcessCommand(ctx, "counter-001", &counterworkflow.IncrementCmd{Amount: 10})
	if rejection != nil {
		log.Fatalf("Failed to process command: %s", rejection.Msg)
	}
	log.Printf("Processed command: Version=%d, Events=%d", state.Version, len(events))
	printState(state)

	log.Println("\n=== Incrementing counter by 3 ===")
	state, events, rejection = repository.ProcessCommand(ctx, "counter-001", &counterworkflow.IncrementCmd{Amount: 3})
	if rejection != nil {
		log.Fatalf("Failed to process command: %s", rejection.Msg)
	}
	log.Printf("Processed command: Version=%d, Events=%d", state.Version, len(events))
	printState(state)

	log.Println("\n=== Resetting counter ===")
	state, events, rejection = repository.ProcessCommand(ctx, "counter-001", &counterworkflow.ResetCmd{})
	if rejection != nil {
		log.Fatalf("Failed to process command: %s", rejection.Msg)
	}
	log.Printf("Processed command: Version=%d, Events=%d", state.Version, len(events))
	printState(state)

	log.Println("\n=== Testing concurrent increments ===")
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _, _ = repository.ProcessCommand(ctx, "counter-001", &counterworkflow.IncrementCmd{Amount: 1})
			}
			log.Printf("Goroutine %d completed", i)
		}(i)
	}
	wg.Wait()

	state, _ = repository.GetCurrentState(ctx, "counter-001")
	log.Println("\n=== Final state after concurrent increments ===")
	printState(state)

	log.Println("\n✅ Example completed successfully!")
	log.Println("Press Ctrl+C to exit...")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down...")
}

func printState(state *model.StoredState) {
	s := state.State.(*counterworkflow.CounterState)
	b, _ := json.MarshalIndent(s, "", "  ")
	fmt.Printf("State: %s\n", string(b))
	fmt.Printf("Version: %d\n\n", state.Version)
	time.Sleep(100 * time.Millisecond)
}
