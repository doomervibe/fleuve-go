//go:build realdeps

// Connectivity checks against live Postgres and NATS. These are I/O-backed tests (real
// sockets and servers), not "integration" in the sense of multi-subsystem or E2E flows.
// Enable with: go test -tags=realdeps ./...
// Requires FLEUVE_DATABASE_URL and FLEUVE_NATS_URL when you expect them to run.

package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

func realdepsURLOrSkip(t *testing.T) (dbURL, natsURL string) {
	t.Helper()
	dbURL = os.Getenv("FLEUVE_DATABASE_URL")
	natsURL = os.Getenv("FLEUVE_NATS_URL")
	if dbURL == "" || natsURL == "" {
		t.Skip("set FLEUVE_DATABASE_URL and FLEUVE_NATS_URL for -tags=realdeps connectivity tests")
	}
	return dbURL, natsURL
}

func TestPostgresPing(t *testing.T) {
	dbURL, _ := realdepsURLOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}
}

func TestMigratedSchemaStoredEvents(t *testing.T) {
	dbURL, _ := realdepsURLOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	var exists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'stored_events'
		)`).Scan(&exists)
	if err != nil {
		t.Fatalf("query schema: %v", err)
	}
	if !exists {
		t.Fatal(`table "stored_events" missing; apply migrations to the test database`)
	}
}

func TestNATSConnect(t *testing.T) {
	_, natsURL := realdepsURLOrSkip(t)

	nc, err := nats.Connect(natsURL, nats.Timeout(10*time.Second))
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	defer nc.Drain()

	if !nc.IsConnected() {
		t.Fatal("expected connected NATS client")
	}
}
