//go:build realdeps

package integration

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// =============================================================================
// Global Test State
// =============================================================================

var (
	testPool   *pgxpool.Pool
	testPoolMu sync.Mutex
)

// =============================================================================
// TestMain - Setup & Teardown
// =============================================================================

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := setupTestDB(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup test DB: %v\n", err)
		os.Exit(1)
	}
	testPool = pool

	code := m.Run()

	teardownTestDB(ctx, pool)
	pool.Close()

	os.Exit(code)
}

// setupTestDB connects to PostgreSQL and runs migrations.
func setupTestDB(ctx context.Context) (*pgxpool.Pool, error) {
	databaseURL := os.Getenv("FLEUVE_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgresql://test:test@localhost:5433/fleuve_test?sslmode=disable"
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	config.MaxConns = 10
	config.MinConns = 2
	config.HealthCheckPeriod = 5 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if err := runMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return pool, nil
}

func teardownTestDB(ctx context.Context, pool *pgxpool.Pool) {
	cleanAllTables(ctx, pool)
}

// =============================================================================
// Migrations
// =============================================================================

func runMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	migrationsDir := "../migrations"

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var upFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".up.sql") {
			upFiles = append(upFiles, name)
		}
	}

	sort.Strings(upFiles)

	for _, file := range upFiles {
		content, err := os.ReadFile(migrationsDir + "/" + file)
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", file, err)
		}

		if _, err := pool.Exec(ctx, string(content)); err != nil {
			return fmt.Errorf("failed to execute migration %s: %w", file, err)
		}
	}

	return nil
}

// =============================================================================
// Table Cleanup
// =============================================================================

func cleanAllTables(ctx context.Context, pool *pgxpool.Pool) {
	tables := []string{
		"outbox",
		"workflow_activities",
		"workflow_search_attributes",
		"delay_schedules",
		"external_subscriptions",
		"subscriptions",
		"workflow_metadata",
		"snapshots",
		"stored_events",
		"offsets",
		"scaling_operations",
	}

	for _, table := range tables {
		_, _ = pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table))
	}
}
