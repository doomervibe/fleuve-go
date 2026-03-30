package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// clearFleuveEnvOverrides blanks env vars that applyEnvOverrides reads, so tests
// that assert on TOML-only values stay correct when the process inherits
// FLEUVE_* (e.g. docker-compose.test.yml test-runner).
func clearFleuveEnvOverrides(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"FLEUVE_DATABASE_URL",
		"FLEUVE_NATS_URL",
		"FLEUVE_SNAPSHOT_INTERVAL",
		"FLEUVE_ENABLE_TRUNCATION",
		"FLEUVE_TRUNCATION_MIN_RETENTION_DAYS",
		"FLEUVE_TRUNCATION_BATCH_SIZE",
		"FLEUVE_MAX_INFLIGHT",
		"FLEUVE_MAX_EVENTS_PER_SECOND",
		"FLEUVE_ENABLE_OTEL",
		"FLEUVE_MAX_CACHE_SIZE",
		"FLEUVE_ENABLE_JETSTREAM",
		"FLEUVE_ENABLE_RECONCILIATION",
		"FLEUVE_ENABLE_EXTERNAL_MESSAGING",
		"FLEUVE_CREATE_TABLES",
		"FLEUVE_OUTBOX_BATCH_SIZE",
		"FLEUVE_OUTBOX_POLL_INTERVAL",
		"FLEUVE_UI_TITLE",
		"FLEUVE_CONFIG",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadFleuveTomlFromFile(t *testing.T) {
	clearFleuveEnvOverrides(t)
	dir := t.TempDir()
	p := filepath.Join(dir, "fleuve.toml")
	if err := os.WriteFile(p, []byte(`
[fleuve]
database_url = "postgres://from-file"
nats_url = "nats://from-file:4222"
max_inflight = 8
enable_truncation = true
outbox_poll_interval = "250ms"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFleuveToml(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://from-file" {
		t.Errorf("DatabaseURL: %q", cfg.DatabaseURL)
	}
	if cfg.NatsURL != "nats://from-file:4222" {
		t.Errorf("NatsURL: %q", cfg.NatsURL)
	}
	if cfg.MaxInflight != 8 {
		t.Errorf("MaxInflight: %d", cfg.MaxInflight)
	}
	if !cfg.EnableTruncation {
		t.Error("EnableTruncation")
	}
	if cfg.OutboxPollInterval != 250*time.Millisecond {
		t.Errorf("OutboxPollInterval: %v", cfg.OutboxPollInterval)
	}
}

func TestEnvOverridesToml(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "fleuve.toml")
	if err := os.WriteFile(p, []byte(`
[fleuve]
max_inflight = 1
`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEUVE_MAX_INFLIGHT", "42")
	t.Setenv("FLEUVE_DATABASE_URL", "postgres://env")
	t.Setenv("FLEUVE_ENABLE_JETSTREAM", "true")
	t.Setenv("FLEUVE_OUTBOX_POLL_INTERVAL", "500ms")
	cfg, err := LoadFleuveToml(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxInflight != 42 {
		t.Errorf("MaxInflight: %d", cfg.MaxInflight)
	}
	if cfg.DatabaseURL != "postgres://env" {
		t.Errorf("DatabaseURL: %q", cfg.DatabaseURL)
	}
	if !cfg.EnableJetstream {
		t.Error("EnableJetstream")
	}
	if cfg.OutboxPollInterval != 500*time.Millisecond {
		t.Errorf("OutboxPollInterval: %v", cfg.OutboxPollInterval)
	}
}

func TestParseBool(t *testing.T) {
	for _, s := range []string{"1", "true", "TRUE", "yes"} {
		if !parseBool(s) {
			t.Errorf("parseBool(%q)", s)
		}
	}
	for _, s := range []string{"", "0", "false", "no", "maybe"} {
		if parseBool(s) {
			t.Errorf("parseBool(%q) should be false", s)
		}
	}
}
