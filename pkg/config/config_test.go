package config

import (
	"os"
	"testing"
)

func TestLoadConfig_fromFile(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	content := `[fleuve]
database_url = "postgres://fromfile"
nats_url = "nats://local:4222"
enable_jetstream = true
snapshot_interval = 10
`
	if err := os.WriteFile("fleuve.toml", []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig("fleuve.toml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://fromfile" {
		t.Fatalf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.NATSURL != "nats://local:4222" {
		t.Fatalf("NATSURL = %q", cfg.NATSURL)
	}
	if !cfg.EnableJetStream {
		t.Fatal("EnableJetStream should be true")
	}
	if cfg.SnapshotInterval != 10 {
		t.Fatalf("SnapshotInterval = %d", cfg.SnapshotInterval)
	}
}

func TestLoadConfig_envOverridesToml(t *testing.T) {
	t.Setenv("FLEUVE_NAMESPACE", "from-env")
	t.Setenv("FLEUVE_ENABLE_TRUNCATION", "true")

	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.WriteFile("fleuve.toml", []byte(`[fleuve]
namespace = "from-toml"
enable_truncation = false
`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig("fleuve.toml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Namespace != "from-env" {
		t.Fatalf("namespace: got %q, want env override", cfg.Namespace)
	}
	if !cfg.EnableTruncation {
		t.Fatal("enable_truncation should be overridden by FLEUVE_ENABLE_TRUNCATION")
	}
}

func TestLoadConfig_missingFleuveTable(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.WriteFile("fleuve.toml", []byte(`other = 1
`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig("fleuve.toml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "" {
		t.Fatalf("expected empty defaults, got database_url %q", cfg.DatabaseURL)
	}
}
