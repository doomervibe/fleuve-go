// Package integration_test holds cross-cutting tests. Most are plain unit/smoke tests;
// files with //go:build realdeps exercise live Postgres/NATS (I/O-backed), not full E2E.
package integration_test

import (
	"testing"

	"github.com/doomervibe/fleuve-go/pkg/config"
)

// Config still resolves when no fleuve.toml is present (CI / compose set FLEUVE_*).
func TestConfigFromEnvironment(t *testing.T) {
	cfg, err := config.LoadFleuveToml("")
	if err != nil {
		t.Fatalf("LoadFleuveToml: %v", err)
	}
	if cfg.MaxInflight <= 0 {
		t.Errorf("MaxInflight should be positive, got %d", cfg.MaxInflight)
	}
}
