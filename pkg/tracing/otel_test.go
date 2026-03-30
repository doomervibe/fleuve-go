package tracing

import "testing"

func TestLoadOTelConfigFromEnv(t *testing.T) {
	cfg := LoadOTelConfigFromEnv()
	if cfg.ServiceName != "fleuve" {
		t.Fatalf("default service name: %q", cfg.ServiceName)
	}
}
