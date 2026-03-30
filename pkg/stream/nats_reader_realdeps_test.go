//go:build realdeps

package stream

import (
	"os"
	"testing"
)

func TestNATSReaderConnectAndClose(t *testing.T) {
	url := os.Getenv("FLEUVE_NATS_URL")
	if url == "" {
		t.Skip("set FLEUVE_NATS_URL for NATS reader realdeps smoke test")
	}
	r, err := NewNATSReader(url, "RealdepsSmokeWorkflow")
	if err != nil {
		t.Fatalf("NewNATSReader: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
