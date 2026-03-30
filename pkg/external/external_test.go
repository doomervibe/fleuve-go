package external

import "testing"

func TestNewExternalMessageConsumer(t *testing.T) {
	c := NewExternalMessageConsumer()
	if c == nil {
		t.Fatal("nil consumer")
	}
}
