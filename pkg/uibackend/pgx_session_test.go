package uibackend

import (
	"testing"
	"time"
)

func TestMaterializedRowScan(t *testing.T) {
	at := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	r := &materializedRow{columns: []any{"CounterWorkflow", int64(3), int64(10), at, []byte(`{"x":1}`)}}
	var wt string
	var wc, ec int
	var last *time.Time
	var body []byte
	if err := r.Scan(&wt, &wc, &ec, &last, &body); err != nil {
		t.Fatal(err)
	}
	if wt != "CounterWorkflow" || wc != 3 || ec != 10 || last == nil || !last.Equal(at) {
		t.Fatalf("got %q %d %d %v", wt, wc, ec, last)
	}
	if string(body) != `{"x":1}` {
		t.Fatalf("body %s", body)
	}
}

func TestMaterializedRowScanNullable(t *testing.T) {
	r := &materializedRow{columns: []any{nil, "x"}}
	var lastAt *time.Time
	var s string
	if err := r.Scan(&lastAt, &s); err != nil {
		t.Fatal(err)
	}
	if lastAt != nil || s != "x" {
		t.Fatalf("got lastAt=%v s=%q", lastAt, s)
	}
}

func TestPGXPoolSessionMakerNilPool(t *testing.T) {
	m := NewPGXPoolSessionMaker(nil)
	_, err := m.NewSession(t.Context())
	if err == nil {
		t.Fatal("expected error")
	}
}
