package uiembed

import (
	"bytes"
	"testing"
)

func TestIndexHTML_replacesPlaceholder(t *testing.T) {
	b := IndexHTML("Acme")
	if !bytes.Contains(b, []byte("Acme UI")) {
		t.Fatalf("expected title containing Acme UI, got %q", string(b))
	}
	if bytes.Contains(b, []byte("{{project_title}}")) {
		t.Fatal("placeholder should be replaced")
	}
}

func TestIndexHTML_defaultTitle(t *testing.T) {
	b := IndexHTML("")
	if !bytes.Contains(b, []byte("Fleuve UI")) {
		t.Fatalf("expected default Fleuve UI, got %q", string(b))
	}
}

func TestDistFS(t *testing.T) {
	fsys := DistFS()
	_, err := fsys.Open("index.html")
	if err != nil {
		t.Fatalf("DistFS open index.html: %v", err)
	}
}
