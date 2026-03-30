package truncation

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNewTruncationService(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	svc := NewTruncationService(db, "MyWorkflow")
	if svc == nil {
		t.Fatal("nil service")
	}
	// Exercise option
	svc2 := NewTruncationService(db, "T", WithBatchSize(50))
	if svc2 == nil {
		t.Fatal("nil service with option")
	}
}
