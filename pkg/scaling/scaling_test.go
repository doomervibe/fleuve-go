package scaling

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestGetMaxOffsetSqlmock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT COALESCE\(MAX\(last_read_event_no\), 0\) FROM offsets WHERE reader LIKE \$1`).
		WithArgs("r%").
		WillReturnRows(sqlmock.NewRows([]string{"coalesce"}).AddRow(int64(42)))

	v, err := GetMaxOffset(context.Background(), db, "r")
	if err != nil {
		t.Fatal(err)
	}
	if v != 42 {
		t.Fatalf("offset %d", v)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
