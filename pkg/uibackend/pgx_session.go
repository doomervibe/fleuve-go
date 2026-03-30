package uibackend

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGXPoolSessionMaker creates one pooled connection per HTTP handler session (Acquire/Release).
type PGXPoolSessionMaker struct {
	pool *pgxpool.Pool
}

// NewPGXPoolSessionMaker returns a SessionMaker backed by PostgreSQL (pgx pool).
func NewPGXPoolSessionMaker(pool *pgxpool.Pool) *PGXPoolSessionMaker {
	return &PGXPoolSessionMaker{pool: pool}
}

func (m *PGXPoolSessionMaker) NewSession(ctx context.Context) (Session, error) {
	if m == nil || m.pool == nil {
		return nil, fmt.Errorf("nil pool")
	}
	c, err := m.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &pgxSession{conn: c}, nil
}

type pgxSession struct {
	conn *pgxpool.Conn
}

func (s *pgxSession) Close() error {
	if s.conn != nil {
		s.conn.Release()
		s.conn = nil
	}
	return nil
}

func (s *pgxSession) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := s.conn.Exec(ctx, query, args...)
	return err
}

func (s *pgxSession) QueryRow(ctx context.Context, query string, args ...interface{}) (Row, error) {
	return &pgxQueryRow{row: s.conn.QueryRow(ctx, query, args...)}, nil
}

type pgxQueryRow struct {
	row pgx.Row
}

func (r *pgxQueryRow) Scan(dest ...interface{}) error {
	return r.row.Scan(dest...)
}

func (s *pgxSession) Query(ctx context.Context, query string, args ...interface{}) ([]Row, error) {
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		cp := make([]any, len(vals))
		copy(cp, vals)
		out = append(out, &materializedRow{columns: cp})
	}
	return out, rows.Err()
}

type materializedRow struct {
	columns []any
}

func (r *materializedRow) Scan(dest ...interface{}) error {
	if len(dest) != len(r.columns) {
		return fmt.Errorf("scan mismatch: %d destinations for %d columns", len(dest), len(r.columns))
	}
	for i, d := range dest {
		if err := scanAssign(d, r.columns[i]); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
	}
	return nil
}

func scanAssign(dest interface{}, src any) error {
	if dest == nil {
		return fmt.Errorf("nil destination")
	}
	if src == nil {
		return assignNull(dest)
	}

	switch d := dest.(type) {
	case *string:
		s, err := asString(src)
		if err != nil {
			return err
		}
		*d = s
		return nil

	case **string:
		if isNullish(src) {
			*d = nil
			return nil
		}
		s, err := asString(src)
		if err != nil {
			return err
		}
		*d = &s
		return nil

	case *int:
		n, err := asInt64(src)
		if err != nil {
			return err
		}
		if n > math.MaxInt32 || n < math.MinInt32 {
			return fmt.Errorf("int overflow")
		}
		*d = int(n)
		return nil

	case *int64:
		n, err := asInt64(src)
		if err != nil {
			return err
		}
		*d = n
		return nil

	case *time.Time:
		t, err := asTime(src)
		if err != nil {
			return err
		}
		*d = t
		return nil

	case **time.Time:
		if isNullish(src) {
			*d = nil
			return nil
		}
		t, err := asTime(src)
		if err != nil {
			return err
		}
		*d = &t
		return nil

	case *[]byte:
		b, err := asBytes(src)
		if err != nil {
			return err
		}
		if b == nil {
			*d = nil
		} else {
			cp := make([]byte, len(b))
			copy(cp, b)
			*d = cp
		}
		return nil

	case *json.RawMessage:
		b, err := asBytes(src)
		if err != nil {
			return err
		}
		if b == nil {
			*d = nil
		} else {
			cp := make([]byte, len(b))
			copy(cp, b)
			*d = json.RawMessage(cp)
		}
		return nil

	case *any:
		*d = src
		return nil

	default:
		return fmt.Errorf("unsupported scan destination %T", dest)
	}
}

func assignNull(dest interface{}) error {
	switch d := dest.(type) {
	case **string:
		*d = nil
		return nil
	case **time.Time:
		*d = nil
		return nil
	case *string:
		*d = ""
		return nil
	case *[]byte:
		*d = nil
		return nil
	case *int:
		*d = 0
		return nil
	case *int64:
		*d = 0
		return nil
	case *time.Time:
		return fmt.Errorf("cannot scan NULL into *time.Time")
	default:
		return fmt.Errorf("cannot scan NULL into %T", dest)
	}
}

func isNullish(src any) bool {
	if src == nil {
		return true
	}
	switch v := src.(type) {
	case pgtype.Text:
		return !v.Valid
	case pgtype.Timestamptz:
		return !v.Valid
	case pgtype.Int8:
		return !v.Valid
	default:
		return false
	}
}

func asString(src any) (string, error) {
	switch v := src.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case pgtype.Text:
		if !v.Valid {
			return "", nil
		}
		return v.String, nil
	default:
		return fmt.Sprintf("%v", src), nil
	}
}

func asInt64(src any) (int64, error) {
	switch v := src.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("uint64 overflow")
		}
		return int64(v), nil
	case float64:
		return int64(v), nil
	case pgtype.Int8:
		if !v.Valid {
			return 0, nil
		}
		return v.Int64, nil
	case pgtype.Int4:
		if !v.Valid {
			return 0, nil
		}
		return int64(v.Int32), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", src)
	}
}

func asTime(src any) (time.Time, error) {
	switch v := src.(type) {
	case time.Time:
		return v, nil
	case pgtype.Timestamptz:
		if !v.Valid {
			return time.Time{}, fmt.Errorf("invalid timestamptz")
		}
		return v.Time, nil
	case pgtype.Timestamp:
		if !v.Valid {
			return time.Time{}, fmt.Errorf("invalid timestamp")
		}
		return v.Time, nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", src)
	}
}

func asBytes(src any) ([]byte, error) {
	switch v := src.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case json.RawMessage:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to []byte", src)
	}
}
