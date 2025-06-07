package sqlt

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"unicode"

	"github.com/jmoiron/sqlx"
)

func Wrap(db *sqlx.DB) DB {
	return &sqlxDB{db: db}
}

func Open(driverName, dataSourceName string) (DB, error) {
	db, err := sqlx.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	mapper := defaultMapper.Load()
	if mapper != nil {
		db.MapperFunc(*mapper)
	}
	return &sqlxDB{db: db}, nil
}

func SetDefaultMapper(mapper func(string) string) {
	defaultMapper.Store(&mapper)
}

func init() {
	defaultMapper.Store(&camalCaseMapper)
}

var defaultMapper atomic.Pointer[func(string) string]

var camalCaseMapper = func(s string) string {
	var buf strings.Builder
	buf.Grow(len(s) + 3)
	for i, r := range s {
		if unicode.IsUpper(r) {
			buf.WriteRune(unicode.ToLower(r))
			// Handles acronyms like HTTP, API, etc.
			if i > 0 && i+1 < len(s) && unicode.IsLower(rune(s[i+1])) {
				buf.WriteByte('_')
			}
		} else {
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

type DB interface {
	SQLX() *sqlx.DB
	Exec(query string, args ...any) (Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (Result, error)
	IDExec(query string, args ...any) (int64, error)
	IDExecContext(ctx context.Context, query string, args ...any) (int64, error)
	AffectedExec(query string, args ...any) (int, error)
	AffectedExecContext(ctx context.Context, query string, args ...any) (int, error)
	Query(query string, args ...any) (*sqlx.Rows, error)
	QueryRow(query string, args ...any) *sqlx.Row
	Prepare(query string) (*sqlx.Stmt, error)
	Preparex(query string) (*sqlx.Stmt, error)
	Rebind(query string) string
	DriverName() string
	BindNamed(query string, arg any) (string, []any, error)
	Get(dest any, query string, args ...any) error
	GetIn(dest any, query string, args ...any) error
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	GetInContext(ctx context.Context, dest any, query string, args ...any) error
	Select(dest any, query string, args ...any) error
	SelectIn(dest any, query string, args ...any) error
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	SelectInSeq(query string, args ...any) *RowsSeq
	SelectSeq(query string, args ...any) *RowsSeq
	NamedExec(query string, arg any) (sql.Result, error)
	NamedQuery(query string, arg any) (*sqlx.Rows, error)
	Close() error

	Tx(fn func(tx Tx) error) error
	TxImm(fn func(tx Tx) error) error
	Txc(ctx context.Context, fn func(tx Tx) error) error
	TxcImm(ctx context.Context, fn func(tx Tx) error) error
}

// DBReader is an interface for reading from the database, implemented by DB and Tx.
type DBReader interface {
	Get(dest any, query string, args ...any) error
	Select(dest any, query string, args ...any) error
}

type sqlxDB struct {
	db         *sqlx.DB
	immidateDB *sqlx.DB
}

func (s *sqlxDB) SQLX() *sqlx.DB {
	return s.db
}

func (s *sqlxDB) Exec(query string, args ...any) (Result, error) {
	r, err := s.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return sqltResult{r}, nil
}

func (s *sqlxDB) ExecContext(ctx context.Context, query string, args ...any) (Result, error) {
	r, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return sqltResult{r}, nil
}

func (s *sqlxDB) IDExec(query string, args ...any) (int64, error) {
	r, err := s.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *sqlxDB) IDExecContext(ctx context.Context, query string, args ...any) (int64, error) {
	r, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *sqlxDB) AffectedExec(query string, args ...any) (int, error) {
	r, err := s.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := r.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(rowsAffected), nil
}

func (s *sqlxDB) AffectedExecContext(ctx context.Context, query string, args ...any) (int, error) {
	r, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := r.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(rowsAffected), nil
}

func (s *sqlxDB) Query(query string, args ...any) (*sqlx.Rows, error) {
	return s.db.Queryx(query, args...)
}

func (s *sqlxDB) QueryRow(query string, args ...any) *sqlx.Row {
	return s.db.QueryRowx(query, args...)
}

func (s *sqlxDB) Prepare(query string) (*sqlx.Stmt, error) {
	return s.db.Preparex(query)
}

func (s *sqlxDB) Preparex(query string) (*sqlx.Stmt, error) {
	return s.db.Preparex(query)
}

func (s *sqlxDB) Rebind(query string) string {
	return s.db.Rebind(query)
}

func (s *sqlxDB) DriverName() string {
	return s.db.DriverName()
}

func (s *sqlxDB) BindNamed(query string, arg any) (string, []any, error) {
	return s.db.BindNamed(query, arg)
}

func (s *sqlxDB) Get(dest any, query string, args ...any) error {
	return s.db.Get(dest, query, args...)
}

func (s *sqlxDB) GetIn(dest any, query string, args ...any) error {
	q, p, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	return s.db.GetContext(context.Background(), dest, q, p...)
}

func (s *sqlxDB) GetInContext(ctx context.Context, dest any, query string, args ...any) error {
	q, p, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	return s.db.GetContext(ctx, dest, q, p...)
}

func (s *sqlxDB) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	return s.db.GetContext(ctx, dest, query, args...)
}

func (s *sqlxDB) Select(dest any, query string, args ...any) error {
	return s.db.Select(dest, query, args...)
}

func (s *sqlxDB) SelectIn(dest any, query string, args ...any) error {
	q, p, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	return s.db.SelectContext(context.Background(), dest, q, p...)
}

func (s *sqlxDB) SelectInSeq(query string, args ...any) *RowsSeq {
	q, p, err := sqlx.In(query, args...)
	if err != nil {
		return &RowsSeq{err: err}
	}
	rows, err := s.db.Queryx(q, p...)
	return &RowsSeq{
		rows: rows,
		err:  err,
	}
}

func (s *sqlxDB) SelectSeq(query string, args ...any) *RowsSeq {
	rows, err := s.db.Queryx(query, args...)
	return &RowsSeq{
		rows: rows,
		err:  err,
	}
}

func (s *sqlxDB) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	return s.db.SelectContext(ctx, dest, query, args...)
}

func (s *sqlxDB) NamedExec(query string, arg any) (sql.Result, error) {
	return s.db.NamedExec(query, arg)
}

func (s *sqlxDB) NamedQuery(query string, arg any) (*sqlx.Rows, error) {
	return s.db.NamedQuery(query, arg)
}

func (s *sqlxDB) Close() error {
	return s.db.Close()
}

func (s *sqlxDB) Tx(fn func(tx Tx) error) error {
	return transaction(context.Background(), s.db, false, fn)
}

func (s *sqlxDB) Txc(ctx context.Context, fn func(tx Tx) error) error {
	return transaction(ctx, s.db, false, fn)
}

func (s *sqlxDB) TxImm(fn func(tx Tx) error) error {
	return transaction(context.Background(), s.db, true, fn)
}

func (s *sqlxDB) TxcImm(ctx context.Context, fn func(tx Tx) error) error {
	return transaction(ctx, s.db, true, fn)
}
