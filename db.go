package sqlt

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

func Wrap(db *sqlx.DB) DB {
	return &sqlxDB{db: db}
}

type DB interface {
	SQLX() *sqlx.DB
	Exec(query string, args ...any) (Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (Result, error)
	Query(query string, args ...any) (*sqlx.Rows, error)
	QueryRow(query string, args ...any) *sqlx.Row
	Prepare(query string) (*sqlx.Stmt, error)
	Preparex(query string) (*sqlx.Stmt, error)
	Rebind(query string) string
	DriverName() string
	BindNamed(query string, arg any) (string, []any, error)
	Get(dest any, query string, args ...any) error
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	Select(dest any, query string, args ...any) error
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	NamedExec(query string, arg any) (sql.Result, error)
	NamedQuery(query string, arg any) (*sqlx.Rows, error)

	Tx(fn func(tx Tx) error) error
	TxImm(fn func(tx Tx) error) error
	Txc(ctx context.Context, fn func(tx Tx) error) error
	TxcImm(ctx context.Context, fn func(tx Tx) error) error
}

type sqlxDB struct {
	db *sqlx.DB
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

func (s *sqlxDB) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	return s.db.GetContext(ctx, dest, query, args...)
}

func (s *sqlxDB) Select(dest any, query string, args ...any) error {
	return s.db.Select(dest, query, args...)
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
