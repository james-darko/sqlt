package sqlt

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type Tx interface {
	// Commit() error
	// Rollback() error
	Exec(query string, args ...any) (Result, error)
	MustExec(query string, args ...any) Result
	Query(query string, args ...any) (*sqlx.Rows, error)
	MustQuery(query string, args ...any) *sqlx.Rows
	QueryRow(query string, args ...any) *sqlx.Row
	MustQueryRow(query string, args ...any) *sqlx.Row
	Get(dest any, query string, args ...any) error
	MustGet(dest any, query string, args ...any)
	Select(dest any, query string, args ...any) error
	MustSelect(dest any, query string, args ...any)
	SelectIn(dest any, query string, args ...any) error
	MustSelectIn(dest any, query string, args ...any)
	NamedExec(query string, arg any) (Result, error)
	// Prepare(query string) (*sql.Stmt, error)
	// Preparex(query string) (*sqlx.Stmt, error)
	// Stmtx(st any) *sqlx.Stmt
	Rebind(query string) string
	DriverName() string
}

type sqlxTx struct {
	ctx        context.Context
	conn       *sqlx.Conn
	driverName string
}

// func (tx *sqlxTx) Commit() error {
// 	_, err := tx.conn.ExecContext(tx.ctx, "COMMIT")
// 	return err
// }
//
// func (tx *sqlxTx) Rollback() error {
// 	_, err := tx.conn.ExecContext(tx.ctx, "ROLLBACK")
// 	return err
// }

func (tx *sqlxTx) Exec(query string, args ...any) (Result, error) {
	r, err := tx.conn.ExecContext(tx.ctx, query, args...)
	return sqltResult{r}, err
}

func (tx *sqlxTx) MustExec(query string, args ...any) Result {
	res, err := tx.Exec(query, args...)
	if err != nil {
		panic(Error{err})
	}
	return sqltResult{res}
}

func (tx *sqlxTx) Query(query string, args ...any) (*sqlx.Rows, error) {
	return tx.conn.QueryxContext(tx.ctx, query, args...)
}

func (tx *sqlxTx) MustQuery(query string, args ...any) *sqlx.Rows {
	rows, err := tx.Query(query, args...)
	if err != nil {
		panic(Error{err})
	}
	return rows
}

func (tx *sqlxTx) QueryRow(query string, args ...any) *sqlx.Row {
	return tx.conn.QueryRowxContext(tx.ctx, query, args...)
}

func (tx *sqlxTx) MustQueryRow(query string, args ...any) *sqlx.Row {
	row := tx.QueryRow(query, args...)
	if row == nil {
		panic(Error{err: sql.ErrNoRows})
	}
	return row
}

func (tx *sqlxTx) Get(dest any, query string, args ...any) error {
	return tx.conn.GetContext(tx.ctx, dest, query, args...)
}

func (tx *sqlxTx) MustGet(dest any, query string, args ...any) {
	err := tx.Get(dest, query, args...)
	if err != nil {
		panic(Error{err})
	}
}

func (tx *sqlxTx) Select(dest any, query string, args ...any) error {
	return tx.conn.SelectContext(tx.ctx, dest, query, args...)
}

func (tx *sqlxTx) MustSelect(dest any, query string, args ...any) {
	err := tx.Select(dest, query, args...)
	if err != nil {
		panic(Error{err})
	}
}

func (tx *sqlxTx) SelectIn(dest any, query string, args ...any) error {
	p, q, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	return tx.conn.SelectContext(tx.ctx, dest, p, q)
}

func (tx *sqlxTx) MustSelectIn(dest any, query string, args ...any) {
	err := tx.SelectIn(dest, query, args...)
	if err != nil {
		panic(Error{err})
	}
}

// func (tx *sqlxTx) Prepare(query string) (*sql.Stmt, error) {
// 	return tx.conn.PrepareContext(context.Background(), query)
// }
//
// func (tx *sqlxTx) Preparex(query string) (*sqlx.Stmt, error) {
// 	return tx.conn.PreparexContext(context.Background(), query)
// }
//
// func (tx *sqlxTx) Stmtx(st any) *sqlx.Stmt {
// 	return tx.conn.Stmtx(st)
// }

// func (tx *sqlxTx) NamedExec(query string, arg any) (Result, error) {
// 	r, err := tx.conn.NamedExec(tx.ctx, query, arg)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return sqltResult{r}, nil
// }

func (tx *sqlxTx) Rebind(query string) string {
	return tx.conn.Rebind(query)
}

func (tx *sqlxTx) DriverName() string {
	return tx.driverName
}
