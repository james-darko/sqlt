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
	ExecMust(query string, args ...any) Result
	Query(query string, args ...any) (*sqlx.Rows, error)
	QueryMust(query string, args ...any) *sqlx.Rows
	QueryRow(query string, args ...any) *sqlx.Row
	QueryRowMust(query string, args ...any) *sqlx.Row
	Get(dest any, query string, args ...any) error
	GetMust(dest any, query string, args ...any) error
	Select(dest any, query string, args ...any) error
	SelectMust(dest any, query string, args ...any) error
	SelectIn(dest any, query string, args ...any) error
	SelectInMust(dest any, query string, args ...any) error
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

func (tx *sqlxTx) ExecMust(query string, args ...any) Result {
	res, err := tx.Exec(query, args...)
	if err != nil {
		panic(Error{err})
	}
	return sqltResult{res}
}

func (tx *sqlxTx) Query(query string, args ...any) (*sqlx.Rows, error) {
	return tx.conn.QueryxContext(tx.ctx, query, args...)
}

func (tx *sqlxTx) QueryMust(query string, args ...any) *sqlx.Rows {
	rows, err := tx.Query(query, args...)
	if err != nil {
		panic(Error{err})
	}
	return rows
}

func (tx *sqlxTx) QueryRow(query string, args ...any) *sqlx.Row {
	return tx.conn.QueryRowxContext(tx.ctx, query, args...)
}

func (tx *sqlxTx) QueryRowMust(query string, args ...any) *sqlx.Row {
	row := tx.QueryRow(query, args...)
	if row == nil {
		panic(Error{err: sql.ErrNoRows})
	}
	return row
}

func (tx *sqlxTx) Get(dest any, query string, args ...any) error {
	return tx.conn.GetContext(tx.ctx, dest, query, args...)
}

func (tx *sqlxTx) GetMust(dest any, query string, args ...any) error {
	err := tx.Get(dest, query, args...)
	if err != nil {
		panic(Error{err})
	}
	return err
}

func (tx *sqlxTx) Select(dest any, query string, args ...any) error {
	return tx.conn.SelectContext(tx.ctx, dest, query, args...)
}

func (tx *sqlxTx) SelectMust(dest any, query string, args ...any) error {
	err := tx.Select(dest, query, args...)
	if err != nil {
		panic(Error{err})
	}
	return err
}

func (tx *sqlxTx) SelectIn(dest any, query string, args ...any) error {
	p, q, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	return tx.conn.SelectContext(tx.ctx, dest, p, q)
}

func (tx *sqlxTx) SelectInMust(dest any, query string, args ...any) error {
	err := tx.SelectIn(dest, query, args...)
	if err != nil {
		panic(Error{err})
	}
	return err
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

func (tx *sqlxTx) Rebind(query string) string {
	return tx.conn.Rebind(query)
}

func (tx *sqlxTx) DriverName() string {
	return tx.driverName
}
