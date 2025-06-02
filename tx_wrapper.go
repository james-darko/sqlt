package sqlt

import (
	"context" // Added import for context

	"github.com/jmoiron/sqlx"
)

type txWrapper struct {
	tx *sqlx.Tx
}

func (tx *txWrapper) Exec(query string, args ...any) (Result, error) {
	r, err := tx.tx.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return sqltResult{r}, nil
}

func (tx *txWrapper) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	return tx.tx.GetContext(ctx, dest, query, args...)
}

func (tx *txWrapper) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	return tx.tx.SelectContext(ctx, dest, query, args...)
}

func (tx *txWrapper) MustExec(query string, args ...any) Result {
	res, err := tx.Exec(query, args...)
	if err != nil {
		panic(Error{err})
	}
	return sqltResult{res}
}

func (tx *txWrapper) IDExec(query string, args ...any) (int64, error) {
	r, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (tx *txWrapper) AffectedExec(query string, args ...any) (int, error) {
	r, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := r.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(rowsAffected), nil
}

func (tx *txWrapper) Query(query string, args ...any) (*sqlx.Rows, error) {
	r, err := tx.tx.Queryx(query, args...)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (tx *txWrapper) MustQuery(query string, args ...any) *sqlx.Rows {
	r, err := tx.Query(query, args...)
	if err != nil {
		panic(Error{err})
	}
	return r
}

func (tx *txWrapper) QueryRow(query string, args ...any) *sqlx.Row {
	r := tx.tx.QueryRowx(query, args...)
	if r == nil {
		return nil
	}
	return r
}

func (tx *txWrapper) MustQueryRow(query string, args ...any) *sqlx.Row {
	r := tx.QueryRow(query, args...)
	if r == nil {
		panic(Error{nil})
	}
	return r
}

func (tx *txWrapper) Get(dest any, query string, args ...any) error {
	err := tx.tx.Get(dest, query, args...)
	if err != nil {
		return err
	}
	return nil
}

func (tx *txWrapper) MustGet(dest any, query string, args ...any) {
	err := tx.Get(dest, query, args...)
	if err != nil {
		panic(Error{err})
	}
}

func (tx *txWrapper) GetIn(dest any, query string, args ...any) error {
	p, q, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	return tx.tx.Get(dest, p, q...)
}

func (tx *txWrapper) MustGetIn(dest any, query string, args ...any) {
	err := tx.GetIn(dest, query, args...)
	if err != nil {
		panic(Error{err})
	}
}

func (tx *txWrapper) Select(dest any, query string, args ...any) error {
	err := tx.tx.Select(dest, query, args...)
	if err != nil {
		return err
	}
	return nil
}

func (tx *txWrapper) MustSelect(dest any, query string, args ...any) {
	err := tx.Select(dest, query, args...)
	if err != nil {
		panic(Error{err})
	}
}

func (tx *txWrapper) Commit() error {
	err := tx.tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (tx *txWrapper) Rollback() error {
	err := tx.tx.Rollback()
	if err != nil {
		return err
	}
	return nil
}

func (tx *txWrapper) DriverName() string {
	return tx.tx.DriverName()
}

func (tx *txWrapper) Rebind(query string) string {
	return tx.tx.Rebind(query)
}

func (tx *txWrapper) SelectIn(dest any, query string, args ...any) error {
	p, q, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	return tx.tx.Select(dest, p, q...)
}

func (tx *txWrapper) MustSelectIn(dest any, query string, args ...any) {
	err := tx.SelectIn(dest, query, args...)
	if err != nil {
		panic(Error{err})
	}
}

func (tx *txWrapper) NamedExec(query string, arg any) (Result, error) {
	r, err := tx.tx.NamedExec(query, arg)
	if err != nil {
		return nil, err
	}
	return sqltResult{r}, nil
}
