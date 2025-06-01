package sqlt

import "github.com/jmoiron/sqlx"

// Handle interface that both DB and Tx implement.
type Handle interface {
	Exec(query string, args ...any) (Result, error)
	Query(query string, args ...any) (*sqlx.Rows, error)
	QueryRow(query string, args ...any) *sqlx.Row
	Get(dest any, query string, args ...any) error
	GetIn(dest any, query string, args ...any) error
	Select(dest any, query string, args ...any) error
	SelectIn(dest any, query string, args ...any) error
}

// If err is not nil, it panics with the error wrapped in the sqlt.Error type.
// Otherswise, it returns the value param
func Mustv[T any](value T, err error) T {
	if err != nil {
		panic(Error{err})
	}
	return value
}

// If err is not nil, it panics with the error wrapped in the sqlt.Error type.
func Must(err error) {
	if err != nil {
		panic(Error{err})
	}
}
