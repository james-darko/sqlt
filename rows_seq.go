package sqlt

import "github.com/jmoiron/sqlx"

type RowsSeq struct {
	err  error
	rows *sqlx.Rows
}

var emptySeq = func(func(any) bool) {}

func (e *RowsSeq) Iter(dest any) func(func(any) bool) {
	if e.err != nil {
		return emptySeq
	} else if err := e.rows.Err(); err != nil {
		e.err = err
		return emptySeq
	} else {
		return func(fn func(any) bool) {
			for e.rows.Next() {
				if err := e.rows.StructScan(dest); err != nil {
					e.err = err
					e.rows.Close()
					return
				}
				if !fn(dest) {
					e.err = e.rows.Close()
					return
				}
			}
			if err := e.rows.Err(); err != nil {
				e.err = err
			} else {
				e.err = e.rows.Close()
			}
		}
	}
}

func (e *RowsSeq) Err() error {
	return e.err
}
