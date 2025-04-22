package sqlt

import "database/sql"

type Result interface {
	// LastInsertId returns the last inserted ID.
	// It is only valid after an INSERT statement.
	LastInsertId() (int64, error)
	// LastInsertIdMust returns the last inserted ID.
	// It is only valid after an INSERT statement.
	// It panics if the last inserted ID is not available.
	LastInsertIdMust() int64

	// RowsAffected returns the number of rows affected by the last statement.
	// It is only valid after an UPDATE or DELETE statement.
	RowsAffected() (int64, error)
	// RowsAffectedMust returns the number of rows affected by the last statement.
	// It is only valid after an UPDATE or DELETE statement.
	// It panics if the number of rows affected is not available.
	RowsAffectedMust() int64
}

type sqltResult struct {
	r sql.Result
}

func (r sqltResult) LastInsertId() (int64, error) {
	id, err := r.r.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r sqltResult) LastInsertIdMust() int64 {
	id, err := r.LastInsertId()
	if err != nil {
		panic(Error{err})
	}
	return id
}

func (r sqltResult) RowsAffected() (int64, error) {
	affected, err := r.r.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func (r sqltResult) RowsAffectedMust() int64 {
	affected, err := r.RowsAffected()
	if err != nil {
		panic(Error{err})
	}
	return affected
}
