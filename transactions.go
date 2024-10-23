package sqlt

import (
	"context"

	"github.com/jmoiron/sqlx"
)

func Tx(db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
	return transaction(db, false, fn)
}

func TxContext(ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
	return transactionContext(ctx, db, false, fn)
}

func TxImm(db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
	return transaction(db, true, fn)
}

func TxImmContext(ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
	return transactionContext(ctx, db, true, fn)
}

func transaction(db *sqlx.DB, write bool, fn func(tx *sqlx.Tx) error) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	if write {
		driver := db.DriverName()
		if driver == "libsql" || driver == "sqlite3" {
			_, err = tx.Exec("ROLLBACK")
			if err != nil {
				return err
			}
			_, err = tx.Exec("BEGIN IMMEDIATE")
			if err != nil {
				return err
			}
		}
	}
	defer tx.Rollback()
	err = fn(tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func transactionContext(ctx context.Context, db *sqlx.DB, write bool, fn func(tx *sqlx.Tx) error) error {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	if write {
		driver := db.DriverName()
		if driver == "libsql" || driver == "sqlite3" {
			_, err = tx.Exec("ROLLBACK")
			if err != nil {
				return err
			}
			_, err = tx.Exec("BEGIN IMMEDIATE")
			if err != nil {
				return err
			}
		}
	}
	defer tx.Rollback()
	err = fn(tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}
