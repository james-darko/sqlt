package sqlt

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

func transaction(ctx context.Context, db *sqlx.DB, imm bool, fn func(conn Tx) error) (rErr error) {
	driver := db.DriverName()
	if driver != "libsql" && driver != "sqlite3" {
		return fmt.Errorf("transactionImm is only supported for libsql and sqlite3 drivers")
	}
	conn, err := db.Connx(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()
	if imm {
		_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE")
	} else {
		_, err = conn.ExecContext(ctx, "BEGIN")
	}
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		if err := recover(); err != nil {
			if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr != nil {
				fmt.Printf("failed to rollback transaction in panic: %v", rollbackErr)
			}
			var e Error
			if ok := errors.As(err.(error), &e); ok {
				rErr = e
			} else {
				panic(err) // Re-panic to propagate the error
			}
		}
	}()
	tx := &sqlxTx{
		ctx:        ctx,
		conn:       conn,
		driverName: driver,
	}
	err = fn(tx)
	if err != nil {
		if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr != nil {
			return fmt.Errorf("failed to rollback transaction: %w", rollbackErr)
		}
		return err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}
