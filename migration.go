package sqlt

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"

	rsql "github.com/rqlite/sql"
)

func PrintTables(ctx context.Context, db DB) error {
	var tables []struct {
		Name string `db:"name"`
		Sql  string `db:"sql"`
	}
	err := db.Select(&tables, "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return fmt.Errorf("could not get tables: %w", err)
	}
	if len(tables) == 0 {
		fmt.Println("no tables found")
		return nil
	}
	fmt.Println("tables:")
	for _, table := range tables {
		fmt.Println("-", table)
	}
	return nil
}

func masterRows(ctx context.Context, db DB) ([]masterRow, error) {
	var rows []masterRow
	err := db.SelectContext(ctx, &rows, "SELECT name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")
	if err != nil {
		return nil, err
	}
	return rows, nil
}

type masterRow struct {
	Name string `db:"name"`
	Sql  string `db:"sql"`
}

var firstQuoted = regexp.MustCompile(`"([^"]+)"`)

func VerifyString(ctx context.Context, db DB, sql string) error {
	return Verify(ctx, db, strings.NewReader(sql))
}

func Verify(ctx context.Context, db DB, reader io.Reader) error {
	masterRows, err := masterRows(ctx, db)
	if err != nil {
		return fmt.Errorf("could not get master rows: %w", err)
	}
	type entry struct {
		sql   string
		found bool
	}
	expected := make(map[string]entry)
	for _, row := range masterRows {
		parser := rsql.NewParser(strings.NewReader(row.Sql))
		stmt, err := parser.ParseStatement()
		if err != nil {
			return fmt.Errorf("could not parse sql for table %s: %w", row.Name, err)
		}
		expected[row.Name] = entry{sql: stmt.String()}
	}
	parser := rsql.NewParser(reader)
	for {
		schemaStmt, err := parser.ParseStatement()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("could not parse sql: %w", err)
		}
		switch schemaStmt.(type) {
		case *rsql.InsertStatement, *rsql.UpdateStatement, *rsql.DeleteStatement, *rsql.SelectStatement:
			// skip these statements
			continue
		}
		schemaStmtStr := schemaStmt.String()
		matches := firstQuoted.FindStringSubmatch(schemaStmtStr)
		if len(matches) < 2 {
			return fmt.Errorf("could not find table name in statement: %s", schemaStmtStr)
		}
		tableName := matches[1]
		entry, ok := expected[tableName]
		if !ok {
			return fmt.Errorf("table %s not found in schema", tableName)
		} else if entry.sql != schemaStmtStr {
			return fmt.Errorf("table %s sql does not match expected sql:\nexpected:\n%s\nfound:\n%s", tableName, entry.sql, schemaStmtStr)
		} else {
			entry.found = true
			expected[tableName] = entry
		}
	}
	for name, entry := range expected {
		if !entry.found {
			return fmt.Errorf("table %s not found in database", name)
		}
	}
	return nil
}

func Migration(ctx context.Context, db DB, versions map[int]func(context.Context, DB) error) error {
	lastVersion := -1
	for {
		var version int
		err := db.Get(&version, "SELECT version FROM version LIMIT 1")
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return err
		}
		fmt.Printf("current database schema version: v%d\n", version)
		if version == lastVersion {
			return nil
		}
		fn, ok := versions[version]
		if !ok {
			return nil
		}
		err = fn(ctx, db)
		if err != nil {
			return fmt.Errorf("migration from version v%d failed: %w", version, err)
		}
		fmt.Printf("migration to database schema v%d complete\n", version+1)
	}
}

type migrateTable struct {
	Table  string `db:"table"`
	RowID  int64  `db:"rowid"`
	Parent string `db:"parent"`
	FKID   int64  `db:"fkid"`
}

type tableInfo struct {
	Type string `db:"type"`
	Sql  string `db:"sql"`
}

type MigrationMap map[int]func(context.Context, DB) error
type MigrationFunc func(context.Context, DB) error

func MigrateFunc(db DB, version int, restoreTables []string, fn func(Tx, func() error) error) MigrationFunc {
	return func(ctx context.Context, db DB) error {
		_, err := db.Exec("PRAGMA foreign_keys=OFF")
		if err != nil {
			return fmt.Errorf("could not turn foreign keys off: %w", err)
		}
		err = db.Txc(ctx, func(tx Tx) error {
			tables := make([]string, len(restoreTables))
			for i, tableName := range restoreTables {
				tables[i] = tableName
			}
			var stmts []string
			if len(tables) > 0 {
				err := tx.SelectIn(&stmts, "SELECT sql FROM sqlite_master WHERE tbl_name IN (?) AND type = 'index'", tables)
				if err != nil {
					return fmt.Errorf("could not get table info: %w", err)
				}
			}
			restore := sync.OnceValue(func() error {
				for _, tableName := range restoreTables {
					_, err = tx.Exec("DROP TABLE " + tableName)
					if err != nil {
						return err
					}
					_, err = tx.Exec(fmt.Sprintf("ALTER TABLE new_%s RENAME TO %s", tableName, tableName))
					if err != nil {
						return err
					}
				}
				for _, stmt := range stmts {
					_, err = tx.Exec(stmt)
					if err != nil {
						return err
					}
				}
				return nil
			})
			err = fn(tx, restore)
			if err != nil {
				return err
			}
			err = restore()
			if err != nil {
				return err
			}
			var mErrors []migrateTable
			err = tx.Select(&mErrors, "PRAGMA foreign_key_check")
			if errors.Is(err, sql.ErrNoRows) {
				//success
			} else if err != nil {
				return err
			} else if len(mErrors) > 0 {
				return fmt.Errorf("foreign_key migration errors: %v", mErrors)
			}
			tx.MustExec(`UPDATE version SET version = version + 1`)
			return nil
		})
		if err != nil {
			_, pErr := db.Exec("PRAGMA foreign_keys=ON")
			if pErr != nil {
				return fmt.Errorf("!!!unable to turn foreign keys back on after failed migaration!!! %v\n%v", err, pErr)
			} else {
				return fmt.Errorf("migration failed: %w", err)
			}
		}
		_, err = db.Exec("PRAGMA foreign_keys=ON")
		if err != nil {
			return fmt.Errorf("!!!unable to turn foreign keys back on after sucessful migration: %v", err)
		}
		return nil
	}
}

func ExecString(ctx context.Context, db DB, sql string) error {
	return Exec(ctx, db, strings.NewReader(sql))
}

func ExecTxString(tx Tx, sql string) error {
	return ExecTx(tx, strings.NewReader(sql))
}

func Exec(ctx context.Context, db DB, reader io.Reader) error {
	return db.Txc(ctx, func(tx Tx) error {
		return ExecTx(tx, reader)
	})
}

func ExecTx(tx Tx, reader io.Reader) error {
	var buf []byte
	scanner := bufio.NewReader(reader)
	for {
		chunk, err := scanner.ReadString(';')
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if strings.Contains(chunk, "CREATE TRIGGER") {
			if !strings.HasSuffix(chunk, "END;") {
				buf = append(buf, chunk...)
				continue
			}
		}
		var stmt string
		if len(buf) > 0 {
			stmt = string(buf) + chunk
			buf = buf[:0]
		} else {
			stmt = chunk
		}

		for commentIndex := strings.Index(stmt, "--"); commentIndex != -1; commentIndex = strings.Index(stmt, "--") {
			endOfComment := strings.Index(stmt[commentIndex:], "\n")
			if endOfComment == -1 { //
				buf = append(buf, stmt...)
				stmt = stmt[:0]
				break
			}
			stmt = stmt[:commentIndex] + stmt[commentIndex+endOfComment+1:]
		}
		if len(stmt) != 0 {
			stmt = strings.TrimSpace(stmt)
			_, err = tx.Exec(stmt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
