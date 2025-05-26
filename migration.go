package sqlt

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/james-darko/gort"
	rsql "github.com/rqlite/sql"
)

// PrintTables prints the names and SQL of all tables in the database.
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
		fmt.Printf("%s - %s\n", table.Name, table.Sql)
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

// the rsql parser quotes all identifiers. Given SQL statement structure, the first quoted text will be the name of the table, index, etc.
var firstQuotedText = regexp.MustCompile(`"([^"]+)"`)

// VerifyString reads the SQL from the provided string and verifies the database schema against it.
func VerifyString(ctx context.Context, db DB, sql string) error {
	return Verify(ctx, db, strings.NewReader(sql))
}

// Reads the file at DATABASE_SCHEMA and verifies the database schema against it.
func VerifyFromEnv(ctx context.Context, db DB) error {
	schemaFile, ok := gort.Env("DATABASE_SCHEMA")
	if !ok {
		return fmt.Errorf("DATABASE_SCHEMA env var not found")
	}
	schema, err := os.Open(schemaFile)
	if err != nil {
		return fmt.Errorf("could not open schema file %s: %w", schemaFile, err)
	}
	defer schema.Close()
	return Verify(ctx, db, schema)
}

// Verify checks if the database schema matches the expected schema from the reader.
func Verify(ctx context.Context, db DB, schema io.Reader) error {
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
	parser := rsql.NewParser(schema)
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
		matches := firstQuotedText.FindStringSubmatch(schemaStmtStr)
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

// Donotes the version table is empty or non-existent.
var ErrNoVersion = errors.New("no version found in database")

// Applies the function in the versions map until a func is not found in the current version.
// The version number denotes the version the function migrates from.
//
// Expects a table named `version` with a `version` column with current version number.
// Returns ErrNoVersion if the version table is not found or empty.
func Migrate(ctx context.Context, db DB, versions map[int]func(context.Context, DB) error) error {
	lastVersion := -1
	for {
		var version int
		err := db.Get(&version, "SELECT version FROM version LIMIT 1")
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no such table: version") {
				return ErrNoVersion
			}
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
	Name string `db:"name"`
	Type string `db:"type"`
	Sql  string `db:"sql"`
}

// MigrationMap is a map of verion numbers to their respective migration functions.
type MigrationMap map[int]func(context.Context, DB) error

// MigrationFunc is a function that migrates the database from one version to another.
type MigrationFunc func(context.Context, DB) error

// Helper wrapper function for migration. Should be used unless you have good reason not to.
//
// db: db handle.
//
// version: The version number the function migrates from. The version will be automatically incremented after the function runs successfully.
//
// migrateTables: Nilable slice of tables that will be migrated.
// see section 7: https://www.sqlite.org/lang_altertable.html
// Contains tables that will follow the above pattern to migrate. The fn is expected to create tables named `new_<tableName>`
// with the new structure. MigrateFunc will take care of all the steps outside of definining and populating the `new_<tableName>`'s.
// The restore function passed to fn will handle the steps required after the new tables are set up.
// If the fn does not call restore, it will be called after fn returns.
// Indexes are re-created automically for migrated tables.
// The section 7 steps are always followed even when migrateTables is empty. Restore will be a no-op in that case.
//
// fn: migration function
func MigrateFunc(db DB, version int, migrateTables []string, fn func(tx Tx, restore func() error) error) MigrationFunc {
	return func(ctx context.Context, db DB) error {
		_, err := db.Exec("PRAGMA foreign_keys=OFF")
		if err != nil {
			return fmt.Errorf("could not turn foreign keys off: %w", err)
		}
		err = db.Txc(ctx, func(tx Tx) error {
			tables := make([]string, len(migrateTables))
			for i, tableName := range migrateTables {
				tables[i] = tableName
			}
			restore := sync.OnceValue(func() error {
				if len(migrateTables) == 0 {
					return nil
				}
				type masterInfo struct {
					Name    string `db:"name"`
					TblName string `db:"tbl_name"`
					Type    string `db:"type"`
					Sql     string `db:"sql"`
					Stmt    *rsql.CreateTableStatement
					Columns []string
				}
				var masterInfos []masterInfo
				err = tx.SelectIn(&masterInfos,
					"SELECT tbl_name, name, type, sql FROM sqlite_master WHERE tbl_name IN (?)", migrateTables)
				if err != nil {
					return fmt.Errorf("could not get master rows: %w", err)
				}
				for _, tableName := range migrateTables {
					_, err = tx.Exec("DROP TABLE " + tableName)
					if err != nil {
						return err
					}
					_, err = tx.Exec(fmt.Sprintf("ALTER TABLE new_%s RENAME TO %s", tableName, tableName))
					if err != nil {
						return err
					}
				}
				masterTableMap := make(map[string]masterInfo)
				for _, tableInfo := range masterInfos {
					if tableInfo.Type == "table" {
						masterTableMap[tableInfo.Name] = tableInfo
					}
				}
				for _, masterInfo := range masterInfos {
					if masterInfo.Type == "table" {
						continue
					}
					tableInfo, ok := masterTableMap[masterInfo.TblName]
					if !ok {
						return fmt.Errorf("could not find table %s of %s", masterInfo.TblName, masterInfo.Type)
					}
					if tableInfo.Stmt == nil {
						parser := rsql.NewParser(strings.NewReader(tableInfo.Sql))
						stmt, err := parser.ParseStatement()
						if err != nil {
							return fmt.Errorf("could not parse sql for master table %s: %w", tableInfo.Name, err)
						}
						var ok bool
						tableInfo.Stmt, ok = stmt.(*rsql.CreateTableStatement)
						if !ok {
							return fmt.Errorf("%s's was not parsed as a create table statement: %T", tableInfo.Name, stmt)
						}
						tableInfo.Columns = make([]string, len(tableInfo.Stmt.Columns))
						for i, column := range tableInfo.Stmt.Columns {
							tableInfo.Columns[i] = column.Name.Name
						}
					}
					parser := rsql.NewParser(strings.NewReader(masterInfo.Sql))
					masterStmt, err := parser.ParseStatement()
					if err != nil {
						return fmt.Errorf("could not parse sql for master row %s: %w", masterInfo.Name, err)
					}
					switch masterInfo.Type {
					case "index":
						s := masterStmt.(*rsql.CreateIndexStatement)
						allPresent := true
						for _, column := range s.Columns {
							if !slices.Contains(masterInfo.Columns, column.X.String()) {
								allPresent = false
								break
							}
						}
						if allPresent {
							_, err = tx.Exec(masterStmt.String())
							if err != nil {
								return fmt.Errorf("could not create index %s: %w", masterInfo.Name, err)
							}
						}
					default:
						_, err := tx.Exec(masterStmt.String())
						if err != nil {
							return fmt.Errorf("could not create %s %s: %w", masterInfo.Type, masterInfo.Name, err)
						}
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
				return err
			}
		}
		_, err = db.Exec("PRAGMA foreign_keys=ON")
		if err != nil {
			return fmt.Errorf("!!!unable to turn foreign keys back on after sucessful migration: %v", err)
		}
		return nil
	}
}

// ExecString executes the SQL from the provided string in a transaction.
// Supports multiple statements separated by semicolons.
func ExecString(ctx context.Context, db DB, sql string) error {
	return Exec(ctx, db, strings.NewReader(sql))
}

// ExecTxString executes the SQL from the provided string in a transaction.
// Supports multiple statements separated by semicolons.
func ExecTxString(tx Tx, sql string) error {
	return ExecTx(tx, strings.NewReader(sql))
}

// Exec executes the SQL from the provided reader in a transaction.
// Supports multiple statements separated by semicolons.
func Exec(ctx context.Context, db DB, reader io.Reader) error {
	return db.Txc(ctx, func(tx Tx) error {
		return ExecTx(tx, reader)
	})
}

// ExecTx executes the SQL from the provided reader in a transaction.
// Supports multiple statements separated by semicolons.
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
