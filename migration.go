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

// AutoMigrate orchestrates the schema migration process.
// It analyzes the desired schema, provided via schemaReader, and compares it
// against the current schema of the database (db).
//
// AutoMigrate performs the following operations:
// 1. Fetches the current schema from the database.
// 2. Parses the desired schema from the schemaReader.
// 3. Identifies differences between the desired and current schemas.
// 4. If there are structural mismatches in tables that cannot be automatically
//    resolved (e.g., column type changes, conflicting constraints), it returns
//    an *ErrSchemaConflicts error containing details of each conflict.
//    In such cases, no changes are made to the database.
// 5. If there are no unresolvable conflicts, it proceeds to apply changes
//    within a single database transaction. This includes:
//    a. Deleting extraneous schema elements (triggers, views, indexes, then tables)
//       that exist in the current schema but not in the desired one.
//    b. Creating new schema elements (tables, then indexes, views, then triggers)
//       that exist in the desired schema but not in the current one.
//    c. Recreating elements (indexes, views, triggers) whose definitions have changed.
//       Tables with changed structures are not automatically recreated by this function;
//       such changes will result in an *ErrSchemaConflicts error.
//
// Parameters:
//   - ctx: The context for database operations.
//   - db: The database connection (implementing the DB interface) to migrate.
//   - schemaReader: An io.Reader providing the SQL statements for the desired schema.
//
// Returns:
//   - nil: If the migration is successful and the database schema aligns with the desired schema.
//   - *ErrSchemaConflicts: If unresolvable table structure mismatches are detected.
//     This error type wraps a slice of SchemaConflictError, each detailing a specific conflict.
//   - error: For other issues, such as I/O errors during schema parsing, database
//     connection problems, or errors during the execution of DDL statements.
//
// Note: The order of operations (deletions before creations, and specific order
// for element types) is crucial to handle dependencies correctly. All DDL changes
// are performed within a single transaction to ensure atomicity.
func AutoMigrate(ctx context.Context, db DB, schemaReader io.Reader) error {
	// 1. Fetch Current DB Schema
	currentSchema, err := FetchDBSchema(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to fetch current DB schema: %w", err)
	}

	// 2. Parse Desired Schema
	desiredSchema, err := ParseSchemaReader(schemaReader)
	if err != nil {
		return fmt.Errorf("failed to parse desired schema: %w", err)
	}

	// 3. Identify Initial Differences
	diffs := IdentifySchemaDifferences(desiredSchema, currentSchema)

	// 4. Process Differences and Perform Detailed Comparisons
	tableConflicts := ProcessSchemaDifferences(diffs, desiredSchema, currentSchema)
	if len(tableConflicts) > 0 {
		return &ErrSchemaConflicts{Conflicts: tableConflicts}
	}

	// 5. Execute Deletions and Creations in a Transaction
	err = db.Txc(ctx, func(tx Tx) error {
		// Execute Deletions
		if err := ExecuteDeletions(tx, &diffs.ToDelete); err != nil {
			return fmt.Errorf("failed during schema deletions: %w", err)
		}

		// Execute Creations
		if err := ExecuteCreations(tx, &diffs.ToCreate); err != nil {
			return fmt.Errorf("failed during schema creations: %w", err)
		}

		return nil // Commit transaction
	})

	if err != nil {
		// Transaction failed (either due to deletions, creations, or other tx issues)
		return fmt.Errorf("schema migration transaction failed: %w", err)
	}

	// 6. Return Value
	return nil // All operations completed successfully
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

// ErrNoVersion indicates that the version table in the database is empty or non-existent.
// This error is typically used by traditional version-based migration systems.
var ErrNoVersion = errors.New("no version found in database")

// Migrate applies versioned migration functions to the database.
// It reads the current schema version from a 'version' table in the database.
// Then, it iteratively applies migration functions from the 'versions' map,
// starting from the current version + 1, until a function for a version is not found.
// Each successful migration function should update the version in the 'version' table.
//
// This is a more traditional, imperative migration approach, contrasted with AutoMigrate's
// declarative, schema-driven approach.
//
// Parameters:
//  - ctx: The context for database operations.
//  - db: The database connection (implementing the DB interface).
//  - versions: A map where keys are schema version numbers and values are migration
//    functions. A function `versions[N]` is expected to migrate the schema
//    from version N to version N+1.
//
// Returns:
//  - nil: If all applicable migrations are successful or if no migrations are needed.
//  - ErrNoVersion: If the 'version' table is not found or is empty.
//  - error: For any other database errors or errors during migration function execution.
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

// MigrationMap defines a map of version numbers to their respective migration functions.
// It's used by the Migrate function to apply versioned schema changes.
type MigrationMap map[int]func(context.Context, DB) error

// MigrationFunc represents a function that migrates the database schema from one
// version to the next. It takes a context and a database connection.
type MigrationFunc func(context.Context, DB) error

// MigrateFunc is a helper function to create a MigrationFunc for the Migrate system.
// It facilitates the common SQLite pattern for altering tables (rename, create new, copy data, drop old, rename new)
// and handles foreign key constraints and index/trigger recreation.
//
// Parameters:
//  - db: The database connection (unused in the returned function's signature matching but captured by closure if needed, though typically the one from MigrationFunc is used).
//  - version: The schema version number this function migrates *from*. The 'version' table
//    in the database will be automatically incremented after this function runs successfully.
//  - migrateTables: A slice of table names that will be migrated using the "rename and recreate" strategy.
//    The provided `fn` is expected to create new tables named `new_<tableName>` with the desired structure
//    and populate them. This helper will then handle dropping the old tables, renaming the new ones,
//    and attempting to recreate associated indexes and other objects. Refer to SQLite documentation
//    on "Making Other Kinds Of Table Schema Changes" (section 7 of lang_altertable.html).
//  - fn: The core migration logic. It receives a transaction `Tx` and a `restore` function.
//    The `restore` function, when called, executes the steps to finalize the table migrations
//    (drop old, rename new, recreate indexes/triggers). It's typically called after `fn` has
//    created and populated the `new_<tableName>` tables. If `fn` does not call `restore`,
//    it will be called automatically after `fn` returns without error.
//
// Returns:
//   A MigrationFunc suitable for use with the Migrate function.
//
// The overall process within the returned MigrationFunc is:
// 1. `PRAGMA foreign_keys=OFF` is executed.
// 2. A transaction is started.
// 3. The user-provided `fn` is executed.
// 4. The `restore` logic (table swaps, index recreation) is executed (if not already by `fn`).
// 5. `PRAGMA foreign_key_check` is run to ensure integrity.
// 6. The schema version in the `version` table is incremented.
// 7. The transaction is committed.
// 8. `PRAGMA foreign_keys=ON` is executed.
// Errors at any critical step lead to a rollback and error propagation.
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

// ExecString executes one or more SQL statements from the provided string.
// The statements are executed within a single transaction.
// Statements should be separated by semicolons.
// It handles multi-line statements and comments (lines starting with "--").
// Special handling is included for `CREATE TRIGGER` statements to ensure the entire
// trigger block is captured correctly.
func ExecString(ctx context.Context, db DB, sql string) error {
	return Exec(ctx, db, strings.NewReader(sql))
}

// ExecTxString executes one or more SQL statements from the provided string within an existing transaction `tx`.
// Statements should be separated by semicolons.
// It handles multi-line statements and comments (lines starting with "--").
// Special handling for `CREATE TRIGGER` statements is included.
func ExecTxString(tx Tx, sql string) error {
	return ExecTx(tx, strings.NewReader(sql))
}

// Exec executes one or more SQL statements from the provided io.Reader.
// The statements are executed within a single transaction.
// Statements should be separated by semicolons.
// It handles multi-line statements and comments (lines starting with "--").
// Special handling for `CREATE TRIGGER` statements is included.
func Exec(ctx context.Context, db DB, reader io.Reader) error {
	return db.Txc(ctx, func(tx Tx) error {
		return ExecTx(tx, reader)
	})
}

// ExecTx executes one or more SQL statements from the provided io.Reader within an existing transaction `tx`.
// Statements should be separated by semicolons.
// It handles multi-line statements and comments (lines starting with "--").
// Special handling for `CREATE TRIGGER` statements ensures that multi-line trigger
// definitions ending with "END;" are processed as a single statement.
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
