package sqlt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"

	// "regexp" // Removed regexp import
	"slices"
	"strings"
	"sync"

	"github.com/james-darko/gort"
	rsql "github.com/rqlite/sql"
)

// quoteIdent wraps an identifier in double quotes for SQLite.
// This is a simple version; proper SQL quoting can be more complex
// and should ideally handle existing quotes or special characters within the identifier.
func quoteIdent(ident string) string {
	// Replace existing double quotes with two double quotes (SQLite's way of escaping them)
	escapedIdent := strings.ReplaceAll(ident, "\"", "\"\"")
	return fmt.Sprintf("\"%s\"", escapedIdent)
}

const (
	statementMatchExact         = iota
	statementMatchReorderNeeded // only for tables
	statementMatchNoMatch
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

// compareStatements compares two SQL statements and returns the match type and a description of the differences.
func compareStatements(dbStmt, schemaStmt rsql.Statement) (matchType int, diffDescription string, err error) {
	// Initial Check:
	if dbStmt == nil && schemaStmt != nil {
		return statementMatchNoMatch, "Database object is nil, schema object is not (new object)", nil
	}
	if dbStmt != nil && schemaStmt == nil {
		return statementMatchNoMatch, "Schema object is nil, database object is not (object to be dropped)", nil
	}
	if dbStmt == nil && schemaStmt == nil {
		return statementMatchExact, "", nil // Both nil, considered exact match.
	}

	dbSQL := dbStmt.String()
	schemaSQL := schemaStmt.String()

	if dbSQL == schemaSQL {
		return statementMatchExact, "", nil
	}

	// Type Check & Deep Comparison for Tables:
	dbTableStmt, dbIsTable := dbStmt.(*rsql.CreateTableStatement)
	schemaTableStmt, schemaIsTable := schemaStmt.(*rsql.CreateTableStatement)

	if dbIsTable && schemaIsTable {
		match, desc := compareTableStatements(dbTableStmt, schemaTableStmt)
		return match, desc, nil
	} else if dbIsTable != schemaIsTable {
		return statementMatchNoMatch, "Object type mismatch (e.g., DB is a table, Schema is an index/view for the same name)", nil
	}

	// String Comparison for Other Types (Indexes, Views, Triggers):
	return statementMatchNoMatch, fmt.Sprintf("Definition mismatch. DB: %s, Schema: %s", dbSQL, schemaSQL), nil
}

func normalizeTypeName(typeName string) string {
	upper := strings.ToUpper(typeName)
	if upper == "INT" {
		return "INTEGER"
	}
	return upper
}

func compareConstraints(dbCons, schemaCons []rsql.Constraint) (bool, string) {
	if len(dbCons) != len(schemaCons) {
		return false, fmt.Sprintf("constraint count mismatch (DB: %d, Schema: %d)", len(dbCons), len(schemaCons))
	}
	dbConsStr := make(map[string]int)
	schemaConsStr := make(map[string]int)
	for _, c := range dbCons {
		dbConsStr[c.String()]++
	}
	for _, c := range schemaCons {
		schemaConsStr[c.String()]++
	}
	for s, count := range dbConsStr {
		if schemaConsStr[s] != count {
			return false, fmt.Sprintf("mismatched constraint: %s (DB count: %d, Schema count: %d)", s, count, schemaConsStr[s])
		}
	}
	for s, count := range schemaConsStr {
		if dbConsStr[s] != count {
			return false, fmt.Sprintf("mismatched constraint: %s (Schema count: %d, DB count: %d)", s, count, dbConsStr[s])
		}
	}
	return true, ""
}

func compareTableStatements(dbStmt, schemaStmt *rsql.CreateTableStatement) (int, string) {
	var diffs []string
	dbCols := make(map[string]*rsql.ColumnDefinition)
	for _, col := range dbStmt.Columns {
		dbCols[col.Name.Name] = col
	}
	schemaCols := make(map[string]*rsql.ColumnDefinition)
	for _, col := range schemaStmt.Columns {
		schemaCols[col.Name.Name] = col
	}

	for name, dbCol := range dbCols {
		schemaCol, ok := schemaCols[name]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("Extra DB column: '%s'", name))
			continue
		}
		if normalizeTypeName(dbCol.Type.Name.Name) != normalizeTypeName(schemaCol.Type.Name.Name) { // .Name added
			diffs = append(diffs, fmt.Sprintf("Column '%s': type mismatch (DB: %s, Schema: %s)", name, dbCol.Type.Name.Name, schemaCol.Type.Name.Name)) // .Name added
		}
		dbInlineCons := getInlineConstraints(dbCol.Constraints)
		schemaInlineCons := getInlineConstraints(schemaCol.Constraints)
		constraintsMatch, conDiff := compareConstraints(dbInlineCons, schemaInlineCons)
		if !constraintsMatch {
			diffs = append(diffs, fmt.Sprintf("Column '%s': %s", name, conDiff))
		}
		delete(schemaCols, name)
	}
	for name := range schemaCols {
		diffs = append(diffs, fmt.Sprintf("Missing Schema column: '%s'", name))
	}

	dbTableConsMatch, tableConsDiff := compareConstraints(getTableLevelConstraints(dbStmt.Constraints), getTableLevelConstraints(schemaStmt.Constraints))
	if !dbTableConsMatch {
		diffs = append(diffs, fmt.Sprintf("Table-level constraints mismatch: %s", tableConsDiff))
	}

	if len(diffs) > 0 {
		return statementMatchNoMatch, strings.Join(diffs, "; ")
	}

	if len(dbStmt.Columns) == len(schemaStmt.Columns) {
		orderMatch := true
		for i := range dbStmt.Columns {
			if dbStmt.Columns[i].Name.Name != schemaStmt.Columns[i].Name.Name {
				orderMatch = false
				break
			}
		}
		if !orderMatch {
			return statementMatchReorderNeeded, "Column order differs"
		}
	}
	return statementMatchExact, ""
}

func getInlineConstraints(constraints []rsql.Constraint) []rsql.Constraint {
	var inline []rsql.Constraint
	for _, c := range constraints {
		switch c.(type) {
		case *rsql.PrimaryKeyConstraint, *rsql.NotNullConstraint, *rsql.UniqueConstraint, *rsql.DefaultConstraint, *rsql.CheckConstraint, *rsql.ForeignKeyConstraint:
			inline = append(inline, c)
		}
	}
	return inline
}

func getTableLevelConstraints(constraints []rsql.Constraint) []rsql.Constraint {
	var tableLevel []rsql.Constraint
	for _, c := range constraints {
		tableLevel = append(tableLevel, c)
	}
	return tableLevel
}

func masterRows(db DBReader) ([]masterRow, error) { // Changed DB to DBReader
	var rows []masterRow
	err := db.Select(&rows, "SELECT name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")
	if err != nil {
		return nil, err
	}
	return rows, nil
}

type masterRow struct {
	Name string `db:"name"`
	Sql  string `db:"sql"`
}

func VerifyString(ctx context.Context, db DB, sql string) error {
	return Verify(ctx, db, strings.NewReader(sql))
}

func ExecSchemaFromEnv(ctx context.Context, db DB) error {
	schemaFile, ok := gort.Env("DATABASE_SCHEMA")
	if !ok {
		return fmt.Errorf("DATABASE_SCHEMA env var not found")
	}
	schema, err := os.Open(schemaFile)
	if err != nil {
		return fmt.Errorf("could not open schema file %s: %w", schemaFile, err)
	}
	defer schema.Close()
	return Exec(ctx, db, schema)
}

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

func Verify(ctx context.Context, db DB, schema io.Reader) error {
	dbMasterRows, err := masterRows(db)
	if err != nil {
		return fmt.Errorf("could not get master rows from DB: %w", err)
	}
	dbObjectsMap := make(map[string]rsql.Statement)
	dbObjectNames := make(map[string]struct{})
	for _, row := range dbMasterRows {
		parser := rsql.NewParser(strings.NewReader(row.Sql))
		stmt, err := parser.ParseStatement()
		if err != nil {
			if strings.Contains(err.Error(), "unexpected token IDENT") && strings.Contains(row.Sql, "sqlite_sequence") {
				continue
			}
			return fmt.Errorf("could not parse SQL for DB object %s (SQL: %s): %w", row.Name, row.Sql, err)
		}
		dbObjectsMap[row.Name] = stmt
		dbObjectNames[row.Name] = struct{}{}
	}

	schemaParser := rsql.NewParser(schema)
	verifiedDbObjects := make(map[string]struct{})
	for {
		schemaStmt, err := schemaParser.ParseStatement()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("could not parse statement from input schema: %w", err)
		}
		switch schemaStmt.(type) {
		case *rsql.InsertStatement, *rsql.UpdateStatement, *rsql.DeleteStatement, *rsql.SelectStatement:
			continue
		}
		schemaObjectName, err := getStatementName(schemaStmt)
		if err != nil {
			return fmt.Errorf("could not extract name from schema statement %s: %w", schemaStmt.String(), err)
		}
		dbStmt, found := dbObjectsMap[schemaObjectName]
		if !found {
			return fmt.Errorf("object '%s' from schema not found in database", schemaObjectName)
		}
		matchType, diffDescription, cmpErr := compareStatements(dbStmt, schemaStmt)
		if cmpErr != nil {
			return fmt.Errorf("error comparing object '%s': %w. DB SQL: %s, Schema SQL: %s", schemaObjectName, cmpErr, dbStmt.String(), schemaStmt.String())
		}
		if matchType != statementMatchExact {
			return fmt.Errorf("schema mismatch for object '%s': %s. DB SQL: \n%s\nSchema SQL: \n%s", schemaObjectName, diffDescription, dbStmt.String(), schemaStmt.String())
		}
		verifiedDbObjects[schemaObjectName] = struct{}{}
	}

	for dbObjName := range dbObjectNames {
		if _, isVerified := verifiedDbObjects[dbObjName]; !isVerified {
			if strings.HasPrefix(dbObjName, "sqlite_") {
				continue
			}
			extraStmtString := ""
			if extraStmt, ok := dbObjectsMap[dbObjName]; ok {
				extraStmtString = extraStmt.String()
			}
			return fmt.Errorf("object '%s' found in database but not in schema. DB SQL: \n%s", dbObjName, extraStmtString)
		}
	}
	return nil
}

func getStatementName(stmt rsql.Statement) (string, error) {
	switch s := stmt.(type) {
	case *rsql.CreateTableStatement:
		return s.Name.Name, nil // Removed .String()
	case *rsql.CreateIndexStatement:
		return s.Name.Name, nil // Use .Name for consistency, assuming Ident.Name is string
	case *rsql.CreateViewStatement:
		return s.Name.Name, nil // Changed to s.Name.Name
	case *rsql.CreateTriggerStatement:
		return s.Name.Name, nil // Use .Name for consistency
	default:
		return "", fmt.Errorf("unsupported statement type for name extraction: %T", stmt)
	}
}

// getObjectType determines the type of a SQL object based on its statement.
// This function was previously moved and then missing; restoring it here.
func getObjectType(stmt rsql.Statement) string {
	switch stmt.(type) {
	case *rsql.CreateTableStatement:
		return "TABLE"
	case *rsql.CreateIndexStatement:
		return "INDEX"
	case *rsql.CreateViewStatement:
		return "VIEW"
	case *rsql.CreateTriggerStatement:
		return "TRIGGER"
	default:
		return "OBJECT" // Fallback for unknown or unhandled types
	}
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
					_, err = tx.Exec("DROP TABLE " + quoteIdent(tableName)) // Used local quoteIdent
					if err != nil {
						return err
					}
					_, err = tx.Exec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quoteIdent("new_"+tableName), quoteIdent(tableName))) // Used local quoteIdent
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
						var okStmt bool
						tableInfo.Stmt, okStmt = stmt.(*rsql.CreateTableStatement)
						if !okStmt {
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
							if !slices.Contains(tableInfo.Columns, column.X.String()) { // Use tableInfo.Columns from the *new* table
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
					default: // Other types like triggers, views associated with the table
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
func ExecString(ctx context.Context, db DB, sql string) error {
	return Exec(ctx, db, strings.NewReader(sql))
}

// ExecTxString executes the SQL from the provided string in a transaction.
func ExecTxString(tx Tx, sql string) error {
	return ExecTx(tx, strings.NewReader(sql))
}

// Exec executes the SQL from the provided reader in a transaction.
func Exec(ctx context.Context, db DB, reader io.Reader) error {
	return db.Txc(ctx, func(tx Tx) error {
		return ExecTx(tx, reader)
	})
}

// ExecTx executes the SQL from the provided reader in a transaction.
func ExecTx(tx Tx, reader io.Reader) error {
	parser := rsql.NewParser(reader)
	for {
		stmt, err := parser.ParseStatement()
		if errors.Is(err, io.EOF) {
			break
		} else if stmt == nil {
			fmt.Println("nil statement")
			break
		}
		_, err = tx.Exec(stmt.String())
		if err != nil {
			return fmt.Errorf("error executing statement: %s\n%w", stmt.String(), err)
		}
	}
	return nil
}
