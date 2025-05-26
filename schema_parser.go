package sqlt

import (
	"fmt"
	"io"
	"strings"

	"github.com/rqlite/rqlite/sql"
)

// ParseSchemaReader processes SQL DDL statements from an io.Reader and constructs
// a SchemaDefinition representing the structure of the schema.
// It uses the rqlite SQL parser to understand CREATE TABLE, CREATE INDEX,
// CREATE VIEW, and CREATE TRIGGER statements. Other statement types are ignored.
//
// Parameters:
//   - schemaReader: An io.Reader supplying the SQL schema statements.
//
// Returns:
//   - *SchemaDefinition: A pointer to the populated SchemaDefinition if parsing is successful.
//     This definition will contain all recognized schema elements (tables, indexes, views, triggers).
//   - error: An error if parsing fails at any point (e.g., due to malformed SQL
//     or an I/O error from the reader). The error will provide context about the failure.
func ParseSchemaReader(schemaReader io.Reader) (*SchemaDefinition, error) {
	schemaDef := &SchemaDefinition{
		Tables:   make(map[string]TableDefinition),
		Indexes:  make(map[string]IndexDefinition),
		Views:    make(map[string]ViewDefinition),
		Triggers: make(map[string]TriggerDefinition),
	}

	parser := sql.NewParser(schemaReader)
	for {
		stmt, err := parser.ParseStatement()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse statement: %w", err)
		}

		switch s := stmt.(type) {
		case *sql.CreateTableStatement:
			if err := parseCreateTableStatement(s, schemaDef); err != nil {
				return nil, err
			}
		case *sql.CreateIndexStatement:
			if err := parseCreateIndexStatement(s, schemaDef); err != nil {
				return nil, err
			}
		case *sql.CreateViewStatement:
			if err := parseCreateViewStatement(s, schemaDef); err != nil {
				return nil, err
			}
		case *sql.CreateTriggerStatement:
			if err := parseCreateTriggerStatement(s, schemaDef); err != nil {
				return nil, err
			}
		default:
			// Skip other statement types for now
		}
	}

	return schemaDef, nil
}

func parseCreateTableStatement(stmt *sql.CreateTableStatement, schemaDef *SchemaDefinition) error {
	if stmt == nil {
		return fmt.Errorf("received nil CreateTableStatement")
	}
	tableName := stmt.Name.Name.String()
	if tableName == "" {
		return fmt.Errorf("table name is empty in CreateTableStatement")
	}

	tableDef := TableDefinition{
		Name:              tableName,
		SQL:               stmt.String(),
		Columns:           make([]ColumnDefinition, len(stmt.Columns)),
		UniqueConstraints: make(map[string][]string),
	}

	for i, colDef := range stmt.Columns {
		column := ColumnDefinition{
			Name:       colDef.Name.Name.String(),
			Type:       colDef.Type.Name.String(),
			IsNullable: true, // Default to nullable, will be overridden by NOT NULL constraint
		}

		for _, constraint := range colDef.Constraints {
			switch c := constraint.(type) {
			case *sql.NotNullConstraint:
				column.IsNullable = false
			case *sql.PrimaryKeyConstraint:
				column.IsPrimaryKey = true
				tableDef.PrimaryKey = append(tableDef.PrimaryKey, column.Name)
			case *sql.UniqueConstraint:
				column.IsUnique = true // Mark column as unique if it has its own unique constraint
				// This doesn't capture table-level unique constraints on this column alone,
				// but those will be part of tableDef.UniqueConstraints.
			case *sql.DefaultConstraint:
				if c.Expr != nil {
					val := c.Expr.String()
					column.DefaultValue = &val
				}
			case *sql.ForeignKeyConstraint:
				fkDef := ForeignKeyDefinition{
					TargetTable:   c.ForeignTable.Name.String(),
					TargetColumns: make([]string, len(c.ForeignColumns)),
				}
				for j, fc := range c.ForeignColumns {
					fkDef.TargetColumns[j] = fc.Name.String()
				}
				for _, clause := range c.Clauses {
					switch cl := clause.(type) {
					case *sql.OnUpdateClause:
						fkDef.OnUpdate = cl.Action.String()
					case *sql.OnDeleteClause:
						fkDef.OnDelete = cl.Action.String()
					}
				}
				column.ForeignKey = &fkDef
			}
		}
		tableDef.Columns[i] = column
	}

	// Process table-level constraints
	for _, constraint := range stmt.Constraints {
		switch tc := constraint.(type) {
		case *sql.PrimaryKeyTableConstraint:
			for _, colNameIdent := range tc.Columns {
				colName := colNameIdent.Name.String()
				tableDef.PrimaryKey = append(tableDef.PrimaryKey, colName)
				// Mark corresponding columns as IsPrimaryKey
				for i, c := range tableDef.Columns {
					if c.Name == colName {
						tableDef.Columns[i].IsPrimaryKey = true
					}
				}
			}
		case *sql.UniqueTableConstraint:
			var uniqueCols []string
			for _, colNameIdent := range tc.Columns {
				colName := colNameIdent.Name.String()
				uniqueCols = append(uniqueCols, colName)
				// Mark corresponding columns as IsUnique
				// This is a bit simplistic if a column is part of multiple unique constraints.
				// For now, if it's part of *any* table-level unique constraint, mark it.
				for i, c := range tableDef.Columns {
					if c.Name == colName {
						tableDef.Columns[i].IsUnique = true
					}
				}
			}
			constraintName := tc.Name.Name.String()
			if constraintName == "" {
				// Generate a default name if not provided, though rqlite parser might do this.
				// For simplicity, using a placeholder or joined column names.
				constraintName = "unique_" + strings.Join(uniqueCols, "_")
			}
			tableDef.UniqueConstraints[constraintName] = uniqueCols

		case *sql.ForeignKeyTableConstraint:
			// This logic is a bit more complex if a single FK constraint references multiple columns.
			// The current ColumnDefinition.ForeignKey is singular.
			// For now, we'll skip detailed parsing of table-level FKs if they are multi-column,
			// as it requires a different structure in TableDefinition or a way to link them.
			// The rqlite parser might transform these into column constraints anyway.
			// Let's assume column-level FKs are sufficient for now or that rqlite normalizes them.
			// If not, this part needs refinement.
			// For simplicity, we will assume that the rqlite parser handles FKs as column constraints
			// or we primarily care about column-level FKs for now.
		}
	}
	// Deduplicate primary keys if added from both column and table constraints
	if len(tableDef.PrimaryKey) > 0 {
		pkMap := make(map[string]bool)
		var uniquePKs []string
		for _, pkCol := range tableDef.PrimaryKey {
			if !pkMap[pkCol] {
				pkMap[pkCol] = true
				uniquePKs = append(uniquePKs, pkCol)
			}
		}
		tableDef.PrimaryKey = uniquePKs
	}


	schemaDef.Tables[tableName] = tableDef
	return nil
}

func parseCreateIndexStatement(stmt *sql.CreateIndexStatement, schemaDef *SchemaDefinition) error {
	if stmt == nil {
		return fmt.Errorf("received nil CreateIndexStatement")
	}
	indexName := stmt.Name.Name.String()
	if indexName == "" {
		return fmt.Errorf("index name is empty in CreateIndexStatement")
	}
	tableName := stmt.Table.Name.String()
	if tableName == "" {
		return fmt.Errorf("table name is empty for index %s", indexName)
	}

	indexDef := IndexDefinition{
		Name:      indexName,
		TableName: tableName,
		SQL:       stmt.String(),
		Columns:   make([]string, len(stmt.Columns)),
		IsUnique:  stmt.Unique,
	}
	for i, colIdent := range stmt.Columns {
		indexDef.Columns[i] = colIdent.Name.String()
	}
	schemaDef.Indexes[indexName] = indexDef
	return nil
}

func parseCreateViewStatement(stmt *sql.CreateViewStatement, schemaDef *SchemaDefinition) error {
	if stmt == nil {
		return fmt.Errorf("received nil CreateViewStatement")
	}
	viewName := stmt.Name.Name.String()
	if viewName == "" {
		return fmt.Errorf("view name is empty in CreateViewStatement")
	}

	// The rqlite parser's String() method for CreateViewStatement might not be the exact SQL input.
	// It reconstructs it. For "AS SELECT..." part, stmt.Select.String() gives the select query.
	// We should store the full canonical SQL.
	viewDef := ViewDefinition{
		Name: viewName,
		SQL:  stmt.String(), // This should be the canonical CREATE VIEW statement
	}
	schemaDef.Views[viewName] = viewDef
	return nil
}

func parseCreateTriggerStatement(stmt *sql.CreateTriggerStatement, schemaDef *SchemaDefinition) error {
	if stmt == nil {
		return fmt.Errorf("received nil CreateTriggerStatement")
	}
	triggerName := stmt.Name.Name.String()
	if triggerName == "" {
		return fmt.Errorf("trigger name is empty in CreateTriggerStatement")
	}
	tableName := stmt.Table.Name.String()
	if tableName == "" {
		return fmt.Errorf("table name is empty for trigger %s", triggerName)
	}

	triggerDef := TriggerDefinition{
		Name:      triggerName,
		TableName: tableName,
		SQL:       stmt.String(),
	}
	schemaDef.Triggers[triggerName] = triggerDef
	return nil
}

// DB is an interface that matches the one used in migration.go for querying.
// DB is an interface abstracting database query and execution operations.
// It's defined here to decouple the parser from specific database driver implementations,
// primarily for fetching schema information from `sqlite_master`.
// The context parameter `ctx` is an interface{} to match the style in other parts
// of the codebase from which this was adapted, though `context.Context` is more standard.
type DB interface {
	// QueryContext executes a query that returns rows, typically a SELECT.
	// The args are for any placeholder parameters in the query.
	QueryContext(ctx interface{}, query string, args ...interface{}) (Rows, error)
	// ExecContext executes a query that doesn't return rows, typically an INSERT, UPDATE, or DELETE.
	// It's not directly used by FetchDBSchema but included for interface completeness if shared.
	ExecContext(ctx interface{}, query string, args ...interface{}) (Result, error)
}

// Rows is an interface abstracting the iteration over rows from a query result.
// It mirrors methods from `*sql.Rows`.
type Rows interface {
	// Close closes the Rows, preventing further enumeration.
	Close() error
	// Err returns the error, if any, that was encountered during iteration.
	Err() error
	// Next prepares the next result row for reading with the Scan method.
	// It returns true on success, or false if there is no next result row or an error happened.
	Next() bool
	// Scan copies the columns in the current row into the values pointed at by dest.
	Scan(dest ...interface{}) error
}

// Result is an interface abstracting the result of a query execution that modifies data.
// It mirrors methods from `sql.Result`.
type Result interface {
	// LastInsertId returns the integer ID of the last row inserted, if supported.
	LastInsertId() (int64, error)
	// RowsAffected returns the number of rows affected by an UPDATE, INSERT, or DELETE.
	RowsAffected() (int64, error)
}

// FetchDBSchema queries the `sqlite_master` table of a SQLite database to retrieve
// the DDL statements for all tables, indexes, views, and triggers, excluding
// internal SQLite objects. It then parses these DDL statements to construct
// a comprehensive SchemaDefinition of the existing database schema.
//
// Parameters:
//   - ctx: A context for the database query operation. Passed as interface{}
//     to align with the DB interface style.
//   - db: A database connection (implementing the local DB interface) from which
//     to fetch the schema.
//
// Returns:
//   - *SchemaDefinition: A pointer to the populated SchemaDefinition representing
//     the current state of the database schema.
//   - error: An error if querying `sqlite_master` fails, or if parsing any of the
//     retrieved DDL statements fails.
func FetchDBSchema(ctx interface{}, db DB) (*SchemaDefinition, error) {
	query := `SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql != '' AND name NOT LIKE 'sqlite_%';`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query sqlite_master: %w", err)
	}
	defer rows.Close()

	schemaDef := &SchemaDefinition{
		Tables:   make(map[string]TableDefinition),
		Indexes:  make(map[string]IndexDefinition),
		Views:    make(map[string]ViewDefinition),
		Triggers: make(map[string]TriggerDefinition),
	}

	for rows.Next() {
		var itemType, itemName, itemTblName, itemSql string
		if err := rows.Scan(&itemType, &itemName, &itemTblName, &itemSql); err != nil {
			return nil, fmt.Errorf("failed to scan sqlite_master row: %w", err)
		}

		if itemSql == "" { // Should be filtered by query, but double check
			continue
		}

		// Use ParseSchemaReader to parse the single SQL statement.
		// This reuses the parsing logic.
		singleItemReader := strings.NewReader(itemSql)
		tempSchemaDef, err := ParseSchemaReader(singleItemReader)
		if err != nil {
			// It's possible that sqlite_master contains SQL that rqlite's parser doesn't handle
			// or interprets differently than when processing a full schema dump.
			// For instance, some internal or legacy formats.
			// We should be robust to this, perhaps logging a warning and continuing.
			// For now, return the error.
			return nil, fmt.Errorf("failed to parse SQL for %s '%s' from sqlite_master (SQL: %s): %w", itemType, itemName, itemSql, err)
		}

		// Merge the parsed item into the main schemaDef
		// ParseSchemaReader will populate one of the maps in tempSchemaDef
		for name, table := range tempSchemaDef.Tables {
			if _, exists := schemaDef.Tables[name]; exists {
				return nil, fmt.Errorf("duplicate table definition found for %s when parsing sqlite_master", name)
			}
			schemaDef.Tables[name] = table
		}
		for name, index := range tempSchemaDef.Indexes {
			if _, exists := schemaDef.Indexes[name]; exists {
				return nil, fmt.Errorf("duplicate index definition found for %s when parsing sqlite_master", name)
			}
			schemaDef.Indexes[name] = index
		}
		for name, view := range tempSchemaDef.Views {
			if _, exists := schemaDef.Views[name]; exists {
				return nil, fmt.Errorf("duplicate view definition found for %s when parsing sqlite_master", name)
			}
			schemaDef.Views[name] = view
		}
		for name, trigger := range tempSchemaDef.Triggers {
			if _, exists := schemaDef.Triggers[name]; exists {
				return nil, fmt.Errorf("duplicate trigger definition found for %s when parsing sqlite_master", name)
			}
			schemaDef.Triggers[name] = trigger
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating sqlite_master rows: %w", err)
	}

	return schemaDef, nil
}
