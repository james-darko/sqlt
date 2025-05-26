package sqlt

import (
	"context"
	"database/sql"
	"errors" // Added for errors.As
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"github.com/rqlite/rqlite/sql/ast"
	"github.com/rqlite/rqlite/sql/parser"
)

// testDBWrapper is a simple wrapper to make *sql.DB compatible with the DB interface
// used by the migration logic.
type testDBWrapper struct {
	*sql.DB
	t *testing.T
}

// QueryContext implements the DB interface.
func (tdb *testDBWrapper) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := tdb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil // *sql.Rows implements the Rows interface
}

// Get implements the DB interface (simplified version).
func (tdb *testDBWrapper) Get(dest interface{}, query string, args ...interface{}) error {
	return tdb.DB.QueryRowContext(context.Background(), query, args...).Scan(dest)
}

// Select implements the DB interface (simplified version).
func (tdb *testDBWrapper) Select(dest interface{}, query string, args ...interface{}) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("destination for Select must be a pointer to a slice, got %T", dest)
	}

	rows, err := tdb.DB.QueryContext(context.Background(), query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	sliceVal := val.Elem()
	elemType := sliceVal.Type().Elem()

	for rows.Next() {
		newElemVal := reflect.New(elemType)
		if elemType == reflect.TypeOf(masterRow{}) {
			mr := newElemVal.Interface().(*masterRow)
			if err := rows.Scan(&mr.Name, &mr.Sql); err != nil {
				return fmt.Errorf("failed to scan into masterRow: %w", err)
			}
		} else if elemType == reflect.TypeOf(struct{ Count int }{}) {
			s := newElemVal.Interface().(*struct{ Count int })
			if err := rows.Scan(&s.Count); err != nil {
				return fmt.Errorf("failed to scan into count struct: %w", err)
			}
		} else if elemType == reflect.TypeOf(migrateTable{}) { // Added for TestMigration if used
			mt := newElemVal.Interface().(*migrateTable)
			if err := rows.Scan(&mt.Table, &mt.RowID, &mt.Parent, &mt.FKID); err != nil {
				return fmt.Errorf("failed to scan into migrateTable: %w", err)
			}
		} else {
			return fmt.Errorf("unsupported type for simplified Select: %T. Add specific handling.", newElemVal.Interface())
		}
		sliceVal = reflect.Append(sliceVal, newElemVal.Elem())
	}
	val.Elem().Set(sliceVal)
	return rows.Err()
}

// ExecContext implements the DB interface.
func (tdb *testDBWrapper) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return tdb.DB.ExecContext(ctx, query, args...)
}

// Exec implements the DB interface
func (tdb *testDBWrapper) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tdb.DB.ExecContext(context.Background(), query, args...)
}

// Txc implements the DB interface.
func (tdb *testDBWrapper) Txc(ctx context.Context, fn func(tx Tx) error) error {
	sqlTx, err := tdb.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	wrappedTx := &testTxWrapper{Tx: sqlTx, t: tdb.t}

	if err := fn(wrappedTx); err != nil {
		if rbErr := sqlTx.Rollback(); rbErr != nil {
			return fmt.Errorf("transaction failed: %v (and rollback failed: %v)", err, rbErr)
		}
		return err
	}

	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// testTxWrapper is a simple wrapper for *sql.Tx to implement the Tx interface.
type testTxWrapper struct {
	*sql.Tx
	t *testing.T
}

// Exec implements the Tx interface.
func (ttx *testTxWrapper) Exec(query string, args ...any) (Result, error) {
	res, err := ttx.Tx.Exec(query, args...)
	return sqltResult{res}, err
}
func (ttx *testTxWrapper) MustExec(query string, args ...any) Result { panic("MustExec not implemented in testTxWrapper") }
func (ttx *testTxWrapper) Query(query string, args ...any) (*interface{}, error) { panic("Query not implemented in testTxWrapper (signature mismatch anyway)") ; return nil, nil }
func (ttx *testTxWrapper) MustQuery(query string, args ...any) *interface{} { panic("MustQuery not implemented in testTxWrapper") ; return nil }
func (ttx *testTxWrapper) QueryRow(query string, args ...any) *interface{} { panic("QueryRow not implemented in testTxWrapper (signature mismatch anyway)") ; return nil }
func (ttx *testTxWrapper) MustQueryRow(query string, args ...any) *interface{} { panic("MustQueryRow not implemented in testTxWrapper"); return nil }
func (ttx *testTxWrapper) Get(dest any, query string, args ...any) error { panic("Get not implemented in testTxWrapper") ; return nil }
func (ttx *testTxWrapper) MustGet(dest any, query string, args ...any) { panic("MustGet not implemented in testTxWrapper") }
func (ttx *testTxWrapper) Select(dest any, query string, args ...any) error { panic("Select not implemented in testTxWrapper") ; return nil }
func (ttx *testTxWrapper) MustSelect(dest any, query string, args ...any) { panic("MustSelect not implemented in testTxWrapper") }
func (ttx *testTxWrapper) SelectIn(dest any, query string, args ...any) error { panic("SelectIn not implemented in testTxWrapper") ; return nil }
func (ttx *testTxWrapper) MustSelectIn(dest any, query string, args ...any) { panic("MustSelectIn not implemented in testTxWrapper") }
func (ttx *testTxWrapper) NamedExec(query string, arg any) (Result, error) { panic("NamedExec not implemented in testTxWrapper") ; return nil, nil }
func (ttx *testTxWrapper) Rebind(query string) string { panic("Rebind not implemented in testTxWrapper") ; return "" }
func (ttx *testTxWrapper) DriverName() string { return "sqlite3" }


// sqltResult adapts sql.Result to the sqlt.Result interface.
type sqltResult struct {
	sql.Result
}

// setupTestDB initializes an in-memory SQLite database for testing.
func setupTestDB(t *testing.T, initialSchemaSQL string) DB {
	t.Helper()
	dbName := fmt.Sprintf("file:%s_%d?mode=memory&cache=shared", t.Name(), reflect.ValueOf(t).Pointer())
	sqlDB, err := sql.Open("sqlite3", dbName)
	if err != nil {
		t.Fatalf("Failed to open in-memory SQLite DB (%s): %v", dbName, err)
	}
	t.Cleanup(func() { sqlDB.Close() })

	testDB := &testDBWrapper{DB: sqlDB, t: t}

	if initialSchemaSQL != "" {
		err := Exec(context.Background(), testDB, strings.NewReader(initialSchemaSQL))
		if err != nil {
			t.Fatalf("Failed to execute initial schema SQL: %v", err)
		}
	}
	return testDB
}

// normalizeSQL attempts to parse and re-serialize SQL to get a canonical form.
func normalizeSQL(sqlStr string) string {
	if strings.TrimSpace(sqlStr) == "" {
		return ""
	}
	p := parser.NewParser(strings.NewReader(sqlStr))
	stmt, err := p.ParseStatement()
	if err != nil {
		return sqlStr // Return original if parsing fails
	}
	return ast.Generate(stmt)
}

// verifySchema fetches the current schema from the DB and compares it with the expected model.
func verifySchema(t *testing.T, db DB, expectedModelSchema *SchemaDefinition, expectEmpty bool) {
	t.Helper()
	ctx := context.Background()

	currentSchema, err := FetchDBSchema(ctx, db)
	if err != nil {
		t.Fatalf("verifySchema: Failed to fetch current DB schema: %v", err)
	}

	if expectEmpty {
		if len(currentSchema.Tables) > 0 || len(currentSchema.Indexes) > 0 || len(currentSchema.Views) > 0 || len(currentSchema.Triggers) > 0 {
			t.Errorf("verifySchema: Expected empty schema, but found elements. Tables: %d, Indexes: %d, Views: %d, Triggers: %d\nDetails: %s",
				len(currentSchema.Tables), len(currentSchema.Indexes), len(currentSchema.Views), len(currentSchema.Triggers), currentSchema.String())
		}
		return
	}

	if expectedModelSchema == nil {
		t.Fatal("verifySchema: expectedModelSchema cannot be nil unless expectEmpty is true")
	}

	normalizeSchemaDefinition(currentSchema)
	normalizeSchemaDefinition(expectedModelSchema)

	if len(currentSchema.Tables) != len(expectedModelSchema.Tables) {
		t.Errorf("verifySchema: Mismatch in number of tables. Expected %d, Got %d.\nExpected: %v\nGot: %v",
			len(expectedModelSchema.Tables), len(currentSchema.Tables), getKeys(expectedModelSchema.Tables), getKeys(currentSchema.Tables))
	}
	for name, expectedTable := range expectedModelSchema.Tables {
		currentTable, exists := currentSchema.Tables[name]
		if !exists {
			t.Errorf("verifySchema: Expected table %s not found in current schema.", name)
			continue
		}
		normExpectedSQL := normalizeSQL(expectedTable.SQL)
		normCurrentSQL := normalizeSQL(currentTable.SQL)
		if normExpectedSQL != normCurrentSQL {
			conflict := CompareTableDefinitions(expectedTable, currentTable)
			if conflict != nil {
				t.Errorf("verifySchema: Table %s definitions differ. Detailed diff: %v\nExpected SQL (normalized):\n%s\nGot SQL (normalized):\n%s\nOriginal Expected SQL:\n%s\nOriginal Current SQL:\n%s",
					name, conflict, normExpectedSQL, normCurrentSQL, expectedTable.SQL, currentTable.SQL)
			} else {
				t.Logf("verifySchema: Table %s SQL strings differ but structures are equivalent.\nExpected SQL (normalized):\n%s\nGot SQL (normalized):\n%s", name, normExpectedSQL, normCurrentSQL)
			}
		}
	}
	for name := range currentSchema.Tables {
		if _, exists := expectedModelSchema.Tables[name]; !exists {
			t.Errorf("verifySchema: Unexpected table %s found in current schema: %s", name, currentSchema.Tables[name].SQL)
		}
	}
	compareSchemaElements(t, "index", currentSchema.Indexes, expectedModelSchema.Indexes)
	compareSchemaElements(t, "view", currentSchema.Views, expectedModelSchema.Views)
	compareSchemaElements(t, "trigger", currentSchema.Triggers, expectedModelSchema.Triggers)
}

func compareSchemaElements[T interface{ GetSQL() string }](t *testing.T, elemType string, current, expected map[string]T) {
	t.Helper()
	if len(current) != len(expected) {
		t.Errorf("verifySchema: Mismatch in number of %ss. Expected %d, Got %d.\nExpected: %v\nGot: %v",
			elemType, len(expected), len(current), getKeys(expected), getKeys(current))
	}
	for name, expectedElem := range expected {
		currentElem, exists := current[name]
		if !exists {
			t.Errorf("verifySchema: Expected %s %s not found.", elemType, name)
			continue
		}
		normExpectedSQL := normalizeSQL(expectedElem.GetSQL())
		normCurrentSQL := normalizeSQL(currentElem.GetSQL())
		if normExpectedSQL != normCurrentSQL {
			t.Errorf("verifySchema: %s %s SQL differ.\nExpected (normalized):\n%s\nGot (normalized):\n%s\nOriginal Expected:\n%s\nOriginal Current:\n%s",
				elemType, name, normExpectedSQL, normCurrentSQL, expectedElem.GetSQL(), currentElem.GetSQL())
		}
	}
	for name, currentElem := range current {
		if _, exists := expected[name]; !exists {
			t.Errorf("verifySchema: Unexpected %s %s found: %s", elemType, name, currentElem.GetSQL())
		}
	}
}

func (i IndexDefinition) GetSQL() string   { return i.SQL }
func (v ViewDefinition) GetSQL() string    { return v.SQL }
func (tr TriggerDefinition) GetSQL() string { return tr.SQL }

func getKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func normalizeSchemaDefinition(schema *SchemaDefinition) {
	if schema == nil {
		return
	}
	for tn, table := range schema.Tables {
		sort.SliceStable(table.Columns, func(i, j int) bool { return table.Columns[i].Name < table.Columns[j].Name })
		sort.Strings(table.PrimaryKey)
		for ucName, constraintCols := range table.UniqueConstraints {
			sort.Strings(constraintCols)
			table.UniqueConstraints[ucName] = constraintCols
		}
		schema.Tables[tn] = table
	}
	for in, index := range schema.Indexes {
		sort.Strings(index.Columns)
		schema.Indexes[in] = index
	}
}

func getObjectSQL(t *testing.T, db DB, objectType, objectName string) string {
	t.Helper()
	var sqlStatement sql.NullString
	query := "SELECT sql FROM sqlite_master WHERE type=? AND name=?;"
	err := db.Get(&sqlStatement, query, objectType, objectName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) { return "" }
		t.Fatalf("Failed to get SQL for %s %s: %v", objectType, objectName, err)
	}
	if !sqlStatement.Valid { return "" }
	return sqlStatement.String
}

// --- Test Cases ---

func TestAutoMigrate_EmptyDB_NewSchema(t *testing.T) {
	db := setupTestDB(t, "")
	ctx := context.Background()
	targetSchemaSQL := `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
		CREATE TABLE audit_log (event_type TEXT, table_name TEXT, row_id INTEGER);
		CREATE INDEX idx_users_email ON users(email);
		CREATE VIEW user_names AS SELECT name FROM users;
		CREATE TRIGGER user_insert_trigger AFTER INSERT ON users
		BEGIN
			INSERT INTO audit_log (event_type, table_name, row_id) VALUES ('INSERT', 'users', NEW.id);
		END;
	`
	err := AutoMigrate(ctx, db, strings.NewReader(targetSchemaSQL))
	if err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}
	expectedSchema, err := ParseSchemaReader(strings.NewReader(targetSchemaSQL))
	if err != nil {
		t.Fatalf("Failed to parse target schema for verification: %v", err)
	}
	verifySchema(t, db, expectedSchema, false)
}

func TestAutoMigrate_DBWithElements_EmptySchema(t *testing.T) {
	initialSchemaSQL := `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE INDEX idx_users_name ON users(name);
		CREATE VIEW user_view AS SELECT name FROM users;
		CREATE TRIGGER user_delete_trigger AFTER DELETE ON users BEGIN SELECT 'deleted'; END;
	`
	db := setupTestDB(t, initialSchemaSQL)
	ctx := context.Background()
	err := AutoMigrate(ctx, db, strings.NewReader(""))
	if err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}
	verifySchema(t, db, &SchemaDefinition{
		Tables:   make(map[string]TableDefinition),
		Indexes:  make(map[string]IndexDefinition),
		Views:    make(map[string]ViewDefinition),
		Triggers: make(map[string]TriggerDefinition),
	}, true)
}

func TestAutoMigrate_MatchingSchemas(t *testing.T) {
	schemaSQL := `CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT); CREATE INDEX idx_b ON t1(b);`
	db := setupTestDB(t, schemaSQL)
	ctx := context.Background()
	var countBefore int
	err := db.Get(&countBefore, "SELECT COUNT(*) FROM sqlite_master WHERE type != 'sqlite_sequence' AND name NOT LIKE 'sqlite_%';")
	if err != nil {
		t.Fatalf("Failed to count objects before migration: %v", err)
	}
	err = AutoMigrate(ctx, db, strings.NewReader(schemaSQL))
	if err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}
	var countAfter int
	err = db.Get(&countAfter, "SELECT COUNT(*) FROM sqlite_master WHERE type != 'sqlite_sequence' AND name NOT LIKE 'sqlite_%';")
	if err != nil {
		t.Fatalf("Failed to count objects after migration: %v", err)
	}
	if countBefore != countAfter {
		t.Errorf("Number of schema objects changed. Before: %d, After: %d", countBefore, countAfter)
	}
	expectedSchema, _ := ParseSchemaReader(strings.NewReader(schemaSQL))
	verifySchema(t, db, expectedSchema, false)
}

func TestAutoMigrate_TableColumnMismatch_Error(t *testing.T) {
	initialSQL := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);`
	db := setupTestDB(t, initialSQL)
	ctx := context.Background()
	targetSQL := `CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL);`
	err := AutoMigrate(ctx, db, strings.NewReader(targetSQL))

	if err == nil {
		t.Fatal("AutoMigrate should have failed due to column type mismatch, but it didn't")
	}
	var schemaConflicts *ErrSchemaConflicts
	if !errors.As(err, &schemaConflicts) {
		t.Fatalf("expected an error of type *ErrSchemaConflicts, got %T: %v", err, err)
	}
	if len(schemaConflicts.Conflicts) == 0 {
		t.Fatalf("expected at least one conflict, got none")
	}
	conflict := schemaConflicts.Conflicts[0]
	if conflict.ElementName != "users" {
		t.Errorf("expected ElementName 'users', got '%s'", conflict.ElementName)
	}
	if conflict.ConflictType != "ColumnTypeMismatch" {
		t.Errorf("expected ConflictType 'ColumnTypeMismatch', got '%s'", conflict.ConflictType)
	}
	if conflict.PropertyName != "Column 'name'.Type" {
		t.Errorf("expected PropertyName for column 'name' type, got '%s'", conflict.PropertyName)
	}
	// Desired type comes from targetSQL, actual from initialSQL
	if conflict.ExpectedValue != "VARCHAR(255)" {
		t.Errorf("expected ExpectedValue 'VARCHAR(255)', got '%s'", conflict.ExpectedValue)
	}
	if conflict.ActualValue != "TEXT" {
		t.Errorf("expected ActualValue 'TEXT', got '%s'", conflict.ActualValue)
	}

	currentSQL := getObjectSQL(t, db, "table", "users")
	if normalizeSQL(currentSQL) != normalizeSQL(initialSQL) {
		t.Errorf("Table 'users' SQL changed.\nExpected (normalized):\n%s\nGot (normalized):\n%s", normalizeSQL(initialSQL), normalizeSQL(currentSQL))
	}
}

func TestAutoMigrate_TableAddNotNullableColumn_Error(t *testing.T) {
	initialSQL := `CREATE TABLE items (id INTEGER PRIMARY KEY, price REAL);`
	db := setupTestDB(t, initialSQL)
	ctx := context.Background()
	targetSQL := `CREATE TABLE items (id INTEGER PRIMARY KEY, price REAL, sku TEXT NOT NULL);`
	err := AutoMigrate(ctx, db, strings.NewReader(targetSQL))

	if err == nil {
		t.Fatal("AutoMigrate should have failed due to adding a NOT NULL column, but it didn't")
	}
	var schemaConflicts *ErrSchemaConflicts
	if !errors.As(err, &schemaConflicts) {
		t.Fatalf("expected an error of type *ErrSchemaConflicts, got %T: %v", err, err)
	}
	if len(schemaConflicts.Conflicts) == 0 {
		t.Fatalf("expected at least one conflict, got none")
	}
	conflict := schemaConflicts.Conflicts[0]
	if conflict.ElementName != "items" {
		t.Errorf("expected ElementName 'items', got '%s'", conflict.ElementName)
	}
	// The conflict should be ColumnCountMismatch as per current CompareTableDefinitions logic
	if conflict.ConflictType != "ColumnCountMismatch" {
		t.Errorf("expected ConflictType 'ColumnCountMismatch', got '%s'", conflict.ConflictType)
	}
	if conflict.PropertyName != "Table Columns" {
		t.Errorf("expected PropertyName 'Table Columns', got '%s'", conflict.PropertyName)
	}
	if conflict.ExpectedValue != "3" { // Desired has 3 columns
		t.Errorf("expected ExpectedValue '3', got '%s'", conflict.ExpectedValue)
	}
	if conflict.ActualValue != "2" { // Current has 2 columns
		t.Errorf("expected ActualValue '2', got '%s'", conflict.ActualValue)
	}

	currentSQL := getObjectSQL(t, db, "table", "items")
	if normalizeSQL(currentSQL) != normalizeSQL(initialSQL) {
		t.Errorf("Table 'items' SQL changed.\nExpected (normalized):\n%s\nGot (normalized):\n%s", normalizeSQL(initialSQL), normalizeSQL(currentSQL))
	}
}

func TestAutoMigrate_TableAddNullableColumn_Error(t *testing.T) {
	initialSQL := `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT);`
	db := setupTestDB(t, initialSQL)
	ctx := context.Background()
	targetSQL := `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, description TEXT NULL);`
	err := AutoMigrate(ctx, db, strings.NewReader(targetSQL))

	if err == nil {
		t.Fatal("AutoMigrate should have failed due to adding a nullable column (strict mode), but it didn't")
	}
	var schemaConflicts *ErrSchemaConflicts
	if !errors.As(err, &schemaConflicts) {
		t.Fatalf("expected an error of type *ErrSchemaConflicts, got %T: %v", err, err)
	}
	if len(schemaConflicts.Conflicts) == 0 {
		t.Fatalf("expected at least one conflict, got none")
	}
	conflict := schemaConflicts.Conflicts[0]
	if conflict.ElementName != "products" {
		t.Errorf("expected ElementName 'products', got '%s'", conflict.ElementName)
	}
	if conflict.ConflictType != "ColumnCountMismatch" {
		t.Errorf("expected ConflictType 'ColumnCountMismatch', got '%s'", conflict.ConflictType)
	}
	if conflict.PropertyName != "Table Columns" {
		t.Errorf("expected PropertyName 'Table Columns', got '%s'", conflict.PropertyName)
	}
	if conflict.ExpectedValue != "3" { // Desired has 3 columns
		t.Errorf("expected ExpectedValue '3', got '%s'", conflict.ExpectedValue)
	}
	if conflict.ActualValue != "2" { // Current has 2 columns
		t.Errorf("expected ActualValue '2', got '%s'", conflict.ActualValue)
	}

	currentSQL := getObjectSQL(t, db, "table", "products")
	if normalizeSQL(currentSQL) != normalizeSQL(initialSQL) {
		t.Errorf("Table 'products' SQL changed.\nExpected (normalized):\n%s\nGot (normalized):\n%s", normalizeSQL(initialSQL), normalizeSQL(currentSQL))
	}
}

func TestAutoMigrate_IndexChange(t *testing.T) {
	initialSQL := `CREATE TABLE data (val TEXT); CREATE INDEX idx_data_val ON data(val);`
	db := setupTestDB(t, initialSQL)
	ctx := context.Background()
	targetSQL := `CREATE TABLE data (val TEXT); CREATE UNIQUE INDEX idx_data_val ON data(val);`
	err := AutoMigrate(ctx, db, strings.NewReader(targetSQL))
	if err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}
	expectedSchema, _ := ParseSchemaReader(strings.NewReader(targetSQL))
	verifySchema(t, db, expectedSchema, false)
}

func TestAutoMigrate_ViewChange(t *testing.T) {
	initialSQL := `CREATE TABLE t1(a INT); CREATE VIEW v1 AS SELECT a FROM t1;`
	db := setupTestDB(t, initialSQL)
	ctx := context.Background()
	targetSQL := `CREATE TABLE t1(a INT); CREATE VIEW v1 AS SELECT a*2 AS a2 FROM t1;`
	err := AutoMigrate(ctx, db, strings.NewReader(targetSQL))
	if err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}
	expectedSchema, _ := ParseSchemaReader(strings.NewReader(targetSQL))
	verifySchema(t, db, expectedSchema, false)
}

func TestAutoMigrate_MissingElementsInDB(t *testing.T) {
	initialSQL := `CREATE TABLE t1(a INT);`
	db := setupTestDB(t, initialSQL)
	ctx := context.Background()
	targetSQL := `CREATE TABLE t1(a INT); CREATE TABLE t2(b INT); CREATE INDEX idx_t1_a ON t1(a);`
	err := AutoMigrate(ctx, db, strings.NewReader(targetSQL))
	if err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}
	expectedSchema, _ := ParseSchemaReader(strings.NewReader(targetSQL))
	verifySchema(t, db, expectedSchema, false)
}

func TestAutoMigrate_ExtraneousElementsInDB(t *testing.T) {
	initialSQL := `CREATE TABLE t1(a INT); CREATE TABLE t2(b INT); CREATE INDEX idx_t1_a ON t1(a);`
	db := setupTestDB(t, initialSQL)
	ctx := context.Background()
	targetSQL := `CREATE TABLE t1(a INT);`
	err := AutoMigrate(ctx, db, strings.NewReader(targetSQL))
	if err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}
	expectedSchema, _ := ParseSchemaReader(strings.NewReader(targetSQL))
	verifySchema(t, db, expectedSchema, false)
}

func TestAutoMigrate_Atomicity_ErrorMidway(t *testing.T) {
	initialSQL := `CREATE TABLE mismatched_table (name TEXT, age INT);`
	db := setupTestDB(t, initialSQL)
	ctx := context.Background()
	targetSQL := `
		CREATE TABLE valid_table (id INT);
		CREATE INDEX valid_idx ON valid_table(id);
		CREATE TABLE mismatched_table (name TEXT); 
	`
	err := AutoMigrate(ctx, db, strings.NewReader(targetSQL))
	if err == nil {
		t.Fatal("AutoMigrate should have failed due to mismatched_table, but it didn't")
	}

	// Check for specific error type
	var schemaConflicts *ErrSchemaConflicts
	if !errors.As(err, &schemaConflicts) {
		t.Fatalf("expected an error of type *ErrSchemaConflicts, got %T: %v", err, err)
	}
	if len(schemaConflicts.Conflicts) == 0 {
		t.Fatalf("expected at least one conflict, got none")
	}
	conflict := schemaConflicts.Conflicts[0]
	if !(conflict.ElementName == "mismatched_table" && conflict.ConflictType == "ColumnCountMismatch") {
		t.Errorf("unexpected conflict details: %+v", conflict)
	}


	if sql := getObjectSQL(t, db, "table", "valid_table"); sql != "" {
		t.Errorf("Table 'valid_table' should not exist, but found: %s", sql)
	}
	if sql := getObjectSQL(t, db, "index", "valid_idx"); sql != "" {
		t.Errorf("Index 'valid_idx' should not exist, but found: %s", sql)
	}
	currentMismatchedSQL := getObjectSQL(t, db, "table", "mismatched_table")
	// Normalize initialSQL for mismatched_table to compare fairly
	initialMismatchedTableDef, _ := ParseSchemaReader(strings.NewReader(initialSQL))
	normInitialMismatchedSQL := normalizeSQL(initialMismatchedTableDef.Tables["mismatched_table"].SQL)

	if normalizeSQL(currentMismatchedSQL) != normInitialMismatchedSQL {
		t.Errorf("Table 'mismatched_table' changed.\nExpected (normalized):\n%s\nGot (normalized):\n%s", normInitialMismatchedSQL, normalizeSQL(currentMismatchedSQL))
	}
}


// String method for SchemaDefinition to help in debugging verifySchema failures
func (s *SchemaDefinition) String() string {
	if s == nil {
		return "<nil SchemaDefinition>"
	}
	var sb strings.Builder
	sb.WriteString("SchemaDefinition:\n")
	if s.Tables != nil {
		sb.WriteString(fmt.Sprintf("  Tables (%d):\n", len(s.Tables)))
		for name, t := range s.Tables {
			sb.WriteString(fmt.Sprintf("    %s: %s\n", name, normalizeSQL(t.SQL)))
		}
	} else {
		sb.WriteString("  Tables: nil\n")
	}
	if s.Indexes != nil {
		sb.WriteString(fmt.Sprintf("  Indexes (%d):\n", len(s.Indexes)))
		for name, i := range s.Indexes {
			sb.WriteString(fmt.Sprintf("    %s: %s\n", name, normalizeSQL(i.SQL)))
		}
	} else {
		sb.WriteString("  Indexes: nil\n")
	}
	if s.Views != nil {
		sb.WriteString(fmt.Sprintf("  Views (%d):\n", len(s.Views)))
		for name, v := range s.Views {
			sb.WriteString(fmt.Sprintf("    %s: %s\n", name, normalizeSQL(v.SQL)))
		}
	} else {
		sb.WriteString("  Views: nil\n")
	}
	if s.Triggers != nil {
		sb.WriteString(fmt.Sprintf("  Triggers (%d):\n", len(s.Triggers)))
		for name, tr := range s.Triggers {
			sb.WriteString(fmt.Sprintf("    %s: %s\n", name, normalizeSQL(tr.SQL)))
		}
	} else {
		sb.WriteString("  Triggers: nil\n")
	}
	return sb.String()
}

// masterRow and migrateTable might be needed if tests use db.Select for these.
// For now, assuming FetchDBSchema uses QueryContext which returns sqlt.Rows (*sql.Rows).
type masterRow struct {
	Name string `db:"name"`
	Sql  string `db:"sql"`
}

type migrateTable struct {
	Table  string `db:"table"`
	RowID  int64  `db:"rowid"`
	Parent string `db:"parent"`
	FKID   int64  `db:"fkid"`
}
