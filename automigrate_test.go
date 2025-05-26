package sqlt_test

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	// "os" // No longer directly needed

	"github.com/jmoiron/sqlx" // Needed for the new getTestDB helper
	_ "github.com/mattn/go-sqlite3" // DB driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/james-darko/gort" // Added gort import
	"github.com/james-darko/sqlt"
)

// Re-introduced getTestDB helper function
func getTestDB(t *testing.T) sqlt.DB {
	db, err := sqlx.Open("sqlite3", "file::memory:?_foreign_keys=on")
	require.NoError(t, err, "Failed to open in-memory database")
	return sqlt.Wrap(db)
}


// Helper function to check if a database object exists (can be shared)
func objectExists(t *testing.T, db sqlt.DBReader, objType string, objName string) bool {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM sqlite_master WHERE type = '%s' AND name = '%s'", objType, objName)
	err := db.GetContext(gort.Context(), &count, query)  // Replaced context.Background()
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false
		}
		t.Fatalf("Error checking object existence for %s %s: %v", objType, objName, err)
	}
	return count > 0
}

// Helper to get SQL definition of an object (can be shared)
func getObjectSQL(t *testing.T, db sqlt.DBReader, objName string) string {
	var sqlDef string 
	err := db.GetContext(gort.Context(), &sqlDef, "SELECT sql FROM sqlite_master WHERE name = ?", objName) // Replaced context.Background()
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ""
		}
		t.Logf("Error getting SQL for object %s: %v", objName, err) 
		return ""
	}
	return sqlDef
}


// --- Test Cases For AutoMigrate Start Here ---

func TestAutoMigrate_PerfectMatch(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close() 
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
CREATE INDEX idx_users_name ON users (name);
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := initialSchema 

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)

	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err, "Verify should pass after AutoMigrate with perfect match")
}

func TestAutoMigrate_EmptyDB_CreateAll(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	targetSchema := `
CREATE TABLE items (
    item_id INTEGER PRIMARY KEY,
    description TEXT,
    price REAL
);
CREATE INDEX idx_items_description ON items (description);
CREATE VIEW cheap_items AS SELECT item_id, description FROM items WHERE price < 10.0;
CREATE TRIGGER update_item_price AFTER UPDATE ON items
BEGIN
    UPDATE items SET price = NEW.price WHERE item_id = OLD.item_id;
END;
`
	err := sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)

	assert.True(t, objectExists(t, wrappedDB, "table", "items"), "Table 'items' should exist")
	assert.True(t, objectExists(t, wrappedDB, "index", "idx_items_description"), "Index 'idx_items_description' should exist")
	assert.True(t, objectExists(t, wrappedDB, "view", "cheap_items"), "View 'cheap_items' should exist")
	assert.True(t, objectExists(t, wrappedDB, "trigger", "update_item_price"), "Trigger 'update_item_price' should exist")

	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err, "Verify should pass after creating all objects")
}

func TestAutoMigrate_EmptySchema_DeleteAll(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE items (item_id INTEGER PRIMARY KEY, description TEXT);
CREATE INDEX idx_items_desc ON items (description);
CREATE VIEW items_view AS SELECT description FROM items;
CREATE TRIGGER items_trigger AFTER INSERT ON items BEGIN SELECT 1; END;
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	emptySchema := `-- This schema is empty`

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(emptySchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)

	assert.False(t, objectExists(t, wrappedDB, "table", "items"), "Table 'items' should not exist")
	assert.False(t, objectExists(t, wrappedDB, "index", "idx_items_desc"), "Index 'idx_items_desc' should not exist")
	assert.False(t, objectExists(t, wrappedDB, "view", "items_view"), "View 'items_view' should not exist")
	assert.False(t, objectExists(t, wrappedDB, "trigger", "items_trigger"), "Trigger 'items_trigger' should not exist")
	
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(emptySchema)) // Use ctx
	assert.NoError(t, err, "Verify with empty schema should pass")
}

func TestAutoMigrate_TableColumnReorder(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	_, err = wrappedDB.ExecContext(ctx, "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')") // Use ctx
	require.NoError(t, err)
	_, err = wrappedDB.ExecContext(ctx, "INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')") // Use ctx
	require.NoError(t, err)

	targetSchema := `CREATE TABLE users (name TEXT, id INTEGER PRIMARY KEY, email TEXT);` 

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)

	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err, "Verify should pass after column reorder")

	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}
	var users []User
	err = wrappedDB.SelectContext(ctx, &users, "SELECT id, name, email FROM users ORDER BY id") // Use ctx
	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, 1, users[0].ID); assert.Equal(t, "Alice", users[0].Name); assert.Equal(t, "alice@example.com", users[0].Email)
	assert.Equal(t, 2, users[1].ID); assert.Equal(t, "Bob", users[1].Name); assert.Equal(t, "bob@example.com", users[1].Email)
}

func TestAutoMigrate_TableColumnMismatch_ConflictError(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE items (id INTEGER PRIMARY KEY, price REAL);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `CREATE TABLE items (id INTEGER PRIMARY KEY, price TEXT);`

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	require.Error(t, err, "AutoMigrate should return an error for type mismatch")

	var conflictErr *sqlt.SchemaConflictError
	require.True(t, errors.As(err, &conflictErr), "Error should be a SchemaConflictError")
	assert.Equal(t, "items", conflictErr.ObjectName)
	assert.Equal(t, "TABLE", conflictErr.ObjectType)
	assert.Contains(t, conflictErr.ConflictDetails, "type mismatch")
}

func TestAutoMigrate_TableUnresolvableConflict_ConflictError(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE data (val TEXT);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)
	_, err = wrappedDB.ExecContext(ctx, "INSERT INTO data (val) VALUES ('some text')") // Use ctx
	require.NoError(t, err)

	targetSchema := `CREATE TABLE data (val INTEGER);`

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	require.Error(t, err, "AutoMigrate should return an error for unresolvable conflict")

	var conflictErr *sqlt.SchemaConflictError
	require.True(t, errors.As(err, &conflictErr), "Error should be a SchemaConflictError")
	assert.Equal(t, "data", conflictErr.ObjectName)
	assert.Equal(t, "TABLE", conflictErr.ObjectType)
	assert.Contains(t, conflictErr.ConflictDetails, "type mismatch") 
}


func TestAutoMigrate_TableMissingColumn_ConflictError(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE products (id INTEGER, name TEXT, description TEXT);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `CREATE TABLE products (id INTEGER, name TEXT);`

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	require.Error(t, err, "AutoMigrate should return SchemaConflictError when DB has extra unhandled column")
	
	var conflictErr *sqlt.SchemaConflictError
	require.True(t, errors.As(err, &conflictErr), "Error should be a SchemaConflictError")
	assert.Equal(t, "products", conflictErr.ObjectName)
	assert.Equal(t, "TABLE", conflictErr.ObjectType)
	assert.Contains(t, conflictErr.ConflictDetails, "Extra DB column: 'description'")
}

func TestAutoMigrate_TableExtraColumnInSchema_ConflictError(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE products (id INTEGER, name TEXT);` 
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)
	
	targetSchema := `CREATE TABLE products (id INTEGER, name TEXT, description TEXT);` 
	
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	require.Error(t, err, "AutoMigrate should return SchemaConflictError if schema has extra column and compareStatements reports NoMatch")
	
	var conflictErr *sqlt.SchemaConflictError
	require.True(t, errors.As(err, &conflictErr), "Error should be a SchemaConflictError")
	assert.Equal(t, "products", conflictErr.ObjectName)
	assert.Equal(t, "TABLE", conflictErr.ObjectType)
	assert.Contains(t, conflictErr.ConflictDetails, "Missing Schema column: 'description'")
}


func TestAutoMigrate_Index_Create(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE logs (message TEXT);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE logs (message TEXT);
CREATE INDEX idx_logs_message ON logs (message);
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.True(t, objectExists(t, wrappedDB, "index", "idx_logs_message"), "Index 'idx_logs_message' should be created")
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_Index_Drop(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE logs (message TEXT);
CREATE INDEX idx_logs_message ON logs (message);
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `CREATE TABLE logs (message TEXT);` 

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.False(t, objectExists(t, wrappedDB, "index", "idx_logs_message"), "Index 'idx_logs_message' should be dropped")
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_Index_Recreate(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE logs (message TEXT, level TEXT);
CREATE INDEX idx_logs ON logs (message);
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE logs (message TEXT, level TEXT);
CREATE INDEX idx_logs ON logs (level); 
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.True(t, objectExists(t, wrappedDB, "index", "idx_logs"), "Index 'idx_logs' should still exist")
	
	idxSQL := getObjectSQL(t, wrappedDB, "idx_logs")
	assert.Contains(t, idxSQL, "(\"level\")") 
	assert.NotContains(t, idxSQL, "(\"message\")") 

	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_View_Create(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE users (id INTEGER, name TEXT, is_active INTEGER);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE users (id INTEGER, name TEXT, is_active INTEGER);
CREATE VIEW active_users AS SELECT name FROM users WHERE is_active = 1;
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.True(t, objectExists(t, wrappedDB, "view", "active_users"))
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_View_Drop(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE users (id INTEGER, name TEXT, is_active INTEGER);
CREATE VIEW active_users AS SELECT name FROM users WHERE is_active = 1;
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `CREATE TABLE users (id INTEGER, name TEXT, is_active INTEGER);`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.False(t, objectExists(t, wrappedDB, "view", "active_users"))
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_View_Recreate(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE users (id INTEGER, name TEXT, is_active INTEGER, email TEXT);
CREATE VIEW user_info AS SELECT name, email FROM users WHERE is_active = 1;
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE users (id INTEGER, name TEXT, is_active INTEGER, email TEXT);
CREATE VIEW user_info AS SELECT name, email, id FROM users WHERE is_active = 0; 
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.True(t, objectExists(t, wrappedDB, "view", "user_info"))
	viewSQL := getObjectSQL(t, wrappedDB, "user_info")
	assert.Contains(t, viewSQL, "\"is_active\" = 0") 
	assert.Contains(t, viewSQL, "\"name\"")      
	assert.Contains(t, viewSQL, "\"email\"")
	assert.Contains(t, viewSQL, "\"id\"")
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_Trigger_Create(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE audit_log (entry TEXT);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE audit_log (entry TEXT);
CREATE TRIGGER audit_trigger AFTER INSERT ON audit_log BEGIN INSERT INTO audit_log VALUES ('new entry'); END;
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.True(t, objectExists(t, wrappedDB, "trigger", "audit_trigger"))
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_Trigger_Drop(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE audit_log (entry TEXT);
CREATE TRIGGER audit_trigger AFTER INSERT ON audit_log BEGIN INSERT INTO audit_log VALUES ('new entry'); END;
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `CREATE TABLE audit_log (entry TEXT);`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.False(t, objectExists(t, wrappedDB, "trigger", "audit_trigger"))
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_Trigger_Recreate(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE audit_log (entry TEXT, ts DATETIME);
CREATE TRIGGER audit_trigger AFTER INSERT ON audit_log BEGIN INSERT INTO audit_log VALUES ('old entry', datetime('now')); END;
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE audit_log (entry TEXT, ts DATETIME);
CREATE TRIGGER audit_trigger AFTER UPDATE ON audit_log BEGIN INSERT INTO audit_log VALUES ('updated entry', datetime('now')); END;
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.True(t, objectExists(t, wrappedDB, "trigger", "audit_trigger"))
	triggerSQL := getObjectSQL(t, wrappedDB, "audit_trigger")
	assert.Contains(t, strings.ToLower(triggerSQL), "after update")
	assert.Contains(t, strings.ToLower(triggerSQL), "'updated entry'")
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_ComplexScenario(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT); 
CREATE TABLE old_table (data TEXT); 
CREATE INDEX idx_users_email ON users(email); 
CREATE VIEW user_emails AS SELECT email FROM users; 
CREATE TRIGGER user_update_trigger AFTER UPDATE ON users BEGIN SELECT 'updated'; END;
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)
	_, err = wrappedDB.ExecContext(ctx, "INSERT INTO users (id, name, email) VALUES (1, 'Initial User', 'initial@example.com')") // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE users (email TEXT, name TEXT, id INTEGER PRIMARY KEY); 
CREATE TABLE new_table (info TEXT); 
CREATE INDEX idx_users_name ON users(name); 
CREATE VIEW user_names AS SELECT name FROM users; 
CREATE TRIGGER user_update_trigger AFTER UPDATE ON users BEGIN SELECT 'updated'; END; 
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err, "AutoMigrate complex scenario failed")

	assert.True(t, objectExists(t, wrappedDB, "table", "users"))
	assert.True(t, objectExists(t, wrappedDB, "table", "new_table"))
	assert.False(t, objectExists(t, wrappedDB, "table", "old_table"))
	assert.True(t, objectExists(t, wrappedDB, "index", "idx_users_name"))
	assert.False(t, objectExists(t, wrappedDB, "index", "idx_users_email"))
	assert.True(t, objectExists(t, wrappedDB, "view", "user_names"))
	assert.False(t, objectExists(t, wrappedDB, "view", "user_emails"))
	assert.True(t, objectExists(t, wrappedDB, "trigger", "user_update_trigger"))

	var user struct { Name string; Email string; ID int }
	err = wrappedDB.GetContext(ctx, &user, "SELECT name, email, id FROM users WHERE id = 1") // Use ctx
	require.NoError(t, err)
	assert.Equal(t, "Initial User", user.Name)
	assert.Equal(t, "initial@example.com", user.Email)

	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err, "Verify should pass after complex AutoMigrate")
}

func TestAutoMigrate_TableAdded_NoError(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE existing_table (id INTEGER);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE existing_table (id INTEGER);
CREATE TABLE new_table (data TEXT); 
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.True(t, objectExists(t, wrappedDB, "table", "new_table"))
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_TableDropped_NoError(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE table_to_keep (id INTEGER);
CREATE TABLE table_to_drop (data TEXT);
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `CREATE TABLE table_to_keep (id INTEGER);` 
	
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.True(t, objectExists(t, wrappedDB, "table", "table_to_keep"))
	assert.False(t, objectExists(t, wrappedDB, "table", "table_to_drop"))
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_SchemaWithOnlyComments(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE my_table (id INTEGER);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `
-- This is a comment.
-- Another comment.
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)
	assert.False(t, objectExists(t, wrappedDB, "table", "my_table"), "Table 'my_table' should be dropped")
}

func TestAutoMigrate_DuplicateObjectNameInSchema_Error(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	targetSchema := `
CREATE TABLE duplicate_table (id INTEGER);
CREATE TABLE duplicate_table (name TEXT); 
`
	err := sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate object name 'duplicate_table' found in schema definition")
}

func TestAutoMigrate_QuotedIdentifiers(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialDBSetup := `CREATE TABLE "order" ("item-name" TEXT, quantity INTEGER);`
	_, err := wrappedDB.ExecContext(ctx, initialDBSetup) // Use ctx
	require.NoError(t, err)
	_, err = wrappedDB.ExecContext(ctx, `INSERT INTO "order" ("item-name", quantity) VALUES ('test item', 5);`) // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE "order" (quantity INTEGER, "item-name" TEXT);
CREATE INDEX "idx_order_item-name" ON "order" ("item-name");
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err, "AutoMigrate with quoted identifiers failed")

	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err, "Verify failed for schema with quoted identifiers")

	var item struct { ItemName string `db:"item-name"`; Quantity int `db:"quantity"` }
	err = wrappedDB.GetContext(ctx, &item, `SELECT "item-name", quantity FROM "order" WHERE quantity = 5`) // Use ctx
	require.NoError(t, err)
	assert.Equal(t, "test item", item.ItemName)
	assert.Equal(t, 5, item.Quantity)

	assert.True(t, objectExists(t, wrappedDB, "table", "order"))
	assert.True(t, objectExists(t, wrappedDB, "index", "idx_order_item-name"))
}

func TestAutoMigrate_TableConflict_DBExtraColumnAndSchemaDiff(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, version INTEGER, unused_db_column TEXT);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)
	
	targetSchema := `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, version TEXT);` 

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	require.Error(t, err, "AutoMigrate should return SchemaConflictError")
	
	var conflictErr *sqlt.SchemaConflictError
	require.True(t, errors.As(err, &conflictErr), "Error should be a SchemaConflictError")
	assert.Equal(t, "products", conflictErr.ObjectName)
	assert.Equal(t, "TABLE", conflictErr.ObjectType)
	assert.Contains(t, conflictErr.ConflictDetails, "type mismatch") 
	assert.Contains(t, conflictErr.ConflictDetails, "Extra DB column: 'unused_db_column'")
}

func TestAutoMigrate_TableNameConflictsWithExistingIndexName(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE some_table (data TEXT);
CREATE INDEX my_object ON some_table (data);
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE some_table (data TEXT); 
CREATE TABLE my_object (id INTEGER); 
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)

	assert.True(t, objectExists(t, wrappedDB, "table", "my_object"))
	assert.False(t, objectExists(t, wrappedDB, "index", "my_object"))
	
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_IndexNameConflictsWithExistingTableName(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `CREATE TABLE my_object (id INTEGER);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE my_other_table (data TEXT);
CREATE INDEX my_object ON my_other_table (data);
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)

	assert.True(t, objectExists(t, wrappedDB, "index", "my_object"))
	assert.False(t, objectExists(t, wrappedDB, "table", "my_object"))

	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err)
}

func TestAutoMigrate_CreateTableWithForeignKey(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

    targetSchema := `
CREATE TABLE parent (id INTEGER PRIMARY KEY);
CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent(id));
`
    err := sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
    assert.NoError(t, err)

    assert.True(t, objectExists(t, wrappedDB, "table", "parent"))
    assert.True(t, objectExists(t, wrappedDB, "table", "child"))

    err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
    assert.NoError(t, err)

    _, err = wrappedDB.ExecContext(ctx, "INSERT INTO child (id, parent_id) VALUES (1, 100);")  // Use ctx
    assert.Error(t, err) 
}

func TestAutoMigrate_TableReorderWithForeignKey(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

	initialSchema := `
CREATE TABLE parent (id INTEGER PRIMARY KEY);
CREATE TABLE child (id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent(id));`
	_, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
	require.NoError(t, err)

	_, err = wrappedDB.ExecContext(ctx, "INSERT INTO parent (id) VALUES (10)") // Use ctx
	require.NoError(t, err)
	_, err = wrappedDB.ExecContext(ctx, "INSERT INTO child (id, name, parent_id) VALUES (1, 'Child A', 10)") // Use ctx
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE parent (id INTEGER PRIMARY KEY);
CREATE TABLE child (parent_id INTEGER, name TEXT, id INTEGER PRIMARY KEY, FOREIGN KEY (parent_id) REFERENCES parent(id));`

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
	assert.NoError(t, err)

	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema)) // Use ctx
	assert.NoError(t, err, "Verify should pass after reordering table with FK")

	var c struct { ID int `db:"id"`; Name string `db:"name"`; ParentID int `db:"parent_id"`	}
	err = wrappedDB.GetContext(ctx, &c, "SELECT id, name, parent_id FROM child WHERE id = 1") // Use ctx
	require.NoError(t, err)
	assert.Equal(t, 1, c.ID); assert.Equal(t, "Child A", c.Name); assert.Equal(t, 10, c.ParentID)

    _, err = wrappedDB.ExecContext(ctx, "INSERT INTO child (id, name, parent_id) VALUES (2, 'Child B', 999);")  // Use ctx
    assert.Error(t, err)
}

func TestAutoMigrate_TableConstraintChanged_ConflictError(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

    initialSchema := `CREATE TABLE data (id INTEGER, val TEXT);`
    _, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
    require.NoError(t, err)

    targetSchema := `CREATE TABLE data (id INTEGER, val TEXT NOT NULL);` 

    err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
    require.Error(t, err, "AutoMigrate should return SchemaConflictError for constraint change")

    var conflictErr *sqlt.SchemaConflictError
    require.True(t, errors.As(err, &conflictErr), "Error should be a SchemaConflictError")
    assert.Equal(t, "data", conflictErr.ObjectName)
    assert.Equal(t, "TABLE", conflictErr.ObjectType)
    assert.Contains(t, conflictErr.ConflictDetails, "constraint") 
}

func TestAutoMigrate_TablePrimaryKeyChanged_ConflictError(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context() // Use gort.Context()

    initialSchema := `CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);`
    _, err := wrappedDB.ExecContext(ctx, initialSchema) // Use ctx
    require.NoError(t, err)

    targetSchema := `CREATE TABLE data (id INTEGER, val TEXT PRIMARY KEY);` 

    err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // Use ctx, allowTableDeletes=true
    require.Error(t, err, "AutoMigrate should return SchemaConflictError for PK change")

    var conflictErr *sqlt.SchemaConflictError
    require.True(t, errors.As(err, &conflictErr), "Error should be a SchemaConflictError")
    assert.Equal(t, "data", conflictErr.ObjectName)
    assert.Equal(t, "TABLE", conflictErr.ObjectType)
    assert.Contains(t, conflictErr.ConflictDetails, "constraint") 
}

// TestDisallowTableDeletes_NoErrorWhenNoTablesDropped tests that AutoMigrate proceeds normally
// when allowTableDeletes is false but no tables would have been dropped anyway.
func TestDisallowTableDeletes_NoErrorWhenNoTablesDropped(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context()

	initialSchema := `CREATE TABLE users (id INTEGER);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema)
	require.NoError(t, err)

	// Target schema adds a table, does not drop 'users'
	targetSchema := `
CREATE TABLE users (id INTEGER);
CREATE TABLE items (name TEXT);
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), false) // allowTableDeletes = false
	assert.NoError(t, err, "AutoMigrate should not error if no tables were to be dropped")
	assert.True(t, objectExists(t, wrappedDB, "table", "users"))
	assert.True(t, objectExists(t, wrappedDB, "table", "items"))
}

// TestDisallowTableDeletes_ErrorWhenTablesWouldBeDropped tests that AutoMigrate returns
// ErrTableDeletionNotAllowed if tables would be dropped and allowTableDeletes is false.
func TestDisallowTableDeletes_ErrorWhenTablesWouldBeDropped(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context()

	initialSchema := `
CREATE TABLE users (id INTEGER);
CREATE TABLE items_to_drop (name TEXT);
CREATE TABLE orders_to_drop (order_id INTEGER);
CREATE INDEX idx_items ON items_to_drop(name); 
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema)
	require.NoError(t, err)

	targetSchema := `CREATE TABLE users (id INTEGER);`

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), false) // allowTableDeletes = false
	require.Error(t, err, "AutoMigrate should return an error when table drops are disallowed")

	var tableDeletionErr sqlt.ErrTableDeletionNotAllowed
	isCorrectErrorType := errors.As(err, &tableDeletionErr)
	require.True(t, isCorrectErrorType, "Error should be of type ErrTableDeletionNotAllowed")
	
	expectedTables := []string{"items_to_drop", "orders_to_drop"}
	actualTables := tableDeletionErr.Tables
	assert.ElementsMatch(t, expectedTables, actualTables, "List of tables to be deleted is not as expected")

	assert.True(t, objectExists(t, wrappedDB, "table", "users"), "Table 'users' should still exist")
	assert.True(t, objectExists(t, wrappedDB, "table", "items_to_drop"), "Table 'items_to_drop' should NOT have been dropped")
	assert.True(t, objectExists(t, wrappedDB, "table", "orders_to_drop"), "Table 'orders_to_drop' should NOT have been dropped")
	assert.True(t, objectExists(t, wrappedDB, "index", "idx_items"), "Index 'idx_items' should NOT have been dropped as its table was not dropped")
}

// TestAllowTableDeletes_SuccessWhenTablesAreDropped tests that AutoMigrate proceeds normally
// and drops tables if allowTableDeletes is true.
func TestAllowTableDeletes_SuccessWhenTablesAreDropped(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context()

	initialSchema := `
CREATE TABLE users (id INTEGER);
CREATE TABLE items_to_drop (name TEXT);
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema)
	require.NoError(t, err)

	targetSchema := `CREATE TABLE users (id INTEGER);`

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), true) // allowTableDeletes = true
	assert.NoError(t, err, "AutoMigrate should proceed with table deletions when allowed")

	assert.True(t, objectExists(t, wrappedDB, "table", "users"))
	assert.False(t, objectExists(t, wrappedDB, "table", "items_to_drop"), "Table 'items_to_drop' should have been dropped")
}

// TestDisallowTableDeletes_ViewDropAllowed tests that non-table objects (like views)
// are still dropped even if allowTableDeletes is false.
func TestDisallowTableDeletes_ViewDropAllowed(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context()

	initialSchema := `
CREATE TABLE users (id INTEGER);
CREATE VIEW user_view AS SELECT id FROM users;
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema)
	require.NoError(t, err)

	targetSchema := `CREATE TABLE users (id INTEGER);` 

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), false) 
	assert.NoError(t, err, "AutoMigrate should allow non-table drops even if table drops are disallowed")
	assert.True(t, objectExists(t, wrappedDB, "table", "users"))
	assert.False(t, objectExists(t, wrappedDB, "view", "user_view"), "View 'user_view' should have been dropped")
}

// TestDisallowTableDeletes_IndexDropAllowed tests that non-table objects (like indexes)
// are still dropped even if allowTableDeletes is false.
func TestDisallowTableDeletes_IndexDropAllowed(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context()

	initialSchema := `
CREATE TABLE users (id INTEGER);
CREATE INDEX idx_user_id ON users(id);
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema)
	require.NoError(t, err)

	targetSchema := `CREATE TABLE users (id INTEGER);` 

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), false) 
	assert.NoError(t, err, "AutoMigrate should allow non-table drops even if table drops are disallowed")
	assert.True(t, objectExists(t, wrappedDB, "table", "users"))
	assert.False(t, objectExists(t, wrappedDB, "index", "idx_user_id"), "Index 'idx_user_id' should have been dropped")
}

// TestDisallowTableDeletes_TriggerDropAllowed tests that non-table objects (like triggers)
// are still dropped even if allowTableDeletes is false.
func TestDisallowTableDeletes_TriggerDropAllowed(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context()

	initialSchema := `
CREATE TABLE users (id INTEGER);
CREATE TRIGGER user_trigger AFTER INSERT ON users BEGIN SELECT 1; END;
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema)
	require.NoError(t, err)

	targetSchema := `CREATE TABLE users (id INTEGER);` 

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), false) 
	assert.NoError(t, err, "AutoMigrate should allow non-table drops even if table drops are disallowed")
	assert.True(t, objectExists(t, wrappedDB, "table", "users"))
	assert.False(t, objectExists(t, wrappedDB, "trigger", "user_trigger"), "Trigger 'user_trigger' should have been dropped")
}

// TestDisallowTableDeletes_TableRecreationConflictError tests that if a table definition
// conflicts (e.g. type change), it returns SchemaConflictError even if allowTableDeletes is false,
// because this isn't a simple deletion but a modification conflict.
func TestDisallowTableDeletes_TableRecreationConflictError(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context()

	initialSchema := `CREATE TABLE users (id INTEGER, name TEXT);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema)
	require.NoError(t, err)

	targetSchema := `CREATE TABLE users (id INTEGER, name INTEGER);` // Type change

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), false) 
	require.Error(t, err)
	
	var conflictErr *sqlt.SchemaConflictError
	isConflictError := errors.As(err, &conflictErr)
	assert.True(t, isConflictError, "Expected a SchemaConflictError due to type change, not ErrTableDeletionNotAllowed")
	if isConflictError {
		assert.Equal(t, "users", conflictErr.ObjectName)
		assert.Contains(t, conflictErr.ConflictDetails, "type mismatch")
	}
	assert.True(t, objectExists(t, wrappedDB, "table", "users"))
	originalSQL := getObjectSQL(t, wrappedDB, "users")
	assert.Contains(t, strings.ToUpper(originalSQL), "NAME TEXT")
}

// TestDisallowTableDeletes_TableReorderAllowed tests that table reordering is allowed
// even if allowTableDeletes is false, as it's not a destructive deletion.
func TestDisallowTableDeletes_TableReorderAllowed(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context()

	initialSchema := `CREATE TABLE users (id INTEGER, name TEXT, email TEXT);`
	_, err := wrappedDB.ExecContext(ctx, initialSchema)
	require.NoError(t, err)

	targetSchema := `CREATE TABLE users (name TEXT, id INTEGER, email TEXT);` // Reorder

	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), false) 
	assert.NoError(t, err, "Table reordering should be allowed even if table deletes are disallowed")
	
	err = sqlt.Verify(ctx, wrappedDB, strings.NewReader(targetSchema))
	assert.NoError(t, err)
}

// TestDisallowTableDeletes_TableReplacedByIndex_ConflictError tests that if a schema
// tries to replace a table with an index of the same name, it's a conflict,
// not a table deletion (even if allowTableDeletes is true, this specific scenario
// is handled by the type mismatch logic first).
func TestDisallowTableDeletes_TableReplacedByIndex_ConflictError(t *testing.T) {
	t.Parallel()
	wrappedDB := getTestDB(t)
	defer wrappedDB.Close()
	ctx := gort.Context()

	initialSchema := `
CREATE TABLE my_object (id INTEGER);
CREATE TABLE another_table (data TEXT);
`
	_, err := wrappedDB.ExecContext(ctx, initialSchema)
	require.NoError(t, err)

	targetSchema := `
CREATE TABLE another_table (data TEXT);
CREATE INDEX my_object ON another_table(data);
`
	err = sqlt.AutoMigrate(ctx, wrappedDB, strings.NewReader(targetSchema), false) 
	require.Error(t, err)

	var tableDeletionErr sqlt.ErrTableDeletionNotAllowed
	isDeletionError := errors.As(err, &tableDeletionErr)
	require.True(t, isDeletionError, "Expected ErrTableDeletionNotAllowed")
	if isDeletionError {
		assert.Contains(t, tableDeletionErr.Tables, "my_object")
	}
	assert.True(t, objectExists(t, wrappedDB, "table", "my_object")) 
	assert.False(t, objectExists(t, wrappedDB, "index", "my_object")) 
}
//[end of automigrate_test.go]
// Removed the duplicated [end of automigrate_test.go] marker
// and the extraneous '[' character if it was present.
// Assuming the file ends cleanly after the last curly brace of the last test function.
