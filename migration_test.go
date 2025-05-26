package sqlt_test

import (
	"fmt" // Keep for TestMigration
	"strings"
	"testing"
	// "os" // No longer needed for t.Setenv

	// "github.com/jmoiron/sqlx" // No longer needed here, getTestDB is in automigrate_test.go
	"github.com/james-darko/gort" 
	"github.com/james-darko/sqlt"
	_ "github.com/mattn/go-sqlite3"
	// "github.com/stretchr/testify/require" // Removed as getTestDB is no longer local
)

// getTestDB helper function removed, will use the one from automigrate_test.go

const base = `
CREATE TABLE version (
    id INTEGER PRIMARY KEY,
    version INTEGER NOT NULL
);
INSERT INTO version (version) VALUES (1);
CREATE INDEX idx_version ON version (version);

CREATE TABLE table_1 (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL
);`

const basePlus2 = base + `
CREATE TABLE table_2 (
	id INTEGER PRIMARY KEY,
	column1 TEXT NOT NULL,
	column2 TEXT NOT NULL
);
CREATE INDEX idx_table_2 ON table_2 (column1);`

const table2WithRemovedColumn = base + `
CREATE TABLE table_2 (
	id INTEGER PRIMARY KEY,
	column2 TEXT NOT NULL
);`

const table2NewWithRemovedColumn = `
CREATE TABLE new_table_2 (
	id INTEGER PRIMARY KEY,
	column2 TEXT NOT NULL
);`

const table3 = `
CREATE TABLE table_3 (
	id INTEGER PRIMARY KEY,
	column1 TEXT NOT NULL,
	column2 TEXT NOT NULL
);
INSERT INTO table_3 (column1, column2) VALUES ('value1', 'value2');
`

func TestVerifySuccess(t *testing.T) {
	t.Parallel()
	db := getTestDB(t) // Uses getTestDB from automigrate_test.go
	defer db.Close()
	
	ctx := gort.Context() 

	// Init db
	err := sqlt.ExecString(ctx, db, basePlus2)
	if err != nil {
		t.Fatalf("Failed to setup test db: %v", err)
	}

	// Verify schema equals itself
	err = sqlt.VerifyString(ctx, db, basePlus2)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
}

func TestVerifyFailure(t *testing.T) {
	t.Parallel()
	db := getTestDB(t) // Uses getTestDB from automigrate_test.go
	defer db.Close()

	ctx := gort.Context()

	// Init db
	err := sqlt.ExecString(ctx, db, basePlus2)
	if err != nil {
		t.Fatalf("Failed to setup test db: %v", err)
	}

	// Verify with a schema that has an extra table
	err = sqlt.VerifyString(ctx, db, basePlus2+"\n"+table3)
	expectedErrorSubstring := "object 'table_3' from schema not found in database"
	if err == nil {
		t.Fatal("Expected Verify to fail, but it succeeded")
	} else if !strings.Contains(err.Error(), expectedErrorSubstring) {
		t.Fatalf("Verify failed with an unexpected error message: %v\nExpected substring: %s", err, expectedErrorSubstring)
	}
}

func TestMigration(t *testing.T) {
	t.Parallel()
	db := getTestDB(t) // Uses getTestDB from automigrate_test.go
	defer db.Close()

	ctx := gort.Context()

	// Init db
	err := sqlt.ExecString(ctx, db, basePlus2)
	if err != nil {
		t.Fatalf("Failed to setup test db: %v", err)
	}
	versions := sqlt.MigrationMap{
		1: sqlt.MigrateFunc(db, 1, []string{"table_2"}, func(tx sqlt.Tx, restore func() error) error {
			err := sqlt.ExecTxString(tx, table3)
			if err != nil {
				return fmt.Errorf("Could not Exec addedTableSchema: %w", err)
			}
			return sqlt.ExecTxString(tx, table2NewWithRemovedColumn)
		}),
	}

	// Migrate
	err = sqlt.Migrate(ctx, db, versions)
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify migration
	err = sqlt.VerifyString(ctx, db, base+"\n"+table3+"\n"+table2WithRemovedColumn)
	if err != nil {
		t.Fatalf("Verify after migration failed: %v", err)
	}
}
