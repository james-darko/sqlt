package sqlt_test

import (
	"strings"
	"testing"

	"github.com/james-darko/gort"
	"github.com/james-darko/sqlt"
	_ "github.com/mattn/go-sqlite3"
)

const schema = `
CREATE TABLE version (
    id INTEGER PRIMARY KEY,
    version INTEGER NOT NULL
);
INSERT INTO version (version) VALUES (1);
CREATE INDEX idx_version ON version (version);

CREATE TABLE test_table_1 (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL
);

CREATE TABLE test_table_2 (
	id INTEGER PRIMARY KEY,
	column1 TEXT NOT NULL,
	column2 TEXT NOT NULL
);
`

const schemaTable3 = `
CREATE TABLE test_table_3 (
	id INTEGER PRIMARY KEY,
	column1 TEXT NOT NULL,
	column2 TEXT NOT NULL
);
INSERT INTO test_table_3 (column1, column2) VALUES ('value1', 'value2');
`

func TestVerifySuccess(t *testing.T) {
	t.Parallel()
	// Setup
	db, err := sqlt.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := gort.Context()

	// Init db
	err = sqlt.ExecString(ctx, db, schema)
	if err != nil {
		t.Fatalf("Failed to setup test db: %v", err)
	}

	// Verify schema equals itself
	err = sqlt.VerifyString(ctx, db, schema)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
}

func TestVerifyFailure(t *testing.T) {
	t.Parallel()
	// Setup
	db, err := sqlt.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := gort.Context()

	// Init db
	err = sqlt.ExecString(ctx, db, schema)
	if err != nil {
		t.Fatalf("Failed to setup test db: %v", err)
	}

	// Verify with a schema that has an extra table
	err = sqlt.VerifyString(ctx, db, schema+"\n"+schemaTable3)
	if err == nil {
		t.Fatal("Expected Verify to fail, but it succeeded")
	}
	if !strings.Contains(err.Error(), "test_table_3 not found") {
		t.Fatalf("Verify failed with unexpected error: %v", err)
	}
}

func TestMigration(t *testing.T) {
	t.Parallel()
	// Setup
	db, err := sqlt.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := gort.Context()

	// Init db
	err = sqlt.ExecString(ctx, db, schema)
	if err != nil {
		t.Fatalf("Failed to setup test db: %v", err)
	}
	versions := sqlt.MigrationMap{
		1: sqlt.MigrateFunc(db, 1, nil, func(tx sqlt.Tx, restore func() error) error {
			return sqlt.ExecTxString(tx, schemaTable3)
		}),
	}

	// Migrate
	err = sqlt.Migration(ctx, db, versions)
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify migration
	err = sqlt.VerifyString(ctx, db, schema+"\n"+schemaTable3)
	if err != nil {
		t.Fatalf("Verify after migration failed: %v", err)
	}
}
