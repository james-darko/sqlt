package sqlt_test

import (
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "github.com/tursodatabase/libsql-client-go/libsql"

	"github.com/james-darko/gort"
	"github.com/james-darko/sqlt"
)

func TestLoadDB_Success_SQLite(t *testing.T) {
	// t.Parallel()
	sqlt.ResetDB()
	t.Setenv("DATABASE_DRIVER", "sqlite3")
	t.Setenv("DATABASE_URL", ":memory:")

	db, err := sqlt.LoadDB()
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	assert.Equal(t, "sqlite3", db.DriverName())

	err = db.SQLX().PingContext(gort.Context())
	assert.NoError(t, err, "Failed to ping SQLite DB opened via LoadDB")
}

func TestLoadDB_Success_SQLite_DSNForeignKeys(t *testing.T) {
	// t.Parallel()
	sqlt.ResetDB()
	t.Setenv("DATABASE_DRIVER", "sqlite3")
	t.Setenv("DATABASE_URL", "file::memory:?_foreign_keys=on")

	db, err := sqlt.LoadDB()
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
	ctx := gort.Context()

	assert.Equal(t, "sqlite3", db.DriverName())

	var fkEnabled int
	err = db.GetContext(ctx, &fkEnabled, "PRAGMA foreign_keys;")
	require.NoError(t, err)
	assert.Equal(t, 1, fkEnabled, "Foreign keys should be enabled via DSN")

	_, err = db.ExecContext(ctx, "CREATE TABLE parent (id INTEGER PRIMARY KEY);")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		"CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent(id));")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO child (id, parent_id) VALUES (1, 100);")
	assert.Error(t, err, "Insert should fail due to foreign key constraint")
	if err != nil {
		assert.Contains(t, strings.ToLower(err.Error()), "foreign key constraint failed")
	}
}

func TestLoadDB_Error_NoURL(t *testing.T) {
	sqlt.ResetDB()
	t.Setenv("DATABASE_DRIVER", "sqlite3")

	originalURL, urlWasSet := os.LookupEnv("DATABASE_URL")
	os.Unsetenv("DATABASE_URL")
	if urlWasSet {
		defer os.Setenv("DATABASE_URL", originalURL)
	}

	_, err := sqlt.LoadDB()
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "DATABASE_URL env var not found")
	}
}
