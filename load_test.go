package sqlt_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "github.com/tursodatabase/libsql-client-go/libsql" 
    _ "github.com/mattn/go-sqlite3" 

	"github.com/james-darko/gort" // Added gort import
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
	
	err = db.SQLX().PingContext(gort.Context()) // Replaced context.Background()
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
	ctx := gort.Context() // Use gort.Context()

	assert.Equal(t, "sqlite3", db.DriverName())

	var fkEnabled int
	err = db.GetContext(ctx, &fkEnabled, "PRAGMA foreign_keys;") // Use ctx
	require.NoError(t, err)
	assert.Equal(t, 1, fkEnabled, "Foreign keys should be enabled via DSN")

	_, err = db.ExecContext(ctx, "CREATE TABLE parent (id INTEGER PRIMARY KEY);") // Use ctx
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent(id));") // Use ctx
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO child (id, parent_id) VALUES (1, 100);") // Use ctx
    assert.Error(t, err, "Insert should fail due to foreign key constraint")
	if err != nil {
		assert.Contains(t, strings.ToLower(err.Error()), "foreign key constraint failed")
	}
}

func TestLoadDB_Error_NoURL(t *testing.T) {
	// t.Parallel() 
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

func TestLoadDB_LibSQL_Success(t *testing.T) {
	// t.Parallel() 
	tursoTestURL := os.Getenv("TURSO_TEST_URL") 
	tursoTestToken := os.Getenv("DATABASE_TOKEN") // Corrected to DATABASE_TOKEN

	if tursoTestURL == "" {
		t.Skip("Skipping Turso test: TURSO_TEST_URL not set. For local dev, use a dummy like 'libsql://local.turso.io?mode=memory'")
	}


	sqlt.ResetDB()
	t.Setenv("DATABASE_URL", tursoTestURL) 
	if tursoTestToken != "" {
		t.Setenv("DATABASE_TOKEN", tursoTestToken)
	} else if strings.Contains(tursoTestURL, "turso.io") && !strings.Contains(tursoTestURL, "mode=memory"){
		t.Skip("Skipping Turso test: DATABASE_TOKEN not set for a remote Turso URL.") // Corrected env var name
	}


	db, err := sqlt.LoadDB()
	require.NoError(t, err, "LoadDB failed for LibSQL setup") 
	require.NotNil(t, db)
	defer db.Close()

	assert.Equal(t, "libsql", db.DriverName())

	pingErr := db.SQLX().PingContext(gort.Context()) // Replaced context.Background()
    if strings.Contains(tursoTestURL, "dummy.turso.io") { 
        assert.Error(t, pingErr, "Ping should fail for a dummy Turso URL if it attempts connection")
    } else if tursoTestURL != "" { 
        assert.NoError(t, pingErr, "Failed to ping LibSQL DB opened via LoadDB with URL: %s", tursoTestURL)
    }
}


func TestLoadDB_LibSQL_NoToken(t *testing.T) {
	// t.Parallel() 
	sqlt.ResetDB()
	t.Setenv("DATABASE_URL", "libsql://my-prod-db.turso.io") 
	
	originalToken, tokenWasSet := os.LookupEnv("DATABASE_TOKEN")
	os.Unsetenv("DATABASE_TOKEN")
	if tokenWasSet {
		defer os.Setenv("DATABASE_TOKEN", originalToken)
	}

	db, err := sqlt.LoadDB()
	
	if err != nil { 
		assert.Error(t, err) 
		assert.Contains(t, err.Error(), "DATABASE_TOKEN env var not found") 
	} else if db != nil { 
		defer db.Close()
		assert.Equal(t, "libsql", db.DriverName())
		pingErr := db.SQLX().PingContext(gort.Context()) // Replaced context.Background()
		assert.Error(t, pingErr, "Ping should fail for LibSQL if token is required and missing and LoadDB didn't catch it.")
		if pingErr != nil {
			assert.Contains(t, strings.ToLower(pingErr.Error()), "auth") 
		}
	} else {
		t.Fatal("LoadDB returned nil db and nil error, which is unexpected if token is required but missing.")
	}
}
