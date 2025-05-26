package sqlt

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"

	"github.com/james-darko/gort"
)

func loadDB() (DB, error) {
	driver := os.Getenv("DATABASE_DRIVER")
	if driver == "" {
		driver = "sqlite3"
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, fmt.Errorf("DATABASE_URL env var not found")
	}
	if strings.HasPrefix(url, "libsql:") {
		driver = "libsql"
		token := os.Getenv("DATABASE_TOKEN")
		if token == "" {
			return nil, fmt.Errorf("DATABASE_TOKEN env var not found")
		}
		url = url + "?authToken=" + token
	}
	db, err := Open(driver, url)
	if err != nil {
		return nil, fmt.Errorf("problem opening database: %v", err)
	}
	return db, nil
}

// LoadDB returns a database handle from environment variables.
// The result is cached after the first call. See ResetDB for resetting the cache.
//
// Env vars:
//
// DATABASE_URL: required
//
// DATABASE_DRIVER: optional. Defaults to "sqlite3". Switches to "libsql" if DATABASE_URL starts with "libsql".
//
// DATABASE_TOKEN: optional. If DATABASE_URL starts with "libsql", DATABASE_TOKEN will be appended accordingly for turso auth.
func LoadDB() (DB, error) {
	dbPtr := loadDBHandle.Load()
	if dbPtr == nil {
		return nil, fmt.Errorf("loadDBHandle is not initialized")
	}
	return (*dbPtr)()
}

// Will apply migrations and verify the schema if provided.
//
// See LoadDB for primary database loading.
//
// If schema is nil, and DATABASE_SCHEMA env file var is set, it will be used for validation.
//
// The result is cached after the first call. See ResetDB for resetting the cache.
func FullLoadDB(ctx context.Context, schema io.Reader, versions MigrationMap) (DB, error) {
	fn := fullLoadDBHandle.Load()
	if fn == nil {
		return nil, fmt.Errorf("fullLoadDBHandle is not initialized")
	}
	return (*fn)(ctx, schema, versions)
}

func fullLoadDB(ctx context.Context, schema io.Reader, versions MigrationMap) (DB, error) {
	db, err := LoadDB()
	if err != nil {
		return nil, err
	}
	if versions != nil {
		if err := Migrate(ctx, db, versions); err != nil {
			return nil, err
		}
	}
	if schema != nil {
		if err := Verify(ctx, db, schema); err != nil {
			return nil, err
		}
	} else if _, ok := gort.Env("DATABASE_SCHEMA"); ok {
		if err := VerifyFromEnv(ctx, db); err != nil {
			return nil, fmt.Errorf("could not verify database schema: %w", err)
		}
	}
	return db, nil
}

// ResetDB resets the cached database LoadDB and derivatives use.
func ResetDB() {
	fn := loadDB
	loadDBHandle.Store(&fn)
	fillFn := fullLoadDB
	fullLoadDBHandle.Store(&fillFn)
}

var loadDBHandle atomic.Pointer[func() (DB, error)]

var fullLoadDBHandle atomic.Pointer[func(context.Context, io.Reader, MigrationMap) (DB, error)]

func init() {
	ResetDB()
}
