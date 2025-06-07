package sqlt_test

import (
	"testing"

	"github.com/james-darko/sqlt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestSeq(t *testing.T) {
	t.Parallel()
	// Create an in-memory SQLite database
	db, err := sqlt.Open("sqlite3", ":memory:")
	assert.NoError(t, err, "Failed to open SQLite database")
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`)
	assert.NoError(t, err, "Failed to create test table")

	// Insert test data
	_, err = db.Exec(`INSERT INTO test (name) VALUES ('Alice'), ('Bob'), ('Charlie')`)
	assert.NoError(t, err, "Failed to insert test data")

	type destType struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}
	var results []destType
	var dest destType
	rows := db.SelectSeq(`SELECT id, name FROM test`)
	for range rows.Iter(&dest) {
		results = append(results, dest)
	}
	if rows.Err() != nil {
		assert.FailNow(t, "Seq encountered an error", rows.Err())
	}
	assert.NoError(t, rows.Err(), "Seq.Err() should return no error")
	assert.Equal(t, 3, len(results), "Should have 3 results")
	assert.Equal(t, "Alice", results[0].Name, "First result should be Alice")
	assert.Equal(t, "Bob", results[1].Name, "Second result should be Bob")
	assert.Equal(t, "Charlie", results[2].Name, "Third result should be Charlie")
}
