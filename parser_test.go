package sqlt_test

import (
	"fmt"
	"strings"
	"testing"

	rsql "github.com/rqlite/sql"
)

var uniqueTable = `
CREATE TABLE domain_kv (
    id INTEGER PRIMARY KEY,
    domain INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    key_ TEXT NOT NULL,
    val TEXT NOT NULL,
    UNIQUE(domain, key_),
    FOREIGN KEY(domain) REFERENCES domain(id) ON DELETE CASCADE
);
`

func TestParser(t *testing.T) {
	buf := strings.NewReader(uniqueTable)
	parser := rsql.NewParser(buf)
	stmt, err := parser.ParseStatement()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(stmt)
}
