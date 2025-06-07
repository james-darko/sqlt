package sqlt

import (
	"testing"
)

func TestImplementations(t *testing.T) {
	// just needs to compile
	db := &sqlxDB{}
	var _ DB = db
	var _ Sqler = db
	tx := &txWrapper{}
	var _ Tx = tx
	var _ Sqler = tx
	var r = &sqltResult{}
	var _ Result = r
}
