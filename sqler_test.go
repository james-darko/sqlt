package sqlt

import (
	"testing"
)

func TestImplementations(t *testing.T) {
	var _ Sqler = &sqlxDB{}
	var _ DB = &sqlxDB{}
	var _ Sqler = &txWrapper{}
	var _ Tx = &txWrapper{}
	var _ Result = &sqltResult{}
}
