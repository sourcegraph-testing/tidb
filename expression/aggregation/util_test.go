package aggregation

import (
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

var _ = check.Suite(&testUtilSuite{})

type testUtilSuite struct {
}

func (s *testUtilSuite) TestDistinct(c *check.C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	dc := createDistinctChecker(sc)
	tests := []struct {
		vals   []any
		expect bool
	}{
		{[]any{1, 1}, true},
		{[]any{1, 1}, false},
		{[]any{1, 2}, true},
		{[]any{1, 2}, false},
		{[]any{1, nil}, true},
		{[]any{1, nil}, false},
	}
	for _, tt := range tests {
		d, err := dc.Check(types.MakeDatums(tt.vals...))
		c.Assert(err, check.IsNil)
		c.Assert(d, check.Equals, tt.expect)
	}
}
