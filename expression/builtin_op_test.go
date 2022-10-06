// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testEvaluatorSuite) TestUnary(c *C) {
	cases := []struct {
		args     any
		expected any
		overflow bool
		getErr   bool
	}{
		{uint64(9223372036854775809), "-9223372036854775809", true, false},
		{uint64(9223372036854775810), "-9223372036854775810", true, false},
		{uint64(9223372036854775808), int64(-9223372036854775808), false, false},
		{int64(math.MinInt64), "9223372036854775808", true, false}, // --9223372036854775808
	}
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.InSelectStmt
	sc.InSelectStmt = true
	defer func() {
		sc.InSelectStmt = origin
	}()

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.UnaryMinus, s.primitiveValsToConstants([]any{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if !t.getErr {
			c.Assert(err, IsNil)
			if !t.overflow {
				c.Assert(d.GetValue(), Equals, t.expected)
			} else {
				c.Assert(d.GetMysqlDecimal().String(), Equals, t.expected)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}

	_, err := funcs[ast.UnaryMinus].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestLogicAnd(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []any
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]any{1, 1}, 1, false, false},
		{[]any{1, 0}, 0, false, false},
		{[]any{0, 1}, 0, false, false},
		{[]any{0, 0}, 0, false, false},
		{[]any{2, -1}, 1, false, false},
		{[]any{"a", "0"}, 0, false, false},
		{[]any{"a", "1"}, 0, false, false},
		{[]any{"1a", "0"}, 0, false, false},
		{[]any{"1a", "1"}, 1, false, false},
		{[]any{0, nil}, 0, false, false},
		{[]any{nil, 0}, 0, false, false},
		{[]any{nil, 1}, 0, true, false},
		{[]any{0.001, 0}, 0, false, false},
		{[]any{0.001, 1}, 1, false, false},
		{[]any{nil, 0.000}, 0, false, false},
		{[]any{nil, 0.001}, 0, true, false},
		{[]any{types.NewDecFromStringForTest("0.000001"), 0}, 0, false, false},
		{[]any{types.NewDecFromStringForTest("0.000001"), 1}, 1, false, false},
		{[]any{types.NewDecFromStringForTest("0.000000"), nil}, 0, false, false},
		{[]any{types.NewDecFromStringForTest("0.000001"), nil}, 0, true, false},

		{[]any{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.LogicAnd, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.LogicAnd, NewZero())
	c.Assert(err, NotNil)

	_, err = funcs[ast.LogicAnd].getFunction(s.ctx, []Expression{NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestLeftShift(c *C) {
	cases := []struct {
		args     []any
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]any{123, 2}, uint64(492), false, false},
		{[]any{-123, 2}, uint64(18446744073709551124), false, false},
		{[]any{nil, 1}, 0, true, false},

		{[]any{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.LeftShift, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}
}

func (s *testEvaluatorSuite) TestRightShift(c *C) {
	cases := []struct {
		args     []any
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]any{123, 2}, uint64(30), false, false},
		{[]any{-123, 2}, uint64(4611686018427387873), false, false},
		{[]any{nil, 1}, 0, true, false},

		{[]any{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.RightShift, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.RightShift, NewZero())
	c.Assert(err, NotNil)

	_, err = funcs[ast.RightShift].getFunction(s.ctx, []Expression{NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestBitXor(c *C) {
	cases := []struct {
		args     []any
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]any{123, 321}, uint64(314), false, false},
		{[]any{-123, 321}, uint64(18446744073709551300), false, false},
		{[]any{nil, 1}, 0, true, false},

		{[]any{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Xor, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.Xor, NewZero())
	c.Assert(err, NotNil)

	_, err = funcs[ast.Xor].getFunction(s.ctx, []Expression{NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestBitOr(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []any
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]any{123, 321}, uint64(379), false, false},
		{[]any{-123, 321}, uint64(18446744073709551557), false, false},
		{[]any{nil, 1}, 0, true, false},

		{[]any{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Or, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.Or, NewZero())
	c.Assert(err, NotNil)

	_, err = funcs[ast.Or].getFunction(s.ctx, []Expression{NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestLogicOr(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []any
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]any{1, 1}, 1, false, false},
		{[]any{1, 0}, 1, false, false},
		{[]any{0, 1}, 1, false, false},
		{[]any{0, 0}, 0, false, false},
		{[]any{2, -1}, 1, false, false},
		{[]any{"a", "0"}, 0, false, false},
		{[]any{"a", "1"}, 1, false, false},
		{[]any{"1a", "0"}, 1, false, false},
		{[]any{"1a", "1"}, 1, false, false},
		{[]any{"0.0a", 0}, 0, false, false},
		{[]any{"0.0001a", 0}, 1, false, false},
		{[]any{1, nil}, 1, false, false},
		{[]any{nil, 1}, 1, false, false},
		{[]any{nil, 0}, 0, true, false},
		{[]any{0.000, 0}, 0, false, false},
		{[]any{0.001, 0}, 1, false, false},
		{[]any{nil, 0.000}, 0, true, false},
		{[]any{nil, 0.001}, 1, false, false},
		{[]any{types.NewDecFromStringForTest("0.000000"), 0}, 0, false, false},
		{[]any{types.NewDecFromStringForTest("0.000000"), 1}, 1, false, false},
		{[]any{types.NewDecFromStringForTest("0.000000"), nil}, 0, true, false},
		{[]any{types.NewDecFromStringForTest("0.000001"), 0}, 1, false, false},
		{[]any{types.NewDecFromStringForTest("0.000001"), 1}, 1, false, false},
		{[]any{types.NewDecFromStringForTest("0.000001"), nil}, 1, false, false},

		{[]any{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.LogicOr, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.LogicOr, NewZero())
	c.Assert(err, NotNil)

	_, err = funcs[ast.LogicOr].getFunction(s.ctx, []Expression{NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestBitAnd(c *C) {
	cases := []struct {
		args     []any
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]any{123, 321}, 65, false, false},
		{[]any{-123, 321}, 257, false, false},
		{[]any{nil, 1}, 0, true, false},

		{[]any{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.And, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.And, NewZero())
	c.Assert(err, NotNil)

	_, err = funcs[ast.And].getFunction(s.ctx, []Expression{NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestBitNeg(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []any
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]any{123}, uint64(18446744073709551492), false, false},
		{[]any{-123}, uint64(122), false, false},
		{[]any{nil}, 0, true, false},

		{[]any{errors.New("must error")}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.BitNeg, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.BitNeg, NewZero(), NewZero())
	c.Assert(err, NotNil)

	_, err = funcs[ast.BitNeg].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestUnaryNot(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []any
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]any{1}, 0, false, false},
		{[]any{0}, 1, false, false},
		{[]any{123}, 0, false, false},
		{[]any{-123}, 0, false, false},
		{[]any{"123"}, 0, false, false},
		{[]any{float64(0.3)}, 0, false, false},
		{[]any{"0.3"}, 0, false, false},
		{[]any{types.NewDecFromFloatForTest(0.3)}, 0, false, false},
		{[]any{nil}, 0, true, false},

		{[]any{errors.New("must error")}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.UnaryNot, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.UnaryNot, NewZero(), NewZero())
	c.Assert(err, NotNil)

	_, err = funcs[ast.UnaryNot].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestIsTrueOrFalse(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	testCases := []struct {
		args    []any
		isTrue  any
		isFalse any
	}{
		{
			args:    []any{-12},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []any{12},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []any{0},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []any{float64(0)},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []any{"aaa"},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []any{""},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []any{"0.3"},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []any{float64(0.3)},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []any{types.NewDecFromFloatForTest(0.3)},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []any{nil},
			isTrue:  0,
			isFalse: 0,
		},
	}

	for _, tc := range testCases {
		isTrueSig, err := funcs[ast.IsTruth].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(isTrueSig, NotNil)

		isTrue, err := evalBuiltinFunc(isTrueSig, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(isTrue, testutil.DatumEquals, types.NewDatum(tc.isTrue))
	}

	for _, tc := range testCases {
		isFalseSig, err := funcs[ast.IsFalsity].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(isFalseSig, NotNil)

		isFalse, err := evalBuiltinFunc(isFalseSig, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(isFalse, testutil.DatumEquals, types.NewDatum(tc.isFalse))
	}
}

func (s *testEvaluatorSuite) TestLogicXor(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []any
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]any{1, 1}, 0, false, false},
		{[]any{1, 0}, 1, false, false},
		{[]any{0, 1}, 1, false, false},
		{[]any{0, 0}, 0, false, false},
		{[]any{2, -1}, 0, false, false},
		{[]any{"a", "0"}, 0, false, false},
		{[]any{"a", "1"}, 1, false, false},
		{[]any{"1a", "0"}, 1, false, false},
		{[]any{"1a", "1"}, 0, false, false},
		{[]any{0, nil}, 0, true, false},
		{[]any{nil, 0}, 0, true, false},
		{[]any{nil, 1}, 0, true, false},
		{[]any{0.5000, 0.4999}, 1, false, false},
		{[]any{0.5000, 1.0}, 0, false, false},
		{[]any{0.4999, 1.0}, 1, false, false},
		{[]any{nil, 0.000}, 0, true, false},
		{[]any{nil, 0.001}, 0, true, false},
		{[]any{types.NewDecFromStringForTest("0.000001"), 0.00001}, 0, false, false},
		{[]any{types.NewDecFromStringForTest("0.000001"), 1}, 1, false, false},
		{[]any{types.NewDecFromStringForTest("0.000000"), nil}, 0, true, false},
		{[]any{types.NewDecFromStringForTest("0.000001"), nil}, 0, true, false},

		{[]any{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.LogicXor, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.LogicXor, NewZero())
	c.Assert(err, NotNil)

	_, err = funcs[ast.LogicXor].getFunction(s.ctx, []Expression{NewZero(), NewZero()})
	c.Assert(err, IsNil)
}
