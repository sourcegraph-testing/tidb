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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tipb/go-tipb"
)

func (s *testEvaluatorSuite) TestSetFlenDecimal4RealOrDecimal(c *C) {
	ret := &types.FieldType{}
	a := &types.FieldType{
		Decimal: 1,
		Flen:    3,
	}
	b := &types.FieldType{
		Decimal: 0,
		Flen:    2,
	}
	setFlenDecimal4RealOrDecimal(ret, a, b, true, false)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, 6)

	b.Flen = 65
	setFlenDecimal4RealOrDecimal(ret, a, b, true, false)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxRealWidth)
	setFlenDecimal4RealOrDecimal(ret, a, b, false, false)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxDecimalWidth)

	b.Flen = types.UnspecifiedLength
	setFlenDecimal4RealOrDecimal(ret, a, b, true, false)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)

	b.Decimal = types.UnspecifiedLength
	setFlenDecimal4RealOrDecimal(ret, a, b, true, false)
	c.Assert(ret.Decimal, Equals, types.UnspecifiedLength)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)

	ret = &types.FieldType{}
	a = &types.FieldType{
		Decimal: 1,
		Flen:    3,
	}
	b = &types.FieldType{
		Decimal: 0,
		Flen:    2,
	}
	setFlenDecimal4RealOrDecimal(ret, a, b, true, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, 8)

	b.Flen = 65
	setFlenDecimal4RealOrDecimal(ret, a, b, true, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxRealWidth)
	setFlenDecimal4RealOrDecimal(ret, a, b, false, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxDecimalWidth)

	b.Flen = types.UnspecifiedLength
	setFlenDecimal4RealOrDecimal(ret, a, b, true, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)

	b.Decimal = types.UnspecifiedLength
	setFlenDecimal4RealOrDecimal(ret, a, b, true, true)
	c.Assert(ret.Decimal, Equals, types.UnspecifiedLength)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)
}

func (s *testEvaluatorSuite) TestSetFlenDecimal4Int(c *C) {
	ret := &types.FieldType{}
	a := &types.FieldType{
		Decimal: 1,
		Flen:    3,
	}
	b := &types.FieldType{
		Decimal: 0,
		Flen:    2,
	}
	setFlenDecimal4Int(ret, a, b)
	c.Assert(ret.Decimal, Equals, 0)
	c.Assert(ret.Flen, Equals, mysql.MaxIntWidth)

	b.Flen = mysql.MaxIntWidth + 1
	setFlenDecimal4Int(ret, a, b)
	c.Assert(ret.Decimal, Equals, 0)
	c.Assert(ret.Flen, Equals, mysql.MaxIntWidth)

	b.Flen = types.UnspecifiedLength
	setFlenDecimal4Int(ret, a, b)
	c.Assert(ret.Decimal, Equals, 0)
	c.Assert(ret.Flen, Equals, mysql.MaxIntWidth)
}

func (s *testEvaluatorSuite) TestArithmeticPlus(c *C) {
	// case: 1
	args := []any{int64(12), int64(1)}

	bf, err := funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	intSig, ok := bf.(*builtinArithmeticPlusIntSig)
	c.Assert(ok, IsTrue)
	c.Assert(intSig, NotNil)

	intResult, isNull, err := intSig.evalInt(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(intResult, Equals, int64(13))

	// case 2
	args = []any{float64(1.01001), float64(-0.01)}

	bf, err = funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok := bf.(*builtinArithmeticPlusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err := realSig.evalReal(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(realResult, Equals, float64(1.00001))

	// case 3
	args = []any{nil, float64(-0.11101)}

	bf, err = funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok = bf.(*builtinArithmeticPlusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))

	// case 4
	args = []any{nil, nil}

	bf, err = funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok = bf.(*builtinArithmeticPlusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))

	// case 5
	hexStr, err := types.ParseHexStr("0x20000000000000")
	c.Assert(err, IsNil)
	args = []any{hexStr, int64(1)}

	bf, err = funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	intSig, ok = bf.(*builtinArithmeticPlusIntSig)
	c.Assert(ok, IsTrue)
	c.Assert(intSig, NotNil)

	intResult, _, err = intSig.evalInt(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(intResult, Equals, int64(9007199254740993))
}

func (s *testEvaluatorSuite) TestArithmeticMinus(c *C) {
	// case: 1
	args := []any{int64(12), int64(1)}

	bf, err := funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	intSig, ok := bf.(*builtinArithmeticMinusIntSig)
	c.Assert(ok, IsTrue)
	c.Assert(intSig, NotNil)

	intResult, isNull, err := intSig.evalInt(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(intResult, Equals, int64(11))

	// case 2
	args = []any{float64(1.01001), float64(-0.01)}

	bf, err = funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok := bf.(*builtinArithmeticMinusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err := realSig.evalReal(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(realResult, Equals, float64(1.02001))

	// case 3
	args = []any{nil, float64(-0.11101)}

	bf, err = funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok = bf.(*builtinArithmeticMinusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))

	// case 4
	args = []any{float64(1.01), nil}

	bf, err = funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok = bf.(*builtinArithmeticMinusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))

	// case 5
	args = []any{nil, nil}

	bf, err = funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok = bf.(*builtinArithmeticMinusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))
}

func (s *testEvaluatorSuite) TestArithmeticMultiply(c *C) {
	testCases := []struct {
		args   []any
		expect any
		err    error
	}{
		{
			args:   []any{int64(11), int64(11)},
			expect: int64(121),
		},
		{
			args:   []any{uint64(11), uint64(11)},
			expect: int64(121),
		},
		{
			args:   []any{float64(11), float64(11)},
			expect: float64(121),
		},
		{
			args:   []any{nil, float64(-0.11101)},
			expect: nil,
		},
		{
			args:   []any{float64(1.01), nil},
			expect: nil,
		},
		{
			args:   []any{nil, nil},
			expect: nil,
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.Mul].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}

func (s *testEvaluatorSuite) TestArithmeticDivide(c *C) {
	testCases := []struct {
		args   []any
		expect any
	}{
		{
			args:   []any{float64(11.1111111), float64(11.1)},
			expect: float64(1.001001),
		},
		{
			args:   []any{float64(11.1111111), float64(0)},
			expect: nil,
		},
		{
			args:   []any{int64(11), int64(11)},
			expect: float64(1),
		},
		{
			args:   []any{int64(11), int64(2)},
			expect: float64(5.5),
		},
		{
			args:   []any{int64(11), int64(0)},
			expect: nil,
		},
		{
			args:   []any{uint64(11), uint64(11)},
			expect: float64(1),
		},
		{
			args:   []any{uint64(11), uint64(2)},
			expect: float64(5.5),
		},
		{
			args:   []any{uint64(11), uint64(0)},
			expect: nil,
		},
		{
			args:   []any{nil, float64(-0.11101)},
			expect: nil,
		},
		{
			args:   []any{float64(1.01), nil},
			expect: nil,
		},
		{
			args:   []any{nil, nil},
			expect: nil,
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.Div].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		switch sig.(type) {
		case *builtinArithmeticIntDivideIntSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_IntDivideInt)
		case *builtinArithmeticIntDivideDecimalSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_IntDivideDecimal)
		}
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}

func (s *testEvaluatorSuite) TestArithmeticIntDivide(c *C) {
	testCases := []struct {
		args   []any
		expect []any
	}{
		{
			args:   []any{int64(13), int64(11)},
			expect: []any{int64(1), nil},
		},
		{
			args:   []any{int64(-13), int64(11)},
			expect: []any{int64(-1), nil},
		},
		{
			args:   []any{int64(13), int64(-11)},
			expect: []any{int64(-1), nil},
		},
		{
			args:   []any{int64(-13), int64(-11)},
			expect: []any{int64(1), nil},
		},
		{
			args:   []any{int64(33), int64(11)},
			expect: []any{int64(3), nil},
		},
		{
			args:   []any{int64(-33), int64(11)},
			expect: []any{int64(-3), nil},
		},
		{
			args:   []any{int64(33), int64(-11)},
			expect: []any{int64(-3), nil},
		},
		{
			args:   []any{int64(-33), int64(-11)},
			expect: []any{int64(3), nil},
		},
		{
			args:   []any{int64(11), int64(0)},
			expect: []any{nil, nil},
		},
		{
			args:   []any{int64(-11), int64(0)},
			expect: []any{nil, nil},
		},
		{
			args:   []any{float64(11.01), float64(1.1)},
			expect: []any{int64(10), nil},
		},
		{
			args:   []any{float64(-11.01), float64(1.1)},
			expect: []any{int64(-10), nil},
		},
		{
			args:   []any{float64(11.01), float64(-1.1)},
			expect: []any{int64(-10), nil},
		},
		{
			args:   []any{float64(-11.01), float64(-1.1)},
			expect: []any{int64(10), nil},
		},
		{
			args:   []any{nil, float64(-0.11101)},
			expect: []any{nil, nil},
		},
		{
			args:   []any{float64(1.01), nil},
			expect: []any{nil, nil},
		},
		{
			args:   []any{nil, int64(-1001)},
			expect: []any{nil, nil},
		},
		{
			args:   []any{int64(101), nil},
			expect: []any{nil, nil},
		},
		{
			args:   []any{nil, nil},
			expect: []any{nil, nil},
		},
		{
			args:   []any{float64(123456789100000.0), float64(-0.00001)},
			expect: []any{nil, "*BIGINT value is out of range in '\\(123456789100000 DIV -0.00001\\)'"},
		},
		{
			args:   []any{int64(-9223372036854775808), float64(-1)},
			expect: []any{nil, "*BIGINT value is out of range in '\\(-9223372036854775808 DIV -1\\)'"},
		},
		{
			args:   []any{uint64(1), float64(-2)},
			expect: []any{0, nil},
		},
		{
			args:   []any{uint64(1), float64(-1)},
			expect: []any{nil, "*BIGINT UNSIGNED value is out of range in '\\(1 DIV -1\\)'"},
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.IntDiv].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		if tc.expect[1] == nil {
			c.Assert(err, IsNil)
			c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect[0]))
		} else {
			c.Assert(err, ErrorMatches, tc.expect[1])
		}
	}
}

func (s *testEvaluatorSuite) TestArithmeticMod(c *C) {
	testCases := []struct {
		args   []any
		expect any
	}{
		{
			args:   []any{int64(13), int64(11)},
			expect: int64(2),
		},
		{
			args:   []any{int64(-13), int64(11)},
			expect: int64(-2),
		},
		{
			args:   []any{int64(13), int64(-11)},
			expect: int64(2),
		},
		{
			args:   []any{int64(-13), int64(-11)},
			expect: int64(-2),
		},
		{
			args:   []any{int64(33), int64(11)},
			expect: int64(0),
		},
		{
			args:   []any{int64(-33), int64(11)},
			expect: int64(0),
		},
		{
			args:   []any{int64(33), int64(-11)},
			expect: int64(0),
		},
		{
			args:   []any{int64(-33), int64(-11)},
			expect: int64(0),
		},
		{
			args:   []any{int64(11), int64(0)},
			expect: nil,
		},
		{
			args:   []any{int64(-11), int64(0)},
			expect: nil,
		},
		{
			args:   []any{int64(1), float64(1.1)},
			expect: float64(1),
		},
		{
			args:   []any{int64(-1), float64(1.1)},
			expect: float64(-1),
		},
		{
			args:   []any{int64(1), float64(-1.1)},
			expect: float64(1),
		},
		{
			args:   []any{int64(-1), float64(-1.1)},
			expect: float64(-1),
		},
		{
			args:   []any{nil, float64(-0.11101)},
			expect: nil,
		},
		{
			args:   []any{float64(1.01), nil},
			expect: nil,
		},
		{
			args:   []any{nil, int64(-1001)},
			expect: nil,
		},
		{
			args:   []any{int64(101), nil},
			expect: nil,
		},
		{
			args:   []any{nil, nil},
			expect: nil,
		},
		{
			args:   []any{"1231", 12},
			expect: 7,
		},
		{
			args:   []any{"1231", "12"},
			expect: float64(7),
		},
		{
			args:   []any{types.Duration{Duration: 45296 * time.Second}, 122},
			expect: 114,
		},
		{
			args:   []any{types.Set{Value: 7, Name: "abc"}, "12"},
			expect: float64(7),
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.Mod].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		switch sig.(type) {
		case *builtinArithmeticModRealSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_ModReal)
		case *builtinArithmeticModIntSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_ModInt)
		case *builtinArithmeticModDecimalSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_ModDecimal)
		}
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}
