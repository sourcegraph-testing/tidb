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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testEvaluatorSuite) TestJSONType(c *C) {
	fc := funcs[ast.JSONType]
	tbl := []struct {
		Input    any
		Expected any
	}{
		{nil, nil},
		{`3`, `INTEGER`},
		{`3.0`, `DOUBLE`},
		{`null`, `NULL`},
		{`true`, `BOOLEAN`},
		{`[]`, `ARRAY`},
		{`{}`, `OBJECT`},
	}
	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expected"][0])
	}
}

func (s *testEvaluatorSuite) TestJSONQuote(c *C) {
	fc := funcs[ast.JSONQuote]
	tbl := []struct {
		Input    any
		Expected any
	}{
		{nil, nil},
		{``, `""`},
		{`""`, `"\"\""`},
		{`a`, `"a"`},
		{`3`, `"3"`},
		{`{"a": "b"}`, `"{\"a\": \"b\"}"`},
		{`{"a":     "b"}`, `"{\"a\":     \"b\"}"`},
		{`hello,"quoted string",world`, `"hello,\"quoted string\",world"`},
		{`hello,"宽字符",world`, `"hello,\"宽字符\",world"`},
		{`Invalid Json string	is OK`, `"Invalid Json string\tis OK"`},
		{`1\u2232\u22322`, `"1\\u2232\\u22322"`},
	}
	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expected"][0])
	}
}

func (s *testEvaluatorSuite) TestJSONUnquote(c *C) {
	fc := funcs[ast.JSONUnquote]
	tbl := []struct {
		Input    any
		Expected any
	}{
		{nil, nil},
		{``, ``},
		{`""`, ``},
		{`''`, `''`},
		{`"a"`, `a`},
		{`3`, `3`},
		{`{"a": "b"}`, `{"a": "b"}`},
		{`{"a":     "b"}`, `{"a":     "b"}`},
		{`"hello,\"quoted string\",world"`, `hello,"quoted string",world`},
		{`"hello,\"宽字符\",world"`, `hello,"宽字符",world`},
		{`Invalid Json string\tis OK`, `Invalid Json string\tis OK`},
		{`"1\\u2232\\u22322"`, `1\u2232\u22322`},
		{`"[{\"x\":\"{\\\"y\\\":12}\"}]"`, `[{"x":"{\"y\":12}"}]`},
		{`[{\"x\":\"{\\\"y\\\":12}\"}]`, `[{\"x\":\"{\\\"y\\\":12}\"}]`},
	}
	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expected"][0])
	}
}

func (s *testEvaluatorSuite) TestJSONExtract(c *C) {
	fc := funcs[ast.JSONExtract]
	jstr := `{"a": [{"aa": [{"aaa": 1}]}], "aaa": 2}`
	tbl := []struct {
		Input    []any
		Expected any
		Success  bool
	}{
		{[]any{nil, nil}, nil, true},
		{[]any{jstr, `$.a[0].aa[0].aaa`, `$.aaa`}, `[1, 2]`, true},
		{[]any{jstr, `$.a[0].aa[0].aaa`, `$InvalidPath`}, nil, false},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.Success {
			c.Assert(err, IsNil)
			switch x := t.Expected.(type) {
			case string:
				var j1 json.BinaryJSON
				j1, err = json.ParseBinaryFromString(x)
				c.Assert(err, IsNil)
				j2 := d.GetMysqlJSON()
				var cmp int
				cmp = json.CompareBinary(j1, j2)
				c.Assert(err, IsNil)
				c.Assert(cmp, Equals, 0)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

// TestJSONSetInsertReplace tests grammar of json_{set,insert,replace}.
func (s *testEvaluatorSuite) TestJSONSetInsertReplace(c *C) {
	tbl := []struct {
		fc           functionClass
		Input        []any
		Expected     any
		BuildSuccess bool
		Success      bool
	}{
		{funcs[ast.JSONSet], []any{nil, nil, nil}, nil, true, true},
		{funcs[ast.JSONSet], []any{`{}`, `$.a`, 3}, `{"a": 3}`, true, true},
		{funcs[ast.JSONInsert], []any{`{}`, `$.a`, 3}, `{"a": 3}`, true, true},
		{funcs[ast.JSONReplace], []any{`{}`, `$.a`, 3}, `{}`, true, true},
		{funcs[ast.JSONSet], []any{`{}`, `$.a`, 3, `$.b`, "3"}, `{"a": 3, "b": "3"}`, true, true},
		{funcs[ast.JSONSet], []any{`{}`, `$.a`, nil, `$.b`, "nil"}, `{"a": null, "b": "nil"}`, true, true},
		{funcs[ast.JSONSet], []any{`{}`, `$.a`, 3, `$.b`}, nil, false, false},
		{funcs[ast.JSONSet], []any{`{}`, `$InvalidPath`, 3}, nil, true, false},
	}
	var err error
	var f builtinFunc
	var d types.Datum
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err = t.fc.getFunction(s.ctx, s.datumsToConstants(args))
		if t.BuildSuccess {
			c.Assert(err, IsNil)
			d, err = evalBuiltinFunc(f, chunk.Row{})
			if t.Success {
				c.Assert(err, IsNil)
				switch x := t.Expected.(type) {
				case string:
					var j1 json.BinaryJSON
					j1, err = json.ParseBinaryFromString(x)
					c.Assert(err, IsNil)
					j2 := d.GetMysqlJSON()
					var cmp int
					cmp = json.CompareBinary(j1, j2)
					c.Assert(cmp, Equals, 0)
				}
				continue
			}
		}
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestJSONMerge(c *C) {
	fc := funcs[ast.JSONMerge]
	tbl := []struct {
		Input    []any
		Expected any
	}{
		{[]any{nil, nil}, nil},
		{[]any{`{}`, `[]`}, `[{}]`},
		{[]any{`{}`, `[]`, `3`, `"4"`}, `[{}, 3, "4"]`},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		switch x := t.Expected.(type) {
		case string:
			j1, err := json.ParseBinaryFromString(x)
			c.Assert(err, IsNil)
			j2 := d.GetMysqlJSON()
			cmp := json.CompareBinary(j1, j2)
			c.Assert(cmp, Equals, 0, Commentf("got %v expect %v", j1.String(), j2.String()))
		case nil:
			c.Assert(d.IsNull(), IsTrue)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONMergePreserve(c *C) {
	fc := funcs[ast.JSONMergePreserve]
	tbl := []struct {
		Input    []any
		Expected any
	}{
		{[]any{nil, nil}, nil},
		{[]any{`{}`, `[]`}, `[{}]`},
		{[]any{`{}`, `[]`, `3`, `"4"`}, `[{}, 3, "4"]`},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		switch x := t.Expected.(type) {
		case string:
			j1, err := json.ParseBinaryFromString(x)
			c.Assert(err, IsNil)
			j2 := d.GetMysqlJSON()
			cmp := json.CompareBinary(j1, j2)
			c.Assert(cmp, Equals, 0, Commentf("got %v expect %v", j1.String(), j2.String()))
		case nil:
			c.Assert(d.IsNull(), IsTrue)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONArray(c *C) {
	fc := funcs[ast.JSONArray]
	tbl := []struct {
		Input    []any
		Expected string
	}{
		{[]any{1}, `[1]`},
		{[]any{nil, "a", 3, `{"a": "b"}`}, `[null, "a", 3, "{\"a\": \"b\"}"]`},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		j1, err := json.ParseBinaryFromString(t.Expected)
		c.Assert(err, IsNil)
		j2 := d.GetMysqlJSON()
		cmp := json.CompareBinary(j1, j2)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testEvaluatorSuite) TestJSONObject(c *C) {
	fc := funcs[ast.JSONObject]
	tbl := []struct {
		Input        []any
		Expected     any
		BuildSuccess bool
		Success      bool
	}{
		{[]any{1, 2, 3}, nil, false, false},
		{[]any{1, 2, "hello", nil}, `{"1": 2, "hello": null}`, true, true},
		{[]any{nil, 2}, nil, true, false},

		// TiDB can only tell booleans from parser.
		{[]any{1, true}, `{"1": 1}`, true, true},
	}
	var err error
	var f builtinFunc
	var d types.Datum
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(args))
		if t.BuildSuccess {
			c.Assert(err, IsNil)
			d, err = evalBuiltinFunc(f, chunk.Row{})
			if t.Success {
				c.Assert(err, IsNil)
				switch x := t.Expected.(type) {
				case string:
					var j1 json.BinaryJSON
					j1, err = json.ParseBinaryFromString(x)
					c.Assert(err, IsNil)
					j2 := d.GetMysqlJSON()
					var cmp int
					cmp = json.CompareBinary(j1, j2)
					c.Assert(cmp, Equals, 0)
				}
				continue
			}
		}
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestJSONRemove(c *C) {
	fc := funcs[ast.JSONRemove]
	tbl := []struct {
		Input    []any
		Expected any
		Success  bool
	}{
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$"}, nil, false},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.*"}, nil, false},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$[*]"}, nil, false},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$**.a"}, nil, false},

		{[]any{nil, "$.a"}, nil, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].aa"}, `{"a": [1, 2, {}]}`, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[1]"}, `{"a": [1, {"aa": "xx"}]}`, true},

		// Tests multi path expressions.
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].aa", "$.a[1]"}, `{"a": [1, {}]}`, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[1]", "$.a[1].aa"}, `{"a": [1, {}]}`, true},

		// Tests path expressions not exists.
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[3]"}, `{"a": [1, 2, {"aa": "xx"}]}`, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.b"}, `{"a": [1, 2, {"aa": "xx"}]}`, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[3]", "$.b"}, `{"a": [1, 2, {"aa": "xx"}]}`, true},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})

		if t.Success {
			c.Assert(err, IsNil)
			switch x := t.Expected.(type) {
			case string:
				var j1 json.BinaryJSON
				j1, err = json.ParseBinaryFromString(x)
				c.Assert(err, IsNil)
				j2 := d.GetMysqlJSON()
				var cmp int
				cmp = json.CompareBinary(j1, j2)
				c.Assert(cmp, Equals, 0, Commentf("got %v expect %v", j2.Value, j1.Value))
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONContains(c *C) {
	fc := funcs[ast.JSONContains]
	tbl := []struct {
		input    []any
		expected any
		err      error
	}{
		// Tests nil arguments
		{[]any{nil, `1`, "$.c"}, nil, nil},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, nil, "$.a[3]"}, nil, nil},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, nil}, nil, nil},
		// Tests with path expression
		{[]any{`[1,2,[1,[5,[3]]]]`, `[1,3]`, "$[2]"}, 1, nil},
		{[]any{`[1,2,[1,[5,{"a":[2,3]}]]]`, `[1,{"a":[3]}]`, "$[2]"}, 1, nil},
		{[]any{`[{"a":1}]`, `{"a":1}`, "$"}, 1, nil},
		{[]any{`[{"a":1,"b":2}]`, `{"a":1,"b":2}`, "$"}, 1, nil},
		{[]any{`[{"a":{"a":1},"b":2}]`, `{"a":1}`, "$.a"}, 0, nil},
		// Tests without path expression
		{[]any{`{}`, `{}`}, 1, nil},
		{[]any{`{"a":1}`, `{}`}, 1, nil},
		{[]any{`{"a":1}`, `1`}, 0, nil},
		{[]any{`{"a":[1]}`, `[1]`}, 0, nil},
		{[]any{`{"b":2, "c":3}`, `{"c":3}`}, 1, nil},
		{[]any{`1`, `1`}, 1, nil},
		{[]any{`[1]`, `1`}, 1, nil},
		{[]any{`[1,2]`, `[1]`}, 1, nil},
		{[]any{`[1,2]`, `[1,3]`}, 0, nil},
		{[]any{`[1,2]`, `["1"]`}, 0, nil},
		{[]any{`[1,2,[1,3]]`, `[1,3]`}, 1, nil},
		{[]any{`[1,2,[1,3]]`, `[1,      3]`}, 1, nil},
		{[]any{`[1,2,[1,[5,[3]]]]`, `[1,3]`}, 1, nil},
		{[]any{`[1,2,[1,[5,{"a":[2,3]}]]]`, `[1,{"a":[3]}]`}, 1, nil},
		{[]any{`[{"a":1}]`, `{"a":1}`}, 1, nil},
		{[]any{`[{"a":1,"b":2}]`, `{"a":1}`}, 1, nil},
		{[]any{`[{"a":{"a":1},"b":2}]`, `{"a":1}`}, 0, nil},
		// Tests path expression contains any asterisk
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.*"}, nil, json.ErrInvalidJSONPathWildcard},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$[*]"}, nil, json.ErrInvalidJSONPathWildcard},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$**.a"}, nil, json.ErrInvalidJSONPathWildcard},
		// Tests path expression does not identify a section of the target document
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.c"}, nil, nil},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.a[3]"}, nil, nil},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.a[2].b"}, nil, nil},
		// For issue 9957: test 'argument 1 and 2 as valid json object'
		{[]any{`[1,2,[1,3]]`, `a:1`}, 1, json.ErrInvalidJSONText},
		{[]any{`a:1`, `1`}, 1, json.ErrInvalidJSONText},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.err == nil {
			c.Assert(err, IsNil)
			if t.expected == nil {
				c.Assert(d.IsNull(), IsTrue)
			} else {
				c.Assert(d.GetInt64(), Equals, int64(t.expected.(int)))
			}
		} else {
			c.Assert(t.err.(*terror.Error).Equal(err), IsTrue)
		}
	}
	// For issue 9957: test 'argument 1 and 2 as valid json object'
	cases := []struct {
		arg1 any
		arg2 any
	}{
		{1, ""},
		{0.05, ""},
		{"", 1},
		{"", 0.05},
	}
	for _, cs := range cases {
		_, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(cs.arg1, cs.arg2)))
		c.Assert(json.ErrInvalidJSONData.Equal(err), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestJSONContainsPath(c *C) {
	fc := funcs[ast.JSONContainsPath]
	jsonString := `{"a": 1, "b": 2, "c": {"d": 4}}`
	invalidJSON := `{"a": 1`
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// Tests nil arguments
		{[]any{nil, json.ContainsPathOne, "$.c"}, nil, true},
		{[]any{nil, json.ContainsPathAll, "$.c"}, nil, true},
		{[]any{jsonString, nil, "$.a[3]"}, nil, true},
		{[]any{jsonString, json.ContainsPathOne, nil}, nil, true},
		{[]any{jsonString, json.ContainsPathAll, nil}, nil, true},
		// Tests with one path expression
		{[]any{jsonString, json.ContainsPathOne, "$.c.d"}, 1, true},
		{[]any{jsonString, json.ContainsPathOne, "$.a.d"}, 0, true},
		{[]any{jsonString, json.ContainsPathAll, "$.c.d"}, 1, true},
		{[]any{jsonString, json.ContainsPathAll, "$.a.d"}, 0, true},
		// Tests with multiple path expression
		{[]any{jsonString, json.ContainsPathOne, "$.a", "$.e"}, 1, true},
		{[]any{jsonString, json.ContainsPathOne, "$.a", "$.c"}, 1, true},
		{[]any{jsonString, json.ContainsPathAll, "$.a", "$.e"}, 0, true},
		{[]any{jsonString, json.ContainsPathAll, "$.a", "$.c"}, 1, true},
		// Tests path expression contains any asterisk
		{[]any{jsonString, json.ContainsPathOne, "$.*"}, 1, true},
		{[]any{jsonString, json.ContainsPathOne, "$[*]"}, 0, true},
		{[]any{jsonString, json.ContainsPathAll, "$.*"}, 1, true},
		{[]any{jsonString, json.ContainsPathAll, "$[*]"}, 0, true},
		// Tests invalid json document
		{[]any{invalidJSON, json.ContainsPathOne, "$.a"}, nil, false},
		{[]any{invalidJSON, json.ContainsPathAll, "$.a"}, nil, false},
		// Tests compatible contains path
		{[]any{jsonString, "ONE", "$.c.d"}, 1, true},
		{[]any{jsonString, "ALL", "$.c.d"}, 1, true},
		{[]any{jsonString, "One", "$.a", "$.e"}, 1, true},
		{[]any{jsonString, "aLl", "$.a", "$.e"}, 0, true},
		{[]any{jsonString, "test", "$.a"}, nil, false},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)
			if t.expected == nil {
				c.Assert(d.IsNull(), IsTrue)
			} else {
				c.Assert(d.GetInt64(), Equals, int64(t.expected.(int)))
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONLength(c *C) {
	fc := funcs[ast.JSONLength]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// Tests scalar arguments
		{[]any{`null`}, 1, true},
		{[]any{`true`}, 1, true},
		{[]any{`false`}, 1, true},
		{[]any{`1`}, 1, true},
		{[]any{`-1`}, 1, true},
		{[]any{`1.1`}, 1, true},
		{[]any{`"1"`}, 1, true},
		{[]any{`"1"`, "$.a"}, 1, true},
		{[]any{`null`, "$.a"}, 1, true},
		// Tests nil arguments
		{[]any{nil}, nil, true},
		{[]any{nil, "a"}, nil, true},
		{[]any{`{"a": 1}`, nil}, nil, true},
		{[]any{nil, nil}, nil, true},
		// Tests with path expression
		{[]any{`[1,2,[1,[5,[3]]]]`, "$[2]"}, 2, true},
		{[]any{`[{"a":1}]`, "$"}, 1, true},
		{[]any{`[{"a":1,"b":2}]`, "$[0].a"}, 1, true},
		{[]any{`{"a":{"a":1},"b":2}`, "$"}, 2, true},
		{[]any{`{"a":{"a":1},"b":2}`, "$.a"}, 1, true},
		{[]any{`{"a":{"a":1},"b":2}`, "$.a.a"}, 1, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].aa"}, 1, true},
		// Tests without path expression
		{[]any{`{}`}, 0, true},
		{[]any{`{"a":1}`}, 1, true},
		{[]any{`{"a":[1]}`}, 1, true},
		{[]any{`{"b":2, "c":3}`}, 2, true},
		{[]any{`[1]`}, 1, true},
		{[]any{`[1,2]`}, 2, true},
		{[]any{`[1,2,[1,3]]`}, 3, true},
		{[]any{`[1,2,[1,[5,[3]]]]`}, 3, true},
		{[]any{`[1,2,[1,[5,{"a":[2,3]}]]]`}, 3, true},
		{[]any{`[{"a":1}]`}, 1, true},
		{[]any{`[{"a":1,"b":2}]`}, 1, true},
		{[]any{`[{"a":{"a":1},"b":2}]`}, 1, true},
		// Tests path expression contains any asterisk
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.*"}, nil, false},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$[*]"}, nil, false},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$**.a"}, nil, false},
		// Tests path expression does not identify a section of the target document
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.c"}, nil, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[3]"}, nil, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].b"}, nil, true},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)

			if t.expected == nil {
				c.Assert(d.IsNull(), IsTrue)
			} else {
				c.Assert(d.GetInt64(), Equals, int64(t.expected.(int)))
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONKeys(c *C) {
	fc := funcs[ast.JSONKeys]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{

		// Tests nil arguments
		{[]any{nil}, nil, true},
		{[]any{nil, "$.c"}, nil, true},
		{[]any{`{"a": 1}`, nil}, nil, true},
		{[]any{nil, nil}, nil, true},

		// Tests with other type
		{[]any{`1`}, nil, true},
		{[]any{`"str"`}, nil, true},
		{[]any{`true`}, nil, true},
		{[]any{`null`}, nil, true},
		{[]any{`[1, 2]`}, nil, true},
		{[]any{`["1", "2"]`}, nil, true},

		// Tests without path expression
		{[]any{`{}`}, `[]`, true},
		{[]any{`{"a": 1}`}, `["a"]`, true},
		{[]any{`{"a": 1, "b": 2}`}, `["a", "b"]`, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`}, `["a", "b"]`, true},

		// Tests with path expression
		{[]any{`{"a": 1}`, "$.a"}, nil, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.a"}, `["c"]`, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.a.c"}, nil, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`, nil}, nil, true},

		// Tests path expression contains any asterisk
		{[]any{`{}`, "$.*"}, nil, false},
		{[]any{`{"a": 1}`, "$.*"}, nil, false},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.*"}, nil, false},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.a.*"}, nil, false},

		// Tests path expression does not identify a section of the target document
		{[]any{`{"a": 1}`, "$.b"}, nil, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.c"}, nil, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.a.d"}, nil, true},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)
			switch x := t.expected.(type) {
			case string:
				var j1 json.BinaryJSON
				j1, err = json.ParseBinaryFromString(x)
				c.Assert(err, IsNil)
				j2 := d.GetMysqlJSON()
				var cmp int
				cmp = json.CompareBinary(j1, j2)
				c.Assert(cmp, Equals, 0)
			case nil:
				c.Assert(d.IsNull(), IsTrue)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONDepth(c *C) {
	fc := funcs[ast.JSONDepth]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// Tests scalar arguments
		{[]any{`null`}, 1, true},
		{[]any{`true`}, 1, true},
		{[]any{`false`}, 1, true},
		{[]any{`1`}, 1, true},
		{[]any{`-1`}, 1, true},
		{[]any{`1.1`}, 1, true},
		{[]any{`"1"`}, 1, true},
		// Tests nil arguments
		{[]any{nil}, nil, true},
		// Tests depth
		{[]any{`{}`}, 1, true},
		{[]any{`[]`}, 1, true},
		{[]any{`[10, 20]`}, 2, true},
		{[]any{`[[], {}]`}, 2, true},
		{[]any{`{"Name": "Homer"}`}, 2, true},
		{[]any{`[10, {"a": 20}]`}, 3, true},
		{[]any{`{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]} }`}, 4, true},
		{[]any{`{"a":1}`}, 2, true},
		{[]any{`{"a":[1]}`}, 3, true},
		{[]any{`{"b":2, "c":3}`}, 2, true},
		{[]any{`[1]`}, 2, true},
		{[]any{`[1,2]`}, 2, true},
		{[]any{`[1,2,[1,3]]`}, 3, true},
		{[]any{`[1,2,[1,[5,[3]]]]`}, 5, true},
		{[]any{`[1,2,[1,[5,{"a":[2,3]}]]]`}, 6, true},
		{[]any{`[{"a":1}]`}, 3, true},
		{[]any{`[{"a":1,"b":2}]`}, 3, true},
		{[]any{`[{"a":{"a":1},"b":2}]`}, 4, true},
		// Tests non-json
		{[]any{`a`}, nil, false},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)

			if t.expected == nil {
				c.Assert(d.IsNull(), IsTrue)
			} else {
				c.Assert(d.GetInt64(), Equals, int64(t.expected.(int)))
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONArrayAppend(c *C) {
	sampleJSON, err := json.ParseBinaryFromString(`{"b": 2}`)
	c.Assert(err, IsNil)
	fc := funcs[ast.JSONArrayAppend]
	tbl := []struct {
		input    []any
		expected any
		err      *terror.Error
	}{
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d`, `z`}, `{"a": 1, "b": [2, 3], "c": 4}`, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$`, `w`}, `[{"a": 1, "b": [2, 3], "c": 4}, "w"]`, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$`, nil}, `[{"a": 1, "b": [2, 3], "c": 4}, null]`, nil},
		{[]any{`{"a": 1}`, `$`, `{"b": 2}`}, `[{"a": 1}, "{\"b\": 2}"]`, nil},
		{[]any{`{"a": 1}`, `$`, sampleJSON}, `[{"a": 1}, {"b": 2}]`, nil},
		{[]any{`{"a": 1}`, `$.a`, sampleJSON}, `{"a": [1, {"b": 2}]}`, nil},

		{[]any{`{"a": 1}`, `$.a`, sampleJSON, `$.a[1]`, sampleJSON}, `{"a": [1, [{"b": 2}, {"b": 2}]]}`, nil},
		{[]any{nil, `$`, nil}, nil, nil},
		{[]any{nil, `$`, `a`}, nil, nil},
		{[]any{`null`, `$`, nil}, `[null, null]`, nil},
		{[]any{`[]`, `$`, nil}, `[null]`, nil},
		{[]any{`{}`, `$`, nil}, `[{}, null]`, nil},
		// Bad arguments.
		{[]any{`asdf`, `$`, nil}, nil, json.ErrInvalidJSONText},
		{[]any{``, `$`, nil}, nil, json.ErrInvalidJSONText},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d`}, nil, ErrIncorrectParameterCount},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.c`, `y`, `$.b`}, nil, ErrIncorrectParameterCount},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, nil, nil}, nil, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `asdf`, nil}, nil, json.ErrInvalidJSONPath},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, 42, nil}, nil, json.ErrInvalidJSONPath},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.*`, nil}, nil, json.ErrInvalidJSONPathWildcard},
		// Following tests come from MySQL doc.
		{[]any{`["a", ["b", "c"], "d"]`, `$[1]`, 1}, `["a", ["b", "c", 1], "d"]`, nil},
		{[]any{`["a", ["b", "c"], "d"]`, `$[0]`, 2}, `[["a", 2], ["b", "c"], "d"]`, nil},
		{[]any{`["a", ["b", "c"], "d"]`, `$[1][0]`, 3}, `["a", [["b", 3], "c"], "d"]`, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.b`, `x`}, `{"a": 1, "b": [2, 3, "x"], "c": 4}`, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.c`, `y`}, `{"a": 1, "b": [2, 3], "c": [4, "y"]}`, nil},
		// Following tests come from MySQL test.
		{[]any{`[1,2,3, {"a":[4,5,6]}]`, `$`, 7}, `[1, 2, 3, {"a": [4, 5, 6]}, 7]`, nil},
		{[]any{`[1,2,3, {"a":[4,5,6]}]`, `$`, 7, `$[3].a`, 3.14}, `[1, 2, 3, {"a": [4, 5, 6, 3.14]}, 7]`, nil},
		{[]any{`[1,2,3, {"a":[4,5,6]}]`, `$`, 7, `$[3].b`, 8}, `[1, 2, 3, {"a": [4, 5, 6]}, 7]`, nil},
	}

	for i, t := range tbl {
		args := types.MakeDatums(t.input...)
		s.ctx.GetSessionVars().StmtCtx.SetWarnings(nil)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		// No error should return in getFunction if t.err is nil.
		if err != nil {
			c.Assert(t.err, NotNil)
			c.Assert(t.err.Equal(err), Equals, true)
			continue
		}

		c.Assert(f, NotNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		comment := Commentf("case:%v \n input:%v \n output: %s \n expected: %v \n warnings: %v \n expected error %v", i, t.input, d.GetMysqlJSON(), t.expected, s.ctx.GetSessionVars().StmtCtx.GetWarnings(), t.err)

		if t.err != nil {
			c.Assert(t.err.Equal(err), Equals, true, comment)
			continue
		}

		c.Assert(err, IsNil, comment)
		c.Assert(int(s.ctx.GetSessionVars().StmtCtx.WarningCount()), Equals, 0, comment)

		if t.expected == nil {
			c.Assert(d.IsNull(), IsTrue, comment)
			continue
		}

		j1, err := json.ParseBinaryFromString(t.expected.(string))

		c.Assert(err, IsNil, comment)
		c.Assert(json.CompareBinary(j1, d.GetMysqlJSON()), Equals, 0, comment)
	}
}

func (s *testEvaluatorSuite) TestJSONSearch(c *C) {
	fc := funcs[ast.JSONSearch]
	jsonString := `["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]`
	jsonString2 := `["abc", [{"k": "10"}, "def"], {"x":"ab%d"}, {"y":"abcd"}]`
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// simple case
		{[]any{jsonString, `one`, `abc`}, `"$[0]"`, true},
		{[]any{jsonString, `all`, `abc`}, `["$[0]", "$[2].x"]`, true},
		{[]any{jsonString, `all`, `ghi`}, nil, true},
		{[]any{jsonString, `ALL`, `ghi`}, nil, true},
		{[]any{jsonString, `all`, `10`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$[*]`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$**.k`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$[*][0].k`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$[1]`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$[1][0]`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `abc`, nil, `$[2]`}, `"$[2].x"`, true},
		{[]any{jsonString, `all`, `abc`, nil, `$[2]`, `$[0]`}, `["$[2].x", "$[0]"]`, true},
		{[]any{jsonString, `all`, `abc`, nil, `$[2]`, `$[2]`}, `"$[2].x"`, true},

		// search pattern
		{[]any{jsonString, `all`, `%a%`}, `["$[0]", "$[2].x"]`, true},
		{[]any{jsonString, `all`, `%b%`}, `["$[0]", "$[2].x", "$[3].y"]`, true},
		{[]any{jsonString, `all`, `%b%`, nil, `$[0]`}, `"$[0]"`, true},
		{[]any{jsonString, `all`, `%b%`, nil, `$[2]`}, `"$[2].x"`, true},
		{[]any{jsonString, `all`, `%b%`, nil, `$[1]`}, nil, true},
		{[]any{jsonString, `all`, `%b%`, ``, `$[1]`}, nil, true},
		{[]any{jsonString, `all`, `%b%`, nil, `$[3]`}, `"$[3].y"`, true},
		{[]any{jsonString2, `all`, `ab_d`}, `["$[2].x", "$[3].y"]`, true},

		// escape char
		{[]any{jsonString2, `all`, `ab%d`}, `["$[2].x", "$[3].y"]`, true},
		{[]any{jsonString2, `all`, `ab\%d`}, `"$[2].x"`, true},
		{[]any{jsonString2, `all`, `ab|%d`, `|`}, `"$[2].x"`, true},

		// error handle
		{[]any{nil, `all`, `abc`}, nil, true},                     // NULL json
		{[]any{`a`, `all`, `abc`}, nil, false},                    // non json
		{[]any{jsonString, `wrong`, `abc`}, nil, false},           // wrong one_or_all
		{[]any{jsonString, `all`, nil}, nil, true},                // NULL search_str
		{[]any{jsonString, `all`, `abc`, `??`}, nil, false},       // wrong escape_char
		{[]any{jsonString, `all`, `abc`, nil, nil}, nil, true},    // NULL path
		{[]any{jsonString, `all`, `abc`, nil, `$xx`}, nil, false}, // wrong path
		{[]any{jsonString, nil, `abc`}, nil, true},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)
			switch x := t.expected.(type) {
			case string:
				var j1, j2 json.BinaryJSON
				j1, err = json.ParseBinaryFromString(x)
				c.Assert(err, IsNil)
				j2 = d.GetMysqlJSON()
				cmp := json.CompareBinary(j1, j2)
				c.Assert(cmp, Equals, 0)
			case nil:
				c.Assert(d.IsNull(), IsTrue)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONArrayInsert(c *C) {
	fc := funcs[ast.JSONArrayInsert]
	tbl := []struct {
		input    []any
		expected any
		success  bool
		err      *terror.Error
	}{
		// Success
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.b[1]`, `z`}, `{"a": 1, "b": [2, "z", 3], "c": 4}`, true, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.a[1]`, `z`}, `{"a": 1, "b": [2, 3], "c": 4}`, true, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d[1]`, `z`}, `{"a": 1, "b": [2, 3], "c": 4}`, true, nil},
		{[]any{`[{"a": 1, "b": [2, 3], "c": 4}]`, `$[1]`, `w`}, `[{"a": 1, "b": [2, 3], "c": 4}, "w"]`, true, nil},
		{[]any{`[{"a": 1, "b": [2, 3], "c": 4}]`, `$[0]`, nil}, `[null, {"a": 1, "b": [2, 3], "c": 4}]`, true, nil},
		{[]any{`[1, 2, 3]`, `$[100]`, `{"b": 2}`}, `[1, 2, 3, "{\"b\": 2}"]`, true, nil},
		// About null
		{[]any{nil, `$`, nil}, nil, true, nil},
		{[]any{nil, `$`, `a`}, nil, true, nil},
		{[]any{`[]`, `$[0]`, nil}, `[null]`, true, nil},
		{[]any{`{}`, `$[0]`, nil}, `{}`, true, nil},
		// Bad arguments
		{[]any{`asdf`, `$`, nil}, nil, false, json.ErrInvalidJSONText},
		{[]any{``, `$`, nil}, nil, false, json.ErrInvalidJSONText},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d`}, nil, false, ErrIncorrectParameterCount},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.c`, `y`, `$.b`}, nil, false, ErrIncorrectParameterCount},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, nil, nil}, nil, true, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `asdf`, nil}, nil, false, json.ErrInvalidJSONPath},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, 42, nil}, nil, false, json.ErrInvalidJSONPath},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.*`, nil}, nil, false, json.ErrInvalidJSONPathWildcard},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.b[0]`, nil, `$.a`, nil}, nil, false, json.ErrInvalidJSONPathArrayCell},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.a`, nil}, nil, false, json.ErrInvalidJSONPathArrayCell},
		// Following tests come from MySQL doc.
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[1]`, `x`}, `["a", "x", {"b": [1, 2]}, [3, 4]]`, true, nil},
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[100]`, `x`}, `["a", {"b": [1, 2]}, [3, 4], "x"]`, true, nil},
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[1].b[0]`, `x`}, `["a", {"b": ["x", 1, 2]}, [3, 4]]`, true, nil},
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[2][1]`, `y`}, `["a", {"b": [1, 2]}, [3, "y", 4]]`, true, nil},
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[0]`, `x`, `$[2][1]`, `y`}, `["x", "a", {"b": [1, 2]}, [3, 4]]`, true, nil},
		// More test cases
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[0]`, `x`, `$[0]`, `y`}, `["y", "x", "a", {"b": [1, 2]}, [3, 4]]`, true, nil},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		// Parameter count error
		if err != nil {
			c.Assert(t.err, NotNil)
			c.Assert(t.err.Equal(err), Equals, true)
			continue
		}

		d, err := evalBuiltinFunc(f, chunk.Row{})

		if t.success {
			c.Assert(err, IsNil)
			switch x := t.expected.(type) {
			case string:
				var j1, j2 json.BinaryJSON
				j1, err = json.ParseBinaryFromString(x)
				c.Assert(err, IsNil)
				j2 = d.GetMysqlJSON()
				var cmp int
				cmp = json.CompareBinary(j1, j2)
				c.Assert(cmp, Equals, 0)
			case nil:
				c.Assert(d.IsNull(), IsTrue)
			}
		} else {
			c.Assert(t.err.Equal(err), Equals, true)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONValid(c *C) {
	fc := funcs[ast.JSONValid]
	tbl := []struct {
		Input    any
		Expected any
	}{
		{`{"a":1}`, 1},
		{`hello`, 0},
		{`"hello"`, 1},
		{`null`, 1},
		{`{}`, 1},
		{`[]`, 1},
		{`2`, 1},
		{`2.5`, 1},
		{`2019-8-19`, 0},
		{`"2019-8-19"`, 1},
		{2, 0},
		{2.5, 0},
		{nil, nil},
	}
	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expected"][0])
	}
}

func (s *testEvaluatorSuite) TestJSONStorageSize(c *C) {
	fc := funcs[ast.JSONStorageSize]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// Tests scalar arguments
		{[]any{`null`}, 4, true},
		{[]any{`true`}, 4, true},
		{[]any{`1`}, 1, true},
		{[]any{`"1"`}, 3, true},
		// Tests nil arguments
		{[]any{nil}, nil, true},
		// Tests valid json documents
		{[]any{`{}`}, 2, true},
		{[]any{`{"a":1}`}, 8, true},
		{[]any{`[{"a":{"a":1},"b":2}]`}, 25, true},
		{[]any{`{"a": 1000, "b": "wxyz", "c": "[1, 3, 5, 7]"}`}, 45, true},
		// Tests invalid json documents
		{[]any{`[{"a":1]`}, 0, false},
		{[]any{`[{a":1]`}, 0, false},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)

			if t.expected == nil {
				c.Assert(d.IsNull(), IsTrue)
			} else {
				c.Assert(d.GetInt64(), Equals, int64(t.expected.(int)))
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}
