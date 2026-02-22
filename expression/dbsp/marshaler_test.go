package dbsp_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/expression/dbsp"
)

// stableJSON checks direction A: construct an expression, marshal it, compile the JSON,
// marshal again, and verify the two JSON strings are identical.
func stableJSON(expr dbsp.Expression) (first, second string) {
	b1, err := json.Marshal(expr)
	Expect(err).NotTo(HaveOccurred(), "first marshal")
	expr2, err := dbsp.Compile(b1)
	Expect(err).NotTo(HaveOccurred(), "compile")
	b2, err := json.Marshal(expr2)
	Expect(err).NotTo(HaveOccurred(), "second marshal")
	return string(b1), string(b2)
}

// canonicalJSON checks direction B: compile a canonical JSON string and verify that
// marshaling the result produces the same JSON string.
func canonicalJSON(input string) string {
	expr, err := dbsp.CompileString(input)
	Expect(err).NotTo(HaveOccurred(), "compile %q", input)
	b, err := json.Marshal(expr)
	Expect(err).NotTo(HaveOccurred(), "marshal")
	return string(b)
}

var _ = Describe("JSON round-trip", func() {
	Describe("Direction A: construct → marshal → compile → marshal is stable", func() {
		It("@nil", func() {
			j1, j2 := stableJSON(dbsp.NewNil())
			Expect(j1).To(Equal("null"))
			Expect(j2).To(Equal(j1))
		})

		It("@bool true", func() {
			j1, j2 := stableJSON(dbsp.NewBool(true))
			Expect(j1).To(Equal(`{"@bool":true}`))
			Expect(j2).To(Equal(j1))
		})

		It("@bool false", func() {
			j1, j2 := stableJSON(dbsp.NewBool(false))
			Expect(j1).To(Equal(`{"@bool":false}`))
			Expect(j2).To(Equal(j1))
		})

		It("@int", func() {
			j1, j2 := stableJSON(dbsp.NewInt(42))
			Expect(j1).To(Equal(`{"@int":42}`))
			Expect(j2).To(Equal(j1))
		})

		It("@float", func() {
			j1, j2 := stableJSON(dbsp.NewFloat(3.14))
			Expect(j1).To(Equal(`{"@float":3.14}`))
			Expect(j2).To(Equal(j1))
		})

		It("@string", func() {
			j1, j2 := stableJSON(dbsp.NewString("hello"))
			Expect(j1).To(Equal(`{"@string":"hello"}`))
			Expect(j2).To(Equal(j1))
		})

		It("@list", func() {
			j1, j2 := stableJSON(dbsp.NewList(dbsp.NewInt(1), dbsp.NewInt(2)))
			Expect(j1).To(Equal(`{"@list":[{"@int":1},{"@int":2}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@list empty", func() {
			j1, j2 := stableJSON(dbsp.NewList())
			Expect(j1).To(Equal(`{"@list":[]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@dict", func() {
			j1, j2 := stableJSON(dbsp.NewDict(map[string]dbsp.Expression{
				"x": dbsp.NewInt(1),
			}))
			Expect(j1).To(Equal(`{"@dict":{"x":{"@int":1}}}`))
			Expect(j2).To(Equal(j1))
		})

		It("@get", func() {
			j1, j2 := stableJSON(dbsp.NewGet("fieldname"))
			Expect(j1).To(Equal(`{"@get":"fieldname"}`))
			Expect(j2).To(Equal(j1))
		})

		It("@add", func() {
			j1, j2 := stableJSON(dbsp.NewAdd(dbsp.NewInt(1), dbsp.NewInt(2)))
			Expect(j1).To(Equal(`{"@add":[{"@int":1},{"@int":2}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@sub", func() {
			j1, j2 := stableJSON(dbsp.NewSub(dbsp.NewInt(5), dbsp.NewInt(3)))
			Expect(j1).To(Equal(`{"@sub":[{"@int":5},{"@int":3}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@mul", func() {
			j1, j2 := stableJSON(dbsp.NewMul(dbsp.NewInt(2), dbsp.NewInt(3)))
			Expect(j1).To(Equal(`{"@mul":[{"@int":2},{"@int":3}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@div", func() {
			j1, j2 := stableJSON(dbsp.NewDiv(dbsp.NewInt(10), dbsp.NewInt(2)))
			Expect(j1).To(Equal(`{"@div":[{"@int":10},{"@int":2}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@mod", func() {
			j1, j2 := stableJSON(dbsp.NewMod(dbsp.NewInt(7), dbsp.NewInt(3)))
			Expect(j1).To(Equal(`{"@mod":[{"@int":7},{"@int":3}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@neg", func() {
			j1, j2 := stableJSON(dbsp.NewNeg(dbsp.NewInt(5)))
			Expect(j1).To(Equal(`{"@neg":{"@int":5}}`))
			Expect(j2).To(Equal(j1))
		})

		It("@and", func() {
			j1, j2 := stableJSON(dbsp.NewAnd(dbsp.NewBool(true), dbsp.NewBool(false)))
			Expect(j1).To(Equal(`{"@and":[{"@bool":true},{"@bool":false}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@or", func() {
			j1, j2 := stableJSON(dbsp.NewOr(dbsp.NewBool(true), dbsp.NewBool(false)))
			Expect(j1).To(Equal(`{"@or":[{"@bool":true},{"@bool":false}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@not", func() {
			j1, j2 := stableJSON(dbsp.NewNot(dbsp.NewBool(true)))
			Expect(j1).To(Equal(`{"@not":{"@bool":true}}`))
			Expect(j2).To(Equal(j1))
		})

		It("@eq", func() {
			j1, j2 := stableJSON(dbsp.NewEq(dbsp.NewGet("x"), dbsp.NewInt(1)))
			Expect(j1).To(Equal(`{"@eq":[{"@get":"x"},{"@int":1}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@neq", func() {
			j1, j2 := stableJSON(dbsp.NewNeq(dbsp.NewGet("x"), dbsp.NewInt(0)))
			Expect(j1).To(Equal(`{"@neq":[{"@get":"x"},{"@int":0}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@gt", func() {
			j1, j2 := stableJSON(dbsp.NewGt(dbsp.NewGet("age"), dbsp.NewInt(18)))
			Expect(j1).To(Equal(`{"@gt":[{"@get":"age"},{"@int":18}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@gte", func() {
			j1, j2 := stableJSON(dbsp.NewGte(dbsp.NewGet("age"), dbsp.NewInt(18)))
			Expect(j1).To(Equal(`{"@gte":[{"@get":"age"},{"@int":18}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@lt", func() {
			j1, j2 := stableJSON(dbsp.NewLt(dbsp.NewGet("age"), dbsp.NewInt(65)))
			Expect(j1).To(Equal(`{"@lt":[{"@get":"age"},{"@int":65}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@lte", func() {
			j1, j2 := stableJSON(dbsp.NewLte(dbsp.NewGet("age"), dbsp.NewInt(65)))
			Expect(j1).To(Equal(`{"@lte":[{"@get":"age"},{"@int":65}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@isnull", func() {
			j1, j2 := stableJSON(dbsp.NewIsNull(dbsp.NewGet("field")))
			Expect(j1).To(Equal(`{"@isnull":{"@get":"field"}}`))
			Expect(j2).To(Equal(j1))
		})

		It("@cond", func() {
			j1, j2 := stableJSON(dbsp.NewCond(
				dbsp.NewBool(true),
				dbsp.NewInt(1),
				dbsp.NewInt(0),
			))
			Expect(j1).To(Equal(`{"@cond":[{"@bool":true},{"@int":1},{"@int":0}]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@sqlbool", func() {
			j1, j2 := stableJSON(dbsp.NewSqlBool(dbsp.NewGet("flag")))
			Expect(j1).To(Equal(`{"@sqlbool":{"@get":"flag"}}`))
			Expect(j2).To(Equal(j1))
		})

		It("nested expression", func() {
			// @add($.x, @neg(@int(1)))
			j1, j2 := stableJSON(dbsp.NewAdd(
				dbsp.NewGet("x"),
				dbsp.NewNeg(dbsp.NewInt(1)),
			))
			Expect(j1).To(Equal(`{"@add":[{"@get":"x"},{"@neg":{"@int":1}}]}`))
			Expect(j2).To(Equal(j1))
		})
	})

	Describe("Direction B: canonical JSON → compile → marshal is stable", func() {
		DescribeTable("canonical forms",
			func(input string) {
				Expect(canonicalJSON(input)).To(Equal(input))
			},
			Entry("@nil", `null`),
			Entry("@bool true", `{"@bool":true}`),
			Entry("@bool false", `{"@bool":false}`),
			Entry("@int", `{"@int":42}`),
			Entry("@float", `{"@float":3.14}`),
			Entry("@string", `{"@string":"hello"}`),
			Entry("@list", `{"@list":[{"@int":1},{"@int":2}]}`),
			Entry("@list empty", `{"@list":[]}`),
			Entry("@get", `{"@get":"fieldname"}`),
			Entry("@getsub", `{"@getsub":"fieldname"}`),
			Entry("@add binary", `{"@add":[{"@int":1},{"@int":2}]}`),
			Entry("@add variadic", `{"@add":[{"@int":1},{"@int":2},{"@int":3}]}`),
			Entry("@sub", `{"@sub":[{"@int":5},{"@int":3}]}`),
			Entry("@mul", `{"@mul":[{"@int":2},{"@int":3}]}`),
			Entry("@div", `{"@div":[{"@int":10},{"@int":2}]}`),
			Entry("@mod", `{"@mod":[{"@int":7},{"@int":3}]}`),
			Entry("@neg", `{"@neg":{"@int":5}}`),
			Entry("@and", `{"@and":[{"@bool":true},{"@bool":false}]}`),
			Entry("@or", `{"@or":[{"@bool":true},{"@bool":false}]}`),
			Entry("@not", `{"@not":{"@bool":true}}`),
			Entry("@eq", `{"@eq":[{"@get":"x"},{"@int":1}]}`),
			Entry("@neq", `{"@neq":[{"@get":"x"},{"@int":0}]}`),
			Entry("@gt", `{"@gt":[{"@get":"age"},{"@int":18}]}`),
			Entry("@gte", `{"@gte":[{"@get":"age"},{"@int":18}]}`),
			Entry("@lt", `{"@lt":[{"@get":"age"},{"@int":65}]}`),
			Entry("@lte", `{"@lte":[{"@get":"age"},{"@int":65}]}`),
			Entry("@isnull", `{"@isnull":{"@get":"field"}}`),
			Entry("@cond", `{"@cond":[{"@bool":true},{"@int":1},{"@int":0}]}`),
			Entry("@switch", `{"@switch":[{"@list":[{"@bool":true},{"@int":1}]},{"@list":[{"@bool":false},{"@int":0}]}]}`),
			Entry("@definedOr", `{"@definedOr":[{"@get":"a"},{"@get":"b"}]}`),
			Entry("@sqlbool", `{"@sqlbool":{"@get":"flag"}}`),
			Entry("@set", `{"@set":[{"@string":"field"},{"@int":42}]}`),
			Entry("@setsub", `{"@setsub":[{"@string":"field"},{"@int":42}]}`),
			Entry("@exists", `{"@exists":"field"}`),
			Entry("@regexp", `{"@regexp":[{"@string":"^foo"},{"@get":"name"}]}`),
			Entry("@upper", `{"@upper":{"@get":"name"}}`),
			Entry("@lower", `{"@lower":{"@get":"name"}}`),
			Entry("@trim", `{"@trim":{"@get":"text"}}`),
			Entry("@substring", `{"@substring":[{"@get":"s"},{"@int":1},{"@int":3}]}`),
			Entry("@replace", `{"@replace":[{"@get":"s"},{"@string":"a"},{"@string":"b"}]}`),
			Entry("@split", `{"@split":[{"@get":"s"},{"@string":","}]}`),
			Entry("@join", `{"@join":[{"@get":"list"},{"@string":","}]}`),
			Entry("@startswith", `{"@startswith":[{"@get":"s"},{"@string":"pre"}]}`),
			Entry("@endswith", `{"@endswith":[{"@get":"s"},{"@string":"suf"}]}`),
			Entry("@contains", `{"@contains":[{"@get":"s"},{"@string":"sub"}]}`),
			Entry("@noop", `{"@noop":null}`),
			Entry("@arg", `{"@arg":null}`),
			Entry("@hash", `{"@hash":{"@get":"x"}}`),
			Entry("@rnd", `{"@rnd":[{"@int":1},{"@int":100}]}`),
			Entry("@concat", `{"@concat":[{"@get":"a"},{"@get":"b"}]}`),
			Entry("@abs", `{"@abs":{"@int":-5}}`),
			Entry("@floor", `{"@floor":{"@float":3.7}}`),
			Entry("@ceil", `{"@ceil":{"@float":3.2}}`),
			Entry("@isnil", `{"@isnil":{"@get":"x"}}`),
			Entry("@map", `{"@map":[{"@arg":null},{"@get":"list"}]}`),
			Entry("@filter", `{"@filter":[{"@bool":true},{"@get":"list"}]}`),
			Entry("@sum", `{"@sum":[{"@int":1},{"@int":2},{"@int":3}]}`),
			Entry("@len", `{"@len":{"@get":"list"}}`),
			Entry("@min", `{"@min":[{"@int":3},{"@int":1},{"@int":2}]}`),
			Entry("@max", `{"@max":[{"@int":3},{"@int":1},{"@int":2}]}`),
			Entry("@in", `{"@in":[{"@int":1},{"@get":"list"}]}`),
			Entry("@range", `{"@range":{"@int":5}}`),
			Entry("@now", `{"@now":null}`),
			Entry("@dict", `{"@dict":{"key":{"@int":1}}}`),
			Entry("nested", `{"@add":[{"@get":"x"},{"@neg":{"@int":1}}]}`),
		)
	})

	Describe("MarshalJSON on constructed expressions", func() {
		It("marshals @now as nullary op", func() {
			expr, err := dbsp.CompileString(`{"@now":null}`)
			Expect(err).NotTo(HaveOccurred())
			b, err := json.Marshal(expr)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(`{"@now":null}`))
		})

		It("marshals @noop as nullary op", func() {
			expr, err := dbsp.CompileString(`{"@noop":null}`)
			Expect(err).NotTo(HaveOccurred())
			b, err := json.Marshal(expr)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(`{"@noop":null}`))
		})

		It("marshals @arg as nullary op", func() {
			expr, err := dbsp.CompileString(`{"@arg":null}`)
			Expect(err).NotTo(HaveOccurred())
			b, err := json.Marshal(expr)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(`{"@arg":null}`))
		})

		It("marshals @list with nested @get expressions", func() {
			// A list containing field references.
			expr, err := dbsp.CompileString(`{"@list":[{"@get":"a"},{"@get":"b"}]}`)
			Expect(err).NotTo(HaveOccurred())
			b, err := json.Marshal(expr)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(`{"@list":[{"@get":"a"},{"@get":"b"}]}`))
		})

		It("marshals @dict with nested expressions", func() {
			expr, err := dbsp.CompileString(`{"@dict":{"name":{"@get":"fullname"},"age":{"@int":30}}}`)
			Expect(err).NotTo(HaveOccurred())
			b, err := json.Marshal(expr)
			Expect(err).NotTo(HaveOccurred())
			// Dict key order is not guaranteed; unmarshal both and compare.
			var got, want map[string]map[string]any
			Expect(json.Unmarshal(b, &got)).To(Succeed())
			Expect(json.Unmarshal([]byte(`{"@dict":{"name":{"@get":"fullname"},"age":{"@int":30}}}`), &want)).To(Succeed())
			Expect(got).To(Equal(want))
		})

		It("error on marshal of constExpr with nil value is null", func() {
			// NewConst(nil) produces constExpr{nil}, which should marshal as null.
			expr := dbsp.NewConst(nil)
			b, err := json.Marshal(expr)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal("null"))
		})
	})
})
