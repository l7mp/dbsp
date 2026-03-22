package dbsp_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/expression/dbsp"
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
			Expect(j1).To(Equal(`true`))
			Expect(j2).To(Equal(j1))
		})

		It("@bool false", func() {
			j1, j2 := stableJSON(dbsp.NewBool(false))
			Expect(j1).To(Equal(`false`))
			Expect(j2).To(Equal(j1))
		})

		It("@int", func() {
			j1, j2 := stableJSON(dbsp.NewInt(42))
			Expect(j1).To(Equal(`42`))
			Expect(j2).To(Equal(j1))
		})

		It("@float", func() {
			j1, j2 := stableJSON(dbsp.NewFloat(3.14))
			Expect(j1).To(Equal(`3.14`))
			Expect(j2).To(Equal(j1))
		})

		It("@string plain", func() {
			j1, j2 := stableJSON(dbsp.NewString("hello"))
			Expect(j1).To(Equal(`"hello"`))
			Expect(j2).To(Equal(j1))
		})

		It("@string with $. prefix stays explicit", func() {
			j1, j2 := stableJSON(dbsp.NewString("$.path"))
			Expect(j1).To(Equal(`{"@string":"$.path"}`))
			Expect(j2).To(Equal(j1))
		})

		It("@string with $$. prefix stays explicit", func() {
			j1, j2 := stableJSON(dbsp.NewString("$$.path"))
			Expect(j1).To(Equal(`{"@string":"$$.path"}`))
			Expect(j2).To(Equal(j1))
		})

		It("@list", func() {
			j1, j2 := stableJSON(dbsp.NewList(dbsp.NewInt(1), dbsp.NewInt(2)))
			Expect(j1).To(Equal(`[1,2]`))
			Expect(j2).To(Equal(j1))
		})

		It("@list empty", func() {
			j1, j2 := stableJSON(dbsp.NewList())
			Expect(j1).To(Equal(`[]`))
			Expect(j2).To(Equal(j1))
		})

		It("@dict", func() {
			j1, j2 := stableJSON(dbsp.NewDict(map[string]dbsp.Expression{
				"x": dbsp.NewInt(1),
			}))
			Expect(j1).To(Equal(`{"x":1}`))
			Expect(j2).To(Equal(j1))
		})

		It("@get", func() {
			j1, j2 := stableJSON(dbsp.NewGet("fieldname"))
			Expect(j1).To(Equal(`"$.fieldname"`))
			Expect(j2).To(Equal(j1))
		})

		It("@add", func() {
			j1, j2 := stableJSON(dbsp.NewAdd(dbsp.NewInt(1), dbsp.NewInt(2)))
			Expect(j1).To(Equal(`{"@add":[1,2]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@sub", func() {
			j1, j2 := stableJSON(dbsp.NewSub(dbsp.NewInt(5), dbsp.NewInt(3)))
			Expect(j1).To(Equal(`{"@sub":[5,3]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@mul", func() {
			j1, j2 := stableJSON(dbsp.NewMul(dbsp.NewInt(2), dbsp.NewInt(3)))
			Expect(j1).To(Equal(`{"@mul":[2,3]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@div", func() {
			j1, j2 := stableJSON(dbsp.NewDiv(dbsp.NewInt(10), dbsp.NewInt(2)))
			Expect(j1).To(Equal(`{"@div":[10,2]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@mod", func() {
			j1, j2 := stableJSON(dbsp.NewMod(dbsp.NewInt(7), dbsp.NewInt(3)))
			Expect(j1).To(Equal(`{"@mod":[7,3]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@neg", func() {
			j1, j2 := stableJSON(dbsp.NewNeg(dbsp.NewInt(5)))
			Expect(j1).To(Equal(`{"@neg":5}`))
			Expect(j2).To(Equal(j1))
		})

		It("@and", func() {
			j1, j2 := stableJSON(dbsp.NewAnd(dbsp.NewBool(true), dbsp.NewBool(false)))
			Expect(j1).To(Equal(`{"@and":[true,false]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@or", func() {
			j1, j2 := stableJSON(dbsp.NewOr(dbsp.NewBool(true), dbsp.NewBool(false)))
			Expect(j1).To(Equal(`{"@or":[true,false]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@not", func() {
			j1, j2 := stableJSON(dbsp.NewNot(dbsp.NewBool(true)))
			Expect(j1).To(Equal(`{"@not":true}`))
			Expect(j2).To(Equal(j1))
		})

		It("@eq", func() {
			j1, j2 := stableJSON(dbsp.NewEq(dbsp.NewGet("x"), dbsp.NewInt(1)))
			Expect(j1).To(Equal(`{"@eq":["$.x",1]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@neq", func() {
			j1, j2 := stableJSON(dbsp.NewNeq(dbsp.NewGet("x"), dbsp.NewInt(0)))
			Expect(j1).To(Equal(`{"@neq":["$.x",0]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@gt", func() {
			j1, j2 := stableJSON(dbsp.NewGt(dbsp.NewGet("age"), dbsp.NewInt(18)))
			Expect(j1).To(Equal(`{"@gt":["$.age",18]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@gte", func() {
			j1, j2 := stableJSON(dbsp.NewGte(dbsp.NewGet("age"), dbsp.NewInt(18)))
			Expect(j1).To(Equal(`{"@gte":["$.age",18]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@lt", func() {
			j1, j2 := stableJSON(dbsp.NewLt(dbsp.NewGet("age"), dbsp.NewInt(65)))
			Expect(j1).To(Equal(`{"@lt":["$.age",65]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@lte", func() {
			j1, j2 := stableJSON(dbsp.NewLte(dbsp.NewGet("age"), dbsp.NewInt(65)))
			Expect(j1).To(Equal(`{"@lte":["$.age",65]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@isnull", func() {
			j1, j2 := stableJSON(dbsp.NewIsNull(dbsp.NewGet("field")))
			Expect(j1).To(Equal(`{"@isnull":"$.field"}`))
			Expect(j2).To(Equal(j1))
		})

		It("@cond", func() {
			j1, j2 := stableJSON(dbsp.NewCond(
				dbsp.NewBool(true),
				dbsp.NewInt(1),
				dbsp.NewInt(0),
			))
			Expect(j1).To(Equal(`{"@cond":[true,1,0]}`))
			Expect(j2).To(Equal(j1))
		})

		It("@sqlbool", func() {
			j1, j2 := stableJSON(dbsp.NewSqlBool(dbsp.NewGet("flag")))
			Expect(j1).To(Equal(`{"@sqlbool":"$.flag"}`))
			Expect(j2).To(Equal(j1))
		})

		It("nested expression", func() {
			// @add($.x, @neg(1))
			j1, j2 := stableJSON(dbsp.NewAdd(
				dbsp.NewGet("x"),
				dbsp.NewNeg(dbsp.NewInt(1)),
			))
			Expect(j1).To(Equal(`{"@add":["$.x",{"@neg":1}]}`))
			Expect(j2).To(Equal(j1))
		})
	})

	Describe("Direction B: canonical JSON → compile → marshal is stable", func() {
		DescribeTable("canonical forms",
			func(input string) {
				Expect(canonicalJSON(input)).To(Equal(input))
			},
			Entry("@nil", `null`),
			Entry("@bool true", `true`),
			Entry("@bool false", `false`),
			Entry("@int", `42`),
			Entry("@float", `3.14`),
			Entry("@string plain", `"hello"`),
			Entry("@string $.path stays explicit", `{"@string":"$.path"}`),
			Entry("@string $$.path stays explicit", `{"@string":"$$.path"}`),
			Entry("@list", `[1,2]`),
			Entry("@list empty", `[]`),
			Entry("@get", `"$.fieldname"`),
			Entry("@getsub", `"$$.fieldname"`),
			Entry("@dict plain", `{"key":1}`),
			Entry("@add binary", `{"@add":[1,2]}`),
			Entry("@add variadic", `{"@add":[1,2,3]}`),
			Entry("@sub", `{"@sub":[5,3]}`),
			Entry("@mul", `{"@mul":[2,3]}`),
			Entry("@div", `{"@div":[10,2]}`),
			Entry("@mod", `{"@mod":[7,3]}`),
			Entry("@neg", `{"@neg":5}`),
			Entry("@and", `{"@and":[true,false]}`),
			Entry("@or", `{"@or":[true,false]}`),
			Entry("@not", `{"@not":true}`),
			Entry("@eq", `{"@eq":["$.x",1]}`),
			Entry("@neq", `{"@neq":["$.x",0]}`),
			Entry("@gt", `{"@gt":["$.age",18]}`),
			Entry("@gte", `{"@gte":["$.age",18]}`),
			Entry("@lt", `{"@lt":["$.age",65]}`),
			Entry("@lte", `{"@lte":["$.age",65]}`),
			Entry("@isnull", `{"@isnull":"$.field"}`),
			Entry("@cond", `{"@cond":[true,1,0]}`),
			Entry("@switch", `{"@switch":[[true,1],[false,0]]}`),
			Entry("@definedOr", `{"@definedOr":["$.a","$.b"]}`),
			Entry("@sqlbool", `{"@sqlbool":"$.flag"}`),
			Entry("@set", `{"@set":["field",42]}`),
			Entry("@setsub", `{"@setsub":["field",42]}`),
			Entry("@exists", `{"@exists":"field"}`),
			Entry("@regexp", `{"@regexp":["^foo","$.name"]}`),
			Entry("@upper", `{"@upper":"$.name"}`),
			Entry("@lower", `{"@lower":"$.name"}`),
			Entry("@trim", `{"@trim":"$.text"}`),
			Entry("@substring", `{"@substring":["$.s",1,3]}`),
			Entry("@replace", `{"@replace":["$.s","a","b"]}`),
			Entry("@split", `{"@split":["$.s",","]}`),
			Entry("@join", `{"@join":["$.list",","]}`),
			Entry("@startswith", `{"@startswith":["$.s","pre"]}`),
			Entry("@endswith", `{"@endswith":["$.s","suf"]}`),
			Entry("@contains", `{"@contains":["$.s","sub"]}`),
			Entry("@noop", `{"@noop":null}`),
			Entry("@subject", `{"@subject":null}`),
			Entry("@copy", `{"@copy":null}`),
			Entry("@hash", `{"@hash":"$.x"}`),
			Entry("@rnd", `{"@rnd":[1,100]}`),
			Entry("@concat", `{"@concat":["$.a","$.b"]}`),
			Entry("@abs", `{"@abs":-5}`),
			Entry("@floor", `{"@floor":3.7}`),
			Entry("@ceil", `{"@ceil":3.2}`),
			Entry("@isnil", `{"@isnil":"$.x"}`),
			Entry("@map", `{"@map":[{"@subject":null},"$.list"]}`),
			Entry("@filter", `{"@filter":[true,"$.list"]}`),
			Entry("@sum", `{"@sum":[1,2,3]}`),
			Entry("@len", `{"@len":"$.list"}`),
			Entry("@sortBy", `{"@sortBy":[{"@switch":[[{"@gt":["$$.a","$$.b"]},1],[{"@eq":["$$.a","$$.b"]},0],[true,-1]]},"$.list"]}`),
			Entry("@min", `{"@min":[3,1,2]}`),
			Entry("@max", `{"@max":[3,1,2]}`),
			Entry("@lexmin", `{"@lexmin":["b","a","c"]}`),
			Entry("@lexmax", `{"@lexmax":["b","a","c"]}`),
			Entry("@in", `{"@in":[1,"$.list"]}`),
			Entry("@range", `{"@range":5}`),
			Entry("@now", `{"@now":null}`),
			Entry("nested", `{"@add":["$.x",{"@neg":1}]}`),
		)

		It("explicit @bool form also parses (normalises to natural form)", func() {
			Expect(canonicalJSON(`{"@bool":true}`)).To(Equal(`true`))
		})

		It("explicit @int form also parses (normalises to natural form)", func() {
			Expect(canonicalJSON(`{"@int":42}`)).To(Equal(`42`))
		})

		It("explicit @string form also parses (normalises to natural form)", func() {
			Expect(canonicalJSON(`{"@string":"hello"}`)).To(Equal(`"hello"`))
		})

		It("explicit @list form also parses (normalises to natural form)", func() {
			Expect(canonicalJSON(`{"@list":[1,2]}`)).To(Equal(`[1,2]`))
		})

		It("explicit @dict form also parses (normalises to natural form)", func() {
			Expect(canonicalJSON(`{"@dict":{"key":1}}`)).To(Equal(`{"key":1}`))
		})

		It("explicit @get form also parses (normalises to natural form)", func() {
			Expect(canonicalJSON(`{"@get":"fieldname"}`)).To(Equal(`"$.fieldname"`))
		})
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

		It("marshals @subject as nullary op", func() {
			expr, err := dbsp.CompileString(`{"@subject":null}`)
			Expect(err).NotTo(HaveOccurred())
			b, err := json.Marshal(expr)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(`{"@subject":null}`))
		})

		It("marshals @list with nested @get expressions", func() {
			expr, err := dbsp.CompileString(`[{"@get":"a"},{"@get":"b"}]`)
			Expect(err).NotTo(HaveOccurred())
			b, err := json.Marshal(expr)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(`["$.a","$.b"]`))
		})

		It("marshals @dict with nested expressions", func() {
			expr, err := dbsp.CompileString(`{"name":{"@get":"fullname"},"age":30}`)
			Expect(err).NotTo(HaveOccurred())
			b, err := json.Marshal(expr)
			Expect(err).NotTo(HaveOccurred())
			// Dict key order is not guaranteed; unmarshal both and compare.
			var got, want map[string]any
			Expect(json.Unmarshal(b, &got)).To(Succeed())
			Expect(json.Unmarshal([]byte(`{"name":"$.fullname","age":30}`), &want)).To(Succeed())
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
