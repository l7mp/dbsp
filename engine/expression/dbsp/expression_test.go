package dbsp_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/ohler55/ojg/jp"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/expression/dbsp"
)

func TestDBSPExpression(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DBSP Expression Suite")
}

// TestDoc is a simple document implementation for testing.
type TestDoc struct {
	fields map[string]any
}

func NewTestDoc(fields map[string]any) *TestDoc {
	return &TestDoc{fields: fields}
}

func (d *TestDoc) String() string {
	return "TestDoc"
}

func (d *TestDoc) Hash() string {
	return "key"
}

func (d *TestDoc) PrimaryKey() (string, error) {
	return "pk", nil
}

func (d *TestDoc) Copy() datamodel.Document {
	newFields := make(map[string]any, len(d.fields))
	for k, v := range d.fields {
		newFields[k] = v
	}
	return &TestDoc{fields: newFields}
}

func (d *TestDoc) Merge(other datamodel.Document) datamodel.Document {
	nd := d.Copy().(*TestDoc)
	if od, ok := other.(*TestDoc); ok {
		for k, v := range od.fields {
			nd.fields[k] = v
		}
	}
	return nd
}

func (d *TestDoc) GetField(key string) (any, error) {
	if strings.HasPrefix(key, "$") {
		expr, err := jp.ParseString(key)
		if err != nil {
			return nil, fmt.Errorf("invalid JSONPath %q: %w", key, err)
		}
		results := expr.Get(d.fields)
		if len(results) == 0 {
			return nil, datamodel.ErrFieldNotFound
		}
		if len(results) == 1 {
			return results[0], nil
		}
		return results, nil
	}

	v, ok := d.fields[key]
	if !ok {
		return nil, datamodel.ErrFieldNotFound
	}
	return v, nil
}

func (d *TestDoc) SetField(key string, value any) error {
	d.fields[key] = value
	return nil
}

func (d *TestDoc) Fields() map[string]any {
	if d == nil || d.fields == nil {
		return nil
	}

	out := make(map[string]any, len(d.fields))
	for k, v := range d.fields {
		out[k] = deepCopyTestValue(v)
	}

	return out
}

func deepCopyTestValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, vv := range x {
			m[k] = deepCopyTestValue(vv)
		}
		return m
	case []any:
		s := make([]any, len(x))
		for i, vv := range x {
			s[i] = deepCopyTestValue(vv)
		}
		return s
	default:
		return v
	}
}

func (d *TestDoc) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.fields)
}

func (d *TestDoc) UnmarshalJSON(data []byte) error {
	if d == nil {
		return fmt.Errorf("TestDoc must be non-nil for JSON decoding")
	}
	return json.Unmarshal(data, &d.fields)
}

func (d *TestDoc) New() datamodel.Document {
	return &TestDoc{fields: make(map[string]any)}
}

var _ = Describe("Literal Operators", func() {
	It("should evaluate @nil", func() {
		expr, err := dbsp.Compile([]byte(`{"@nil": null}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeNil())
	})

	It("should evaluate @bool", func() {
		expr, err := dbsp.Compile([]byte(`{"@bool": true}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @int", func() {
		expr, err := dbsp.Compile([]byte(`{"@int": 42}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(42)))
	})

	It("should evaluate @float", func() {
		expr, err := dbsp.Compile([]byte(`{"@float": 3.14}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(3.14))
	})

	It("should evaluate @string", func() {
		expr, err := dbsp.Compile([]byte(`{"@string": "hello"}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("hello"))
	})

	It("should evaluate @list", func() {
		expr, err := dbsp.Compile([]byte(`{"@list": [1, 2, 3]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal([]any{int64(1), int64(2), int64(3)}))
	})

	It("should evaluate @list with nested expressions", func() {
		expr, err := dbsp.Compile([]byte(`{"@list": [1, {"@add": [2, 3]}]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal([]any{int64(1), int64(5)}))
	})

	It("should evaluate @dict", func() {
		expr, err := dbsp.Compile([]byte(`{"@dict": {"x": 1, "y": 2}}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(map[string]any{"x": int64(1), "y": int64(2)}))
	})

	It("should evaluate @dict with nested expressions", func() {
		expr, err := dbsp.Compile([]byte(`{"@dict": {"sum": {"@add": [1, 2]}}}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(map[string]any{"sum": int64(3)}))
	})
})

var _ = Describe("Boolean Operators", func() {
	It("should evaluate @and with all true", func() {
		expr, err := dbsp.Compile([]byte(`{"@and": [true, true, true]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @and with false (short-circuit)", func() {
		expr, err := dbsp.Compile([]byte(`{"@and": [true, false, true]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(false))
	})

	It("should evaluate @or with all false", func() {
		expr, err := dbsp.Compile([]byte(`{"@or": [false, false, false]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(false))
	})

	It("should evaluate @or with true (short-circuit)", func() {
		expr, err := dbsp.Compile([]byte(`{"@or": [false, true, false]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @not", func() {
		expr, err := dbsp.Compile([]byte(`{"@not": true}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(false))
	})
})

var _ = Describe("Arithmetic Operators", func() {
	It("should evaluate @add with integers", func() {
		expr, err := dbsp.Compile([]byte(`{"@add": [1, 2, 3]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(6)))
	})

	It("should evaluate @add with floats", func() {
		expr, err := dbsp.Compile([]byte(`{"@add": [1.5, 2.5]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(4.0))
	})

	It("should evaluate @add with mixed types (promotes to float)", func() {
		expr, err := dbsp.Compile([]byte(`{"@add": [1, 2.5]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(3.5))
	})

	It("should evaluate @sub", func() {
		expr, err := dbsp.Compile([]byte(`{"@sub": [10, 3]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(7)))
	})

	It("should evaluate @mul", func() {
		expr, err := dbsp.Compile([]byte(`{"@mul": [2, 3, 4]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(24)))
	})

	It("should evaluate @div with integers", func() {
		expr, err := dbsp.Compile([]byte(`{"@div": [10, 3]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(3))) // Integer division.
	})

	It("should evaluate @div with floats", func() {
		// Use actual floats (with fractional parts) to ensure float division.
		expr, err := dbsp.Compile([]byte(`{"@div": [10.5, 2.5]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeNumerically("~", 4.2, 0.01))
	})

	It("should handle division by zero", func() {
		expr, err := dbsp.Compile([]byte(`{"@div": [10, 0]}`))
		Expect(err).NotTo(HaveOccurred())

		_, err = expr.Evaluate(expression.NewContext(nil))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("division by zero"))
	})
})

var _ = Describe("Comparison Operators", func() {
	It("should evaluate @eq with equal integers", func() {
		expr, err := dbsp.Compile([]byte(`{"@eq": [10, 10]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @eq with different integers", func() {
		expr, err := dbsp.Compile([]byte(`{"@eq": [10, 20]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(false))
	})

	It("should evaluate @gt", func() {
		expr, err := dbsp.Compile([]byte(`{"@gt": [10, 5]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @gte", func() {
		expr, err := dbsp.Compile([]byte(`{"@gte": [10, 10]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @lt", func() {
		expr, err := dbsp.Compile([]byte(`{"@lt": [5, 10]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @lte", func() {
		expr, err := dbsp.Compile([]byte(`{"@lte": [10, 10]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should compare strings", func() {
		expr, err := dbsp.Compile([]byte(`{"@lt": ["apple", "banana"]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})
})

var _ = Describe("Field Operators", func() {
	var doc *TestDoc

	BeforeEach(func() {
		doc = NewTestDoc(map[string]any{
			"name":  "Alice",
			"age":   int64(30),
			"score": 95.5,
		})
	})

	It("should evaluate @get", func() {
		expr, err := dbsp.Compile([]byte(`{"@get": "name"}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(doc))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("Alice"))
	})

	It("should evaluate $.field shorthand", func() {
		expr, err := dbsp.Compile([]byte(`"$.age"`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(doc))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(30)))
	})

	It("should evaluate bracketed document JSONPath shorthand", func() {
		doc := NewTestDoc(map[string]any{
			"EndpointSlice": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{
						"kubernetes.io/service-name": "test-service-1",
					},
				},
			},
		})

		expr, err := dbsp.Compile([]byte(`"$[\"EndpointSlice\"][\"metadata\"][\"labels\"][\"kubernetes.io/service-name\"]"`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(doc))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("test-service-1"))
	})

	It("should evaluate $. as document copy", func() {
		expr, err := dbsp.Compile([]byte(`"$."`))
		Expect(err).NotTo(HaveOccurred())

		doc := NewTestDoc(map[string]any{"x": int64(1)})
		result, err := expr.Evaluate(expression.NewContext(doc))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(map[string]any{"x": float64(1)}))
	})

	It("should return error for missing field", func() {
		expr, err := dbsp.Compile([]byte(`{"@get": "missing"}`))
		Expect(err).NotTo(HaveOccurred())

		_, err = expr.Evaluate(expression.NewContext(doc))
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(datamodel.ErrFieldNotFound))
	})

	It("should evaluate @exists for existing field", func() {
		expr, err := dbsp.Compile([]byte(`{"@exists": "name"}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(doc))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @exists for missing field", func() {
		expr, err := dbsp.Compile([]byte(`{"@exists": "missing"}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(doc))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(false))
	})

	It("should evaluate @set", func() {
		expr, err := dbsp.Compile([]byte(`{"@set": ["newField", 42]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(doc))
		Expect(err).NotTo(HaveOccurred())
		// @set returns the modified document, not the value.
		Expect(result).To(Equal(doc))

		// Verify field was set on the document.
		v, _ := doc.GetField("newField")
		Expect(v).To(Equal(int64(42)))
	})
})

var _ = Describe("List Operators", func() {
	It("should evaluate @sum", func() {
		expr, err := dbsp.Compile([]byte(`{"@sum": [1, 2, 3]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(6)))
	})

	It("should evaluate @len on list", func() {
		expr, err := dbsp.Compile([]byte(`{"@len": {"@list": [1, 2, 3, 4, 5]}}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(5)))
	})

	It("should evaluate @min", func() {
		expr, err := dbsp.Compile([]byte(`{"@min": [3, 1, 4, 1, 5]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(1)))
	})

	It("should evaluate @max", func() {
		expr, err := dbsp.Compile([]byte(`{"@max": [3, 1, 4, 1, 5]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(5)))
	})

	It("should evaluate @lexmin", func() {
		expr, err := dbsp.Compile([]byte(`{"@lexmin": ["b", "a", "c"]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("a"))
	})

	It("should evaluate @lexmax", func() {
		expr, err := dbsp.Compile([]byte(`{"@lexmax": ["b", "a", "c"]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("c"))
	})

	It("should evaluate @in (element found)", func() {
		expr, err := dbsp.Compile([]byte(`{"@in": [2, [1, 2, 3]]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @in (element not found)", func() {
		expr, err := dbsp.Compile([]byte(`{"@in": [5, [1, 2, 3]]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(false))
	})

	It("should evaluate @range", func() {
		expr, err := dbsp.Compile([]byte(`{"@range": 5}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal([]any{int64(1), int64(2), int64(3), int64(4), int64(5)}))
	})

	It("should evaluate @map", func() {
		// Map each element by adding 10.
		expr, err := dbsp.Compile([]byte(`{"@map": [{"@add": ["$$.", 10]}, [1, 2, 3]]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal([]any{int64(11), int64(12), int64(13)}))
	})

	It("should evaluate @filter", func() {
		// Filter elements > 2.
		expr, err := dbsp.Compile([]byte(`{"@filter": [{"@gt": ["$$.", 2]}, [1, 2, 3, 4]]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal([]any{int64(3), int64(4)}))
	})

	It("should evaluate @sortBy with comparator subject fields", func() {
		expr, err := dbsp.Compile([]byte(`{"@sortBy": [
			{"@switch": [
				[{"@gt": ["$$.a", "$$.b"]}, 1],
				[{"@eq": ["$$.a", "$$.b"]}, 0],
				[true, -1]
			]},
			[3, 1, 4, 1, 5, 9]
		]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal([]any{int64(1), int64(1), int64(3), int64(4), int64(5), int64(9)}))
	})

	It("should return error when @sortBy comparator does not return -1, 0 or 1", func() {
		expr, err := dbsp.Compile([]byte(`{"@sortBy": [{"@add": ["$$.a", "$$.b"]}, [2, 1]]}`))
		Expect(err).NotTo(HaveOccurred())

		_, err = expr.Evaluate(expression.NewContext(nil))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("comparator must return -1, 0, or 1"))
	})

})

var _ = Describe("Conditional Operators", func() {
	It("should evaluate @cond (true branch)", func() {
		expr, err := dbsp.Compile([]byte(`{"@cond": [true, "yes", "no"]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("yes"))
	})

	It("should evaluate @cond (false branch)", func() {
		expr, err := dbsp.Compile([]byte(`{"@cond": [false, "yes", "no"]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("no"))
	})

	It("should evaluate @definedOr", func() {
		expr, err := dbsp.Compile([]byte(`{"@definedOr": [null, null, 42]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(42)))
	})
})

var _ = Describe("Utility Operators", func() {
	It("should evaluate @noop", func() {
		expr, err := dbsp.Compile([]byte(`{"@noop": null}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeNil())
	})

	It("should evaluate @hash", func() {
		expr, err := dbsp.Compile([]byte(`{"@hash": "test"}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(HaveLen(16)) // 8 bytes = 16 hex chars.
	})

	It("should evaluate @concat", func() {
		expr, err := dbsp.Compile([]byte(`{"@concat": ["hello", " ", "world"]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("hello world"))
	})

	It("should evaluate @abs with negative integer", func() {
		// Note: JSON numbers without fractional part are parsed as int64.
		// -42 is parsed as int64(-42), so abs returns int64(42).
		expr, err := dbsp.Compile([]byte(`{"@abs": {"@int": -42}}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(42)))
	})

	It("should evaluate @abs with negative float", func() {
		expr, err := dbsp.Compile([]byte(`{"@abs": -3.14}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(BeNumerically("~", 3.14, 0.01))
	})

	It("should evaluate @isnil", func() {
		expr, err := dbsp.Compile([]byte(`{"@isnil": null}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})
})

var _ = Describe("String Operators", func() {
	It("should evaluate @regexp", func() {
		expr, err := dbsp.CompileString(`{"@regexp": ["^hello", "hello world"]}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @regexp (no match)", func() {
		expr, err := dbsp.CompileString(`{"@regexp": ["^world", "hello world"]}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(false))
	})

	It("should evaluate @upper", func() {
		expr, err := dbsp.CompileString(`{"@upper": "hello"}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("HELLO"))
	})

	It("should evaluate @lower", func() {
		expr, err := dbsp.CompileString(`{"@lower": "HELLO"}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("hello"))
	})

	It("should evaluate @trim", func() {
		expr, err := dbsp.CompileString(`{"@trim": "  hello  "}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("hello"))
	})

	It("should evaluate @substring with start only", func() {
		expr, err := dbsp.CompileString(`{"@substring": ["hello world", 7]}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("world"))
	})

	It("should evaluate @substring with start and length", func() {
		expr, err := dbsp.CompileString(`{"@substring": ["hello world", 1, 5]}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("hello"))
	})

	It("should evaluate @replace", func() {
		expr, err := dbsp.CompileString(`{"@replace": ["hello world", "world", "universe"]}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("hello universe"))
	})

	It("should evaluate @split", func() {
		expr, err := dbsp.CompileString(`{"@split": ["a,b,c", ","]}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal([]any{"a", "b", "c"}))
	})

	It("should evaluate @join", func() {
		expr, err := dbsp.CompileString(`{"@join": [["a", "b", "c"], "-"]}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("a-b-c"))
	})

	It("should evaluate @startswith", func() {
		expr, err := dbsp.CompileString(`{"@startswith": ["hello world", "hello"]}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @endswith", func() {
		expr, err := dbsp.CompileString(`{"@endswith": ["hello world", "world"]}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})

	It("should evaluate @contains", func() {
		expr, err := dbsp.CompileString(`{"@contains": ["hello world", "lo wo"]}`)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(true))
	})
})

var _ = Describe("Complex Expressions", func() {
	It("should evaluate nested expressions", func() {
		// (1 + 2) * (3 + 4) = 21
		expr, err := dbsp.Compile([]byte(`{"@mul": [{"@add": [1, 2]}, {"@add": [3, 4]}]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(21)))
	})

	It("should preserve document shape with @dict", func() {
		expr, err := dbsp.Compile([]byte(`{"@dict": {"result": {"@add": [1, 2, 3]}, "name": "test"}}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(map[string]any{"result": int64(6), "name": "test"}))
	})

	It("should access document fields in expressions", func() {
		doc := NewTestDoc(map[string]any{
			"a": int64(10),
			"b": int64(20),
		})

		expr, err := dbsp.Compile([]byte(`{"@add": ["$.a", "$.b"]}`))
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(doc))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(30)))
	})
})

var _ = Describe("Registry", func() {
	It("should allow registering custom operators", func() {
		registry := dbsp.DefaultRegistry.Clone()

		// Register custom operator that doubles its input.
		registry.MustRegister("@double", func(args any) (dbsp.Expression, error) {
			operand, ok := args.(dbsp.Expression)
			if !ok {
				// Literal value: wrap it.
				return expression.Func(func(ctx *expression.EvalContext) (any, error) {
					i, err := dbsp.AsInt(args)
					if err != nil {
						return nil, err
					}
					return i * 2, nil
				}), nil
			}
			return expression.Func(func(ctx *expression.EvalContext) (any, error) {
				v, err := operand.Evaluate(ctx)
				if err != nil {
					return nil, err
				}
				i, err := dbsp.AsInt(v)
				if err != nil {
					return nil, err
				}
				return i * 2, nil
			}), nil
		})

		expr, err := dbsp.CompileWithRegistry([]byte(`{"@double": 21}`), registry)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(42)))
	})

	It("should allow overriding built-in operators", func() {
		registry := dbsp.DefaultRegistry.Clone()

		// Override @add to always return 999.
		err := registry.Override("@add", func(args any) (dbsp.Expression, error) {
			return expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return int64(999), nil
			}), nil
		})
		Expect(err).NotTo(HaveOccurred())

		expr, err := dbsp.CompileWithRegistry([]byte(`{"@add": [1, 2]}`), registry)
		Expect(err).NotTo(HaveOccurred())

		result, err := expr.Evaluate(expression.NewContext(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(999)))
	})
})
