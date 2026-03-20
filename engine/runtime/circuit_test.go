package runtime_test

import (
	"errors"

	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/compiler/aggregation"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Circuit", func() {
	It("executes incremental mode on delta input", func() {
		q := mustCompileCircuitQuery()
		c, err := runtime.NewCircuit(runtime.CircuitConfig{
			Circuit:     q.Circuit,
			InputMap:    q.InputMap,
			OutputMap:   q.OutputMap,
			Incremental: true,
		})
		Expect(err).NotTo(HaveOccurred())

		delta := zset.New()
		doc := unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil)
		delta.Insert(doc, 1)

		outs, err := c.Execute(runtime.Input{Name: "Pod", Data: delta})
		Expect(err).NotTo(HaveOccurred())
		Expect(outs).To(HaveLen(1))
		Expect(outs[0].Name).To(Equal("output"))
		Expect(outs[0].Data.Equal(delta)).To(BeTrue())
	})

	It("executes snapshot mode on state input", func() {
		q := mustCompileCircuitQuery()
		c, err := runtime.NewCircuit(runtime.CircuitConfig{
			Circuit:     q.Circuit,
			InputMap:    q.InputMap,
			OutputMap:   q.OutputMap,
			Incremental: false,
		})
		Expect(err).NotTo(HaveOccurred())

		s1 := zset.New()
		s1.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)

		outs, err := c.Execute(runtime.Input{Name: "Pod", Data: s1})
		Expect(err).NotTo(HaveOccurred())
		Expect(outs).To(HaveLen(1))
		Expect(outs[0].Data.Equal(s1)).To(BeTrue())

		s2 := zset.New()
		s2.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-b"}}, nil), 1)

		outs, err = c.Execute(runtime.Input{Name: "Pod", Data: s2})
		Expect(err).NotTo(HaveOccurred())
		Expect(outs).To(HaveLen(1))
		Expect(outs[0].Data.Equal(s2)).To(BeTrue())
	})

	It("rejects unknown input names", func() {
		q := mustCompileCircuitQuery()
		c, err := runtime.NewCircuit(runtime.CircuitConfig{
			Circuit:     q.Circuit,
			InputMap:    q.InputMap,
			OutputMap:   q.OutputMap,
			Incremental: true,
		})
		Expect(err).NotTo(HaveOccurred())

		_, err = c.Execute(runtime.Input{Name: "Deployment", Data: zset.New()})
		Expect(errors.Is(err, runtime.ErrUnknownInput)).To(BeTrue())
	})
})

func mustCompileCircuitQuery() *compiler.Query {
	c := aggregation.New([]string{"Pod"}, []string{"output"})
	q, err := c.CompileString(`[{"@project":{"$.":"$."}}]`)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return q
}
