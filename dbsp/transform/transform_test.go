package transform

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/operator"
	"github.com/l7mp/dbsp/expression"
)

func TestTransform(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Transform Suite")
}

var _ = Describe("Incrementalize", func() {
	Describe("Linear operators", func() {
		It("passes through unchanged", func() {
			// in -> select -> out.
			c := circuit.New("linear-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("sel", operator.NewSelect("σ", expression.Func(func(e datamodel.Document) (any, error) {
				return true, nil
			}))))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "out", 0))

			incr, err := Incrementalize(c)
			Expect(err).NotTo(HaveOccurred())
			Expect(incr.Name()).To(Equal("linear-test^Δ"))

			// Structure should be same: in -> sel -> out.
			Expect(incr.Node("in").Kind).To(Equal(circuit.NodeInput))
			Expect(incr.Node("sel").Kind).To(Equal(circuit.NodeOperator))
			Expect(incr.Node("out").Kind).To(Equal(circuit.NodeOutput))

			edges := incr.EdgesTo("sel")
			Expect(edges).To(HaveLen(1))
			Expect(edges[0].From).To(Equal("in"))
		})
	})

	Describe("Bilinear operators", func() {
		It("expands to three terms with integrators", func() {
			// left,right -> product -> out.
			c := circuit.New("bilinear-test")
			c.AddNode(circuit.Input("left"))
			c.AddNode(circuit.Input("right"))
			c.AddNode(circuit.Op("prod", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("left", "prod", 0))
			c.AddEdge(circuit.NewEdge("right", "prod", 1))
			c.AddEdge(circuit.NewEdge("prod", "out", 0))

			incr, err := Incrementalize(c)
			Expect(err).NotTo(HaveOccurred())

			// Should have integrators.
			Expect(incr.Node("prod_int_left").Kind).To(Equal(circuit.NodeIntegrate))
			Expect(incr.Node("prod_int_right").Kind).To(Equal(circuit.NodeIntegrate))

			// Should have three terms.
			Expect(incr.Node("prod_t1").Kind).To(Equal(circuit.NodeOperator))
			Expect(incr.Node("prod_t2").Kind).To(Equal(circuit.NodeOperator))
			Expect(incr.Node("prod_t3").Kind).To(Equal(circuit.NodeOperator))

			// Should have sums.
			Expect(incr.Node("prod_sum12").Kind).To(Equal(circuit.NodeOperator))
			Expect(incr.Node("prod_sum").Kind).To(Equal(circuit.NodeOperator))

			// Output should connect from sum.
			edges := incr.EdgesTo("out")
			Expect(edges).To(HaveLen(1))
			Expect(edges[0].From).To(Equal("prod_sum"))
		})
	})

	Describe("Non-linear operators", func() {
		It("wraps with integrate and differentiate", func() {
			// in -> distinct -> out.
			c := circuit.New("nonlinear-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("dist", operator.NewDistinct("H")))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "dist", 0))
			c.AddEdge(circuit.NewEdge("dist", "out", 0))

			incr, err := Incrementalize(c)
			Expect(err).NotTo(HaveOccurred())

			// Should have integrate -> op -> differentiate.
			Expect(incr.Node("dist_int").Kind).To(Equal(circuit.NodeIntegrate))
			Expect(incr.Node("dist_op").Kind).To(Equal(circuit.NodeOperator))
			Expect(incr.Node("dist_diff").Kind).To(Equal(circuit.NodeDifferentiate))

			// Verify chain: in -> int -> op -> diff -> out.
			intEdges := incr.EdgesTo("dist_int")
			Expect(intEdges).To(HaveLen(1))
			Expect(intEdges[0].From).To(Equal("in"))

			opEdges := incr.EdgesTo("dist_op")
			Expect(opEdges).To(HaveLen(1))
			Expect(opEdges[0].From).To(Equal("dist_int"))

			diffEdges := incr.EdgesTo("dist_diff")
			Expect(diffEdges).To(HaveLen(1))
			Expect(diffEdges[0].From).To(Equal("dist_op"))

			outEdges := incr.EdgesTo("out")
			Expect(outEdges).To(HaveLen(1))
			Expect(outEdges[0].From).To(Equal("dist_diff"))
		})
	})

	Describe("Primitive nodes", func() {
		It("passes delay through unchanged", func() {
			c := circuit.New("delay-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Delay("z-1"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "z-1", 0))
			c.AddEdge(circuit.NewEdge("z-1", "out", 0))

			incr, err := Incrementalize(c)
			Expect(err).NotTo(HaveOccurred())

			Expect(incr.Node("z-1").Kind).To(Equal(circuit.NodeDelay))
		})

		It("bypasses integrate nodes", func() {
			// in -> integrate -> out should become in -> out.
			c := circuit.New("integrate-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "out", 0))

			incr, err := Incrementalize(c)
			Expect(err).NotTo(HaveOccurred())

			// Integrate node should be bypassed.
			Expect(incr.Node("int")).To(BeNil())

			// in should connect directly to out.
			edges := incr.EdgesTo("out")
			Expect(edges).To(HaveLen(1))
			Expect(edges[0].From).To(Equal("in"))
		})

		It("bypasses differentiate nodes", func() {
			// in -> differentiate -> out should become in -> out.
			c := circuit.New("diff-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "out", 0))

			incr, err := Incrementalize(c)
			Expect(err).NotTo(HaveOccurred())

			// Differentiate node should be bypassed.
			Expect(incr.Node("diff")).To(BeNil())

			// in should connect directly to out.
			edges := incr.EdgesTo("out")
			Expect(edges).To(HaveLen(1))
			Expect(edges[0].From).To(Equal("in"))
		})

		It("preserves delta0 nodes", func() {
			c := circuit.New("delta0-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Delta0("d0"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "d0", 0))
			c.AddEdge(circuit.NewEdge("d0", "out", 0))

			incr, err := Incrementalize(c)
			Expect(err).NotTo(HaveOccurred())

			Expect(incr.Node("d0").Kind).To(Equal(circuit.NodeDelta0))
		})
	})

	Describe("Complex circuits", func() {
		It("incrementalizes Join (bilinear + linear composition)", func() {
			predicate := expression.Func(func(e datamodel.Document) (any, error) {
				return true, nil
			})
			c := circuit.Join("join-test", predicate)

			incr, err := Incrementalize(c)
			Expect(err).NotTo(HaveOccurred())

			// Should have original inputs and output.
			Expect(incr.Inputs()).To(HaveLen(2))
			Expect(incr.Outputs()).To(HaveLen(1))

			// Product should be expanded to three terms.
			Expect(incr.Node("product_t1")).NotTo(BeNil())
			Expect(incr.Node("product_t2")).NotTo(BeNil())
			Expect(incr.Node("product_t3")).NotTo(BeNil())

			// Select should pass through unchanged.
			Expect(incr.Node("select")).NotTo(BeNil())
		})
	})
})
