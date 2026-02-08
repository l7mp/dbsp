package circuit_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/circuit"
	"github.com/l7mp/dbsp/expr"
	"github.com/l7mp/dbsp/operator"
	"github.com/l7mp/dbsp/zset"
)

func TestCircuit(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Circuit Suite")
}

var _ = Describe("Circuit", func() {
	Describe("Node", func() {
		It("creates input nodes", func() {
			n := circuit.Input("in")
			Expect(n.ID).To(Equal("in"))
			Expect(n.Kind).To(Equal(circuit.NodeInput))
			Expect(n.Kind.String()).To(Equal("Input"))
		})

		It("creates output nodes", func() {
			n := circuit.Output("out")
			Expect(n.ID).To(Equal("out"))
			Expect(n.Kind).To(Equal(circuit.NodeOutput))
		})

		It("creates operator nodes", func() {
			op := operator.NewNegate()
			n := circuit.Op("neg", op)
			Expect(n.ID).To(Equal("neg"))
			Expect(n.Kind).To(Equal(circuit.NodeOperator))
			Expect(n.Operator).To(Equal(op))
		})

		It("creates delay nodes", func() {
			n := circuit.Delay("z-1")
			Expect(n.ID).To(Equal("z-1"))
			Expect(n.Kind).To(Equal(circuit.NodeDelay))
			Expect(n.Kind.String()).To(Equal("Delay"))
		})

		It("creates integrate nodes", func() {
			n := circuit.Integrate("int")
			Expect(n.ID).To(Equal("int"))
			Expect(n.Kind).To(Equal(circuit.NodeIntegrate))
		})

		It("creates differentiate nodes", func() {
			n := circuit.Differentiate("diff")
			Expect(n.ID).To(Equal("diff"))
			Expect(n.Kind).To(Equal(circuit.NodeDifferentiate))
		})

		It("creates delta0 nodes", func() {
			n := circuit.Delta0("d0")
			Expect(n.ID).To(Equal("d0"))
			Expect(n.Kind).To(Equal(circuit.NodeDelta0))
		})
	})

	Describe("Edge", func() {
		It("creates edges with port", func() {
			e := circuit.NewEdge("a", "b", 0)
			Expect(e.From).To(Equal("a"))
			Expect(e.To).To(Equal("b"))
			Expect(e.Port).To(Equal(0))
		})
	})

	Describe("Circuit", func() {
		var c *circuit.Circuit

		BeforeEach(func() {
			c = circuit.New("test")
		})

		It("has a name", func() {
			Expect(c.Name()).To(Equal("test"))
		})

		It("adds nodes", func() {
			err := c.AddNode(circuit.Input("in"))
			Expect(err).NotTo(HaveOccurred())
			Expect(c.Node("in")).NotTo(BeNil())
		})

		It("rejects duplicate node IDs", func() {
			c.AddNode(circuit.Input("x"))
			err := c.AddNode(circuit.Output("x"))
			Expect(err).To(HaveOccurred())
		})

		It("adds edges", func() {
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Output("b"))

			err := c.AddEdge(circuit.NewEdge("a", "b", 0))
			Expect(err).NotTo(HaveOccurred())
			Expect(c.Edges()).To(HaveLen(1))
		})

		It("rejects edges with missing source", func() {
			c.AddNode(circuit.Output("b"))
			err := c.AddEdge(circuit.NewEdge("missing", "b", 0))
			Expect(err).To(HaveOccurred())
		})

		It("rejects edges with missing target", func() {
			c.AddNode(circuit.Input("a"))
			err := c.AddEdge(circuit.NewEdge("a", "missing", 0))
			Expect(err).To(HaveOccurred())
		})

		It("returns all nodes", func() {
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Output("b"))
			Expect(c.Nodes()).To(HaveLen(2))
		})

		It("returns edges to a node", func() {
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Input("b"))
			c.AddNode(circuit.Output("c"))
			c.AddEdge(circuit.NewEdge("a", "c", 0))
			c.AddEdge(circuit.NewEdge("b", "c", 1))

			edges := c.EdgesTo("c")
			Expect(edges).To(HaveLen(2))
		})

		It("returns edges from a node", func() {
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Output("b"))
			c.AddNode(circuit.Output("c"))
			c.AddEdge(circuit.NewEdge("a", "b", 0))
			c.AddEdge(circuit.NewEdge("a", "c", 0))

			edges := c.EdgesFrom("a")
			Expect(edges).To(HaveLen(2))
		})

		It("returns input nodes", func() {
			c.AddNode(circuit.Input("in1"))
			c.AddNode(circuit.Input("in2"))
			c.AddNode(circuit.Output("out"))

			inputs := c.Inputs()
			Expect(inputs).To(HaveLen(2))
		})

		It("returns output nodes", func() {
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Output("out1"))
			c.AddNode(circuit.Output("out2"))

			outputs := c.Outputs()
			Expect(outputs).To(HaveLen(2))
		})

		It("clones the circuit", func() {
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "b", 0))

			clone := c.Clone()
			Expect(clone.Name()).To(Equal("test"))
			Expect(clone.Nodes()).To(HaveLen(2))
			Expect(clone.Edges()).To(HaveLen(1))

			// Verify independence.
			clone.AddNode(circuit.Input("c"))
			Expect(c.Nodes()).To(HaveLen(2))
			Expect(clone.Nodes()).To(HaveLen(3))
		})
	})

	Describe("Validation", func() {
		It("accepts circuits without cycles", func() {
			c := circuit.New("acyclic")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "b", 0))

			errs := c.Validate()
			Expect(errs).To(BeEmpty())
		})

		It("accepts cycles with delay", func() {
			c := circuit.New("delayed-cycle")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("op", operator.NewPlus()))
			c.AddNode(circuit.Delay("z-1"))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("in", "op", 0))
			c.AddEdge(circuit.NewEdge("z-1", "op", 1))
			c.AddEdge(circuit.NewEdge("op", "z-1", 0))
			c.AddEdge(circuit.NewEdge("op", "out", 0))

			errs := c.Validate()
			Expect(errs).To(BeEmpty())
		})

		It("rejects cycles without delay", func() {
			c := circuit.New("no-delay-cycle")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("b", operator.NewNegate()))
			c.AddNode(circuit.Op("c", operator.NewNegate()))

			c.AddEdge(circuit.NewEdge("a", "b", 0))
			c.AddEdge(circuit.NewEdge("b", "c", 0))
			c.AddEdge(circuit.NewEdge("c", "b", 0)) // Creates cycle b -> c -> b.

			errs := c.Validate()
			Expect(errs).To(HaveLen(1))
		})
	})

	Describe("FindSCCs", func() {
		It("finds strongly connected components", func() {
			c := circuit.New("scc-test")
			c.AddNode(circuit.Op("a", operator.NewNegate()))
			c.AddNode(circuit.Op("b", operator.NewNegate()))
			c.AddNode(circuit.Op("c", operator.NewNegate()))

			c.AddEdge(circuit.NewEdge("a", "b", 0))
			c.AddEdge(circuit.NewEdge("b", "c", 0))
			c.AddEdge(circuit.NewEdge("c", "a", 0))

			sccs := c.FindSCCs()
			// All three nodes are in one SCC.
			var found bool
			for _, scc := range sccs {
				if len(scc) == 3 {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue())
		})
	})

	Describe("Patterns", func() {
		Describe("Join", func() {
			It("creates a join circuit", func() {
				predicate := expr.Func(func(e zset.Document) (any, error) {
					return true, nil
				})
				c := circuit.Join("test-join", predicate)

				Expect(c.Name()).To(Equal("test-join"))
				Expect(c.Inputs()).To(HaveLen(2))
				Expect(c.Outputs()).To(HaveLen(1))

				// Verify structure: left/right -> product -> select -> out.
				Expect(c.Node("left")).NotTo(BeNil())
				Expect(c.Node("right")).NotTo(BeNil())
				Expect(c.Node("product")).NotTo(BeNil())
				Expect(c.Node("select")).NotTo(BeNil())
				Expect(c.Node("out")).NotTo(BeNil())
			})
		})

		Describe("BilinearIncremental", func() {
			It("creates the three-term pattern", func() {
				op := operator.NewCartesianProduct("×")
				c := circuit.BilinearIncremental("bilinear-incr", op)

				Expect(c.Name()).To(Equal("bilinear-incr"))
				Expect(c.Inputs()).To(HaveLen(2))
				Expect(c.Outputs()).To(HaveLen(1))

				// Verify integrators.
				Expect(c.Node("int_a").Kind).To(Equal(circuit.NodeIntegrate))
				Expect(c.Node("int_b").Kind).To(Equal(circuit.NodeIntegrate))

				// Verify three terms.
				Expect(c.Node("term1")).NotTo(BeNil())
				Expect(c.Node("term2")).NotTo(BeNil())
				Expect(c.Node("term3")).NotTo(BeNil())

				// Verify sums.
				Expect(c.Node("sum12")).NotTo(BeNil())
				Expect(c.Node("sum_all")).NotTo(BeNil())
			})
		})

		Describe("NonLinearIncremental", func() {
			It("creates the D ∘ O ∘ ∫ pattern", func() {
				op := operator.NewDistinct("H")
				c := circuit.NonLinearIncremental("nonlinear-incr", op)

				Expect(c.Name()).To(Equal("nonlinear-incr"))
				Expect(c.Inputs()).To(HaveLen(1))
				Expect(c.Outputs()).To(HaveLen(1))

				// Verify structure: delta -> int -> op -> diff -> out.
				Expect(c.Node("delta").Kind).To(Equal(circuit.NodeInput))
				Expect(c.Node("int").Kind).To(Equal(circuit.NodeIntegrate))
				Expect(c.Node("op").Kind).To(Equal(circuit.NodeOperator))
				Expect(c.Node("diff").Kind).To(Equal(circuit.NodeDifferentiate))
				Expect(c.Node("out").Kind).To(Equal(circuit.NodeOutput))
			})
		})
	})
})
