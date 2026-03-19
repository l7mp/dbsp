package circuit

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/dbsp/expression"
	"github.com/l7mp/dbsp/dbsp/operator"
)

func TestCircuit(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Circuit Suite")
}

var _ = Describe("Circuit", func() {
	Describe("Node", func() {
		It("creates input nodes", func() {
			n := Input("in")
			Expect(n.ID).To(Equal("in"))
			Expect(n.Kind()).To(Equal(operator.KindInput))
		})

		It("creates output nodes", func() {
			n := Output("out")
			Expect(n.ID).To(Equal("out"))
			Expect(n.Kind()).To(Equal(operator.KindOutput))
		})

		It("creates operator nodes", func() {
			op := operator.NewNegate()
			n := Op("neg", op)
			Expect(n.ID).To(Equal("neg"))
			Expect(n.Kind()).To(Equal(operator.KindNegate))
			Expect(n.Operator).To(Equal(op))
		})

		It("creates delay nodes", func() {
			n := Delay("z-1")
			Expect(n.ID).To(Equal("z-1"))
			Expect(n.Kind()).To(Equal(operator.KindDelay))
		})

		It("creates integrate nodes", func() {
			n := Integrate("int")
			Expect(n.ID).To(Equal("int"))
			Expect(n.Kind()).To(Equal(operator.KindIntegrate))
		})

		It("creates differentiate nodes", func() {
			n := Differentiate("diff")
			Expect(n.ID).To(Equal("diff"))
			Expect(n.Kind()).To(Equal(operator.KindDifferentiate))
		})

		It("creates delta0 nodes", func() {
			n := Delta0("d0")
			Expect(n.ID).To(Equal("d0"))
			Expect(n.Kind()).To(Equal(operator.KindDelta0))
		})
	})

	Describe("Edge", func() {
		It("creates edges with port", func() {
			e := NewEdge("a", "b", 0)
			Expect(e.From).To(Equal("a"))
			Expect(e.To).To(Equal("b"))
			Expect(e.Port).To(Equal(0))
		})
	})

	Describe("Circuit", func() {
		var c *Circuit

		BeforeEach(func() {
			c = New("test")
		})

		It("has a name", func() {
			Expect(c.Name()).To(Equal("test"))
		})

		It("adds nodes", func() {
			err := c.AddNode(Input("in"))
			Expect(err).NotTo(HaveOccurred())
			Expect(c.Node("in")).NotTo(BeNil())
		})

		It("rejects duplicate node IDs", func() {
			c.AddNode(Input("x"))
			err := c.AddNode(Output("x"))
			Expect(err).To(HaveOccurred())
		})

		It("adds edges", func() {
			c.AddNode(Input("a"))
			c.AddNode(Output("b"))

			err := c.AddEdge(NewEdge("a", "b", 0))
			Expect(err).NotTo(HaveOccurred())
			Expect(c.Edges()).To(HaveLen(1))
		})

		It("rejects edges with missing source", func() {
			c.AddNode(Output("b"))
			err := c.AddEdge(NewEdge("missing", "b", 0))
			Expect(err).To(HaveOccurred())
		})

		It("rejects edges with missing target", func() {
			c.AddNode(Input("a"))
			err := c.AddEdge(NewEdge("a", "missing", 0))
			Expect(err).To(HaveOccurred())
		})

		It("returns all nodes", func() {
			c.AddNode(Input("a"))
			c.AddNode(Output("b"))
			Expect(c.Nodes()).To(HaveLen(2))
		})

		It("returns edges to a node", func() {
			c.AddNode(Input("a"))
			c.AddNode(Input("b"))
			c.AddNode(Output("c"))
			c.AddEdge(NewEdge("a", "c", 0))
			c.AddEdge(NewEdge("b", "c", 1))

			edges := c.EdgesTo("c")
			Expect(edges).To(HaveLen(2))
		})

		It("returns edges from a node", func() {
			c.AddNode(Input("a"))
			c.AddNode(Output("b"))
			c.AddNode(Output("c"))
			c.AddEdge(NewEdge("a", "b", 0))
			c.AddEdge(NewEdge("a", "c", 0))

			edges := c.EdgesFrom("a")
			Expect(edges).To(HaveLen(2))
		})

		It("returns input nodes", func() {
			c.AddNode(Input("in1"))
			c.AddNode(Input("in2"))
			c.AddNode(Output("out"))

			inputs := c.Inputs()
			Expect(inputs).To(HaveLen(2))
		})

		It("returns output nodes", func() {
			c.AddNode(Input("in"))
			c.AddNode(Output("out1"))
			c.AddNode(Output("out2"))

			outputs := c.Outputs()
			Expect(outputs).To(HaveLen(2))
		})

		It("clones the circuit", func() {
			c.AddNode(Input("a"))
			c.AddNode(Output("b"))
			c.AddEdge(NewEdge("a", "b", 0))

			clone := c.Clone()
			Expect(clone.Name()).To(Equal("test"))
			Expect(clone.Nodes()).To(HaveLen(2))
			Expect(clone.Edges()).To(HaveLen(1))

			// Verify independence.
			clone.AddNode(Input("c"))
			Expect(c.Nodes()).To(HaveLen(2))
			Expect(clone.Nodes()).To(HaveLen(3))
		})
	})

	Describe("Validation", func() {
		It("accepts circuits without cycles", func() {
			c := New("acyclic")
			c.AddNode(Input("a"))
			c.AddNode(Output("b"))
			c.AddEdge(NewEdge("a", "b", 0))

			errs := c.Validate()
			Expect(errs).To(BeEmpty())
		})

		It("accepts cycles with delay", func() {
			c := New("delayed-cycle")
			c.AddNode(Input("in"))
			c.AddNode(Op("op", operator.NewPlus()))
			c.AddNode(Delay("z-1"))
			c.AddNode(Output("out"))

			c.AddEdge(NewEdge("in", "op", 0))
			c.AddEdge(NewEdge("z-1", "op", 1))
			c.AddEdge(NewEdge("op", "z-1", 0))
			c.AddEdge(NewEdge("op", "out", 0))

			errs := c.Validate()
			Expect(errs).To(BeEmpty())
		})

		It("rejects cycles without delay", func() {
			c := New("no-delay-cycle")
			c.AddNode(Input("a"))
			c.AddNode(Op("b", operator.NewNegate()))
			c.AddNode(Op("c", operator.NewNegate()))

			c.AddEdge(NewEdge("a", "b", 0))
			c.AddEdge(NewEdge("b", "c", 0))
			c.AddEdge(NewEdge("c", "b", 0)) // Creates cycle b -> c -> b.

			errs := c.Validate()
			Expect(errs).To(HaveLen(1))
		})
	})

	Describe("FindSCCs", func() {
		It("finds strongly connected components", func() {
			c := New("scc-test")
			c.AddNode(Op("a", operator.NewNegate()))
			c.AddNode(Op("b", operator.NewNegate()))
			c.AddNode(Op("c", operator.NewNegate()))

			c.AddEdge(NewEdge("a", "b", 0))
			c.AddEdge(NewEdge("b", "c", 0))
			c.AddEdge(NewEdge("c", "a", 0))

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

	Describe("Mutation", func() {
		Describe("RemoveNode", func() {
			It("removes a node and its edges", func() {
				c := New("rm-test")
				c.AddNode(Input("a"))
				c.AddNode(Op("b", operator.NewNegate()))
				c.AddNode(Output("c"))
				c.AddEdge(NewEdge("a", "b", 0))
				c.AddEdge(NewEdge("b", "c", 0))

				err := c.RemoveNode("b")
				Expect(err).NotTo(HaveOccurred())
				Expect(c.Node("b")).To(BeNil())
				Expect(c.Nodes()).To(HaveLen(2))
				Expect(c.Edges()).To(BeEmpty())
			})

			It("returns error for non-existent node", func() {
				c := New("rm-err")
				err := c.RemoveNode("missing")
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("RemoveEdge", func() {
			It("removes a specific edge", func() {
				c := New("rmedge-test")
				c.AddNode(Input("a"))
				c.AddNode(Op("b", operator.NewNegate()))
				c.AddNode(Output("c"))
				c.AddEdge(NewEdge("a", "b", 0))
				c.AddEdge(NewEdge("b", "c", 0))

				err := c.RemoveEdge("a", "b", 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(c.Edges()).To(HaveLen(1))
				Expect(c.EdgesTo("b")).To(BeEmpty())
			})

			It("returns error for non-existent edge", func() {
				c := New("rmedge-err")
				c.AddNode(Input("a"))
				c.AddNode(Output("b"))
				err := c.RemoveEdge("a", "b", 0)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("BypassNode", func() {
			It("bypasses a node with one input and one output", func() {
				c := New("bypass-test")
				c.AddNode(Input("a"))
				c.AddNode(Integrate("int"))
				c.AddNode(Output("b"))
				c.AddEdge(NewEdge("a", "int", 0))
				c.AddEdge(NewEdge("int", "b", 0))

				err := c.BypassNode("int")
				Expect(err).NotTo(HaveOccurred())
				Expect(c.Node("int")).To(BeNil())
				Expect(c.Nodes()).To(HaveLen(2))
				edges := c.EdgesTo("b")
				Expect(edges).To(HaveLen(1))
				Expect(edges[0].From).To(Equal("a"))
			})

			It("bypasses a node with one input and multiple outputs", func() {
				c := New("bypass-multi")
				c.AddNode(Input("a"))
				c.AddNode(Integrate("int"))
				c.AddNode(Output("b"))
				c.AddNode(Output("d"))
				c.AddEdge(NewEdge("a", "int", 0))
				c.AddEdge(NewEdge("int", "b", 0))
				c.AddEdge(NewEdge("int", "d", 0))

				err := c.BypassNode("int")
				Expect(err).NotTo(HaveOccurred())
				Expect(c.EdgesTo("b")).To(HaveLen(1))
				Expect(c.EdgesTo("b")[0].From).To(Equal("a"))
				Expect(c.EdgesTo("d")).To(HaveLen(1))
				Expect(c.EdgesTo("d")[0].From).To(Equal("a"))
			})

			It("preserves port numbers on outgoing edges", func() {
				c := New("bypass-port")
				c.AddNode(Input("in"))
				c.AddNode(Integrate("int"))
				c.AddNode(Op("plus", operator.NewPlus()))
				c.AddEdge(NewEdge("in", "int", 0))
				c.AddEdge(NewEdge("int", "plus", 1))

				err := c.BypassNode("int")
				Expect(err).NotTo(HaveOccurred())
				edges := c.EdgesTo("plus")
				Expect(edges).To(HaveLen(1))
				Expect(edges[0].From).To(Equal("in"))
				Expect(edges[0].Port).To(Equal(1))
			})

			It("returns error for node with multiple different inputs", func() {
				c := New("bypass-multi-in")
				c.AddNode(Input("x"))
				c.AddNode(Input("y"))
				c.AddNode(Op("plus", operator.NewPlus()))
				c.AddEdge(NewEdge("x", "plus", 0))
				c.AddEdge(NewEdge("y", "plus", 1))

				err := c.BypassNode("plus")
				Expect(err).To(HaveOccurred())
			})

			It("handles duplicate edges from same source", func() {
				c := New("bypass-dup")
				c.AddNode(Input("a"))
				c.AddNode(Integrate("int"))
				c.AddNode(Output("b"))
				c.AddEdge(NewEdge("a", "int", 0))
				c.AddEdge(NewEdge("a", "int", 0)) // Duplicate edge.
				c.AddEdge(NewEdge("int", "b", 0))

				err := c.BypassNode("int")
				Expect(err).NotTo(HaveOccurred())
				Expect(c.Node("int")).To(BeNil())
				edges := c.EdgesTo("b")
				Expect(edges).To(HaveLen(1))
				Expect(edges[0].From).To(Equal("a"))
			})

			It("returns error for non-existent node", func() {
				c := New("bypass-err")
				err := c.BypassNode("missing")
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("Patterns", func() {
		Describe("Join", func() {
			It("creates a join circuit", func() {
				predicate := expression.Func(func(ctx *expression.EvalContext) (any, error) {
					return true, nil
				})
				c := Join("test-join", predicate)

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
				op := operator.NewCartesianProduct()
				c := BilinearIncremental("bilinear-incr", op)

				Expect(c.Name()).To(Equal("bilinear-incr"))
				Expect(c.Inputs()).To(HaveLen(2))
				Expect(c.Outputs()).To(HaveLen(1))

				// Verify integrators.
				Expect(c.Node("int_a").Kind()).To(Equal(operator.KindIntegrate))
				Expect(c.Node("int_b").Kind()).To(Equal(operator.KindIntegrate))

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
				op := operator.NewDistinct()
				c := NonLinearIncremental("nonlinear-incr", op)

				Expect(c.Name()).To(Equal("nonlinear-incr"))
				Expect(c.Inputs()).To(HaveLen(1))
				Expect(c.Outputs()).To(HaveLen(1))

				// Verify structure: delta -> int -> op -> diff -> out.
				Expect(c.Node("delta").Kind()).To(Equal(operator.KindInput))
				Expect(c.Node("int").Kind()).To(Equal(operator.KindIntegrate))
				Expect(c.Node("op").Kind()).To(Equal(operator.KindDistinct))
				Expect(c.Node("diff").Kind()).To(Equal(operator.KindDifferentiate))
				Expect(c.Node("out").Kind()).To(Equal(operator.KindOutput))
			})
		})
	})
})
