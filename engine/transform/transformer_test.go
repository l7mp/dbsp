package transform

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/operator"
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
			c.AddNode(circuit.Op("sel", operator.NewSelect(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			}))))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "out", 0))

			incr, err := NewIncrementalizer().Transform(c)
			Expect(err).NotTo(HaveOccurred())
			Expect(incr.Name()).To(Equal("linear-test^Δ"))

			// Structure should be same: in -> sel -> out.
			Expect(incr.Node("in").Kind()).To(Equal(operator.KindInput))
			Expect(incr.Node("sel^Δ").Kind()).To(Equal(operator.KindSelect))
			Expect(incr.Node("out").Kind()).To(Equal(operator.KindOutput))

			edges := incr.EdgesTo("sel^Δ")
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
			c.AddNode(circuit.Op("prod", operator.NewCartesianProduct()))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("left", "prod", 0))
			c.AddEdge(circuit.NewEdge("right", "prod", 1))
			c.AddEdge(circuit.NewEdge("prod", "out", 0))

			incr, err := NewIncrementalizer().Transform(c)
			Expect(err).NotTo(HaveOccurred())

			// Should have integrators.
			Expect(incr.Node("prod^Δ_int_left").Kind()).To(Equal(operator.KindIntegrate))
			Expect(incr.Node("prod^Δ_int_right").Kind()).To(Equal(operator.KindIntegrate))

			// Should have three terms.
			Expect(incr.Node("prod^Δ_t1").Kind()).To(Equal(operator.KindCartesian))
			Expect(incr.Node("prod^Δ_t2").Kind()).To(Equal(operator.KindCartesian))
			Expect(incr.Node("prod^Δ_t3").Kind()).To(Equal(operator.KindCartesian))

			// Should have sums.
			Expect(incr.Node("prod^Δ_sum12").Kind()).To(Equal(operator.KindLinearCombination))
			Expect(incr.Node("prod^Δ_sum").Kind()).To(Equal(operator.KindLinearCombination))

			// Output should connect from sum.
			edges := incr.EdgesTo("out")
			Expect(edges).To(HaveLen(1))
			Expect(edges[0].From).To(Equal("prod^Δ_sum"))
		})
	})

	Describe("Non-linear operators", func() {
		It("wraps with integrate and differentiate", func() {
			// in -> non-linear op -> out.
			c := circuit.New("nonlinear-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("nlin", newTestNonLinearOp()))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "nlin", 0))
			c.AddEdge(circuit.NewEdge("nlin", "out", 0))

			incr, err := NewIncrementalizer().Transform(c)
			Expect(err).NotTo(HaveOccurred())

			// Should have integrate -> op -> differentiate.
			Expect(incr.Node("nlin^Δ_int").Kind()).To(Equal(operator.KindIntegrate))
			Expect(incr.Node("nlin^Δ_op").Operator.Linearity()).To(Equal(operator.NonLinear))
			Expect(incr.Node("nlin^Δ_diff").Kind()).To(Equal(operator.KindDifferentiate))

			// Verify chain: in -> int -> op -> diff -> out.
			intEdges := incr.EdgesTo("nlin^Δ_int")
			Expect(intEdges).To(HaveLen(1))
			Expect(intEdges[0].From).To(Equal("in"))

			opEdges := incr.EdgesTo("nlin^Δ_op")
			Expect(opEdges).To(HaveLen(1))
			Expect(opEdges[0].From).To(Equal("nlin^Δ_int"))

			diffEdges := incr.EdgesTo("nlin^Δ_diff")
			Expect(diffEdges).To(HaveLen(1))
			Expect(diffEdges[0].From).To(Equal("nlin^Δ_op"))

			outEdges := incr.EdgesTo("out")
			Expect(outEdges).To(HaveLen(1))
			Expect(outEdges[0].From).To(Equal("nlin^Δ_diff"))
		})

		It("substitutes group_by with incremental operator", func() {
			// in -> group_by -> out.
			c := circuit.New("distinct-pi-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("dpi", operator.NewGroupBy(nil, expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return ctx.Subject(), nil
			}))))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "dpi", 0))
			c.AddEdge(circuit.NewEdge("dpi", "out", 0))

			incr, err := NewIncrementalizer().Transform(c)
			Expect(err).NotTo(HaveOccurred())

			// Should NOT use generic D∘O∘I pattern — no external int/delay/op/diff.
			Expect(incr.Node("dpi^Δ_int")).To(BeNil())
			Expect(incr.Node("dpi^Δ_delay")).To(BeNil())
			Expect(incr.Node("dpi^Δ_op")).To(BeNil())
			Expect(incr.Node("dpi^Δ_diff")).To(BeNil())

			// Should have a single incremental group_by operator node.
			Expect(incr.Node("dpi^Δ").Kind()).To(Equal(operator.KindGroupByIncremental))

			// group_by receives one input: delta (port 0).
			aEdges := incr.EdgesTo("dpi^Δ")
			Expect(aEdges).To(HaveLen(1))
			Expect(aEdges[0].From).To(Equal("in"))
			Expect(aEdges[0].Port).To(Equal(0))

			// Output connects from group_by.
			outEdges := incr.EdgesTo("out")
			Expect(outEdges).To(HaveLen(1))
			Expect(outEdges[0].From).To(Equal("dpi^Δ"))
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

			incr, err := NewIncrementalizer().Transform(c)
			Expect(err).NotTo(HaveOccurred())

			Expect(incr.Node("z-1^Δ").Kind()).To(Equal(operator.KindDelay))
		})

		It("bypasses integrate nodes", func() {
			// in -> integrate -> out should become in -> out.
			c := circuit.New("integrate-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "out", 0))

			incr, err := NewIncrementalizer().Transform(c)
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

			incr, err := NewIncrementalizer().Transform(c)
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

			incr, err := NewIncrementalizer().Transform(c)
			Expect(err).NotTo(HaveOccurred())

			Expect(incr.Node("d0^Δ").Kind()).To(Equal(operator.KindDelta0))
		})
	})

	Describe("Complex circuits", func() {
		It("incrementalizes Join (bilinear + linear composition)", func() {
			predicate := expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			})
			c := circuit.Join("join-test", predicate)

			incr, err := NewIncrementalizer().Transform(c)
			Expect(err).NotTo(HaveOccurred())

			// Should have original inputs and output.
			Expect(incr.Inputs()).To(HaveLen(2))
			Expect(incr.Outputs()).To(HaveLen(1))

			// Product should be expanded to three terms.
			Expect(incr.Node("product^Δ_t1")).NotTo(BeNil())
			Expect(incr.Node("product^Δ_t2")).NotTo(BeNil())
			Expect(incr.Node("product^Δ_t3")).NotTo(BeNil())

			// Select should pass through unchanged.
			Expect(incr.Node("select^Δ")).NotTo(BeNil())
		})
	})
})

var _ = Describe("InputIntegrators", func() {
	It("inserts an Integrate node after each input", func() {
		c := circuit.New("input-integrators-test")
		c.AddNode(circuit.Input("pods"))
		c.AddNode(circuit.Input("services"))
		c.AddNode(circuit.Op("join", operator.NewCartesianProduct()))
		c.AddNode(circuit.Output("out"))
		c.AddEdge(circuit.NewEdge("pods", "join", 0))
		c.AddEdge(circuit.NewEdge("services", "join", 1))
		c.AddEdge(circuit.NewEdge("join", "out", 0))

		adapted, err := NewInputIntegrators().Transform(c)
		Expect(err).NotTo(HaveOccurred())

		Expect(adapted.Node("pods_int")).NotTo(BeNil())
		Expect(adapted.Node("pods_int").Kind()).To(Equal(operator.KindIntegrate))
		Expect(adapted.Node("services_int")).NotTo(BeNil())
		Expect(adapted.Node("services_int").Kind()).To(Equal(operator.KindIntegrate))

		joinIn := adapted.EdgesTo("join")
		Expect(joinIn).To(HaveLen(2))
		from := map[string]bool{}
		for _, e := range joinIn {
			from[e.From] = true
		}
		Expect(from["pods_int"]).To(BeTrue())
		Expect(from["services_int"]).To(BeTrue())
		Expect(from["pods"]).To(BeFalse())
		Expect(from["services"]).To(BeFalse())
	})

	It("is eliminated by incrementalization", func() {
		c := circuit.New("input-integrators-cancel")
		c.AddNode(circuit.Input("left"))
		c.AddNode(circuit.Input("right"))
		c.AddNode(circuit.Op("prod", operator.NewCartesianProduct()))
		c.AddNode(circuit.Output("out"))
		c.AddEdge(circuit.NewEdge("left", "prod", 0))
		c.AddEdge(circuit.NewEdge("right", "prod", 1))
		c.AddEdge(circuit.NewEdge("prod", "out", 0))

		withI, err := NewInputIntegrators().Transform(c)
		Expect(err).NotTo(HaveOccurred())

		incrWithI, err := NewIncrementalizer().Transform(withI)
		Expect(err).NotTo(HaveOccurred())

		Expect(incrWithI.Node("left_int")).To(BeNil())
		Expect(incrWithI.Node("right_int")).To(BeNil())
		Expect(incrWithI.Node("prod^Δ_int_left")).NotTo(BeNil())
		Expect(incrWithI.Node("prod^Δ_int_right")).NotTo(BeNil())
		Expect(incrWithI.Validate()).To(BeEmpty())
	})

	It("supports selecting specific inputs", func() {
		c := circuit.New("input-integrators-selective")
		c.AddNode(circuit.Input("pods"))
		c.AddNode(circuit.Input("services"))
		c.AddNode(circuit.Op("join", operator.NewCartesianProduct()))
		c.AddNode(circuit.Output("out"))
		c.AddEdge(circuit.NewEdge("pods", "join", 0))
		c.AddEdge(circuit.NewEdge("services", "join", 1))
		c.AddEdge(circuit.NewEdge("join", "out", 0))

		adapted, err := NewInputIntegrators("pods").Transform(c)
		Expect(err).NotTo(HaveOccurred())

		Expect(adapted.Node("pods_int")).NotTo(BeNil())
		Expect(adapted.Node("services_int")).To(BeNil())

		joinIn := adapted.EdgesTo("join")
		Expect(joinIn).To(HaveLen(2))
		from := map[string]bool{}
		for _, e := range joinIn {
			from[e.From] = true
		}
		Expect(from["pods_int"]).To(BeTrue())
		Expect(from["services"]).To(BeTrue())
	})

	It("rejects unknown selected input names", func() {
		c := circuit.New("input-integrators-unknown")
		c.AddNode(circuit.Input("pods"))
		c.AddNode(circuit.Output("out"))
		c.AddEdge(circuit.NewEdge("pods", "out", 0))

		_, err := NewInputIntegrators("services").Transform(c)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("input \"services\" not found"))
	})
})

var _ = Describe("Transformer factory", func() {
	It("creates Incrementalizer", func() {
		t, err := New(Incrementalizer)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Incrementalizer))
	})

	It("creates InputIntegrators", func() {
		t, err := New(InputIntegrators)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(InputIntegrators))
	})

	It("creates InputIntegrators with selected inputs", func() {
		t, err := New(InputIntegrators, []string{"pods"})
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(InputIntegrators))
	})

	It("rejects InputIntegrators options with wrong type", func() {
		_, err := New(InputIntegrators, "pods")
		Expect(err).To(HaveOccurred())
	})

	It("creates Rewriter with default rules", func() {
		t, err := New(Rewriter)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Rewriter))
	})

	It("creates Rewriter with named rule set", func() {
		t, err := New(Rewriter, "Post")
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Rewriter))
	})

	It("creates Reconciler with explicit pairs", func() {
		pairs := []ReconcilerPair{{InputID: "in", OutputID: "out"}}
		t, err := New(Reconciler, pairs)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Reconciler))
	})

	It("creates Regularizer", func() {
		t, err := New(Regularizer)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Regularizer))
	})

	It("creates Optimizer", func() {
		t, err := New(Optimizer)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Optimizer))
	})

	It("optimizer applies transforms in canonical order", func() {
		c := circuit.New("x")
		Expect(c.AddNode(circuit.Input("input_foo"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("noop", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_foo"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_foo", "noop", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("noop", "output_foo", 0))).To(Succeed())

		t, err := New(Optimizer)
		Expect(err).NotTo(HaveOccurred())
		incr, err := t.Transform(c)
		Expect(err).NotTo(HaveOccurred())
		Expect(incr.Name()).To(Equal("x^Δ"))
	})

	It("optimizer rejects already incremental circuits", func() {
		c := circuit.New("already-incremental")
		Expect(c.AddNode(circuit.Input("in"))).To(Succeed())
		Expect(c.AddNode(circuit.Integrate("int"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("out"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("in", "int", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("int", "out", 0))).To(Succeed())

		t, err := New(Optimizer)
		Expect(err).NotTo(HaveOccurred())
		_, err = t.Transform(c)
		Expect(err).To(HaveOccurred())
	})

	It("rejects unknown transformer", func() {
		_, err := New(TransformerType("Bogus"))
		Expect(err).To(HaveOccurred())
	})
})
