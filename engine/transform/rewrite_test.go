package transform

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/operator"
)

var _ = Describe("Rewrite", func() {
	Describe("EliminateDI", func() {
		It("eliminates I → D pattern", func() {
			// a → I → D → b  =>  a → b.
			c := circuit.New("di-test")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "b", 0))

			changed := EliminateDI(c)
			Expect(changed).To(BeTrue())
			Expect(c.Node("int")).To(BeNil())
			Expect(c.Node("diff")).To(BeNil())
			Expect(c.Nodes()).To(HaveLen(2))
			edges := c.EdgesTo("b")
			Expect(edges).To(HaveLen(1))
			Expect(edges[0].From).To(Equal("a"))
		})

		It("does not eliminate standalone D or I", func() {
			c := circuit.New("no-match")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "b", 0))

			changed := EliminateDI(c)
			Expect(changed).To(BeFalse())
			Expect(c.Nodes()).To(HaveLen(3))
		})

		It("does not eliminate D → I (wrong order for this rule)", func() {
			c := circuit.New("wrong-order")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "b", 0))

			changed := EliminateDI(c)
			Expect(changed).To(BeFalse())
		})
	})

	Describe("EliminateID", func() {
		It("eliminates D → I pattern", func() {
			// a → D → I → b  =>  a → b.
			c := circuit.New("id-test")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "b", 0))

			changed := EliminateID(c)
			Expect(changed).To(BeTrue())
			Expect(c.Node("int")).To(BeNil())
			Expect(c.Node("diff")).To(BeNil())
			Expect(c.Nodes()).To(HaveLen(2))
			edges := c.EdgesTo("b")
			Expect(edges).To(HaveLen(1))
			Expect(edges[0].From).To(Equal("a"))
		})

		It("does not eliminate I → D (wrong order for this rule)", func() {
			c := circuit.New("wrong-order")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "b", 0))

			changed := EliminateID(c)
			Expect(changed).To(BeFalse())
		})
	})

	Describe("ConsolidateDistinct", func() {
		It("eliminates duplicate distinct", func() {
			c := circuit.New("distinct-test")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("d1", operator.NewDistinct()))
			c.AddNode(circuit.Op("d2", operator.NewDistinct()))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "d1", 0))
			c.AddEdge(circuit.NewEdge("d1", "d2", 0))
			c.AddEdge(circuit.NewEdge("d2", "b", 0))

			changed := ConsolidateDistinct(c)
			Expect(changed).To(BeTrue())
			// The upstream distinct (d1) is bypassed, d2 remains.
			Expect(c.Node("d1")).To(BeNil())
			Expect(c.Node("d2")).NotTo(BeNil())
			Expect(c.Nodes()).To(HaveLen(3))
			edges := c.EdgesTo("d2")
			Expect(edges).To(HaveLen(1))
			Expect(edges[0].From).To(Equal("a"))
		})

		It("does not consolidate non-distinct operators", func() {
			c := circuit.New("non-distinct")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("neg", operator.NewNegate()))
			c.AddNode(circuit.Op("d1", operator.NewDistinct()))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "neg", 0))
			c.AddEdge(circuit.NewEdge("neg", "d1", 0))
			c.AddEdge(circuit.NewEdge("d1", "b", 0))

			changed := ConsolidateDistinct(c)
			Expect(changed).To(BeFalse())
		})
	})

	Describe("Rewrite (fixed-point)", func() {
		It("applies rules until fixed point", func() {
			// a → I1 → D1 → I2 → D2 → b.
			// First pass eliminates one I/D pair, second pass eliminates the other.
			c := circuit.New("chain")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Integrate("i1"))
			c.AddNode(circuit.Differentiate("d1"))
			c.AddNode(circuit.Integrate("i2"))
			c.AddNode(circuit.Differentiate("d2"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "i1", 0))
			c.AddEdge(circuit.NewEdge("i1", "d1", 0))
			c.AddEdge(circuit.NewEdge("d1", "i2", 0))
			c.AddEdge(circuit.NewEdge("i2", "d2", 0))
			c.AddEdge(circuit.NewEdge("d2", "b", 0))

			passes := Rewrite(c, DefaultRules()...)
			Expect(passes).To(BeNumerically(">=", 2))
			Expect(c.Nodes()).To(HaveLen(2)) // Only a, b remain.
			edges := c.EdgesTo("b")
			Expect(edges).To(HaveLen(1))
			Expect(edges[0].From).To(Equal("a"))
		})

		It("converges in one pass when no rules match", func() {
			c := circuit.New("no-match")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "b", 0))

			passes := Rewrite(c, DefaultRules()...)
			Expect(passes).To(Equal(1))
			Expect(c.Nodes()).To(HaveLen(2))
		})
	})

	Describe("DistinctPastLinear", func() {
		It("pushes distinct past select", func() {
			// a → Distinct → σ → b  =>  a → σ → Distinct → b.
			sel := operator.NewSelect(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			}))
			c := circuit.New("dpl-test")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("dist", operator.NewDistinct()))
			c.AddNode(circuit.Op("sel", sel))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "dist", 0))
			c.AddEdge(circuit.NewEdge("dist", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "b", 0))

			changed := DistinctPastLinear(c)
			Expect(changed).To(BeTrue())

			// "dist" node should now hold the Select operator.
			Expect(c.Node("dist").Operator.Linearity()).To(Equal(operator.Linear))
			// "sel" node should now hold the Distinct operator.
			Expect(isDistinct(c.Node("sel"))).To(BeTrue())
		})

		It("pushes distinct past project", func() {
			proj := operator.NewProject(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return ctx.Document(), nil
			}))
			c := circuit.New("dpl-proj")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("dist", operator.NewDistinct()))
			c.AddNode(circuit.Op("proj", proj))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "dist", 0))
			c.AddEdge(circuit.NewEdge("dist", "proj", 0))
			c.AddEdge(circuit.NewEdge("proj", "b", 0))

			changed := DistinctPastLinear(c)
			Expect(changed).To(BeTrue())
			Expect(isDistinct(c.Node("proj"))).To(BeTrue())
		})

		It("does not push distinct past negate", func() {
			c := circuit.New("dpl-negate")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("dist", operator.NewDistinct()))
			c.AddNode(circuit.Op("neg", operator.NewNegate()))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "dist", 0))
			c.AddEdge(circuit.NewEdge("dist", "neg", 0))
			c.AddEdge(circuit.NewEdge("neg", "b", 0))

			changed := DistinctPastLinear(c)
			Expect(changed).To(BeFalse())
		})

		It("does not match when predecessor is not distinct", func() {
			sel := operator.NewSelect(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			}))
			c := circuit.New("dpl-no-match")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("neg", operator.NewNegate()))
			c.AddNode(circuit.Op("sel", sel))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "neg", 0))
			c.AddEdge(circuit.NewEdge("neg", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "b", 0))

			changed := DistinctPastLinear(c)
			Expect(changed).To(BeFalse())
		})
	})

	Describe("DistinctDistribution", func() {
		It("removes upstream distinct when separated by select", func() {
			// a → Distinct → σ → Distinct → b  =>  a → σ → Distinct → b.
			sel := operator.NewSelect(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			}))
			c := circuit.New("dd-test")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("d1", operator.NewDistinct()))
			c.AddNode(circuit.Op("sel", sel))
			c.AddNode(circuit.Op("d2", operator.NewDistinct()))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "d1", 0))
			c.AddEdge(circuit.NewEdge("d1", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "d2", 0))
			c.AddEdge(circuit.NewEdge("d2", "b", 0))

			changed := DistinctDistribution(c)
			Expect(changed).To(BeTrue())
			Expect(c.Node("d1")).To(BeNil()) // Upstream distinct removed.
			Expect(c.Node("sel")).NotTo(BeNil())
			Expect(c.Node("d2")).NotTo(BeNil())
			edges := c.EdgesTo("sel")
			Expect(edges).To(HaveLen(1))
			Expect(edges[0].From).To(Equal("a"))
		})

		It("does not match when middle op is not commutable", func() {
			c := circuit.New("dd-no-match")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("d1", operator.NewDistinct()))
			c.AddNode(circuit.Op("neg", operator.NewNegate()))
			c.AddNode(circuit.Op("d2", operator.NewDistinct()))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "d1", 0))
			c.AddEdge(circuit.NewEdge("d1", "neg", 0))
			c.AddEdge(circuit.NewEdge("neg", "d2", 0))
			c.AddEdge(circuit.NewEdge("d2", "b", 0))

			changed := DistinctDistribution(c)
			Expect(changed).To(BeFalse())
		})
	})

	Describe("SwapDifferentiateLinear", func() {
		It("swaps D past a unary linear operator", func() {
			// a → D → σ → b  =>  a → σ → D → b.
			sel := operator.NewSelect(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			}))
			c := circuit.New("sdl-test")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Op("sel", sel))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "b", 0))

			changed := SwapDifferentiateLinear(c)
			Expect(changed).To(BeTrue())

			// "diff" node should now hold the Select operator.
			Expect(c.Node("diff").Kind()).To(Equal(operator.KindSelect))
			// "sel" node should now be KindDifferentiate.
			Expect(c.Node("sel").Kind()).To(Equal(operator.KindDifferentiate))
		})

		It("swaps D past negate", func() {
			c := circuit.New("sdl-negate")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Op("neg", operator.NewNegate()))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "neg", 0))
			c.AddEdge(circuit.NewEdge("neg", "b", 0))

			changed := SwapDifferentiateLinear(c)
			Expect(changed).To(BeTrue())
			Expect(c.Node("diff").Kind()).To(Equal(operator.KindNegate))
			Expect(c.Node("neg").Kind()).To(Equal(operator.KindDifferentiate))
		})

		It("does not swap D past non-linear operator", func() {
			c := circuit.New("sdl-nonlinear")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Op("dist", operator.NewDistinct()))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "dist", 0))
			c.AddEdge(circuit.NewEdge("dist", "b", 0))

			changed := SwapDifferentiateLinear(c)
			Expect(changed).To(BeFalse())
		})
	})

	Describe("SwapLinearIntegrate", func() {
		It("swaps unary linear operator past I", func() {
			// a → σ → I → b  =>  a → I → σ → b.
			sel := operator.NewSelect(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			}))
			c := circuit.New("sli-test")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("sel", sel))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "b", 0))

			changed := SwapLinearIntegrate(c)
			Expect(changed).To(BeTrue())

			// "sel" node should now be KindIntegrate.
			Expect(c.Node("sel").Kind()).To(Equal(operator.KindIntegrate))
			// "int" node should now hold the Select operator.
			Expect(c.Node("int").Kind()).To(Equal(operator.KindSelect))
		})

		It("does not swap non-linear operator past I", func() {
			c := circuit.New("sli-nonlinear")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Op("dist", operator.NewDistinct()))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "dist", 0))
			c.AddEdge(circuit.NewEdge("dist", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "b", 0))

			changed := SwapLinearIntegrate(c)
			Expect(changed).To(BeFalse())
		})
	})

	Describe("PreRules pipeline", func() {
		It("consolidates distinct operators in a 6.3.1-style circuit", func() {
			// σ → π → Distinct → σ → π → Distinct → out.
			// PreRules should push Distinct downstream and consolidate.
			sel1 := operator.NewSelect(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			}))
			proj1 := operator.NewProject(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return ctx.Document(), nil
			}))
			sel2 := operator.NewSelect(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			}))
			proj2 := operator.NewProject(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return ctx.Document(), nil
			}))

			c := circuit.New("pipeline")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("sel1", sel1))
			c.AddNode(circuit.Op("proj1", proj1))
			c.AddNode(circuit.Op("dist1", operator.NewDistinct()))
			c.AddNode(circuit.Op("sel2", sel2))
			c.AddNode(circuit.Op("proj2", proj2))
			c.AddNode(circuit.Op("dist2", operator.NewDistinct()))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "sel1", 0))
			c.AddEdge(circuit.NewEdge("sel1", "proj1", 0))
			c.AddEdge(circuit.NewEdge("proj1", "dist1", 0))
			c.AddEdge(circuit.NewEdge("dist1", "sel2", 0))
			c.AddEdge(circuit.NewEdge("sel2", "proj2", 0))
			c.AddEdge(circuit.NewEdge("proj2", "dist2", 0))
			c.AddEdge(circuit.NewEdge("dist2", "out", 0))

			Rewrite(c, PreRules()...)

			// After PreRules, only one Distinct should remain (at the end).
			distinctCount := 0
			for _, n := range c.Nodes() {
				if isDistinct(n) {
					distinctCount++
				}
			}
			Expect(distinctCount).To(Equal(1))
		})
	})

	Describe("PostRules: D-push + cancellation", func() {
		It("pushes D past linear op to cancel with I", func() {
			// a → D → σ → I → b.
			// SwapDifferentiateLinear: D → σ becomes σ → D.
			// Circuit: a → σ → D → I → b.
			// EliminateID: D → I cancels.
			// Result: a → σ → b.
			sel := operator.NewSelect(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			}))
			c := circuit.New("d-push-cancel")
			c.AddNode(circuit.Input("a"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Op("sel", sel))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Output("b"))
			c.AddEdge(circuit.NewEdge("a", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "b", 0))

			Rewrite(c, PostRules()...)

			// D and I should be gone. Only a → σ → b remains.
			Expect(c.Nodes()).To(HaveLen(3)) // a, sel-node, b.
			// Find the operator node.
			var opNode *circuit.Node
			for _, n := range c.Nodes() {
				if n.Kind() == operator.KindSelect {
					opNode = n
					break
				}
			}
			Expect(opNode).NotTo(BeNil())
			Expect(opNode.Operator.Linearity()).To(Equal(operator.Linear))

			// Verify connectivity: a → op → b.
			inEdges := c.EdgesTo(opNode.ID)
			Expect(inEdges).To(HaveLen(1))
			Expect(inEdges[0].From).To(Equal("a"))
			outEdges := c.EdgesTo("b")
			Expect(outEdges).To(HaveLen(1))
			Expect(outEdges[0].From).To(Equal(opNode.ID))
		})
	})

	Describe("End-to-end: Incrementalize + Rewrite", func() {
		It("simplifies two sequential non-linear operators", func() {
			// Original: in → distinct1 → distinct2 → out.
			// After Incrementalize: in → I1 → Op1 → D1 → I2 → Op2 → D2 → out.
			// After Rewrite:
			//   1. EliminateID cancels D1/I2: in → I1 → Op1 → Op2 → D2 → out.
			//   2. ConsolidateDistinct merges Op1/Op2: in → I1 → Op2 → D2 → out.
			// Final form is the canonical D ∘ O ∘ ∫ for a single Distinct.
			c := circuit.New("two-nonlinear")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("dist1", operator.NewDistinct()))
			c.AddNode(circuit.Op("dist2", operator.NewDistinct()))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "dist1", 0))
			c.AddEdge(circuit.NewEdge("dist1", "dist2", 0))
			c.AddEdge(circuit.NewEdge("dist2", "out", 0))

			incr, err := Incrementalize(c)
			Expect(err).NotTo(HaveOccurred())

			// Before rewrite: all expansion nodes should exist.
			Expect(incr.Node("dist1^Δ_int")).NotTo(BeNil())
			Expect(incr.Node("dist1^Δ_op")).NotTo(BeNil())
			Expect(incr.Node("dist1^Δ_diff")).NotTo(BeNil())
			Expect(incr.Node("dist2^Δ_int")).NotTo(BeNil())
			Expect(incr.Node("dist2^Δ_op")).NotTo(BeNil())
			Expect(incr.Node("dist2^Δ_diff")).NotTo(BeNil())

			Rewrite(incr, DefaultRules()...)

			// After rewrite: D1, I2, and Op1 should be eliminated.
			Expect(incr.Node("dist1^Δ_diff")).To(BeNil())
			Expect(incr.Node("dist2^Δ_int")).To(BeNil())
			Expect(incr.Node("dist1^Δ_op")).To(BeNil())

			// Remaining: in → I1 → Op2 → D2 → out.
			Expect(incr.Node("dist1^Δ_int")).NotTo(BeNil())
			Expect(incr.Node("dist2^Δ_op")).NotTo(BeNil())
			Expect(incr.Node("dist2^Δ_diff")).NotTo(BeNil())

			// Verify connectivity.
			intEdges := incr.EdgesTo("dist1^Δ_int")
			Expect(intEdges).To(HaveLen(1))
			Expect(intEdges[0].From).To(Equal("in"))

			opEdges := incr.EdgesTo("dist2^Δ_op")
			Expect(opEdges).To(HaveLen(1))
			Expect(opEdges[0].From).To(Equal("dist1^Δ_int"))

			diffEdges := incr.EdgesTo("dist2^Δ_diff")
			Expect(diffEdges).To(HaveLen(1))
			Expect(diffEdges[0].From).To(Equal("dist2^Δ_op"))

			outEdges := incr.EdgesTo("out")
			Expect(outEdges).To(HaveLen(1))
			Expect(outEdges[0].From).To(Equal("dist2^Δ_diff"))
		})
	})
})
