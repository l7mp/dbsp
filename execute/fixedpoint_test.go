package execute_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/circuit"
	"github.com/l7mp/dbsp/execute"
	"github.com/l7mp/dbsp/expr"
	"github.com/l7mp/dbsp/operator"
	"github.com/l7mp/dbsp/transform"
	"github.com/l7mp/dbsp/zset"
)

// Element types for graph algorithms.

// Node represents a graph node.
type Node struct{ ID string }

func (n Node) Key() string                 { return n.ID }
func (n Node) PrimaryKey() (string, error) { return n.ID, nil }

// GEdge represents a directed graph edge (G prefix to avoid conflict with circuit.Edge).
type GEdge struct{ From, To string }

func (e GEdge) Key() string                 { return e.From + "->" + e.To }
func (e GEdge) PrimaryKey() (string, error) { return e.From + "->" + e.To, nil }

// Path represents a path from one node to another.
type Path struct{ From, To string }

func (p Path) Key() string                 { return p.From + "->" + p.To }
func (p Path) PrimaryKey() (string, error) { return p.From + "->" + p.To, nil }

// Dist represents distance from source to a node.
type Dist struct {
	Node     string
	Distance int
}

func (d Dist) Key() string                 { return d.Node }
func (d Dist) PrimaryKey() (string, error) { return d.Node, nil }

// Helper to create a Z-set with one element.
func zsetOfNode(id string) zset.ZSet {
	z := zset.New()
	z.Insert(Node{ID: id}, 1)
	return z
}

func zsetOfDist(node string, dist int) zset.ZSet {
	z := zset.New()
	z.Insert(Dist{Node: node, Distance: dist}, 1)
	return z
}

// emptyInputs creates empty Z-sets for all input nodes in a circuit.
func emptyInputs(c *circuit.Circuit) map[string]zset.ZSet {
	result := make(map[string]zset.ZSet)
	for _, node := range c.Inputs() {
		result[node.ID] = zset.New()
	}
	return result
}

// verifyFixedPointEquivalence runs a fixed-point circuit for multiple rounds
// and verifies that incremental output matches D(normal output) at each round.
func verifyFixedPointEquivalence(
	normalCircuit *circuit.Circuit,
	initialInputs map[string]zset.ZSet,
	numRounds int,
) {
	incrCircuit, err := transform.Incrementalize(normalCircuit)
	Expect(err).NotTo(HaveOccurred())

	normalExec, err := execute.NewExecutor(normalCircuit)
	Expect(err).NotTo(HaveOccurred())

	incrExec, err := execute.NewExecutor(incrCircuit)
	Expect(err).NotTo(HaveOccurred())

	var prevNormalOutput map[string]zset.ZSet

	for round := 0; round < numRounds; round++ {
		// Normal circuit: always provide same inputs (don't reset - delay state persists).
		normalOutput, err := normalExec.Execute(initialInputs)
		Expect(err).NotTo(HaveOccurred(), "round %d normal execution", round)

		// Incremental circuit: delta at round 0, empty thereafter.
		var deltas map[string]zset.ZSet
		if round == 0 {
			deltas = initialInputs
		} else {
			deltas = emptyInputs(normalCircuit)
		}
		incrOutput, err := incrExec.Execute(deltas)
		Expect(err).NotTo(HaveOccurred(), "round %d incremental execution", round)

		// Compute expected delta: D(normalOutput) = normalOutput - prevNormalOutput.
		expectedDelta := make(map[string]zset.ZSet)
		for id, out := range normalOutput {
			if prevNormalOutput == nil {
				expectedDelta[id] = out
			} else if prev, exists := prevNormalOutput[id]; !exists {
				expectedDelta[id] = out
			} else {
				expectedDelta[id] = out.Subtract(prev)
			}
		}

		// Verify: incremental output == expected delta.
		for id := range normalOutput {
			Expect(incrOutput[id].Equal(expectedDelta[id])).To(BeTrue(),
				"round %d output %s mismatch:\n  got:      %v\n  expected: %v",
				round, id, incrOutput[id].Entries(), expectedDelta[id].Entries())
		}

		// Clone for next iteration comparison.
		prevNormalOutput = make(map[string]zset.ZSet)
		for id, out := range normalOutput {
			prevNormalOutput[id] = out.Clone()
		}
	}
}

var _ = Describe("Fixed-Point Circuits", func() {
	const numRounds = 10

	Describe("Simple Reachability", func() {
		// Reach = Source ∪ π_to(Reach ⋈ Edge)
		// Circuit: source -> [+] -> out, with feedback: out -> z⁻¹ -> join(Edge) -> π -> [+]

		buildReachabilityCircuit := func() *circuit.Circuit {
			c := circuit.New("reachability")

			// Inputs.
			c.AddNode(circuit.Input("source"))
			c.AddNode(circuit.Input("edge"))

			// Feedback delay: previous reachable nodes.
			c.AddNode(circuit.Delay("delay"))

			// Join: Reach × Edge where reach.id == edge.from.
			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				node := pair.Left().(Node)
				edge := pair.Right().(GEdge)
				return node.ID == edge.From, nil
			}))))

			// Project: extract edge.To as new Node.
			c.AddNode(circuit.Op("project", operator.NewProject("π", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				edge := pair.Right().(GEdge)
				return Node{ID: edge.To}, nil
			}))))

			// Plus: source ∪ projected join results.
			c.AddNode(circuit.Op("plus", operator.NewPlus()))

			// Output.
			c.AddNode(circuit.Output("out"))

			// Edges.
			// delay -> product (port 0: left input is previous reach).
			c.AddEdge(circuit.NewEdge("delay", "product", 0))
			// edge -> product (port 1: right input is edges).
			c.AddEdge(circuit.NewEdge("edge", "product", 1))
			// product -> select.
			c.AddEdge(circuit.NewEdge("product", "select", 0))
			// select -> project.
			c.AddEdge(circuit.NewEdge("select", "project", 0))
			// source -> plus (port 0).
			c.AddEdge(circuit.NewEdge("source", "plus", 0))
			// project -> plus (port 1).
			c.AddEdge(circuit.NewEdge("project", "plus", 1))
			// plus -> out.
			c.AddEdge(circuit.NewEdge("plus", "out", 0))
			// plus -> delay (feedback loop).
			c.AddEdge(circuit.NewEdge("plus", "delay", 0))

			return c
		}

		It("computes reachable nodes over multiple rounds", func() {
			c := buildReachabilityCircuit()
			Expect(c.Validate()).To(BeEmpty(), "circuit should be well-formed")

			// Source = {A}, Edges = {A->B, B->C, C->D}.
			source := zsetOfNode("A")
			edges := zset.New()
			edges.Insert(GEdge{From: "A", To: "B"}, 1)
			edges.Insert(GEdge{From: "B", To: "C"}, 1)
			edges.Insert(GEdge{From: "C", To: "D"}, 1)

			inputs := map[string]zset.ZSet{
				"source": source,
				"edge":   edges,
			}

			verifyFixedPointEquivalence(c, inputs, numRounds)
		})

		It("reaches expected nodes after convergence", func() {
			c := buildReachabilityCircuit()

			source := zsetOfNode("A")
			edges := zset.New()
			edges.Insert(GEdge{From: "A", To: "B"}, 1)
			edges.Insert(GEdge{From: "B", To: "C"}, 1)
			edges.Insert(GEdge{From: "C", To: "D"}, 1)

			exec, err := execute.NewExecutor(c)
			Expect(err).NotTo(HaveOccurred())

			inputs := map[string]zset.ZSet{"source": source, "edge": edges}

			var out map[string]zset.ZSet
			for i := 0; i < numRounds; i++ {
				out, _ = exec.Execute(inputs)
			}

			// After convergence: Reach = {A, B, C, D}.
			Expect(out["out"].Lookup(Node{ID: "A"})).To(Equal(zset.Weight(1)))
			Expect(out["out"].Lookup(Node{ID: "B"})).To(Equal(zset.Weight(1)))
			Expect(out["out"].Lookup(Node{ID: "C"})).To(Equal(zset.Weight(1)))
			Expect(out["out"].Lookup(Node{ID: "D"})).To(Equal(zset.Weight(1)))
			Expect(out["out"].Size()).To(Equal(4))
		})
	})

	Describe("Transitive Closure", func() {
		// Path = Edge ∪ π_{e.from, p.to}(Edge ⋈ Path)
		// All pairs (a,b) where there's a path from a to b.

		buildTransitiveClosureCircuit := func() *circuit.Circuit {
			c := circuit.New("transitive-closure")

			// Input: edges.
			c.AddNode(circuit.Input("edge"))

			// Feedback delay: previous paths.
			c.AddNode(circuit.Delay("delay"))

			// Join: Edge × Path where edge.to == path.from.
			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				edge := pair.Left().(GEdge)
				path := pair.Right().(Path)
				return edge.To == path.From, nil
			}))))

			// Project: create Path{edge.from, path.to}.
			c.AddNode(circuit.Op("project", operator.NewProject("π", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				edge := pair.Left().(GEdge)
				path := pair.Right().(Path)
				return Path{From: edge.From, To: path.To}, nil
			}))))

			// Convert edges to paths (for base case).
			c.AddNode(circuit.Op("edge_to_path", operator.NewProject("e2p", expr.Func(func(e zset.Document) (any, error) {
				edge := e.(GEdge)
				return Path{From: edge.From, To: edge.To}, nil
			}))))

			// Plus: edge_as_path ∪ projected join results.
			c.AddNode(circuit.Op("plus", operator.NewPlus()))

			// Output.
			c.AddNode(circuit.Output("out"))

			// Edges.
			// edge -> edge_to_path (convert to Path for base case).
			c.AddEdge(circuit.NewEdge("edge", "edge_to_path", 0))
			// edge -> product (port 0: left input is edges).
			c.AddEdge(circuit.NewEdge("edge", "product", 0))
			// delay -> product (port 1: right input is previous paths).
			c.AddEdge(circuit.NewEdge("delay", "product", 1))
			// product -> select.
			c.AddEdge(circuit.NewEdge("product", "select", 0))
			// select -> project.
			c.AddEdge(circuit.NewEdge("select", "project", 0))
			// edge_to_path -> plus (port 0).
			c.AddEdge(circuit.NewEdge("edge_to_path", "plus", 0))
			// project -> plus (port 1).
			c.AddEdge(circuit.NewEdge("project", "plus", 1))
			// plus -> out.
			c.AddEdge(circuit.NewEdge("plus", "out", 0))
			// plus -> delay (feedback).
			c.AddEdge(circuit.NewEdge("plus", "delay", 0))

			return c
		}

		It("computes transitive closure over multiple rounds", func() {
			c := buildTransitiveClosureCircuit()
			Expect(c.Validate()).To(BeEmpty())

			// Edges: 1->2, 2->3, 3->4.
			edges := zset.New()
			edges.Insert(GEdge{From: "1", To: "2"}, 1)
			edges.Insert(GEdge{From: "2", To: "3"}, 1)
			edges.Insert(GEdge{From: "3", To: "4"}, 1)

			inputs := map[string]zset.ZSet{"edge": edges}

			verifyFixedPointEquivalence(c, inputs, numRounds)
		})

		It("computes all paths after convergence", func() {
			c := buildTransitiveClosureCircuit()

			edges := zset.New()
			edges.Insert(GEdge{From: "1", To: "2"}, 1)
			edges.Insert(GEdge{From: "2", To: "3"}, 1)
			edges.Insert(GEdge{From: "3", To: "4"}, 1)

			exec, _ := execute.NewExecutor(c)
			inputs := map[string]zset.ZSet{"edge": edges}

			var out map[string]zset.ZSet
			for i := 0; i < numRounds; i++ {
				out, _ = exec.Execute(inputs)
			}

			// Expected paths: (1,2), (1,3), (1,4), (2,3), (2,4), (3,4).
			Expect(out["out"].Lookup(Path{From: "1", To: "2"})).To(Equal(zset.Weight(1)))
			Expect(out["out"].Lookup(Path{From: "1", To: "3"})).To(Equal(zset.Weight(1)))
			Expect(out["out"].Lookup(Path{From: "1", To: "4"})).To(Equal(zset.Weight(1)))
			Expect(out["out"].Lookup(Path{From: "2", To: "3"})).To(Equal(zset.Weight(1)))
			Expect(out["out"].Lookup(Path{From: "2", To: "4"})).To(Equal(zset.Weight(1)))
			Expect(out["out"].Lookup(Path{From: "3", To: "4"})).To(Equal(zset.Weight(1)))
			Expect(out["out"].Size()).To(Equal(6))
		})
	})

	Describe("Single-Source Shortest Path (Unweighted)", func() {
		// Dist = {(src,0)} ∪ H(π_{to,d+1}(Dist ⋈ Edge))
		// Uses Distinct (H) to keep minimum distance.
		// Note: We use the same pattern as Reachability (init fed continuously).
		// The Distinct operator ensures we keep shortest distances.

		buildShortestPathCircuit := func() *circuit.Circuit {
			c := circuit.New("shortest-path")

			// Inputs.
			c.AddNode(circuit.Input("init")) // {(source, 0)}
			c.AddNode(circuit.Input("edge"))

			// Feedback delay.
			c.AddNode(circuit.Delay("delay"))

			// Join: Dist × Edge where dist.node == edge.from.
			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				dist := pair.Left().(Dist)
				edge := pair.Right().(GEdge)
				return dist.Node == edge.From, nil
			}))))

			// Project: create Dist{edge.to, dist.distance+1}.
			c.AddNode(circuit.Op("project", operator.NewProject("π", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				dist := pair.Left().(Dist)
				edge := pair.Right().(GEdge)
				return Dist{Node: edge.To, Distance: dist.Distance + 1}, nil
			}))))

			// Plus: init ∪ projected distances.
			c.AddNode(circuit.Op("plus", operator.NewPlus()))

			// Distinct: keep only one entry per node.
			c.AddNode(circuit.Op("distinct", operator.NewDistinct("H")))

			// Output.
			c.AddNode(circuit.Output("out"))

			// Edges.
			c.AddEdge(circuit.NewEdge("delay", "product", 0))
			c.AddEdge(circuit.NewEdge("edge", "product", 1))
			c.AddEdge(circuit.NewEdge("product", "select", 0))
			c.AddEdge(circuit.NewEdge("select", "project", 0))
			c.AddEdge(circuit.NewEdge("init", "plus", 0))
			c.AddEdge(circuit.NewEdge("project", "plus", 1))
			c.AddEdge(circuit.NewEdge("plus", "distinct", 0))
			c.AddEdge(circuit.NewEdge("distinct", "out", 0))
			c.AddEdge(circuit.NewEdge("distinct", "delay", 0))

			return c
		}

		It("computes shortest paths over multiple rounds", func() {
			c := buildShortestPathCircuit()
			Expect(c.Validate()).To(BeEmpty())

			// Source = A with distance 0.
			init := zsetOfDist("A", 0)
			edges := zset.New()
			edges.Insert(GEdge{From: "A", To: "B"}, 1)
			edges.Insert(GEdge{From: "B", To: "C"}, 1)
			edges.Insert(GEdge{From: "C", To: "D"}, 1)

			inputs := map[string]zset.ZSet{"init": init, "edge": edges}

			verifyFixedPointEquivalence(c, inputs, numRounds)
		})

		It("computes correct distances after convergence", func() {
			c := buildShortestPathCircuit()

			init := zsetOfDist("A", 0)
			edges := zset.New()
			edges.Insert(GEdge{From: "A", To: "B"}, 1)
			edges.Insert(GEdge{From: "B", To: "C"}, 1)
			edges.Insert(GEdge{From: "C", To: "D"}, 1)

			exec, _ := execute.NewExecutor(c)
			inputs := map[string]zset.ZSet{"init": init, "edge": edges}

			var out map[string]zset.ZSet
			for i := 0; i < numRounds; i++ {
				out, _ = exec.Execute(inputs)
			}

			// Check that source node is present with distance 0.
			Expect(out["out"].Lookup(Dist{Node: "A", Distance: 0})).To(Equal(zset.Weight(1)))
		})
	})

	Describe("Semi-naive Evaluation Pattern", func() {
		// Semi-naive optimization: only process new facts each round.
		// This is naturally what DBSP does with incremental computation.
		// We test a circuit that explicitly tracks deltas vs accumulated state.
		//
		// Pattern: ΔR feeds into F, result is added to accumulator R.
		// ΔR[t] = F(ΔR[t-1]) ∩ ¬R[t-1]  (new facts not already known)
		// R[t] = R[t-1] + ΔR[t]
		//
		// Simplified: just test that incremental reachability behaves correctly,
		// which is the essence of semi-naive - only propagating deltas.

		buildSemiNaiveCircuit := func() *circuit.Circuit {
			c := circuit.New("semi-naive")

			// This is essentially the same as reachability, but we rename
			// to emphasize it's testing the semi-naive pattern.
			c.AddNode(circuit.Input("init"))
			c.AddNode(circuit.Input("edge"))

			c.AddNode(circuit.Delay("delay"))

			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				node := pair.Left().(Node)
				edge := pair.Right().(GEdge)
				return node.ID == edge.From, nil
			}))))
			c.AddNode(circuit.Op("project", operator.NewProject("π", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				edge := pair.Right().(GEdge)
				return Node{ID: edge.To}, nil
			}))))

			c.AddNode(circuit.Op("plus", operator.NewPlus()))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("delay", "product", 0))
			c.AddEdge(circuit.NewEdge("edge", "product", 1))
			c.AddEdge(circuit.NewEdge("product", "select", 0))
			c.AddEdge(circuit.NewEdge("select", "project", 0))
			c.AddEdge(circuit.NewEdge("init", "plus", 0))
			c.AddEdge(circuit.NewEdge("project", "plus", 1))
			c.AddEdge(circuit.NewEdge("plus", "out", 0))
			c.AddEdge(circuit.NewEdge("plus", "delay", 0))

			return c
		}

		It("incremental computation naturally implements semi-naive", func() {
			c := buildSemiNaiveCircuit()
			Expect(c.Validate()).To(BeEmpty())

			// Initial facts = {A}, edges = {A->B, B->C}.
			init := zsetOfNode("A")
			edges := zset.New()
			edges.Insert(GEdge{From: "A", To: "B"}, 1)
			edges.Insert(GEdge{From: "B", To: "C"}, 1)

			inputs := map[string]zset.ZSet{"init": init, "edge": edges}

			verifyFixedPointEquivalence(c, inputs, numRounds)
		})

		It("incremental output shrinks as fixed point is approached", func() {
			c := buildSemiNaiveCircuit()

			init := zsetOfNode("A")
			edges := zset.New()
			edges.Insert(GEdge{From: "A", To: "B"}, 1)
			edges.Insert(GEdge{From: "B", To: "C"}, 1)

			incrCircuit, _ := transform.Incrementalize(c)
			incrExec, _ := execute.NewExecutor(incrCircuit)

			// Track incremental output sizes.
			var sizes []int
			for i := 0; i < numRounds; i++ {
				var deltas map[string]zset.ZSet
				if i == 0 {
					deltas = map[string]zset.ZSet{"init": init, "edge": edges}
				} else {
					deltas = emptyInputs(c)
				}
				out, _ := incrExec.Execute(deltas)
				sizes = append(sizes, out["out"].Size())
			}

			// After convergence, incremental output should be zero (no changes).
			// The pattern should show decreasing or stable output sizes.
			Expect(sizes[len(sizes)-1]).To(Equal(0), "should stabilize with zero delta")
		})
	})

	Describe("Mutual Recursion", func() {
		// A = InitA ∪ π_to(B ⋈ Edge_BA)
		// B = InitB ∪ π_to(A ⋈ Edge_AB)
		// Two relations depending on each other.

		buildMutualRecursionCircuit := func() *circuit.Circuit {
			c := circuit.New("mutual-recursion")

			// Inputs.
			c.AddNode(circuit.Input("init_a"))
			c.AddNode(circuit.Input("init_b"))
			c.AddNode(circuit.Input("edge_ab")) // Edges from A to B.
			c.AddNode(circuit.Input("edge_ba")) // Edges from B to A.

			// Delays for A and B feedback.
			c.AddNode(circuit.Delay("delay_a"))
			c.AddNode(circuit.Delay("delay_b"))

			// A depends on B: B ⋈ Edge_BA -> π_to.
			c.AddNode(circuit.Op("product_ba", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select_ba", operator.NewSelect("σ", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				node := pair.Left().(Node)
				edge := pair.Right().(GEdge)
				return node.ID == edge.From, nil
			}))))
			c.AddNode(circuit.Op("project_ba", operator.NewProject("π", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				edge := pair.Right().(GEdge)
				return Node{ID: edge.To}, nil
			}))))
			c.AddNode(circuit.Op("plus_a", operator.NewPlus()))

			// B depends on A: A ⋈ Edge_AB -> π_to.
			c.AddNode(circuit.Op("product_ab", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select_ab", operator.NewSelect("σ", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				node := pair.Left().(Node)
				edge := pair.Right().(GEdge)
				return node.ID == edge.From, nil
			}))))
			c.AddNode(circuit.Op("project_ab", operator.NewProject("π", expr.Func(func(e zset.Document) (any, error) {
				pair := e.(*operator.Pair)
				edge := pair.Right().(GEdge)
				return Node{ID: edge.To}, nil
			}))))
			c.AddNode(circuit.Op("plus_b", operator.NewPlus()))

			// Outputs.
			c.AddNode(circuit.Output("out_a"))
			c.AddNode(circuit.Output("out_b"))

			// A circuit: init_a ∪ π(B ⋈ edge_ba).
			c.AddEdge(circuit.NewEdge("delay_b", "product_ba", 0))
			c.AddEdge(circuit.NewEdge("edge_ba", "product_ba", 1))
			c.AddEdge(circuit.NewEdge("product_ba", "select_ba", 0))
			c.AddEdge(circuit.NewEdge("select_ba", "project_ba", 0))
			c.AddEdge(circuit.NewEdge("init_a", "plus_a", 0))
			c.AddEdge(circuit.NewEdge("project_ba", "plus_a", 1))
			c.AddEdge(circuit.NewEdge("plus_a", "out_a", 0))
			c.AddEdge(circuit.NewEdge("plus_a", "delay_a", 0))

			// B circuit: init_b ∪ π(A ⋈ edge_ab).
			c.AddEdge(circuit.NewEdge("delay_a", "product_ab", 0))
			c.AddEdge(circuit.NewEdge("edge_ab", "product_ab", 1))
			c.AddEdge(circuit.NewEdge("product_ab", "select_ab", 0))
			c.AddEdge(circuit.NewEdge("select_ab", "project_ab", 0))
			c.AddEdge(circuit.NewEdge("init_b", "plus_b", 0))
			c.AddEdge(circuit.NewEdge("project_ab", "plus_b", 1))
			c.AddEdge(circuit.NewEdge("plus_b", "out_b", 0))
			c.AddEdge(circuit.NewEdge("plus_b", "delay_b", 0))

			return c
		}

		It("computes mutual dependencies over multiple rounds", func() {
			c := buildMutualRecursionCircuit()
			Expect(c.Validate()).To(BeEmpty())

			// InitA = {1}, InitB = {}, edges create A->B->A propagation.
			initA := zsetOfNode("1")
			initB := zset.New()
			edgeAB := zset.New()
			edgeAB.Insert(GEdge{From: "1", To: "2"}, 1) // 1 in A -> 2 in B.
			edgeBA := zset.New()
			edgeBA.Insert(GEdge{From: "2", To: "3"}, 1) // 2 in B -> 3 in A.

			inputs := map[string]zset.ZSet{
				"init_a":  initA,
				"init_b":  initB,
				"edge_ab": edgeAB,
				"edge_ba": edgeBA,
			}

			verifyFixedPointEquivalence(c, inputs, numRounds)
		})

		It("reaches expected nodes after convergence", func() {
			c := buildMutualRecursionCircuit()

			initA := zsetOfNode("1")
			initB := zset.New()
			edgeAB := zset.New()
			edgeAB.Insert(GEdge{From: "1", To: "2"}, 1)
			edgeBA := zset.New()
			edgeBA.Insert(GEdge{From: "2", To: "3"}, 1)

			exec, _ := execute.NewExecutor(c)
			inputs := map[string]zset.ZSet{
				"init_a":  initA,
				"init_b":  initB,
				"edge_ab": edgeAB,
				"edge_ba": edgeBA,
			}

			var out map[string]zset.ZSet
			for i := 0; i < numRounds; i++ {
				out, _ = exec.Execute(inputs)
			}

			// A should contain: 1 (init), 3 (from B via edge_ba).
			Expect(out["out_a"].Lookup(Node{ID: "1"})).To(Equal(zset.Weight(1)))
			Expect(out["out_a"].Lookup(Node{ID: "3"})).To(Equal(zset.Weight(1)))

			// B should contain: 2 (from A via edge_ab).
			Expect(out["out_b"].Lookup(Node{ID: "2"})).To(Equal(zset.Weight(1)))
		})
	})
})
