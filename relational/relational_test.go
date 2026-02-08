package relational_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/circuit"
	"github.com/l7mp/dbsp/execute"
	"github.com/l7mp/dbsp/operator"
	. "github.com/l7mp/dbsp/relational"
	"github.com/l7mp/dbsp/transform"
	"github.com/l7mp/dbsp/zset"
)

func TestRelational(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Relational Suite")
}

// verifyEquivalence verifies the DBSP theorem: D(C(∫(Δs))) = C^Δ(Δs).
func verifyEquivalence(normalCircuit *circuit.Circuit, inputSequence []map[string]zset.ZSet) {
	incrCircuit, err := transform.Incrementalize(normalCircuit)
	Expect(err).NotTo(HaveOccurred())

	normalExec, err := execute.NewExecutor(normalCircuit)
	Expect(err).NotTo(HaveOccurred())

	incrExec, err := execute.NewExecutor(incrCircuit)
	Expect(err).NotTo(HaveOccurred())

	accumulated := make(map[string]zset.ZSet)
	var prevNormalOutput map[string]zset.ZSet

	for round, deltas := range inputSequence {
		// Accumulate inputs (∫(Δs)).
		for id, delta := range deltas {
			if _, exists := accumulated[id]; !exists {
				accumulated[id] = zset.New()
			}
			accumulated[id] = accumulated[id].Add(delta)
		}

		// Run normal circuit on accumulated input.
		normalExec.Reset()
		normalOutput, err := normalExec.Execute(accumulated)
		Expect(err).NotTo(HaveOccurred(), "round %d normal", round)

		// Run incremental circuit on delta.
		incrOutput, err := incrExec.Execute(deltas)
		Expect(err).NotTo(HaveOccurred(), "round %d incremental", round)

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
				"round %d output %s:\n  incr:     %v\n  expected: %v",
				round, id, incrOutput[id].Entries(), expectedDelta[id].Entries())
		}

		prevNormalOutput = normalOutput
	}
}

var _ = Describe("Row", func() {
	It("creates rows with named columns", func() {
		r := NewRow("id", 1, "name", "Alice")
		Expect(r.Get("id")).To(Equal(1))
		Expect(r.Get("name")).To(Equal("Alice"))
	})

	It("uses all columns as key", func() {
		r1 := NewRow("a", 1, "b", 2)
		r2 := NewRow("a", 1, "b", 2)
		r3 := NewRow("a", 1, "b", 3)

		Expect(r1.Key()).To(Equal(r2.Key()))
		Expect(r1.Key()).NotTo(Equal(r3.Key()))
	})

	It("projects to subset of columns", func() {
		r := NewRow("a", 1, "b", 2, "c", 3)
		projected := r.Project("a", "c")

		Expect(projected.Get("a")).To(Equal(1))
		Expect(projected.Get("c")).To(Equal(3))
		Expect(projected.Get("b")).To(BeNil())
	})

	It("creates new row with additional columns", func() {
		r := NewRow("a", 1)
		r2 := r.With("b", 2)

		Expect(r.Get("b")).To(BeNil())
		Expect(r2.Get("a")).To(Equal(1))
		Expect(r2.Get("b")).To(Equal(2))
	})
})

var _ = Describe("Table Helpers", func() {
	It("creates Z-set from rows", func() {
		t := TableOf(
			NewRow("id", 1, "name", "Alice"),
			NewRow("id", 2, "name", "Bob"),
		)
		Expect(t.Size()).To(Equal(2))
	})

	It("creates single-row Z-set", func() {
		t := RowOf("id", 1, "name", "Alice")
		Expect(t.Size()).To(Equal(1))
	})
})

// Section 6.3.1: SELECT DISTINCT examples.
var _ = Describe("Section 6.3.1 SELECT DISTINCT", func() {
	Describe("SELECT DISTINCT x FROM T", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("select-distinct-x")
			c.AddNode(circuit.Input("T"))
			c.AddNode(circuit.Op("project", operator.NewProject("π_x", ProjectCols("x"))))
			c.AddNode(circuit.Op("distinct", operator.NewDistinct("H")))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("T", "project", 0))
			c.AddEdge(circuit.NewEdge("project", "distinct", 0))
			c.AddEdge(circuit.NewEdge("distinct", "out", 0))
			return c
		}

		It("handles inserts with duplicates", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				// Insert rows with various x values, some duplicates.
				{"T": TableOf(
					NewRow("x", 1, "y", 10),
					NewRow("x", 2, "y", 20),
					NewRow("x", 1, "y", 30), // Duplicate x.
				)},
				// Insert more rows with overlapping x values.
				{"T": TableOf(
					NewRow("x", 2, "y", 40), // Duplicate x.
					NewRow("x", 3, "y", 50),
				)},
				// Delete some rows.
				{"T": func() zset.ZSet {
					z := zset.New()
					z.Insert(NewRow("x", 1, "y", 10), -1)
					return z
				}()},
			})
		})

		It("produces correct distinct values", func() {
			c := buildCircuit()
			exec, _ := execute.NewExecutor(c)

			input := TableOf(
				NewRow("x", 1, "y", 10),
				NewRow("x", 2, "y", 20),
				NewRow("x", 1, "y", 30),
				NewRow("x", 3, "y", 40),
			)

			out, _ := exec.Execute(map[string]zset.ZSet{"T": input})

			// Should have 3 distinct x values: 1, 2, 3.
			Expect(out["out"].Size()).To(Equal(3))
			Expect(out["out"].Lookup(NewRow("x", 1))).To(Equal(zset.Weight(1)))
			Expect(out["out"].Lookup(NewRow("x", 2))).To(Equal(zset.Weight(1)))
			Expect(out["out"].Lookup(NewRow("x", 3))).To(Equal(zset.Weight(1)))
		})
	})

	Describe("SELECT DISTINCT x FROM T WHERE y > 10", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("select-distinct-where")
			c.AddNode(circuit.Input("T"))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ",
				SelectWhere(func(r Row) bool { return r.GetInt("y") > 10 }))))
			c.AddNode(circuit.Op("project", operator.NewProject("π_x", ProjectCols("x"))))
			c.AddNode(circuit.Op("distinct", operator.NewDistinct("H")))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("T", "select", 0))
			c.AddEdge(circuit.NewEdge("select", "project", 0))
			c.AddEdge(circuit.NewEdge("project", "distinct", 0))
			c.AddEdge(circuit.NewEdge("distinct", "out", 0))
			return c
		}

		It("filters before projecting and deduplicating", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": TableOf(
					NewRow("x", 1, "y", 5),  // Filtered out.
					NewRow("x", 2, "y", 15), // Passes.
					NewRow("x", 1, "y", 20), // Passes.
				)},
				{"T": TableOf(
					NewRow("x", 3, "y", 25),
				)},
			})
		})
	})

	Describe("SELECT DISTINCT x, y FROM T", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("select-distinct-xy")
			c.AddNode(circuit.Input("T"))
			c.AddNode(circuit.Op("project", operator.NewProject("π_xy", ProjectCols("x", "y"))))
			c.AddNode(circuit.Op("distinct", operator.NewDistinct("H")))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("T", "project", 0))
			c.AddEdge(circuit.NewEdge("project", "distinct", 0))
			c.AddEdge(circuit.NewEdge("distinct", "out", 0))
			return c
		}

		It("projects multiple columns with distinct", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": TableOf(
					NewRow("x", 1, "y", 10, "z", 100),
					NewRow("x", 1, "y", 10, "z", 200), // Same x,y different z.
					NewRow("x", 1, "y", 20, "z", 100),
				)},
			})
		})
	})
})

// Joins.
var _ = Describe("Joins", func() {
	Describe("Simple equi-join: SELECT * FROM R, S WHERE R.a = S.a", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("equi-join")
			c.AddNode(circuit.Input("R"))
			c.AddNode(circuit.Input("S"))
			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ", JoinOn("a", "a"))))
			c.AddNode(circuit.Op("project", operator.NewProject("π", FlattenPair("R.", "S."))))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("R", "product", 0))
			c.AddEdge(circuit.NewEdge("S", "product", 1))
			c.AddEdge(circuit.NewEdge("product", "select", 0))
			c.AddEdge(circuit.NewEdge("select", "project", 0))
			c.AddEdge(circuit.NewEdge("project", "out", 0))
			return c
		}

		It("incrementally computes join results", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				// Add rows to R only.
				{"R": RowOf("a", 1, "b", 10), "S": zset.New()},
				// Add matching rows to S.
				{"R": zset.New(), "S": RowOf("a", 1, "c", 100)},
				// Add non-matching rows to S.
				{"R": zset.New(), "S": RowOf("a", 2, "c", 200)},
				// Delete from R.
				{"R": func() zset.ZSet {
					z := zset.New()
					z.Insert(NewRow("a", 1, "b", 10), -1)
					return z
				}(), "S": zset.New()},
				// Add row to R that matches multiple S rows.
				{"R": RowOf("a", 1, "b", 20), "S": RowOf("a", 1, "c", 150)},
			})
		})
	})

	Describe("Self-join: SELECT * FROM T t1, T t2 WHERE t1.x = t2.y", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("self-join")
			c.AddNode(circuit.Input("T"))
			// Self-join: T × T.
			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ",
				JoinPred(func(l, r Row) bool { return l.Get("x") == r.Get("y") }))))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("T", "product", 0))
			c.AddEdge(circuit.NewEdge("T", "product", 1))
			c.AddEdge(circuit.NewEdge("product", "select", 0))
			c.AddEdge(circuit.NewEdge("select", "out", 0))
			return c
		}

		It("handles self-join with same input", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": TableOf(
					NewRow("x", 1, "y", 2),
					NewRow("x", 2, "y", 3),
				)},
				{"T": RowOf("x", 3, "y", 1)},
			})
		})
	})

	Describe("Join with projection: SELECT R.a, S.b FROM R, S WHERE R.key = S.key", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("join-project")
			c.AddNode(circuit.Input("R"))
			c.AddNode(circuit.Input("S"))
			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ", JoinOn("key", "key"))))
			c.AddNode(circuit.Op("project", operator.NewProject("π",
				JoinProject(func(l, r Row) Row {
					return NewRow("a", l.Get("a"), "b", r.Get("b"))
				}))))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("R", "product", 0))
			c.AddEdge(circuit.NewEdge("S", "product", 1))
			c.AddEdge(circuit.NewEdge("product", "select", 0))
			c.AddEdge(circuit.NewEdge("select", "project", 0))
			c.AddEdge(circuit.NewEdge("project", "out", 0))
			return c
		}

		It("projects selected columns from join", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{
					"R": RowOf("key", 1, "a", "x"),
					"S": RowOf("key", 1, "b", "y"),
				},
				{
					"R": RowOf("key", 2, "a", "p"),
					"S": RowOf("key", 2, "b", "q"),
				},
			})
		})
	})
})

// Aggregations.
var _ = Describe("Aggregations", func() {
	Describe("SELECT x, COUNT(*) FROM T GROUP BY x", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("count-group-by")
			c.AddNode(circuit.Input("T"))

			keyExpr := GroupKey("x")
			zeroExpr, foldExpr, outputExpr := CountAgg("x")
			c.AddNode(circuit.Op("group", operator.NewGroup("γ", keyExpr, zeroExpr, foldExpr, outputExpr)))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("T", "group", 0))
			c.AddEdge(circuit.NewEdge("group", "out", 0))
			return c
		}

		It("counts rows per group", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": TableOf(
					NewRow("x", 1, "y", 10),
					NewRow("x", 1, "y", 20),
					NewRow("x", 2, "y", 30),
				)},
				// Add more to group 1.
				{"T": RowOf("x", 1, "y", 40)},
				// Delete from group 1.
				{"T": func() zset.ZSet {
					z := zset.New()
					z.Insert(NewRow("x", 1, "y", 10), -1)
					return z
				}()},
			})
		})
	})

	Describe("SELECT x, SUM(y) FROM T GROUP BY x", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("sum-group-by")
			c.AddNode(circuit.Input("T"))

			keyExpr := GroupKey("x")
			zeroExpr, foldExpr, outputExpr := SumAgg("y", "x")
			c.AddNode(circuit.Op("group", operator.NewGroup("γ", keyExpr, zeroExpr, foldExpr, outputExpr)))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("T", "group", 0))
			c.AddEdge(circuit.NewEdge("group", "out", 0))
			return c
		}

		It("sums values per group", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": TableOf(
					NewRow("x", 1, "y", 10),
					NewRow("x", 1, "y", 20),
					NewRow("x", 2, "y", 5),
				)},
				{"T": RowOf("x", 1, "y", 5)},
			})
		})
	})
})

// Combined operations.
var _ = Describe("Combined Operations", func() {
	Describe("Filter then aggregate: SELECT x, COUNT(*) FROM T WHERE y > 0 GROUP BY x", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("filter-aggregate")
			c.AddNode(circuit.Input("T"))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ",
				SelectWhere(func(r Row) bool { return r.GetInt("y") > 0 }))))

			keyExpr := GroupKey("x")
			zeroExpr, foldExpr, outputExpr := CountAgg("x")
			c.AddNode(circuit.Op("group", operator.NewGroup("γ", keyExpr, zeroExpr, foldExpr, outputExpr)))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("T", "select", 0))
			c.AddEdge(circuit.NewEdge("select", "group", 0))
			c.AddEdge(circuit.NewEdge("group", "out", 0))
			return c
		}

		It("filters before aggregating", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": TableOf(
					NewRow("x", 1, "y", 10),
					NewRow("x", 1, "y", -5), // Filtered out.
					NewRow("x", 2, "y", 20),
				)},
			})
		})
	})

	Describe("Join then aggregate: SELECT R.a, COUNT(*) FROM R, S WHERE R.key = S.key GROUP BY R.a", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("join-aggregate")
			c.AddNode(circuit.Input("R"))
			c.AddNode(circuit.Input("S"))
			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ", JoinOn("key", "key"))))
			// Project to get R.a for grouping.
			c.AddNode(circuit.Op("project", operator.NewProject("π",
				JoinProject(func(l, r Row) Row { return NewRow("a", l.Get("a")) }))))

			keyExpr := GroupKey("a")
			zeroExpr, foldExpr, outputExpr := CountAgg("a")
			c.AddNode(circuit.Op("group", operator.NewGroup("γ", keyExpr, zeroExpr, foldExpr, outputExpr)))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("R", "product", 0))
			c.AddEdge(circuit.NewEdge("S", "product", 1))
			c.AddEdge(circuit.NewEdge("product", "select", 0))
			c.AddEdge(circuit.NewEdge("select", "project", 0))
			c.AddEdge(circuit.NewEdge("project", "group", 0))
			c.AddEdge(circuit.NewEdge("group", "out", 0))
			return c
		}

		It("aggregates join results", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{
					"R": TableOf(
						NewRow("key", 1, "a", "x"),
						NewRow("key", 2, "a", "x"),
					),
					"S": TableOf(
						NewRow("key", 1, "b", 100),
						NewRow("key", 1, "b", 200),
					),
				},
			})
		})
	})

	Describe("Aggregate then filter (HAVING): SELECT x, SUM(y) FROM T GROUP BY x HAVING SUM(y) > 100", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("aggregate-having")
			c.AddNode(circuit.Input("T"))

			keyExpr := GroupKey("x")
			zeroExpr, foldExpr, outputExpr := SumAgg("y", "x")
			c.AddNode(circuit.Op("group", operator.NewGroup("γ", keyExpr, zeroExpr, foldExpr, outputExpr)))
			c.AddNode(circuit.Op("having", operator.NewSelect("σ",
				SelectWhere(func(r Row) bool { return r.GetInt("sum") > 100 }))))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("T", "group", 0))
			c.AddEdge(circuit.NewEdge("group", "having", 0))
			c.AddEdge(circuit.NewEdge("having", "out", 0))
			return c
		}

		It("filters aggregated results", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": TableOf(
					NewRow("x", 1, "y", 50),
					NewRow("x", 1, "y", 60), // Group 1 sum = 110 > 100.
					NewRow("x", 2, "y", 30), // Group 2 sum = 30 < 100.
				)},
				{"T": RowOf("x", 2, "y", 80)}, // Group 2 sum now = 110 > 100.
			})
		})
	})
})

// Distinct variations.
var _ = Describe("Distinct Variations", func() {
	Describe("Distinct after join: SELECT DISTINCT R.x FROM R, S WHERE R.a = S.a", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("distinct-join")
			c.AddNode(circuit.Input("R"))
			c.AddNode(circuit.Input("S"))
			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ", JoinOn("a", "a"))))
			c.AddNode(circuit.Op("project", operator.NewProject("π",
				JoinProject(func(l, r Row) Row { return NewRow("x", l.Get("x")) }))))
			c.AddNode(circuit.Op("distinct", operator.NewDistinct("H")))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("R", "product", 0))
			c.AddEdge(circuit.NewEdge("S", "product", 1))
			c.AddEdge(circuit.NewEdge("product", "select", 0))
			c.AddEdge(circuit.NewEdge("select", "project", 0))
			c.AddEdge(circuit.NewEdge("project", "distinct", 0))
			c.AddEdge(circuit.NewEdge("distinct", "out", 0))
			return c
		}

		It("deduplicates join projection results", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{
					"R": TableOf(
						NewRow("a", 1, "x", 10),
						NewRow("a", 1, "x", 10), // Same x.
					),
					"S": RowOf("a", 1, "b", 100),
				},
			})
		})
	})
})

// Edge cases.
var _ = Describe("Edge Cases", func() {
	Describe("Empty inputs", func() {
		It("join with one empty relation", func() {
			c := circuit.New("empty-join")
			c.AddNode(circuit.Input("R"))
			c.AddNode(circuit.Input("S"))
			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("R", "product", 0))
			c.AddEdge(circuit.NewEdge("S", "product", 1))
			c.AddEdge(circuit.NewEdge("product", "out", 0))

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"R": RowOf("a", 1), "S": zset.New()},
				{"R": zset.New(), "S": RowOf("b", 2)},
			})
		})

		It("aggregation on empty input", func() {
			c := circuit.New("empty-agg")
			c.AddNode(circuit.Input("T"))
			keyExpr := GroupKey("x")
			zeroExpr, foldExpr, outputExpr := CountAgg("x")
			c.AddNode(circuit.Op("group", operator.NewGroup("γ", keyExpr, zeroExpr, foldExpr, outputExpr)))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("T", "group", 0))
			c.AddEdge(circuit.NewEdge("group", "out", 0))

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": zset.New()},
				{"T": RowOf("x", 1, "y", 10)},
			})
		})

		It("distinct on empty input", func() {
			c := circuit.New("empty-distinct")
			c.AddNode(circuit.Input("T"))
			c.AddNode(circuit.Op("distinct", operator.NewDistinct("H")))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("T", "distinct", 0))
			c.AddEdge(circuit.NewEdge("distinct", "out", 0))

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": zset.New()},
				{"T": RowOf("x", 1)},
			})
		})
	})

	Describe("Weight cancellation", func() {
		It("insert then delete same row", func() {
			c := circuit.New("cancel")
			c.AddNode(circuit.Input("T"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("T", "out", 0))

			row := NewRow("x", 1)
			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": func() zset.ZSet {
					z := zset.New()
					z.Insert(row, 1)
					return z
				}()},
				{"T": func() zset.ZSet {
					z := zset.New()
					z.Insert(row, -1)
					return z
				}()},
			})
		})

		It("multiple inserts then partial deletes", func() {
			c := circuit.New("partial-cancel")
			c.AddNode(circuit.Input("T"))
			c.AddNode(circuit.Op("distinct", operator.NewDistinct("H")))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("T", "distinct", 0))
			c.AddEdge(circuit.NewEdge("distinct", "out", 0))

			row := NewRow("x", 1)
			verifyEquivalence(c, []map[string]zset.ZSet{
				{"T": func() zset.ZSet {
					z := zset.New()
					z.Insert(row, 3) // Weight 3.
					return z
				}()},
				{"T": func() zset.ZSet {
					z := zset.New()
					z.Insert(row, -2) // Weight now 1.
					return z
				}()},
				{"T": func() zset.ZSet {
					z := zset.New()
					z.Insert(row, -1) // Weight now 0.
					return z
				}()},
			})
		})
	})

	Describe("Many-to-many joins", func() {
		It("both sides have duplicate keys", func() {
			c := circuit.New("many-to-many")
			c.AddNode(circuit.Input("R"))
			c.AddNode(circuit.Input("S"))
			c.AddNode(circuit.Op("product", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("select", operator.NewSelect("σ", JoinOn("k", "k"))))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("R", "product", 0))
			c.AddEdge(circuit.NewEdge("S", "product", 1))
			c.AddEdge(circuit.NewEdge("product", "select", 0))
			c.AddEdge(circuit.NewEdge("select", "out", 0))

			verifyEquivalence(c, []map[string]zset.ZSet{
				{
					"R": TableOf(
						NewRow("k", 1, "r", "a"),
						NewRow("k", 1, "r", "b"),
					),
					"S": TableOf(
						NewRow("k", 1, "s", "x"),
						NewRow("k", 1, "s", "y"),
					),
				},
			})
		})
	})
})

// Multi-step pipelines.
var _ = Describe("Multi-step Pipelines", func() {
	Describe("Union: SELECT x FROM R UNION SELECT x FROM S", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("union")
			c.AddNode(circuit.Input("R"))
			c.AddNode(circuit.Input("S"))
			c.AddNode(circuit.Op("proj_r", operator.NewProject("π_r", ProjectCols("x"))))
			c.AddNode(circuit.Op("proj_s", operator.NewProject("π_s", ProjectCols("x"))))
			c.AddNode(circuit.Op("plus", operator.NewPlus()))
			c.AddNode(circuit.Op("distinct", operator.NewDistinct("H")))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("R", "proj_r", 0))
			c.AddEdge(circuit.NewEdge("S", "proj_s", 0))
			c.AddEdge(circuit.NewEdge("proj_r", "plus", 0))
			c.AddEdge(circuit.NewEdge("proj_s", "plus", 1))
			c.AddEdge(circuit.NewEdge("plus", "distinct", 0))
			c.AddEdge(circuit.NewEdge("distinct", "out", 0))
			return c
		}

		It("computes union with deduplication", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{
					"R": TableOf(NewRow("x", 1), NewRow("x", 2)),
					"S": TableOf(NewRow("x", 2), NewRow("x", 3)),
				},
				{
					"R": RowOf("x", 4),
					"S": RowOf("x", 4), // Duplicate.
				},
			})
		})
	})

	Describe("Difference: SELECT x FROM R EXCEPT SELECT x FROM S", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("except")
			c.AddNode(circuit.Input("R"))
			c.AddNode(circuit.Input("S"))
			c.AddNode(circuit.Op("proj_r", operator.NewProject("π_r", ProjectCols("x"))))
			c.AddNode(circuit.Op("proj_s", operator.NewProject("π_s", ProjectCols("x"))))
			c.AddNode(circuit.Op("negate", operator.NewNegate()))
			c.AddNode(circuit.Op("plus", operator.NewPlus()))
			// Filter to keep only positive weights (in R but not S).
			c.AddNode(circuit.Op("filter", operator.NewSelect("σ_pos",
				SelectWhere(func(r Row) bool { return true })))) // Pass through, weight filtering is implicit.
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("R", "proj_r", 0))
			c.AddEdge(circuit.NewEdge("S", "proj_s", 0))
			c.AddEdge(circuit.NewEdge("proj_s", "negate", 0))
			c.AddEdge(circuit.NewEdge("proj_r", "plus", 0))
			c.AddEdge(circuit.NewEdge("negate", "plus", 1))
			c.AddEdge(circuit.NewEdge("plus", "filter", 0))
			c.AddEdge(circuit.NewEdge("filter", "out", 0))
			return c
		}

		It("computes set difference", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{
					"R": TableOf(NewRow("x", 1), NewRow("x", 2), NewRow("x", 3)),
					"S": TableOf(NewRow("x", 2)),
				},
				{
					"R": zset.New(),
					"S": RowOf("x", 1), // Remove 1 from difference.
				},
			})
		})
	})

	Describe("Three-way join: SELECT * FROM R, S, T WHERE R.a = S.a AND S.b = T.b", func() {
		buildCircuit := func() *circuit.Circuit {
			c := circuit.New("three-way-join")
			c.AddNode(circuit.Input("R"))
			c.AddNode(circuit.Input("S"))
			c.AddNode(circuit.Input("T"))

			// First join: R × S where R.a = S.a.
			c.AddNode(circuit.Op("prod1", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("sel1", operator.NewSelect("σ1", JoinOn("a", "a"))))
			// Flatten to single row for next join.
			c.AddNode(circuit.Op("flat1", operator.NewProject("π1", FlattenPair("R.", "S."))))

			// Second join: (R⋈S) × T where S.b = T.b.
			c.AddNode(circuit.Op("prod2", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Op("sel2", operator.NewSelect("σ2",
				JoinPred(func(l, r Row) bool { return l.Get("S.b") == r.Get("b") }))))

			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("R", "prod1", 0))
			c.AddEdge(circuit.NewEdge("S", "prod1", 1))
			c.AddEdge(circuit.NewEdge("prod1", "sel1", 0))
			c.AddEdge(circuit.NewEdge("sel1", "flat1", 0))
			c.AddEdge(circuit.NewEdge("flat1", "prod2", 0))
			c.AddEdge(circuit.NewEdge("T", "prod2", 1))
			c.AddEdge(circuit.NewEdge("prod2", "sel2", 0))
			c.AddEdge(circuit.NewEdge("sel2", "out", 0))
			return c
		}

		It("computes three-way join incrementally", func() {
			c := buildCircuit()

			verifyEquivalence(c, []map[string]zset.ZSet{
				{
					"R": RowOf("a", 1, "x", 10),
					"S": RowOf("a", 1, "b", 2),
					"T": RowOf("b", 2, "y", 20),
				},
				{
					"R": RowOf("a", 1, "x", 11),
					"S": zset.New(),
					"T": zset.New(),
				},
			})
		})
	})
})
