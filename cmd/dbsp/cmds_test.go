package main

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/dbsp/zset"
)

// simpleCircuit is a helper that builds a minimal pass-through circuit
// (input → output) in state and sets it as the current circuit.
func simpleCircuit(run func(...string) error, name string) {
	ExpectWithOffset(1, run("circuit", "create", name)).To(Succeed())
	ExpectWithOffset(1, run("circuit", "node", "add", name, "in", "input")).To(Succeed())
	ExpectWithOffset(1, run("circuit", "node", "add", name, "out", "output")).To(Succeed())
	ExpectWithOffset(1, run("circuit", "edge", "add", name, "in", "out", "0")).To(Succeed())
}

var _ = Describe("ZSet commands", func() {
	var (
		state *appState
		run   func(...string) error
	)

	BeforeEach(func() {
		state, run = newTestEnv()
	})

	Describe("create", func() {
		It("creates a new Z-set", func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
			Expect(state.zsets).To(HaveKey("foo"))
		})

		It("errors on duplicate name", func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
			Expect(run("zset", "create", "foo")).To(MatchError(ContainSubstring("already exists")))
		})

		It("errors when --table is given", func() {
			Expect(run("zset", "create", "foo", "--table", "t")).
				To(MatchError(ContainSubstring("not yet implemented")))
		})

		It("accepts --pk shorthand expression", func() {
			Expect(run("zset", "create", "foo", "--pk", "$.id")).To(Succeed())
		})

	})

	Describe("insert", func() {
		BeforeEach(func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
		})

		It("inserts a single document", func() {
			Expect(run("zset", "insert", "foo", `({"id":1},1)`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(1))
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(1)))
		})

		It("inserts a single document with a custom weight", func() {
			Expect(run("zset", "insert", "foo", `({"id":1},-2)`)).To(Succeed())
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(-2)))
		})

		It("accumulates weight for the same document", func() {
			Expect(run("zset", "insert", "foo", `({"id":1},1)`)).To(Succeed())
			Expect(run("zset", "insert", "foo", `({"id":1},2)`)).To(Succeed())
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(3)))
		})

		It("removes the entry when cumulative weight reaches zero", func() {
			Expect(run("zset", "insert", "foo", `({"id":1},1)`)).To(Succeed())
			Expect(run("zset", "insert", "foo", `({"id":1},-1)`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(0))
		})

		It("errors on unknown zset name", func() {
			Expect(run("zset", "insert", "nope", `({"id":1},1)`)).
				To(MatchError(ContainSubstring("not found")))
		})

		It("inserts multiple documents from a list literal", func() {
			Expect(run("zset", "insert", "foo", `[({"id":1},2),({"id":2},3)]`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(2))
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(2)))
			Expect(entries[1].Weight).To(Equal(zset.Weight(3)))
		})

		It("accumulates list-form inserts into existing entries", func() {
			Expect(run("zset", "insert", "foo", `({"id":1},1)`)).To(Succeed())
			Expect(run("zset", "insert", "foo", `[({"id":1},4)]`)).To(Succeed())
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(5)))
		})
	})

	Describe("weight", func() {
		BeforeEach(func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
			Expect(run("zset", "insert", "foo", `({"id":1},1)`)).To(Succeed())
		})

		It("sets the absolute weight of a document", func() {
			Expect(run("zset", "weight", "foo", `({"id":1},5)`)).To(Succeed())
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(5)))
		})

		It("inserts a new document if it did not exist", func() {
			Expect(run("zset", "weight", "foo", `({"id":99},3)`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(2))
		})

		It("removes a document when weight is set to zero", func() {
			Expect(run("zset", "weight", "foo", `({"id":1},0)`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(0))
		})
	})

	Describe("negate", func() {
		It("flips all weights", func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
			Expect(run("zset", "insert", "foo", `({"id":1},1)`)).To(Succeed())
			Expect(run("zset", "negate", "foo")).To(Succeed())
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(-1)))
		})
	})

	Describe("set", func() {
		BeforeEach(func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
		})

		It("replaces contents with the given weighted entries", func() {
			Expect(run("zset", "set", "foo", `[( {"id":1} , 2 ),( {"id":2} , 3 )]`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(2))
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(2)))
			Expect(entries[1].Weight).To(Equal(zset.Weight(3)))
		})

		It("replaces existing contents (not accumulates)", func() {
			Expect(run("zset", "insert", "foo", `({"id":1},1)`)).To(Succeed())
			Expect(run("zset", "set", "foo", `[({"id":2},5)]`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(1))
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(5)))
		})

		It("supports negative weights", func() {
			Expect(run("zset", "set", "foo", `[({"id":1},1),({"id":2},-1)]`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(2))
		})

		It("handles nested JSON values", func() {
			Expect(run("zset", "set", "foo", `[({"a":{"b":1}},3)]`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(1))
		})

		It("also accepts a single-tuple form", func() {
			Expect(run("zset", "set", "foo", `({"id":1},7)`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(1))
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(7)))
		})

		It("errors on malformed input", func() {
			Expect(run("zset", "set", "foo", `not-a-list`)).To(MatchError(ContainSubstring("expected")))
			Expect(run("zset", "set", "foo", `[(bad-json,1)]`)).To(MatchError(ContainSubstring("JSON")))
			Expect(run("zset", "set", "foo", `[({"id":1},bad)]`)).To(MatchError(ContainSubstring("weight")))
		})

		It("errors on unknown zset name", func() {
			Expect(run("zset", "set", "nope", `[({"id":1},1)]`)).
				To(MatchError(ContainSubstring("not found")))
		})
	})

	Describe("clear", func() {
		It("removes all entries", func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
			Expect(run("zset", "insert", "foo", `({"id":1},1)`)).To(Succeed())
			Expect(run("zset", "clear", "foo")).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(0))
		})
	})

	Describe("delete", func() {
		It("removes the Z-set from state", func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
			Expect(run("zset", "delete", "foo")).To(Succeed())
			Expect(state.zsets).NotTo(HaveKey("foo"))
		})

		It("errors on unknown name", func() {
			Expect(run("zset", "delete", "nope")).To(MatchError(ContainSubstring("not found")))
		})
	})

	Describe("primary key binding", func() {
		k8sPKExpr := `{"@cond":[{"@exists":"metadata.namespace"},{"@concat":["$.metadata.name","/","$.metadata.namespace"]},{"@concat":["$.metadata.name","/",""]}]}`

		It("computes PrimaryKey from --pk expression", func() {
			Expect(run("zset", "create", "foo", "--pk", "$.id")).To(Succeed())
			Expect(run("zset", "insert", "foo", `({"id":123,"x":1},1)`)).To(Succeed())

			entries := sortedEntries(state.zsets["foo"].data)
			pk, err := entries[0].Document.PrimaryKey()
			Expect(err).NotTo(HaveOccurred())
			Expect(pk).To(Equal("123"))
		})

		It("returns PrimaryKey error when evaluation fails", func() {
			Expect(run("zset", "create", "foo", "--pk", "$.missing.deep")).To(Succeed())
			Expect(run("zset", "insert", "foo", `({"id":1},1)`)).To(Succeed())

			entries := sortedEntries(state.zsets["foo"].data)
			_, err := entries[0].Document.PrimaryKey()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("primary key expression evaluate"))
		})

		It("builds Kubernetes primary key name/namespace with @exists + @concat", func() {
			Expect(run("zset", "create", "k8s", "--pk", k8sPKExpr)).To(Succeed())
			Expect(run("zset", "insert", "k8s", `({"metadata":{"name":"pod-a","namespace":"ns-a"}},1)`)).To(Succeed())
			Expect(run("zset", "insert", "k8s", `({"metadata":{"name":"pod-b"}},1)`)).To(Succeed())
			Expect(run("zset", "insert", "k8s", `({"metadata":{"name":"pod-c","namespace":42}},1)`)).To(Succeed())

			entries := sortedEntries(state.zsets["k8s"].data)
			pks := map[string]bool{}
			for _, e := range entries {
				pk, err := e.Document.PrimaryKey()
				Expect(err).NotTo(HaveOccurred())
				pks[pk] = true
			}

			Expect(pks).To(HaveKey("pod-a/ns-a"))
			Expect(pks).To(HaveKey("pod-b/"))
			Expect(pks).To(HaveKey("pod-c/42"))
		})

		It("returns PrimaryKey error when metadata.name is missing", func() {
			Expect(run("zset", "create", "k8s-bad", "--pk", k8sPKExpr)).To(Succeed())
			Expect(run("zset", "insert", "k8s-bad", `({"metadata":{"namespace":"ns-a"}},1)`)).To(Succeed())

			entries := sortedEntries(state.zsets["k8s-bad"].data)
			_, err := entries[0].Document.PrimaryKey()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("primary key expression evaluate"))
		})
	})
})

var _ = Describe("Circuit commands", func() {
	var (
		state *appState
		run   func(...string) error
	)

	BeforeEach(func() {
		state, run = newTestEnv()
	})

	Describe("create", func() {
		It("creates a circuit", func() {
			Expect(run("circuit", "create", "c")).To(Succeed())
			Expect(state.circuits).To(HaveKey("c"))
		})

		It("errors on duplicate name", func() {
			Expect(run("circuit", "create", "c")).To(Succeed())
			Expect(run("circuit", "create", "c")).To(MatchError(ContainSubstring("already exists")))
		})
	})

	Describe("node operations", func() {
		BeforeEach(func() {
			Expect(run("circuit", "create", "c")).To(Succeed())
		})

		It("adds an input node", func() {
			Expect(run("circuit", "node", "add", "c", "in", "input")).To(Succeed())
			Expect(state.circuits["c"].Node("in")).NotTo(BeNil())
		})

		It("deletes a node", func() {
			Expect(run("circuit", "node", "add", "c", "in", "input")).To(Succeed())
			Expect(run("circuit", "node", "delete", "c", "in")).To(Succeed())
			Expect(state.circuits["c"].Node("in")).To(BeNil())
		})

		It("sets a node state from a Z-set", func() {
			Expect(run("circuit", "node", "add", "c", "int", "integrate")).To(Succeed())
			Expect(run("zset", "create", "seed")).To(Succeed())
			Expect(run("zset", "set", "seed", `({"id":1},2)`)).To(Succeed())

			Expect(run("circuit", "node", "set", "c", "int", "seed")).To(Succeed())

			node := state.circuits["c"].Node("int")
			out, err := node.Apply(zset.New())
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(1))
			entries := sortedEntries(out)
			Expect(entries[0].Weight).To(Equal(zset.Weight(2)))
		})
	})

	Describe("edge operations", func() {
		BeforeEach(func() {
			simpleCircuit(run, "c")
		})

		It("connects two nodes", func() {
			Expect(state.circuits["c"].Edges()).To(HaveLen(1))
		})

		It("deletes an edge", func() {
			Expect(run("circuit", "edge", "delete", "c", "in", "out", "0")).To(Succeed())
			Expect(state.circuits["c"].Edges()).To(BeEmpty())
		})
	})

	Describe("validate", func() {
		It("succeeds for a well-formed circuit", func() {
			simpleCircuit(run, "c")
			Expect(run("circuit", "validate", "c")).To(Succeed())
		})
	})

	Describe("incrementalize", func() {
		BeforeEach(func() {
			simpleCircuit(run, "c")
		})

		It("stores the incremental circuit under a new name", func() {
			Expect(run("circuit", "incrementalize", "c", "c-inc")).To(Succeed())
			Expect(state.circuits).To(HaveKey("c-inc"))
		})

		It("errors when the target name already exists", func() {
			Expect(run("circuit", "incrementalize", "c", "c-inc")).To(Succeed())
			Expect(run("circuit", "incrementalize", "c", "c-inc")).
				To(MatchError(ContainSubstring("already exists")))
		})
	})

	Describe("delete", func() {
		It("removes the circuit from state", func() {
			Expect(run("circuit", "create", "c")).To(Succeed())
			Expect(run("circuit", "delete", "c")).To(Succeed())
			Expect(state.circuits).NotTo(HaveKey("c"))
		})
	})
})

var _ = Describe("Executor commands", func() {
	var (
		state *appState
		run   func(...string) error
	)

	BeforeEach(func() {
		state, run = newTestEnv()
		simpleCircuit(run, "c")
	})

	Describe("create", func() {
		It("creates an executor", func() {
			Expect(run("executor", "create", "e", "--circuit", "c")).To(Succeed())
			Expect(state.executors).To(HaveKey("e"))
			Expect(state.executors["e"].circuitName).To(Equal("c"))
		})

		It("errors when --circuit is omitted", func() {
			Expect(run("executor", "create", "e")).
				To(MatchError(ContainSubstring("circuit name required")))
		})

		It("errors on unknown circuit name", func() {
			Expect(run("executor", "create", "e", "--circuit", "nope")).
				To(MatchError(ContainSubstring("not found")))
		})

		It("errors on duplicate executor name", func() {
			Expect(run("executor", "create", "e", "--circuit", "c")).To(Succeed())
			Expect(run("executor", "create", "e", "--circuit", "c")).
				To(MatchError(ContainSubstring("already exists")))
		})
	})

	Describe("execute", func() {
		BeforeEach(func() {
			Expect(run("executor", "create", "e", "--circuit", "c")).To(Succeed())
			Expect(run("zset", "create", "input-data")).To(Succeed())
			Expect(run("zset", "insert", "input-data", `({"id":1},1)`)).To(Succeed())
		})

		It("stores the output Z-set under the assigned name", func() {
			Expect(run("executor", "execute", "e", "in=input-data", "out=result")).To(Succeed())
			Expect(state.zsets).To(HaveKey("result"))
			Expect(state.zsets["result"].data.Size()).To(Equal(1))
		})

		It("overwrites an existing Z-set with the same name", func() {
			Expect(run("zset", "create", "result")).To(Succeed())
			Expect(run("executor", "execute", "e", "in=input-data", "out=result")).To(Succeed())
			Expect(state.zsets["result"].data.Size()).To(Equal(1))
		})

		It("defaults the output name to <executor>-<node> for a single output", func() {
			Expect(run("executor", "execute", "e", "in=input-data")).To(Succeed())
			Expect(state.zsets).To(HaveKey("e-out"))
		})

		It("passes the document through a pass-through circuit unchanged", func() {
			Expect(run("executor", "execute", "e", "in=input-data", "out=r")).To(Succeed())
			outEntries := sortedEntries(state.zsets["r"].data)
			inEntries := sortedEntries(state.zsets["input-data"].data)
			Expect(outEntries[0].Document.Hash()).To(Equal(inEntries[0].Document.Hash()))
		})

		It("errors on an unknown input Z-set", func() {
			Expect(run("executor", "execute", "e", "in=nope")).
				To(MatchError(ContainSubstring("not found")))
		})
	})

	Describe("incrementalize", func() {
		BeforeEach(func() {
			Expect(run("executor", "create", "e", "--circuit", "c")).To(Succeed())
		})

		It("creates an incremental circuit and executor", func() {
			Expect(run("executor", "incrementalize", "e", "e-inc")).To(Succeed())
			Expect(state.circuits).To(HaveKey("c-inc"))
			Expect(state.executors).To(HaveKey("e-inc"))
			Expect(state.executors["e-inc"].circuitName).To(Equal("c-inc"))
		})

		It("errors if the incremental circuit already exists", func() {
			Expect(run("executor", "incrementalize", "e", "e-inc")).To(Succeed())
			Expect(run("executor", "incrementalize", "e", "e-inc2")).
				To(MatchError(ContainSubstring("already exists")))
		})

		It("errors if the new executor name already exists", func() {
			Expect(run("executor", "create", "e-inc", "--circuit", "c")).To(Succeed())
			Expect(run("executor", "incrementalize", "e", "e-inc")).
				To(MatchError(ContainSubstring("already exists")))
		})
	})

	Describe("reset", func() {
		It("resets executor state without error", func() {
			Expect(run("executor", "create", "e", "--circuit", "c")).To(Succeed())
			Expect(run("executor", "reset", "e")).To(Succeed())
		})
	})

	Describe("delete", func() {
		It("removes the executor from state", func() {
			Expect(run("executor", "create", "e", "--circuit", "c")).To(Succeed())
			Expect(run("executor", "delete", "e")).To(Succeed())
			Expect(state.executors).NotTo(HaveKey("e"))
		})
	})
})
