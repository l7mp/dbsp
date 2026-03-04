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
	})

	Describe("insert", func() {
		BeforeEach(func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
		})

		It("inserts a document with default weight 1", func() {
			Expect(run("zset", "insert", "foo", `{"id":1}`)).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(1))
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(1)))
		})

		It("inserts a document with a custom weight", func() {
			Expect(run("zset", "insert", "foo", `{"id":1}`, "--weight", "-2")).To(Succeed())
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(-2)))
		})

		It("accumulates weight for the same document", func() {
			Expect(run("zset", "insert", "foo", `{"id":1}`)).To(Succeed())
			Expect(run("zset", "insert", "foo", `{"id":1}`, "--weight", "2")).To(Succeed())
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(3)))
		})

		It("removes the entry when cumulative weight reaches zero", func() {
			Expect(run("zset", "insert", "foo", `{"id":1}`)).To(Succeed())
			Expect(run("zset", "insert", "foo", `{"id":1}`, "--weight", "-1")).To(Succeed())
			Expect(state.zsets["foo"].data.Size()).To(Equal(0))
		})

		It("errors on unknown zset name", func() {
			Expect(run("zset", "insert", "nope", `{"id":1}`)).
				To(MatchError(ContainSubstring("not found")))
		})
	})

	Describe("weight", func() {
		BeforeEach(func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
			Expect(run("zset", "insert", "foo", `{"id":1}`)).To(Succeed())
		})

		It("changes the weight of an entry by 1-based index", func() {
			Expect(run("zset", "weight", "foo", "1", "5")).To(Succeed())
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(5)))
		})

		It("errors on out-of-range index", func() {
			Expect(run("zset", "weight", "foo", "99", "1")).To(MatchError(ContainSubstring("out of range")))
		})
	})

	Describe("negate", func() {
		It("flips all weights", func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
			Expect(run("zset", "insert", "foo", `{"id":1}`)).To(Succeed())
			Expect(run("zset", "negate", "foo")).To(Succeed())
			entries := sortedEntries(state.zsets["foo"].data)
			Expect(entries[0].Weight).To(Equal(zset.Weight(-1)))
		})
	})

	Describe("clear", func() {
		It("removes all entries", func() {
			Expect(run("zset", "create", "foo")).To(Succeed())
			Expect(run("zset", "insert", "foo", `{"id":1}`)).To(Succeed())
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
			Expect(run("zset", "insert", "input-data", `{"id":1}`)).To(Succeed())
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
