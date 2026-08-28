package transform

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/operator"
)

var _ = Describe("Incrementalizer and time-variant expressions", func() {
	build := func(src string) *circuit.Circuit {
		proj, err := dbspexpr.CompileString(src)
		Expect(err).NotTo(HaveOccurred())
		c := circuit.New("tv")
		Expect(c.AddNode(circuit.Input("in"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("proj", operator.NewProject(proj)))).To(Succeed())
		Expect(c.AddNode(circuit.Output("out"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("in", "proj", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("proj", "out", 0))).To(Succeed())
		return c
	}

	It("rejects a projection that reads the clock", func() {
		_, err := NewIncrementalizer().Transform(build(`{"id":"$.id","at":{"@now":null}}`))
		Expect(err).To(MatchError("incrementalizer: node proj: expression @now is not time-invariant"))
	})

	It("rejects a random draw nested in a branch", func() {
		_, err := NewIncrementalizer().Transform(build(`{"id":{"@cond":[true,{"@rnd":[1,9]},0]}}`))
		Expect(err).To(MatchError(And(ContainSubstring("@rnd"), ContainSubstring("is not time-invariant"))))
	})

	It("accepts a pure projection", func() {
		_, err := NewIncrementalizer().Transform(build(`{"id":"$.id","h":{"@hash":"$.id"}}`))
		Expect(err).NotTo(HaveOccurred())
	})
})
