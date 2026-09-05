package operator

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/internal/testutils"
	"github.com/l7mp/dbsp/engine/zset"
)

var _ = Describe("Delay", func() {
	elem := func(id string) zset.ZSet {
		z := zset.New()
		z.Insert(testutils.StringElem(id), 1)
		return z
	}

	// step runs one timestep against a paired (emit, absorb): the executor's
	// topological order evaluates the emit half before the absorb half.
	step := func(emit *DelayOp, absorb *DelayAbsorbOp, in zset.ZSet) zset.ZSet {
		out, err := emit.Apply(nil)
		Expect(err).NotTo(HaveOccurred())
		_, err = absorb.Apply(nil, in)
		Expect(err).NotTo(HaveOccurred())
		return out
	}

	It("delays by one step at k = 1", func() {
		emit, absorb := NewDelay(1)
		Expect(emit.K()).To(Equal(1))
		Expect(emit.String()).To(Equal("z⁻¹"))

		Expect(step(emit, absorb, elem("a")).IsZero()).To(BeTrue())
		Expect(step(emit, absorb, elem("b")).Equal(elem("a"))).To(BeTrue())
		Expect(step(emit, absorb, elem("c")).Equal(elem("b"))).To(BeTrue())
	})

	It("delays by k steps at k = 3", func() {
		emit, absorb := NewDelay(3)
		Expect(emit.K()).To(Equal(3))
		Expect(emit.String()).To(Equal("z⁻3"))

		Expect(step(emit, absorb, elem("a")).IsZero()).To(BeTrue())
		Expect(step(emit, absorb, elem("b")).IsZero()).To(BeTrue())
		Expect(step(emit, absorb, elem("c")).IsZero()).To(BeTrue())
		Expect(step(emit, absorb, elem("d")).Equal(elem("a"))).To(BeTrue())
		Expect(step(emit, absorb, elem("e")).Equal(elem("b"))).To(BeTrue())
		Expect(step(emit, absorb, elem("f")).Equal(elem("c"))).To(BeTrue())
	})

	It("matches a chain of k unit delays", func() {
		e1, a1 := NewDelay(1)
		e2, a2 := NewDelay(1)
		emitK, absorbK := NewDelay(2)

		ins := []string{"a", "b", "c", "d", "e"}
		for _, id := range ins {
			mid := step(e1, a1, elem(id))
			chained := step(e2, a2, mid)
			ring := step(emitK, absorbK, elem(id))
			Expect(ring.Equal(chained)).To(BeTrue(), "input %s", id)
		}
	})

	It("clamps k below 1 to 1", func() {
		emit, _ := NewDelay(0)
		Expect(emit.K()).To(Equal(1))
	})

	It("resets the whole ring and pre-seeds the next emission on Set", func() {
		emit, absorb := NewDelay(3)
		step(emit, absorb, elem("a"))
		step(emit, absorb, elem("b"))

		emit.Set(elem("s"))
		Expect(step(emit, absorb, elem("c")).Equal(elem("s"))).To(BeTrue())
		Expect(step(emit, absorb, elem("d")).IsZero()).To(BeTrue())
		Expect(step(emit, absorb, elem("e")).IsZero()).To(BeTrue())
		Expect(step(emit, absorb, elem("f")).Equal(elem("c"))).To(BeTrue())
	})

	It("round-trips the depth through JSON", func() {
		emit, _ := NewDelay(5)
		data, err := json.Marshal(emit)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(MatchJSON(`{"type":"delay","k":5}`))

		op, err := UnmarshalOperator(data)
		Expect(err).NotTo(HaveOccurred())
		Expect(op.(*DelayOp).K()).To(Equal(5))
	})

	It("round-trips the unit delay without a k field", func() {
		emit, _ := NewDelay(1)
		data, err := json.Marshal(emit)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(MatchJSON(`{"type":"delay"}`))

		op, err := UnmarshalOperator(data)
		Expect(err).NotTo(HaveOccurred())
		Expect(op.(*DelayOp).K()).To(Equal(1))
	})
})
