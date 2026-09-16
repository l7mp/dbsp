package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/zset"
)

// testObject is a connector's native object: keyed on "name", carrying a
// "v" the plant records.
type testObject struct {
	name string
	v    string
}

// testPlant is a minimal plant: a keyed store that can be made to refuse
// or to go unreachable.
type testPlant struct {
	mu      sync.Mutex
	state   map[string]string
	writes  []string
	fail    ApplyResult
	failing int
	suggest time.Duration
}

func newPlant() *testPlant {
	return &testPlant{state: map[string]string{}}
}

// Adapt implements Adapter: a document must carry a name to be writable.
func (p *testPlant) Adapt(doc datamodel.Document) (string, any, error) {
	fields := doc.Fields()
	name, ok := fields["name"].(string)
	if !ok || name == "" {
		return "", nil, fmt.Errorf("document carries no name")
	}
	v, _ := fields["v"].(string)
	return name, &testObject{name: name, v: v}, nil
}

// Equal implements Adapter.
func (p *testPlant) Equal(a, b any) bool {
	oa, okA := a.(*testObject)
	ob, okB := b.(*testObject)
	return okA && okB && *oa == *ob
}

// Apply implements Adapter: an assertion writes, a retraction deletes.
func (p *testPlant) Apply(_ context.Context, key string, old, new any) (ApplyResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.failing > 0 {
		p.failing--
		err := fmt.Errorf("plant %s: %s", p.fail, key)
		if p.suggest > 0 {
			return p.fail, delayedError{error: err, d: p.suggest}
		}
		return p.fail, err
	}

	from := "-"
	if o, ok := old.(*testObject); ok {
		from = o.v
	}
	if n, ok := new.(*testObject); ok {
		p.state[key] = n.v
		p.writes = append(p.writes, fmt.Sprintf("%s:%s->%s", key, from, n.v))
	} else {
		delete(p.state, key)
		p.writes = append(p.writes, fmt.Sprintf("%s:%s->-", key, from))
	}
	return Applied, nil
}

func (p *testPlant) get(key string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state[key]
}

func (p *testPlant) log() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.writes...)
}

func (p *testPlant) refuseNext(n int, outcome ApplyResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failing, p.fail = n, outcome
}

// fixedBackoff is a deterministic retry schedule: the tests assert on
// what the emitter does, never on how long a real schedule waits.
type fixedBackoff struct {
	d     time.Duration
	steps int
}

func (b *fixedBackoff) Step() time.Duration {
	b.steps++
	return b.d
}

// delayedError carries a plant-suggested retry delay.
type delayedError struct {
	error
	d time.Duration
}

func (e delayedError) RetryAfter() time.Duration { return e.d }

func obj(name, v string) datamodel.Document {
	return unstructured.New(map[string]any{"name": name, "v": v})
}

// delta builds one event's Z-set from (document, weight) pairs.
func delta(entries ...any) zset.ZSet {
	z := zset.New()
	for i := 0; i < len(entries); i += 2 {
		z.Insert(entries[i].(datamodel.Document), zset.Weight(entries[i+1].(int)))
	}
	return z
}

// newPump builds a pump over a plant, collecting reported errors.
func newPump(plant *testPlant) (*WritePump, *[]error) {
	p, reported, _ := newPumpWithBackoff(plant, time.Millisecond)
	return p, reported
}

// newPumpWithBackoff builds a pump over a plant with a schedule the test
// can inspect.
func newPumpWithBackoff(plant *testPlant, delay time.Duration) (*WritePump, *[]error, *fixedBackoff) {
	var mu sync.Mutex
	reported := &[]error{}
	schedule := &fixedBackoff{d: delay}
	p := NewWritePump(WritePumpConfig{
		Adapter: plant,
		Report: func(e error) {
			mu.Lock()
			defer mu.Unlock()
			*reported = append(*reported, e)
		},
		NewBackoff: func() Backoff { return schedule },
		Logger:     logr.Discard(),
	})
	return p, reported, schedule
}

var _ = Describe("PairByKey", func() {
	plant := newPlant()

	It("pairs a retraction and an assertion for the same key", func() {
		pairs, errs := PairByKey(delta(obj("a", "1"), -1, obj("a", "2"), 1), plant.Adapt)
		Expect(errs).To(BeEmpty())
		Expect(pairs).To(HaveLen(1))
		Expect(pairs[0].Key).To(Equal("a"))
		Expect(pairs[0].Old.(*testObject).v).To(Equal("1"))
		Expect(pairs[0].New.(*testObject).v).To(Equal("2"))
	})

	It("folds bare assertions and retractions", func() {
		pairs, errs := PairByKey(delta(obj("a", "1"), 1), plant.Adapt)
		Expect(errs).To(BeEmpty())
		Expect(pairs[0].Old).To(BeNil())
		Expect(pairs[0].New).NotTo(BeNil())

		pairs, errs = PairByKey(delta(obj("a", "1"), -1), plant.Adapt)
		Expect(errs).To(BeEmpty())
		Expect(pairs[0].Old).NotTo(BeNil())
		Expect(pairs[0].New).To(BeNil())
	})

	It("absorbs duplicate deliveries of the same change", func() {
		pairs, errs := PairByKey(delta(obj("a", "1"), -2, obj("a", "2"), 2), plant.Adapt)
		Expect(errs).To(BeEmpty())
		Expect(pairs).To(HaveLen(1))
		Expect(pairs[0].Old.(*testObject).v).To(Equal("1"))
		Expect(pairs[0].New.(*testObject).v).To(Equal("2"))
	})

	It("reports several distinct documents under one key", func() {
		pairs, errs := PairByKey(delta(obj("a", "1"), 1, obj("a", "2"), 1), plant.Adapt)
		Expect(pairs).To(BeEmpty())
		Expect(errs).To(HaveLen(1))
		Expect(errs[0].Key).To(Equal("a"))
		Expect(errs[0].Error()).To(ContainSubstring("2 asserted documents"))

		_, errs = PairByKey(delta(obj("a", "1"), -1, obj("a", "2"), -1), plant.Adapt)
		Expect(errs[0].Error()).To(ContainSubstring("2 retracted documents"))
	})

	It("reports an unaddressable document instead of skipping it", func() {
		pairs, errs := PairByKey(delta(unstructured.New(map[string]any{"v": "1"}), 1), plant.Adapt)
		Expect(pairs).To(BeEmpty())
		Expect(errs).To(HaveLen(1))
		Expect(errs[0].Key).To(BeEmpty())
	})

	It("rejects one key without blocking the others", func() {
		pairs, errs := PairByKey(delta(obj("a", "1"), 1, obj("a", "2"), 1, obj("b", "1"), 1), plant.Adapt)
		Expect(pairs).To(HaveLen(1))
		Expect(pairs[0].Key).To(Equal("b"))
		Expect(errs).To(HaveLen(1))
	})

	It("returns pairs in deterministic key order", func() {
		pairs, _ := PairByKey(delta(obj("c", "1"), 1, obj("a", "1"), 1, obj("b", "1"), 1), plant.Adapt)
		Expect([]string{pairs[0].Key, pairs[1].Key, pairs[2].Key}).To(Equal([]string{"a", "b", "c"}))
	})
})

var _ = Describe("Write queue", func() {
	It("composes an arriving change onto the outstanding job", func() {
		plant := newPlant()
		q := &queue{equal: plant.Equal, jobs: map[string]job{}}

		q.absorb("a", job{old: &testObject{"a", "1"}, new: &testObject{"a", "2"}})
		q.absorb("a", job{old: &testObject{"a", "2"}, new: &testObject{"a", "3"}})
		Expect(q.len()).To(Equal(1), "two changes to one object are one job")

		j, ok := q.take("a")
		Expect(ok).To(BeTrue())
		Expect(j.old.(*testObject).v).To(Equal("1"), "the earliest source we still owe a diff from")
		Expect(j.new.(*testObject).v).To(Equal("3"), "the latest target we want")
	})

	It("drops a job that composes to identity", func() {
		plant := newPlant()
		q := &queue{equal: plant.Equal, jobs: map[string]job{}}

		q.absorb("a", job{old: &testObject{"a", "1"}, new: &testObject{"a", "2"}})
		q.absorb("a", job{old: &testObject{"a", "2"}, new: &testObject{"a", "1"}})
		Expect(q.len()).To(BeZero())
	})

	It("drops an assertion the pipeline retracts before the plant sees it", func() {
		plant := newPlant()
		q := &queue{equal: plant.Equal, jobs: map[string]job{}}

		q.absorb("a", job{new: &testObject{"a", "1"}})
		q.absorb("a", job{old: &testObject{"a", "1"}})
		Expect(q.len()).To(BeZero(), "creating and deleting an object the plant never saw is no work")
	})

	It("composes a restored job under what arrived while it was in flight", func() {
		plant := newPlant()
		q := &queue{equal: plant.Equal, jobs: map[string]job{}}

		q.absorb("a", job{old: &testObject{"a", "1"}, new: &testObject{"a", "2"}})
		j, _ := q.take("a")
		q.absorb("a", job{old: &testObject{"a", "1"}, new: &testObject{"a", "3"}})
		q.restore("a", j)

		got, ok := q.take("a")
		Expect(ok).To(BeTrue())
		Expect(got.old.(*testObject).v).To(Equal("1"), "the returned job is the older source")
		Expect(got.new.(*testObject).v).To(Equal("3"), "the target absorbed meanwhile still wins")
	})

	It("is idle only when nothing is queued and nothing is in flight", func() {
		plant := newPlant()
		q := &queue{equal: plant.Equal, jobs: map[string]job{}}
		Expect(q.idle()).To(BeTrue())

		q.absorb("a", job{new: &testObject{"a", "1"}})
		Expect(q.idle()).To(BeFalse())

		j, _ := q.take("a")
		Expect(q.len()).To(BeZero())
		Expect(q.idle()).To(BeFalse(), "a taken job is still owed until it settles")

		q.restore("a", j)
		j, _ = q.take("a")
		q.done()
		Expect(q.idle()).To(BeTrue())
	})
})

var _ = Describe("WritePump", func() {
	It("feeds and emits one change per plant object", func() {
		plant := newPlant()
		p, reported := newPump(plant)

		Expect(p.Feed(delta(obj("a", "1"), 1, obj("b", "1"), 1))).To(Succeed())
		Expect(p.Outstanding()).To(Equal(2))

		p.drain(context.Background())
		Expect(plant.get("a")).To(Equal("1"))
		Expect(plant.get("b")).To(Equal("1"))
		Expect(p.Idle()).To(BeTrue())
		Expect(*reported).To(BeEmpty())
	})

	It("writes the pair, so the plant sees what changed", func() {
		plant := newPlant()
		p, _ := newPump(plant)

		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		p.drain(context.Background())
		Expect(p.Feed(delta(obj("a", "1"), -1, obj("a", "2"), 1))).To(Succeed())
		p.drain(context.Background())
		Expect(p.Feed(delta(obj("a", "2"), -1))).To(Succeed())
		p.drain(context.Background())

		Expect(plant.log()).To(Equal([]string{"a:-->1", "a:1->2", "a:2->-"}))
	})

	It("returns adapter errors to the feeder and keeps the sound keys", func() {
		plant := newPlant()
		p, _ := newPump(plant)

		err := p.Feed(delta(unstructured.New(map[string]any{"v": "x"}), 1, obj("b", "1"), 1))
		Expect(err).To(HaveOccurred())
		Expect(p.Outstanding()).To(Equal(1))

		p.drain(context.Background())
		Expect(plant.get("b")).To(Equal("1"))
	})

	It("reports a refused write and drops it", func() {
		plant := newPlant()
		p, reported := newPump(plant)
		plant.refuseNext(1, Refused)

		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		p.drain(context.Background())

		Expect(*reported).To(HaveLen(1))
		Expect(p.Idle()).To(BeTrue(), "the plant decided, so nothing is owed")
		Expect(plant.get("a")).To(BeEmpty())
	})

	It("keeps an unreachable write owed and stops the cycle", func() {
		plant := newPlant()
		p, reported := newPump(plant)
		plant.refuseNext(1, Unreachable)

		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		_, stalled := p.drain(context.Background())
		Expect(stalled).To(BeTrue())
		Expect(p.Outstanding()).To(Equal(1))
		Expect(*reported).To(BeEmpty(), "an unreachable plant is not a fault to report")

		p.drain(context.Background())
		Expect(plant.get("a")).To(Equal("1"))
		Expect(p.Idle()).To(BeTrue())
	})

	It("waits out the plant's own suggested delay", func() {
		plant := newPlant()
		p, _, _ := newPumpWithBackoff(plant, time.Millisecond)
		plant.refuseNext(1, Unreachable)
		plant.suggest = 300 * time.Millisecond

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = p.Run(ctx) }()

		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())

		// The schedule alone would retry within a millisecond; the plant
		// asked to be left alone for longer, and that wins.
		Consistently(p.Idle, 100*time.Millisecond, 10*time.Millisecond).Should(BeFalse())
		Eventually(p.Idle, 2*time.Second, 10*time.Millisecond).Should(BeTrue())
		Expect(plant.get("a")).To(Equal("1"))
	})

	It("takes a fresh schedule once the plant answers", func() {
		plant := newPlant()
		p, _, schedule := newPumpWithBackoff(plant, time.Millisecond)
		plant.refuseNext(2, Unreachable)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = p.Run(ctx) }()

		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		Eventually(p.Idle, time.Second, 5*time.Millisecond).Should(BeTrue())
		Expect(schedule.steps).To(Equal(2), "one step per attempt that did not reach the plant")
	})

	It("drains on its own until the plant answers", func() {
		plant := newPlant()
		p, _ := newPump(plant)
		plant.refuseNext(2, Unreachable)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = p.Run(ctx) }()

		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		Eventually(p.Idle, time.Second, 5*time.Millisecond).Should(BeTrue())
		Expect(plant.get("a")).To(Equal("1"))
	})

	It("absorbs re-emitted corrections while the plant is unreachable", func() {
		plant := newPlant()
		p, reported := newPump(plant)
		plant.refuseNext(1, Unreachable)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = p.Run(ctx) }()

		// The same outstanding correction, delivered three times.
		for range 3 {
			Expect(p.Feed(delta(obj("a", "1"), -1, obj("a", "2"), 1))).To(Succeed())
		}
		Eventually(p.Idle, time.Second, 5*time.Millisecond).Should(BeTrue())

		Expect(plant.log()).To(Equal([]string{"a:1->2"}), "delivered many times, written once")
		Expect(*reported).To(BeEmpty())
	})

	It("stops when the context is cancelled", func() {
		plant := newPlant()
		p, _ := newPump(plant)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = p.Run(ctx); close(done) }()

		cancel()
		Eventually(done, time.Second).Should(BeClosed())
	})
})
