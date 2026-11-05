package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	// block makes every write hang until its context expires.
	block bool
	// adapts counts the documents adapted.
	adapts atomic.Int64
}

func newPlant() *testPlant {
	return &testPlant{state: map[string]string{}}
}

// Adapt implements Adapter: a document must carry a name to be writable.
func (p *testPlant) Adapt(doc datamodel.Document) (string, any, error) {
	p.adapts.Add(1)
	fields := doc.Fields()
	name, ok := fields["name"].(string)
	if !ok || name == "" {
		return "", nil, fmt.Errorf("document carries no name")
	}
	v, _ := fields["v"].(string)
	return name, &testObject{name: name, v: v}, nil
}

// tj builds a job between two test objects, hashed as their documents
// would be.
func tj(old, new *testObject) job {
	j := job{}
	if old != nil {
		j.old, j.oldHash = old, obj(old.name, old.v).Hash()
	}
	if new != nil {
		j.new, j.newHash = new, obj(new.name, new.v).Hash()
	}
	return j
}

// Apply implements Adapter: an assertion writes, a retraction deletes.
func (p *testPlant) Apply(ctx context.Context, key string, old, new any) (ApplyResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.block {
		p.mu.Unlock()
		<-ctx.Done()
		p.mu.Lock()
		return Unreachable, ctx.Err()
	}
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

// newPump builds a pump over a plant with a millisecond timeout (and so
// retry pause), collecting reported errors.
func newPump(plant *testPlant) (*WritePump, *[]error) {
	var mu sync.Mutex
	reported := &[]error{}
	p := NewWritePump(WritePumpConfig{
		Adapter: plant,
		Timeout: time.Millisecond,
		Report: func(e error) {
			mu.Lock()
			defer mu.Unlock()
			*reported = append(*reported, e)
		},
		Logger: logr.Discard(),
	})
	return p, reported
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
		q := newQueue(false, 0)

		q.absorb("a", tj(&testObject{"a", "1"}, &testObject{"a", "2"}))
		q.absorb("a", tj(&testObject{"a", "2"}, &testObject{"a", "3"}))
		Expect(q.len()).To(Equal(1), "two changes to one object are one job")

		kj, ok := q.pop()
		Expect(ok).To(BeTrue())
		Expect(kj.old.(*testObject).v).To(Equal("1"), "the earliest source we still owe a diff from")
		Expect(kj.new.(*testObject).v).To(Equal("3"), "the latest target we want")
	})

	It("pops the keys in arrival order, skipping a slot whose job cancelled", func() {
		q := newQueue(false, 0)

		q.absorb("b", tj(nil, &testObject{"b", "1"}))
		q.absorb("a", tj(nil, &testObject{"a", "1"}))
		q.absorb("c", tj(nil, &testObject{"c", "1"}))
		q.absorb("a", tj(&testObject{"a", "1"}, nil))
		Expect(q.len()).To(Equal(2))

		kj, ok := q.pop()
		Expect(ok).To(BeTrue())
		Expect(kj.key).To(Equal("b"))
		kj, ok = q.pop()
		Expect(ok).To(BeTrue())
		Expect(kj.key).To(Equal("c"), "the cancelled key's slot is skipped")
		_, ok = q.pop()
		Expect(ok).To(BeFalse())

		q.absorb("a", tj(nil, &testObject{"a", "2"}))
		kj, ok = q.pop()
		Expect(ok).To(BeTrue())
		Expect(kj.key).To(Equal("a"), "a key takes a fresh slot once its old one is gone")
	})

	It("drops a job that composes to identity", func() {
		q := newQueue(false, 0)

		q.absorb("a", tj(&testObject{"a", "1"}, &testObject{"a", "2"}))
		q.absorb("a", tj(&testObject{"a", "2"}, &testObject{"a", "1"}))
		Expect(q.len()).To(BeZero())
	})

	It("drops an assertion the pipeline retracts before the plant sees it", func() {
		q := newQueue(false, 0)

		q.absorb("a", tj(nil, &testObject{"a", "1"}))
		q.absorb("a", tj(&testObject{"a", "1"}, nil))
		Expect(q.len()).To(BeZero(), "creating and deleting an object the plant never saw is no work")
	})

	It("composes a put-back job under what arrived while it was in flight", func() {
		q := newQueue(false, 0)

		q.absorb("a", tj(&testObject{"a", "1"}, &testObject{"a", "2"}))
		kj, _ := q.pop()
		q.absorb("a", tj(&testObject{"a", "1"}, &testObject{"a", "3"}))
		q.unpop(kj)

		got, ok := q.pop()
		Expect(ok).To(BeTrue())
		Expect(got.old.(*testObject).v).To(Equal("1"), "the returned job is the older source")
		Expect(got.new.(*testObject).v).To(Equal("3"), "the target absorbed meanwhile still wins")
	})

	It("is idle only when nothing is queued and nothing is in flight", func() {
		q := newQueue(false, 0)
		Expect(q.idle()).To(BeTrue())

		q.absorb("a", tj(nil, &testObject{"a", "1"}))
		Expect(q.idle()).To(BeFalse())

		kj, _ := q.pop()
		Expect(q.len()).To(BeZero())
		Expect(q.idle()).To(BeFalse(), "a taken job is still owed until it settles")

		q.unpop(kj)
		_, _ = q.pop()
		q.done()
		Expect(q.idle()).To(BeTrue())
	})
})

var _ = Describe("WritePump", func() {
	It("as the plain actuator writes every emitted change, in order", func() {
		plant := newPlant()
		var reported []error
		p := NewWritePump(WritePumpConfig{
			Adapter: plant,
			Plain:   true,
			Report:  func(e error) { reported = append(reported, e) },
			Logger:  logr.Discard(),
		})

		// The same correction emitted three times before any write: three
		// writes, where the absorbing pump makes one.
		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		Expect(p.Outstanding()).To(Equal(3))
		p.drain(context.Background())
		Expect(plant.log()).To(Equal([]string{"a:-->1", "a:-->1", "a:-->1"}))
		Expect(p.Idle()).To(BeTrue())
		Expect(reported).To(BeEmpty())

		// An unreachable plant keeps the order: the failed write is retried
		// first.
		plant.refuseNext(1, Unreachable)
		Expect(p.Feed(delta(obj("b", "1"), 1))).To(Succeed())
		Expect(p.Feed(delta(obj("c", "1"), 1))).To(Succeed())
		_, stalled := p.drain(context.Background())
		Expect(stalled).To(BeTrue())
		Expect(p.Outstanding()).To(Equal(2))
		p.drain(context.Background())
		Expect(plant.log()[3:]).To(Equal([]string{"b:-->1", "c:-->1"}))
	})

	It("composes re-emissions into one write per object", func() {
		plant := newPlant()
		p, _ := newPump(plant)
		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		Expect(p.Outstanding()).To(Equal(1))
		p.drain(context.Background())
		Expect(plant.log()).To(Equal([]string{"a:-->1"}))
	})

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
		p, _ := newPump(plant)
		plant.refuseNext(1, Unreachable)
		plant.suggest = 300 * time.Millisecond

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = p.Run(ctx) }()

		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())

		// The pause alone would retry within a millisecond; the plant
		// asked to be left alone for longer, and that wins.
		Consistently(p.Idle, 100*time.Millisecond, 10*time.Millisecond).Should(BeFalse())
		Eventually(p.Idle, 2*time.Second, 10*time.Millisecond).Should(BeTrue())
		Expect(plant.get("a")).To(Equal("1"))
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

	Context("with a hold", func() {
		// A pump whose clock the test advances, holding for a minute.
		var (
			plant *testPlant
			p     *WritePump
			now   time.Time
			rep   *[]error
		)
		BeforeEach(func() {
			plant = newPlant()
			now = time.Unix(1000, 0)
			var mu sync.Mutex
			rep = &[]error{}
			p = NewWritePump(WritePumpConfig{
				Adapter: plant,
				Hold:    time.Minute,
				Report: func(e error) {
					mu.Lock()
					defer mu.Unlock()
					*rep = append(*rep, e)
				},
				Logger: logr.Discard(),
			})
			p.queue.now = func() time.Time { return now }
		})

		It("absorbs a re-emission of a written object within the hold", func() {
			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			p.drain(context.Background())
			Expect(plant.log()).To(Equal([]string{"a:-->1"}))

			// The circuit re-emits the correction until its feedback
			// confirms it: nothing is queued, nothing is written.
			now = now.Add(30 * time.Second)
			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			Expect(p.Outstanding()).To(BeZero())
			Expect(p.Idle()).To(BeTrue())

			// Past the hold the same re-emission is written again.
			now = now.Add(31 * time.Second)
			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			Expect(p.Outstanding()).To(Equal(1))
			p.drain(context.Background())
			Expect(plant.log()).To(Equal([]string{"a:-->1", "a:-->1"}))
		})

		It("absorbs the re-emission of an in-flight write and queues a different target", func() {
			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			_, ok := p.queue.pop()
			Expect(ok).To(BeTrue(), "the write is in flight")

			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			Expect(p.Outstanding()).To(BeZero(), "the in-flight target, re-emitted, is absorbed")

			Expect(p.Feed(delta(obj("a", "1"), -1, obj("a", "2"), 1))).To(Succeed())
			Expect(p.Outstanding()).To(Equal(1), "a new target is owed")
			p.queue.done()
			p.drain(context.Background())
			Expect(plant.log()).To(Equal([]string{"a:1->2"}))
		})

		It("absorbs a retraction it has sent, not an assertion after it", func() {
			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			p.drain(context.Background())
			Expect(p.Feed(delta(obj("a", "1"), -1))).To(Succeed())
			p.drain(context.Background())
			Expect(plant.log()).To(Equal([]string{"a:-->1", "a:1->-"}))

			Expect(p.Feed(delta(obj("a", "1"), -1))).To(Succeed())
			Expect(p.Outstanding()).To(BeZero(), "the retraction re-emitted")
			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			Expect(p.Outstanding()).To(Equal(1), "the object asserted anew")
		})

		It("writes a correction whose source moved under the hold", func() {
			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			p.drain(context.Background())

			// The plant was overwritten behind our back: the loop's next
			// emission retracts what the plant holds and asserts our
			// target again. Same target, another source: not ours to
			// absorb.
			Expect(p.Feed(delta(obj("a", "x"), -1, obj("a", "1"), 1))).To(Succeed())
			Expect(p.Outstanding()).To(Equal(1))
			p.drain(context.Background())
			Expect(plant.log()).To(Equal([]string{"a:-->1", "a:x->1"}))
		})

		It("keeps absorbing a refused target for the hold", func() {
			plant.refuseNext(1, Refused)
			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			p.drain(context.Background())
			Expect(*rep).To(HaveLen(1))

			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			Expect(p.Outstanding()).To(BeZero(), "the refused object, re-emitted, costs no second write")
			Expect(p.Feed(delta(obj("a", "1"), -1, obj("a", "2"), 1))).To(Succeed())
			Expect(p.Outstanding()).To(Equal(1), "a repaired object is written")
		})

		It("composes onto a restored job rather than absorbing it", func() {
			plant.refuseNext(1, Unreachable)
			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			_, stalled := p.drain(context.Background())
			Expect(stalled).To(BeTrue())

			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			Expect(p.Outstanding()).To(Equal(1), "the job is back in the queue, so the re-emission composes onto it")
			p.drain(context.Background())
			Expect(plant.log()).To(Equal([]string{"a:-->1"}))
		})

		It("pairs the documents of a sent record without adapting them", func() {
			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			p.drain(context.Background())
			adapts := plant.adapts.Load()

			Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
			Expect(plant.adapts.Load()).To(Equal(adapts), "the re-emission is known by hash")
			Expect(p.Feed(delta(obj("a", "1"), -1, obj("a", "2"), 1))).To(Succeed())
			Expect(plant.adapts.Load()).To(Equal(adapts+1), "only the new target is adapted")
			Expect(p.Outstanding()).To(Equal(1))
		})

		It("sweeps the expired sent records", func() {
			Expect(p.Feed(delta(obj("a", "1"), 1, obj("b", "1"), 1))).To(Succeed())
			p.drain(context.Background())
			Expect(p.queue.sent).To(HaveLen(2))

			now = now.Add(30 * time.Second)
			Expect(p.Feed(delta(obj("b", "1"), -1, obj("b", "2"), 1))).To(Succeed())
			p.drain(context.Background())
			now = now.Add(31 * time.Second)
			p.queue.sweep()
			Expect(p.queue.sent).To(HaveKey("b"))
			Expect(p.queue.sent).NotTo(HaveKey("a"))
			Expect(p.queue.known).To(HaveLen(2), "the swept record's documents leave the index with it")
		})
	})

	It("bounds one write by the timeout and retries it", func() {
		plant := newPlant()
		plant.block = true
		p := NewWritePump(WritePumpConfig{
			Adapter: plant,
			Timeout: 20 * time.Millisecond,
			Report:  func(error) {},
			Logger:  logr.Discard(),
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = p.Run(ctx) }()

		Expect(p.Feed(delta(obj("a", "1"), 1))).To(Succeed())
		Consistently(p.Idle, 50*time.Millisecond, 5*time.Millisecond).Should(BeFalse())

		plant.mu.Lock()
		plant.block = false
		plant.mu.Unlock()
		Eventually(p.Idle, time.Second, 5*time.Millisecond).Should(BeTrue())
		Expect(plant.get("a")).To(Equal("1"))
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
