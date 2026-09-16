package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/zset"
)

// The write path: the shared half of every plant-writing connector.
//
// A circuit emits Z-set deltas, which are content-addressed, weighted and
// consumed once, while the plant (e.g., the Kubernetes apiserver) takes keyed
// objects and takes imperative commands rather than values (create, modify,
// delete, etc).  Crossing that gap takes five steps, and all but the
// connector-specific ones live here:
//
//	adapt    a document becomes the connector's native object plus the
//	         plant key a write to it addresses
//	pair     one delta is re-indexed from content identity to plant key,
//	         yielding the retracted and the asserted document per key
//	absorb   the pair is composed into the outstanding job for that key
//	emit     the outstanding job is handed to the connector
//	settle   applied jobs leave, refused jobs are reported, unreachable
//	         jobs return to the queue and arm the backoff

// ApplyResult classifies the result of one write against the plant: Applied
// means an object is accepted by the plant and the connector can forget about
// it; Unreachable failures lack an ack and MUST be retried with backoff;
// Refused failures MUST NOT be retried because the plant refused to accept the
// object, so the command is reported and dropped.
type ApplyResult int

const (
	// Applied: the plant accepted the write.
	Applied ApplyResult = iota
	// Unreachable: the command never reached the plant (transport errors,
	// timeouts, throttling, server overload). Retry the same command.
	Unreachable
	// Refused: the plant received the command and rejected it. Report and
	// drop.
	Refused
)

// String implements fmt.Stringer.
func (r ApplyResult) String() string {
	switch r {
	case Applied:
		return "applied"
	case Unreachable:
		return "unreachable"
	case Refused:
		return "refused"
	}
	return "unknown"
}

// Backoff schedules the retries of a write that did not reach the plant:
// Step returns the delay to wait before the next attempt and advances the
// schedule. A connector supplies its own, because how long a plant wants
// to be left alone is the connector's knowledge;
// k8s.io/apimachinery/pkg/util/wait.Backoff is one, and the Kubernetes
// connector passes exactly that.
type Backoff interface {
	Step() time.Duration
}

// RetryAfter is implemented by errors carrying a delay the plant asked for
// before the next attempt, such as an HTTP 429 Retry-After. The emitter
// stretches its backoff to honour it. This is how a plant-specific status
// crosses the connector boundary without the write path having to know
// the plant's error types.
type RetryAfter interface {
	RetryAfter() time.Duration
}

// KeyError reports a document the write path could not turn into a
// command. Key is empty when the document could not be keyed at all.
type KeyError struct {
	Key string
	Err error
}

// Error implements the error interface.
func (e KeyError) Error() string {
	if e.Key == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("object %q: %s", e.Key, e.Err)
}

// Unwrap returns the underlying error.
func (e KeyError) Unwrap() error { return e.Err }

// AdaptFunc converts a pipeline document into the connector's native
// object and the plant key a write to it addresses. The object is opaque
// to the write path, which hands it back to the connector exactly as
// produced. A document naming no plant object is an error, never a silent
// skip: a write that cannot be addressed is a pipeline fault, and the only
// useful response is to say so.
type AdaptFunc func(doc datamodel.Document) (key string, obj any, err error)

// Pair is the effective change for one plant object: Old is the object the
// pipeline retracted (nil when the key is newly asserted), New the one it
// asserted (nil when the key is retracted). Absence is always an untyped
// nil, because the write path only ever carries objects an AdaptFunc
// produced.
type Pair struct {
	Key      string
	Old, New any
}

// PairByKey re-indexes one delta from content identity to plant identity,
// deriving at most one Pair per key, in deterministic key order. Entries
// with the same content net out inside the Z-set itself, so per key the
// delta holds at most one retracted and one asserted document; weights
// past one in either direction are duplicate (at-least-once) deliveries of
// the same effective change, which the plant absorbs, so they fold into
// the same Pair as a unit weight does.
//
// Several distinct documents under one key and sign are a different
// matter: the plant holds one object per key and the write path has no
// basis for choosing between candidates, so the key is reported and
// omitted. Repairing that is the pipeline's job (the keyed Distincter
// selects a representative), never the connector's.
func PairByKey(z zset.ZSet, adapt AdaptFunc) ([]Pair, []KeyError) {
	type group struct {
		neg, pos []any
	}
	groups := map[string]*group{}
	var errs []KeyError

	for _, e := range z.Entries() {
		k, obj, err := adapt(e.Document)
		if err != nil {
			errs = append(errs, KeyError{Err: err})
			continue
		}
		g, ok := groups[k]
		if !ok {
			g = &group{}
			groups[k] = g
		}
		if e.Weight < 0 {
			g.neg = append(g.neg, obj)
		} else {
			g.pos = append(g.pos, obj)
		}
	}

	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	pairs := make([]Pair, 0, len(keys))
	for _, k := range keys {
		g := groups[k]
		if len(g.neg) > 1 {
			errs = append(errs, KeyError{Key: k, Err: fmt.Errorf("%d retracted documents in one delta", len(g.neg))})
			continue
		}
		if len(g.pos) > 1 {
			errs = append(errs, KeyError{Key: k, Err: fmt.Errorf("%d asserted documents in one delta", len(g.pos))})
			continue
		}
		p := Pair{Key: k}
		if len(g.neg) == 1 {
			p.Old = g.neg[0]
		}
		if len(g.pos) == 1 {
			p.New = g.pos[0]
		}
		pairs = append(pairs, p)
	}

	return pairs, errs
}

// Adapter is the connector-specific half of the write path: the data model
// on one side and the plant's command set on the other.
type Adapter interface {
	// Adapt converts a pipeline document into the connector's native
	// object and the plant key a write to it addresses.
	Adapt(doc datamodel.Document) (key string, obj any, err error)

	// Equal reports whether two native objects leave the plant in the same
	// state. It is the test that lets an outstanding job cancel itself
	// when the pipeline takes a change back before it was written.
	Equal(a, b any) bool

	// Apply performs one write. Exactly one of old and new may be nil: a
	// nil old asserts a new object, a nil new retracts it. Both are
	// objects this Adapter produced.
	Apply(ctx context.Context, key string, old, new any) (ApplyResult, error)
}

// job is one outstanding change for one plant object: the object we last
// asserted and the one we want to assert now.
type job struct {
	old, new any
}

// queue holds the outstanding jobs, at most one per plant key. It is the
// only state the write path keeps, and it is empty whenever the plant is
// reachable and the pipeline is quiet.
type queue struct {
	mu    sync.Mutex
	equal func(a, b any) bool
	jobs  map[string]job
	// inflight counts the jobs taken out of the map and handed to the
	// plant, so that "nothing outstanding" can be told apart from
	// "nothing left to take".
	inflight int
}

// absorb composes an arriving pair onto the outstanding job for its key.
// The law is that composing (o1,n1) with (o2,n2) yields (o1,n2): the
// earliest source we still owe a diff from, the latest target we want.
// This is what makes a re-emitted correction idempotent and a superseded
// one harmless.
func (q *queue) absorb(key string, j job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if cur, ok := q.jobs[key]; ok {
		j = job{old: cur.old, new: j.new}
	}
	q.store(key, j)
}

// restore takes back a job whose write did not reach the plant. It is the
// same composition with the arguments reversed: what comes back is older
// than anything absorbed while the write was in flight.
func (q *queue) restore(key string, j job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.inflight--
	if cur, ok := q.jobs[key]; ok {
		j = job{old: j.old, new: cur.new}
	}
	q.store(key, j)
}

// store records a composed job, dropping one that composed to identity.
// Identity has two shapes: a change whose target is the object we last
// asserted owes the plant nothing, and so does one that asserts an object
// and retracts it again before either reached the plant. This is the
// cancellation the Z-set performs by content hash, recovered in the key
// domain.
func (q *queue) store(key string, j job) {
	switch {
	case j.old == nil && j.new == nil:
	case j.old != nil && j.new != nil && q.equal(j.old, j.new):
	default:
		q.jobs[key] = j
		return
	}
	delete(q.jobs, key)
}

// keys returns the outstanding keys in order: the drain sequence.
func (q *queue) keys() []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	keys := make([]string, 0, len(q.jobs))
	for k := range q.jobs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// take removes the job for key and reports whether one was outstanding.
// A taken job counts as in flight until it is settled or restored.
func (q *queue) take(key string) (job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	j, ok := q.jobs[key]
	if !ok {
		return job{}, false
	}
	delete(q.jobs, key)
	q.inflight++
	return j, true
}

// done marks an in-flight job settled.
func (q *queue) done() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.inflight--
}

// idle reports whether the plant owes us no answer: nothing queued and
// nothing in flight.
func (q *queue) idle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.jobs) == 0 && q.inflight == 0
}

// len returns the number of outstanding jobs.
func (q *queue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.jobs)
}

// WritePumpConfig configures a WritePump.
type WritePumpConfig struct {
	Adapter

	// Report receives errors the emitter cannot return to a caller:
	// refused writes and emitter faults. Required.
	Report func(error)

	// NewBackoff returns a fresh retry schedule for a plant that stopped
	// answering. The emitter asks for a new one every time the plant
	// answers again, which is how the schedule resets. Required.
	NewBackoff func() Backoff

	logr.Logger
}

// WritePump drives the write path: Feed turns circuit events into
// outstanding jobs, Run drains them into the plant. The two are decoupled
// on purpose, so the plant's availability never becomes the circuit's
// problem.
type WritePump struct {
	adapter Adapter
	queue   *queue
	wake    chan struct{}
	report  func(error)
	log     logr.Logger

	// backoff is the current retry schedule and newBackoff makes a fresh
	// one; both are touched by the emitter only.
	newBackoff func() Backoff
	backoff    Backoff
}

// NewWritePump creates a write pump for a connector's adapter.
func NewWritePump(cfg WritePumpConfig) *WritePump {
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	return &WritePump{
		adapter:    cfg.Adapter,
		queue:      &queue{equal: cfg.Adapter.Equal, jobs: map[string]job{}},
		wake:       make(chan struct{}, 1),
		report:     cfg.Report,
		log:        log,
		newBackoff: cfg.NewBackoff,
		backoff:    cfg.NewBackoff(),
	}
}

// Feed turns one output event into outstanding jobs. It never touches the
// plant, so an unreachable plant cannot stall the circuit that publishes
// here. Documents that cannot be adapted and keys carrying several
// distinct documents are returned as errors; everything else is absorbed.
func (p *WritePump) Feed(z zset.ZSet) error {
	pairs, kerrs := PairByKey(z, p.adapter.Adapt)

	for _, pr := range pairs {
		p.queue.absorb(pr.Key, job{old: pr.Old, new: pr.New})
	}

	select {
	case p.wake <- struct{}{}:
	default:
	}

	errs := make([]error, 0, len(kerrs))
	for _, ke := range kerrs {
		errs = append(errs, ke)
	}
	return errors.Join(errs...)
}

// Outstanding returns the number of jobs waiting to be written.
func (p *WritePump) Outstanding() int { return p.queue.len() }

// Idle reports whether the write path is caught up: nothing waiting and
// nothing in flight, so everything fed so far has reached the plant.
func (p *WritePump) Idle() bool { return p.queue.idle() }

// Run is the emitter. It drains the outstanding jobs into the plant, waits
// for a feed when there is nothing owed, and waits out the backoff after a
// write that did not reach the plant, so an unreachable plant costs one
// attempt per backoff step. A feed arriving during the backoff does not
// shorten it: the job it absorbed will be written by the attempt that is
// already scheduled.
func (p *WritePump) Run(ctx context.Context) error {
	retry := time.NewTimer(time.Hour)
	if !retry.Stop() {
		<-retry.C
	}
	defer retry.Stop()

	for {
		if ctx.Err() != nil {
			return nil
		}

		suggested, stalled := p.drain(ctx)
		if stalled {
			delay := p.backoff.Step()
			if suggested > delay {
				delay = suggested
			}
			retry.Reset(delay)
			select {
			case <-ctx.Done():
				return nil
			case <-retry.C:
			}
			continue
		}

		p.backoff = p.newBackoff()
		select {
		case <-ctx.Done():
			return nil
		case <-p.wake:
		}
	}
}

// drain applies the outstanding jobs in key order. The first write that
// does not reach the plant ends the cycle: the plant is not answering, so
// the remaining jobs would only repeat the failure. That job goes back
// into the queue, composed under anything absorbed while it was in
// flight.
func (p *WritePump) drain(ctx context.Context) (time.Duration, bool) {
	for _, key := range p.queue.keys() {
		if ctx.Err() != nil {
			return 0, false
		}

		j, ok := p.queue.take(key)
		if !ok {
			continue
		}

		outcome, err := p.adapter.Apply(ctx, key, j.old, j.new)
		switch outcome {
		case Applied:
			p.queue.done()
		case Unreachable:
			p.queue.restore(key, j)
			p.log.V(1).Info("plant unreachable, write pending", "object", key, "error", err)
			var ra RetryAfter
			if errors.As(err, &ra) {
				return ra.RetryAfter(), true
			}
			return 0, true
		default:
			// Refused, and anything an adapter returns that is not one
			// of the three: the plant decided, so report and drop.
			p.queue.done()
			p.report(err)
		}
	}

	return 0, false
}
