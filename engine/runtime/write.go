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
//	         jobs return to the queue and pause the emitter
//
// Documents are compared by content hash throughout, the identity the
// Z-set already carries, so no step of the path compares objects. A
// closed loop re-emits its whole outstanding correction every step until
// the plant's feedback confirms it; every document of such a re-emission
// is one the path has sent, and the sent records index them by hash, so a
// re-emission is paired and absorbed without adapting a single document.

// ApplyResult classifies the result of one write against the plant: Applied
// means an object is accepted by the plant and the connector can forget about
// it; Unreachable failures lack an ack and MUST be retried after a pause;
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

// RetryAfter is implemented by errors carrying a delay the plant asked for
// before the next attempt, such as an HTTP 429 Retry-After. The emitter
// stretches its retry pause to honour it. This is how a plant-specific
// status crosses the connector boundary without the write path having to
// know the plant's error types.
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
	// OldHash and NewHash are the content hashes of the retracted and
	// the asserted document, empty where the document is absent.
	OldHash, NewHash string
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
	type adapted struct {
		obj  any
		hash string
	}
	type group struct {
		neg, pos []adapted
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
		a := adapted{obj: obj, hash: e.Document.Hash()}
		if e.Weight < 0 {
			g.neg = append(g.neg, a)
		} else {
			g.pos = append(g.pos, a)
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
			p.Old, p.OldHash = g.neg[0].obj, g.neg[0].hash
		}
		if len(g.pos) == 1 {
			p.New, p.NewHash = g.pos[0].obj, g.pos[0].hash
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

	// Apply performs one write. Exactly one of old and new may be nil: a
	// nil old asserts a new object, a nil new retracts it. Both are
	// objects this Adapter produced.
	Apply(ctx context.Context, key string, old, new any) (ApplyResult, error)
}

// job is one outstanding change for one plant object: the object we last
// asserted and the one we want to assert now, each with the content hash
// of the document it was adapted from (empty where absent). The hashes
// are the identity of the job: two documents of one hash leave the plant
// in the same state.
type job struct {
	old, new         any
	oldHash, newHash string
}

// keyedJob is one job with the plant key it addresses: what the emitter
// takes from the queue.
type keyedJob struct {
	key string
	job
}

// queue holds the outstanding jobs. Absorbing (the default), it keeps at
// most one job per plant key, in the order the keys arrived; plain, it
// keeps every job in arrival order and composes nothing. Besides the jobs
// it keeps only the sent records, which age out, so it is empty whenever
// the plant is reachable and the pipeline has been quiet for a hold.
type queue struct {
	mu sync.Mutex
	// plain selects the plain actuator: fifo holds every change in
	// arrival order, and the absorbing fields are unused.
	plain bool
	fifo  []keyedJob
	// The absorbing queue: jobs is the one job per key, order the keys
	// with a slot in arrival order, one slot per key (queued marks the
	// keys holding one). A key whose job composed to identity keeps its
	// slot until the slot is popped and skipped.
	jobs   map[string]job
	order  []string
	queued map[string]bool
	// inflight counts the jobs taken out of the queue and handed to the
	// plant, so that "nothing outstanding" can be told apart from
	// "nothing left to take".
	inflight int
	// sent remembers, per key, the last job taken for the plant and when
	// it was taken; hold is how long that record absorbs re-emissions of
	// the same job (zero: never). known indexes the documents of the sent
	// records by content hash, so a re-emitted document is paired without
	// being adapted again. now is the clock.
	sent  map[string]sent
	known map[string]knownDoc
	hold  time.Duration
	now   func() time.Time
}

// sent is the record of the last write taken for one key: the job and the
// time it was taken, whatever the plant answered afterwards.
type sent struct {
	job
	at time.Time
}

// knownDoc is what adapting a document of a sent record produced.
type knownDoc struct {
	key string
	obj any
}

func newQueue(plain bool, hold time.Duration) *queue {
	return &queue{
		plain:  plain,
		jobs:   map[string]job{},
		queued: map[string]bool{},
		sent:   map[string]sent{},
		known:  map[string]knownDoc{},
		hold:   hold,
		now:    time.Now,
	}
}

// absorb composes an arriving pair onto the outstanding job for its key.
// The law is that composing (o1,n1) with (o2,n2) yields (o1,n2): the
// earliest source we still owe a diff from, the latest target we want.
// This is what makes a re-emitted correction idempotent and a superseded
// one harmless.
//
// A pair for a key with no outstanding job is dropped when it is the job
// the plant was sent for that key less than hold ago, source and target
// alike: the write is in flight or acked, and the pair is the circuit
// re-emitting it until the plant's own feedback confirms it. A pair with
// another source is a correction of something the plant did behind our
// back, and is queued however recent the record: the plant's feedback
// retracts what we wrote and asserts what it holds, so the loop's next
// emission names that as the source. Past the hold a re-emission is
// written again, and that is the circuit's reconciliation doing its job.
func (q *queue) absorb(key string, j job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.plain {
		if !q.identity(j) {
			q.fifo = append(q.fifo, keyedJob{key: key, job: j})
		}
		return
	}
	cur, ok := q.jobs[key]
	if !ok {
		if s, sent := q.sent[key]; sent && q.now().Sub(s.at) < q.hold && s.oldHash == j.oldHash && s.newHash == j.newHash {
			return
		}
	} else {
		j = job{old: cur.old, oldHash: cur.oldHash, new: j.new, newHash: j.newHash}
	}
	if q.store(key, j) && !q.queued[key] {
		q.queued[key] = true
		q.order = append(q.order, key)
	}
}

// adapted returns what adapting the document of the given hash produced,
// if the document is one of a sent record.
func (q *queue) adapted(hash string) (knownDoc, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	d, ok := q.known[hash]
	return d, ok
}

// forget drops the sent record of a key and its documents from the index.
func (q *queue) forget(key string) {
	s, ok := q.sent[key]
	if !ok {
		return
	}
	delete(q.known, s.oldHash)
	delete(q.known, s.newHash)
	delete(q.sent, key)
}

// sweep forgets the sent records older than hold: a record past the hold
// absorbs nothing, so this only bounds the memory of a long-running pump.
func (q *queue) sweep() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for key, s := range q.sent {
		if q.now().Sub(s.at) >= q.hold {
			q.forget(key)
		}
	}
}

// pop takes the oldest outstanding job. A taken job counts as in flight
// until it is settled or put back, and on the absorbing queue it is the
// key's sent record from this moment on.
func (q *queue) pop() (keyedJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.plain {
		if len(q.fifo) == 0 {
			return keyedJob{}, false
		}
		kj := q.fifo[0]
		q.fifo = q.fifo[1:]
		q.inflight++
		return kj, true
	}
	for len(q.order) > 0 {
		key := q.order[0]
		q.order = q.order[1:]
		delete(q.queued, key)
		j, ok := q.jobs[key]
		if !ok {
			// The job composed to identity after the key took its slot.
			continue
		}
		delete(q.jobs, key)
		q.inflight++
		if q.hold > 0 {
			q.forget(key)
			q.sent[key] = sent{job: j, at: q.now()}
			if j.old != nil {
				q.known[j.oldHash] = knownDoc{key: key, obj: j.old}
			}
			if j.new != nil {
				q.known[j.newHash] = knownDoc{key: key, obj: j.new}
			}
		}
		return keyedJob{key: key, job: j}, true
	}
	return keyedJob{}, false
}

// unpop puts back a job whose write did not reach the plant, at the head,
// so it is retried in order. On the absorbing queue it is the same
// composition as absorb with the arguments reversed: what comes back is
// older than anything absorbed while the write was in flight, and the job
// keeps the slot that arrival took.
func (q *queue) unpop(kj keyedJob) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.inflight--
	if q.plain {
		q.fifo = append([]keyedJob{kj}, q.fifo...)
		return
	}
	j := kj.job
	if cur, ok := q.jobs[kj.key]; ok {
		j = job{old: j.old, oldHash: j.oldHash, new: cur.new, newHash: cur.newHash}
	}
	if q.store(kj.key, j) && !q.queued[kj.key] {
		q.queued[kj.key] = true
		q.order = append([]string{kj.key}, q.order...)
	}
}

// identity reports whether a job owes the plant nothing (see store).
func (q *queue) identity(j job) bool {
	return j.oldHash == j.newHash
}

// store records a composed job and reports whether one is outstanding,
// dropping one that composed to identity. Identity has two shapes: a
// change whose target is the object we last asserted owes the plant
// nothing, and so does one that asserts an object and retracts it again
// before either reached the plant. This is the cancellation the Z-set
// performs by content hash, recovered in the key domain.
func (q *queue) store(key string, j job) bool {
	if q.identity(j) {
		delete(q.jobs, key)
		return false
	}
	q.jobs[key] = j
	return true
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

	return len(q.jobs) == 0 && len(q.fifo) == 0 && q.inflight == 0
}

// len returns the number of outstanding jobs.
func (q *queue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.jobs) + len(q.fifo)
}

// WritePumpConfig configures a WritePump.
type WritePumpConfig struct {
	Adapter

	// Plain selects the plain actuator: every emitted change is written as
	// it arrives, in order, and nothing is composed across emissions, so a
	// re-emitted correction costs a second write. The default is the
	// absorbing queue: re-emissions compose into one outstanding job per
	// plant object, and the re-emissions of a write already issued are
	// absorbed for the Hold.
	Plain bool

	// Hold is how long a written object absorbs re-emissions of the same
	// change: a pair arriving within Hold of the write that carried
	// exactly that pair is dropped, whether the write is still in flight
	// or acked and awaiting the plant's feedback. Zero holds nothing; the
	// plain actuator ignores it.
	Hold time.Duration

	// Timeout bounds one write against the plant; a write that does not
	// answer in time counts as unreachable and is retried like one. It is
	// also the pause before an unreachable plant is tried again, so a
	// plant that is down costs one attempt per Timeout. Zero means no
	// bound and a pause of one second.
	Timeout time.Duration

	// Report receives errors the emitter cannot return to a caller:
	// refused writes and emitter faults. Required.
	Report func(error)

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
	timeout time.Duration
}

// retryPause is how long the emitter waits after an unreachable write
// when no Timeout bounds the writes.
const retryPause = time.Second

// NewWritePump creates a write pump for a connector's adapter.
func NewWritePump(cfg WritePumpConfig) *WritePump {
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	return &WritePump{
		adapter: cfg.Adapter,
		queue:   newQueue(cfg.Plain, cfg.Hold),
		wake:    make(chan struct{}, 1),
		report:  cfg.Report,
		log:     log,
		timeout: cfg.Timeout,
	}
}

// Feed turns one output event into outstanding jobs. It never touches the
// plant, so an unreachable plant cannot stall the circuit that publishes
// here. Documents that cannot be adapted and keys carrying several
// distinct documents are returned as errors; everything else is absorbed.
// A document of a sent record is paired from the record, not adapted
// again.
func (p *WritePump) Feed(z zset.ZSet) error {
	pairs, kerrs := PairByKey(z, func(doc datamodel.Document) (string, any, error) {
		if d, ok := p.queue.adapted(doc.Hash()); ok {
			return d.key, d.obj, nil
		}
		return p.adapter.Adapt(doc)
	})

	for _, pr := range pairs {
		p.queue.absorb(pr.Key, job{old: pr.Old, oldHash: pr.OldHash, new: pr.New, newHash: pr.NewHash})
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
// for a feed when there is nothing owed, and pauses one timeout after a
// write that did not reach the plant, or as long as the plant asked for,
// so an unreachable plant costs one attempt per pause. A feed arriving
// during the pause does not shorten it: the job it absorbed will be
// written by the attempt that is already scheduled. Once per hold it also
// forgets the sent records that have expired.
func (p *WritePump) Run(ctx context.Context) error {
	retry := time.NewTimer(time.Hour)
	if !retry.Stop() {
		<-retry.C
	}
	defer retry.Stop()

	var sweep <-chan time.Time
	if p.queue.hold > 0 && !p.queue.plain {
		ticker := time.NewTicker(p.queue.hold)
		defer ticker.Stop()
		sweep = ticker.C
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		suggested, stalled := p.drain(ctx)
		if stalled {
			delay := p.timeout
			if delay == 0 {
				delay = retryPause
			}
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

		select {
		case <-ctx.Done():
			return nil
		case <-p.wake:
		case <-sweep:
			p.queue.sweep()
		}
	}
}

// drain writes the outstanding jobs in order. The first write that does
// not reach the plant ends the cycle: the plant is not answering, so the
// remaining jobs would only repeat the failure. That job goes back to the
// head of the queue, composed under anything absorbed while it was in
// flight.
func (p *WritePump) drain(ctx context.Context) (time.Duration, bool) {
	for {
		if ctx.Err() != nil {
			return 0, false
		}
		kj, ok := p.queue.pop()
		if !ok {
			return 0, false
		}
		outcome, err := p.apply(ctx, kj.key, kj.job)
		switch outcome {
		case Applied:
			p.queue.done()
		case Unreachable:
			p.queue.unpop(kj)
			p.log.V(1).Info("plant unreachable, write pending", "object", kj.key, "error", err)
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
}

// apply performs one write under the pump's timeout.
func (p *WritePump) apply(ctx context.Context, key string, j job) (ApplyResult, error) {
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}
	return p.adapter.Apply(ctx, key, j.old, j.new)
}
