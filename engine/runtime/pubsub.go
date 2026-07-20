package runtime

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/zset"
)

const (
	// EventBufferSize is the default buffer for runtime event channels.
	EventBufferSize = 128
)

var (
	// ErrChannelFull indicates that a non-blocking channel write could not proceed.
	ErrChannelFull = errors.New("runtime channel full")
	// ErrChannelClosed indicates that a channel was closed while publishing.
	ErrChannelClosed = errors.New("runtime channel closed")
)

// Event is a named payload sent through runtime endpoints.
type Event struct {
	Name string
	Data zset.ZSet
}

func sendEvent(ch chan Event, event Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %s", ErrChannelClosed, event.Name)
		}
	}()
	select {
	case ch <- event:
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrChannelFull, event.Name)
	}
}

func sendEventBlocking(ch chan Event, event Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %s", ErrChannelClosed, event.Name)
		}
	}()

	ch <- event
	return nil
}

// Publisher emits runtime events.
type Publisher interface {
	Publish(event Event) error
}

// PublishFunc adapts a function to Publisher.
type PublishFunc func(Event) error

// Publish calls f(event).
func (f PublishFunc) Publish(event Event) error { return f(event) }

type publisher struct {
	pubsub *PubSub
}

// Publish folds the event into the topic's retained integral and sends it to
// all current subscribers of event.Name. On a full subscriber channel it logs
// the overflow and then blocks until the event is accepted, preserving
// backpressure. Folding and fan-out happen atomically under the topic lock, so
// every subscriber sees a gap-free, duplicate-free delta stream relative to
// the integral it received on subscription.
func (p *publisher) Publish(event Event) error {
	ts := p.pubsub.topic(event.Name)
	ts.mu.Lock()
	defer ts.mu.Unlock()

	event.Data.Iter(func(doc datamodel.Document, w zset.Weight) bool {
		ts.acc.Insert(doc, w)
		return true
	})

	for _, ch := range ts.subs {
		if err := sendEvent(ch, event); err != nil {
			if errors.Is(err, ErrChannelFull) {
				log.Printf("runtime: event channel full, blocking publish: topic=%s err=%v", event.Name, err)
				if err := sendEventBlocking(ch, event); err != nil {
					return err
				}
				continue
			}
			return err
		}
	}
	return nil
}

// Subscriber can consume events from topic channels.
type Subscriber interface {
	Subscribe(topic string)
	Unsubscribe(topic string)
	// UnsubscribeAll unsubscribes from every registered topic, closing the
	// internal delivery channel. Use with context.AfterFunc to unblock Next
	// when a context is cancelled.
	UnsubscribeAll()
	// Next blocks until the next event arrives or all topics have been
	// unsubscribed. Returns (event, true) on success and (zero, false) when
	// the subscriber is done.
	Next() (Event, bool)
}

type subscriber struct {
	pubsub *PubSub

	mu     sync.Mutex
	ch     chan Event
	topics map[string]struct{}
}

// Subscribe registers interest in a topic. If the topic has a non-empty
// retained integral (state accumulated from events published before this
// subscription), it is delivered first as a single synthetic event, so a late
// subscriber bootstraps to the current state before receiving live deltas.
// Replay and registration happen atomically under the topic lock: no delta
// published after the replayed integral can be missed or double-counted.
func (s *subscriber) Subscribe(topic string) {

	s.mu.Lock()
	if _, ok := s.topics[topic]; ok {
		s.mu.Unlock()
		return
	}
	s.topics[topic] = struct{}{}
	ch := s.ch
	s.mu.Unlock()

	ts := s.pubsub.topic(topic)
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if !ts.acc.IsZero() {
		replay := Event{Name: topic, Data: ts.acc.ShallowCopy()}
		if err := sendEvent(ch, replay); err != nil {
			if errors.Is(err, ErrChannelFull) {
				log.Printf("runtime: event channel full, blocking replay: topic=%s err=%v", topic, err)
				err = sendEventBlocking(ch, replay)
			}
			if err != nil {
				log.Printf("runtime: cannot replay retained state: topic=%s err=%v", topic, err)
			}
		}
	}

	ts.subs = append(ts.subs, ch)
}

// Unsubscribe unregisters a topic. The channel closes when the last topic is
// removed. The topic's retained integral is kept: retention outlives
// subscriber churn.
func (s *subscriber) Unsubscribe(topic string) {

	s.mu.Lock()
	_, ok := s.topics[topic]
	if ok {
		delete(s.topics, topic)
	}
	ch := s.ch
	empty := len(s.topics) == 0
	s.mu.Unlock()
	if !ok {
		return
	}

	ts := s.pubsub.topic(topic)
	ts.mu.Lock()
	keep := ts.subs[:0]
	for _, c := range ts.subs {
		if c != ch {
			keep = append(keep, c)
		}
	}
	ts.subs = keep
	ts.mu.Unlock()

	if empty {
		close(ch)
	}
}

// Next blocks until the next event arrives or the delivery channel is closed.
func (s *subscriber) Next() (Event, bool) {
	event, ok := <-s.ch
	return event, ok
}

// GetChannel returns the underlying delivery channel. This is a low-level
// escape hatch for callers that need to select on the channel directly (e.g.
// test helpers). Prefer Next() for all normal consumption.
func (s *subscriber) GetChannel() <-chan Event { return s.ch }

// UnsubscribeAll unsubscribes from every registered topic. The last
// Unsubscribe call closes the delivery channel, unblocking any pending Next.
func (s *subscriber) UnsubscribeAll() {
	s.mu.Lock()
	topics := make([]string, 0, len(s.topics))
	for t := range s.topics {
		topics = append(topics, t)
	}
	s.mu.Unlock()

	for _, t := range topics {
		s.Unsubscribe(t)
	}
}

// topicState holds the per-topic subscriber list and the retained integral.
// Publish and Subscribe for one topic serialize on mu; distinct topics
// proceed concurrently. Sends may block while mu is held, so a consumer that
// publishes back into the very topic it consumes can deadlock once its own
// channel fills up; publishing to any other topic is always safe.
type topicState struct {
	mu   sync.Mutex
	subs []chan Event
	// acc is the retained integral I(stream): the running sum of every Z-set
	// published to the topic. For well-formed delta streams it equals the
	// topic's current state, and it is replayed to late subscribers.
	acc zset.ZSet
}

// PubSub is a topic-indexed subscription registry with per-topic state
// retention for bootstrapping late subscribers.
type PubSub struct {
	mu     sync.RWMutex
	topics map[string]*topicState
}

func NewPubSub() *PubSub {
	return &PubSub{topics: map[string]*topicState{}}
}

// topic returns the state for a topic, creating it on first use.
func (ps *PubSub) topic(name string) *topicState {
	ps.mu.RLock()
	ts, ok := ps.topics[name]
	ps.mu.RUnlock()
	if ok {
		return ts
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ts, ok := ps.topics[name]; ok {
		return ts
	}
	ts = &topicState{acc: zset.New()}
	ps.topics[name] = ts
	return ts
}

// NewPublisher creates a publisher bound to this PubSub.
func (ps *PubSub) NewPublisher() *publisher {
	return &publisher{pubsub: ps}
}

// NewSubscriber creates a single-channel subscriber bound to this PubSub.
func (ps *PubSub) NewSubscriber() *subscriber {
	return &subscriber{pubsub: ps, ch: make(chan Event, EventBufferSize), topics: map[string]struct{}{}}
}
