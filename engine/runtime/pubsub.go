package runtime

import (
	"errors"
	"fmt"
	"log"
	"sync"

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
	// ErrPublisherClosed indicates a publish through a closed publisher.
	ErrPublisherClosed = errors.New("runtime publisher closed")
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

// PublisherCloser is a Publisher that can be closed. Closing fences the
// publisher: further publishes are refused, so a straggling component
// cannot write into a successor's stream. Components close their
// publisher on teardown.
type PublisherCloser interface {
	Publisher
	Close()
}

// PublishFunc adapts a function to Publisher.
type PublishFunc func(Event) error

// Publish calls f(event).
func (f PublishFunc) Publish(event Event) error { return f(event) }

type publisher struct {
	pubsub *PubSub

	mu     sync.Mutex
	closed bool
}

// Publish delivers the event through the fence: a closed publisher
// refuses to write into a successor's stream.
func (p *publisher) Publish(event Event) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("%w: topic %s", ErrPublisherClosed, event.Name)
	}
	p.mu.Unlock()

	return p.pubsub.Publish(event)
}

// Publish delivers the event to all subscribers of event.Name registered
// at this moment; an event published to a topic with no subscribers is
// gone. This is the one-shot form for hosts; long-lived components
// publish through a Publisher, whose Close fences a straggler. On a full
// subscriber channel the publish logs the overflow and then blocks until
// the event is accepted, preserving backpressure.
func (ps *PubSub) Publish(event Event) error {
	ts := ps.topic(event.Name)
	ts.mu.Lock()
	defer ts.mu.Unlock()

	for _, ch := range ts.subs {
		if err := sendEvent(ch, event); err != nil {
			if errors.Is(err, ErrChannelFull) {
				ps.onStall(event.Name)
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

// Close fences the publisher: further publishes are refused, so a
// straggling component cannot write into a successor's stream.
func (p *publisher) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
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
	// QueueSize returns the number of events currently queued for delivery.
	// The owning consumer is the only reader of the delivery channel, so
	// Next is guaranteed not to block for this many consecutive calls.
	QueueSize() int
}

type subscriber struct {
	pubsub *PubSub

	mu     sync.Mutex
	ch     chan Event
	topics map[string]struct{}
}

// Subscribe registers interest in a topic. Only events published after
// the registration are delivered: the pub/sub retains nothing, so a
// subscriber that must see a stream from its beginning subscribes before
// the stream's producers start.
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
	ts.subs = append(ts.subs, ch)
}

// Unsubscribe unregisters a topic. The channel closes when the last topic
// is removed.
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

// QueueSize returns the number of events currently queued for delivery.
func (s *subscriber) QueueSize() int { return len(s.ch) }

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

// topicState holds the per-topic subscriber list. Publish and Subscribe
// for one topic serialize on mu; distinct topics proceed concurrently.
// Sends may block while mu is held, so a consumer that publishes back
// into the very topic it consumes can deadlock once its own channel fills
// up; publishing to any other topic is always safe.
type topicState struct {
	mu   sync.Mutex
	subs []chan Event
}

// PubSub is a topic-indexed subscription registry: pure fan-out
// transport with no state. Nothing is retained, so setup order matters:
// subscribers first, then producers.
type PubSub struct {
	mu     sync.RWMutex
	topics map[string]*topicState

	// onStall is called once per publish that finds a subscriber channel
	// full and falls back to the blocking send. Never nil: a no-op on a
	// bare PubSub, rewired by NewRuntime to the runtime's stats counters
	// before the first publish.
	onStall func(topic string)
}

func NewPubSub() *PubSub {
	return &PubSub{topics: map[string]*topicState{}, onStall: func(string) {}}
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
	ts = &topicState{}
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
