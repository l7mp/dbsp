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

// Publish sends an unsolicited event to all current subscribers of event.Name.
// On a full subscriber channel it logs the overflow and then blocks until the
// event is accepted, preserving backpressure.
func (p *publisher) Publish(event Event) error {
	p.pubsub.mu.RLock()
	defer p.pubsub.mu.RUnlock()

	for _, ch := range p.pubsub.subs[event.Name] {
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
	GetChannel() <-chan Event
}

type subscriber struct {
	pubsub *PubSub

	mu     sync.Mutex
	ch     chan Event
	topics map[string]struct{}
}

// Subscribe registers interest in a topic.
func (s *subscriber) Subscribe(topic string) {

	s.mu.Lock()
	if _, ok := s.topics[topic]; ok {
		s.mu.Unlock()
		return
	}
	s.topics[topic] = struct{}{}
	ch := s.ch
	s.mu.Unlock()

	s.pubsub.mu.Lock()
	s.pubsub.subs[topic] = append(s.pubsub.subs[topic], ch)
	s.pubsub.mu.Unlock()

}

// Unsubscribe unregisters a topic. The channel closes when the last topic is removed.
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

	s.pubsub.mu.Lock()
	list := s.pubsub.subs[topic]
	keep := list[:0]
	for _, c := range list {
		if c != ch {
			keep = append(keep, c)
		}
	}
	if len(keep) == 0 {
		delete(s.pubsub.subs, topic)
	} else {
		s.pubsub.subs[topic] = keep
	}
	s.pubsub.mu.Unlock()

	if empty {
		close(ch)
	}
}

func (s *subscriber) GetChannel() <-chan Event {
	return s.ch
}

// PubSub is a topic-indexed subscription registry.
type PubSub struct {
	mu   sync.RWMutex
	subs map[string][]chan Event
}

func NewPubSub() *PubSub {
	return &PubSub{subs: map[string][]chan Event{}}
}

// NewPublisher creates a publisher bound to this PubSub.
func (ps *PubSub) NewPublisher() *publisher {
	return &publisher{pubsub: ps}
}

// NewSubscriber creates a single-channel subscriber bound to this PubSub.
func (ps *PubSub) NewSubscriber() *subscriber {
	return &subscriber{pubsub: ps, ch: make(chan Event, EventBufferSize), topics: map[string]struct{}{}}
}
