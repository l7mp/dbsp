package runtime

import (
	"encoding/json"
	"fmt"
)

// Producer is a runnable event source.
//
// Implementations typically embed a Publisher created from Runtime.NewPublisher.
type Producer interface {
	Runnable
	Publisher
	fmt.Stringer
	json.Marshaler
}

// Consumer is a runnable event sink.
//
// Implementations typically embed a Subscriber created from Runtime.NewSubscriber.
// A Subscriber has one channel and can subscribe that channel to multiple topics.
type Consumer interface {
	Runnable
	Subscriber
	fmt.Stringer
	json.Marshaler
}

// Processor is both a Producer and a Consumer.
type Processor interface {
	Producer
	Consumer
}
