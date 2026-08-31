package misc

import (
	"fmt"
	"time"

	"github.com/go-logr/logr"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// TriggerSpec is the serialized configuration of the connector's trigger
// producers, the one wire form every frontend funnels through (the
// JavaScript misc.tick/misc.init options object and the serialized
// controller's source parameters deserialize into it alike). Kind names
// one of the connector's resource kinds (Timer today) and is carried by
// the trigger documents; timers sharing a topic are distinguished by
// Name.
type TriggerSpec struct {
	Kind      string `json:"kind"`
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	// Period is the tick cadence as a Go duration string; Tick only.
	Period string `json:"period,omitempty"`
}

// Deps carries the non-serializable runtime dependencies of a trigger
// producer.
type Deps struct {
	Runtime *dbspruntime.Runtime
	Logger  logr.Logger
}

func (s TriggerSpec) validate(verb string) error {
	if s.Kind == "" {
		return fmt.Errorf("%s: kind is required", verb)
	}
	if s.Kind != "Timer" {
		return fmt.Errorf("%s: unknown kind %q", verb, s.Kind)
	}
	return nil
}

// NewTick builds the periodic trigger producer publishing to topic.
func NewTick(topic string, spec TriggerSpec, deps Deps) (*PeriodicProducer, error) {
	if err := spec.validate("misc.tick"); err != nil {
		return nil, err
	}
	if spec.Period == "" {
		return nil, fmt.Errorf("misc.tick: period is required")
	}
	period, err := time.ParseDuration(spec.Period)
	if err != nil {
		return nil, fmt.Errorf("misc.tick: period: %w", err)
	}
	return NewPeriodicProducer(PeriodicConfig{
		Name:        fmt.Sprintf("misc/tick/%s", topic),
		InputName:   topic,
		TriggerKind: spec.Kind,
		Namespace:   spec.Namespace,
		TriggerName: spec.Name,
		Period:      period,
		Runtime:     deps.Runtime,
		Logger:      deps.Logger,
	})
}

// NewInit builds the at-startup trigger producer publishing to topic.
func NewInit(topic string, spec TriggerSpec, deps Deps) (*OneShotProducer, error) {
	if err := spec.validate("misc.init"); err != nil {
		return nil, err
	}
	return NewOneShotProducer(OneShotConfig{
		Name:        fmt.Sprintf("misc/init/%s", topic),
		InputName:   topic,
		TriggerKind: spec.Kind,
		Namespace:   spec.Namespace,
		TriggerName: spec.Name,
		Runtime:     deps.Runtime,
		Logger:      deps.Logger,
	})
}
